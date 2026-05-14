#!/usr/bin/env python3
"""
Attention → Close dialer call enrichment.

For each recent Close dialer call (recording_url + duration ≥ MIN_DURATION):
  1. Look up its Attention conversation by external_id (= Close call activity ID)
  2. If found and fully processed (scorecard + extracted intelligence ready),
     create a Custom Activity instance on the matched Close lead with all
     analysis fields populated.

Why drive the loop from the Close side rather than Attention:
Attention's API stores applicationName / applicationExternalID on imported
conversations but does NOT return them in either the list or GET response
shapes (verified empirically). The only reliable way to use those IDs is
to look up a conversation BY external_id (which works) — i.e. we iterate
Close call IDs and ask Attention "do you have a conversation for this one?"
rather than the reverse.

Result: a clean closed loop without depending on undocumented response
shape from Attention.

Combined with the rest of the integration:
  :00 - existing Attention → Close sync (video calls → lead fields)
  :15 - close_to_attention_sync (Close dialer calls → Attention)
  :30 - this script (Attention dialer analyses → Close Custom Activities)

Required GitHub secrets:
  CLOSE_API_KEY        Close API key (Basic auth)
  ATTENTION_API_KEY    Attention API key (header value, no "Bearer ")
  ANTHROPIC_API_KEY    Anthropic API key (for Claude Haiku enrichment)

Optional env vars:
  HOURS_BACK           Window of recent Close calls to consider (default: 24)
  MIN_DURATION         Skip Close calls shorter than this in seconds (default: 180)
  DRY_RUN              If "1", log payloads without writing to Close
"""

import os
import sys
import time
import json
import base64
import re
import requests
from datetime import datetime, timezone, timedelta

# ===== Config =====
CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
ATTENTION_API_KEY = os.environ["ATTENTION_API_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
HOURS_BACK = int(os.environ.get("HOURS_BACK", "24"))
MIN_DURATION = int(os.environ.get("MIN_DURATION", "180"))
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

CLOSE_API_BASE = "https://api.close.com/api/v1"
ATTENTION_API_BASE = "https://api.attention.tech/v2"
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"

CUSTOM_ACTIVITY_TYPE_NAME = "Attention - Close Dialer Call Analysis"
HAIKU_MODEL = "claude-haiku-4-5-20251001"

# Primary Objection dropdown choices. Must match the values configured on
# the Close Custom Field exactly, or POSTs will fail with a 400.
OBJECTION_CHOICES = ("Timing", "Investment", "Fit", "Other")

CLOSE_REQUEST_DELAY = 0.5

# Auth setup
_close_auth_b64 = base64.b64encode(f"{CLOSE_API_KEY}:".encode()).decode()
CLOSE_HEADERS = {"Authorization": f"Basic {_close_auth_b64}"}
ATTENTION_HEADERS = {"Authorization": ATTENTION_API_KEY}
ANTHROPIC_HEADERS = {
    "x-api-key": ANTHROPIC_API_KEY,
    "anthropic-version": "2023-06-01",
    "Content-Type": "application/json",
}


# ===== Logging =====
def log(msg, indent=0):
    print(f"{'  ' * indent}{msg}", flush=True)


def section(label):
    print(f"\n{'=' * 60}\n{label}\n{'=' * 60}", flush=True)


def normalize_field_name(name):
    """
    Strip leading decorative characters (emoji icons, whitespace) before
    the first ASCII letter. Close field names are often prefixed with ⚡
    or similar icons for visual grouping; we want to match on the
    semantic name only.
    """
    return re.sub(r"^[^a-zA-Z]+", "", name).strip()


def html_wrap(text):
    """
    Wrap plain text in HTML for Close Custom Activity Textarea fields.

    Close's Custom Activity "Textarea" field type is parsed as HTML server-
    side AND specifically requires the value to be wrapped in <body>...</body>
    tags. Sending raw strings produces a 400 with `"Start tag expected, '<'
    not found"`; sending bare <p> tags without a <body> wrapper produces
    `"HTML rich text fields in Close are expected to start with a '<body>'
    tag and end with '</body>'"`. This helper:
      - HTML-escapes special characters (`&`, `<`, `>`) in the source
      - Splits on blank lines into <p> paragraphs
      - Preserves single newlines as <br>
      - Wraps the whole thing in <body>...</body>
    For empty / None input, returns the input unchanged so the caller's
    "skip empty values" logic still applies.
    """
    if not text:
        return text
    escaped = (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
    paragraphs = [p for p in escaped.split("\n\n") if p.strip()]
    if not paragraphs:
        return f"<body><p>{escaped}</p></body>"
    inner = "".join(
        f"<p>{p.replace(chr(10), '<br>')}</p>" for p in paragraphs
    )
    return f"<body>{inner}</body>"


# ===== Close API =====
def close_get(path, params=None):
    url = path if path.startswith("http") else f"{CLOSE_API_BASE}{path}"
    for attempt in range(6):
        resp = requests.get(url, headers=CLOSE_HEADERS, params=params)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", "2"))
            log(f"[Close] 429 rate limited, waiting {wait}s...", indent=1)
            time.sleep(wait)
            continue
        time.sleep(CLOSE_REQUEST_DELAY)
        return resp
    raise Exception(f"Close GET {path} exhausted retries")


def close_post(path, json_data):
    url = path if path.startswith("http") else f"{CLOSE_API_BASE}{path}"
    headers = {**CLOSE_HEADERS, "Content-Type": "application/json"}
    for attempt in range(6):
        resp = requests.post(url, headers=headers, json=json_data)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", "2"))
            time.sleep(wait)
            continue
        time.sleep(CLOSE_REQUEST_DELAY)
        return resp
    raise Exception(f"Close POST {path} exhausted retries")


# ===== Attention API =====
def attention_get(path, params=None):
    url = path if path.startswith("http") else f"{ATTENTION_API_BASE}{path}"
    for attempt in range(3):
        resp = requests.get(url, headers=ATTENTION_HEADERS, params=params)
        if resp.status_code in (502, 503, 504):
            time.sleep(2 ** attempt)
            continue
        return resp
    raise Exception(f"Attention GET {path} exhausted retries")


def attention_get_conversation_by_external_id(external_id):
    """
    Look up an Attention conversation by its applicationExternalID (which
    we set to the Close call activity ID on import). Returns the full
    attributes dict or None if not found.
    """
    resp = attention_get(
        f"/conversations/{external_id}",
        params={"by": "external_id"},
    )
    if resp.status_code == 404:
        return None
    if not resp.ok:
        log(f"⚠️  Attention by-external-id lookup failed for {external_id}: {resp.status_code}: {resp.text[:200]}", indent=1)
        return None

    body = resp.json()
    return body.get("attributes", body)


# ===== Anthropic (Claude Haiku) =====
def haiku_classify_objection(doubt_text):
    """Classify the prospect's objection into Timing / Investment / Fit / Other."""
    if not doubt_text or len(doubt_text.strip()) < 20:
        return "Other"

    prompt = f"""Classify the prospect's primary objection from this sales call into EXACTLY ONE category:

- Timing: Not ready yet, busy season, want to wait, need more time
- Investment: Cost, budget, financing, can't afford, too expensive
- Fit: Wrong product/service for them, doesn't match their needs, unsuitable
- Other: Anything not matching the above

Objection text:
{doubt_text[:3000]}

Respond with ONLY ONE WORD: Timing, Investment, Fit, or Other."""

    payload = {
        "model": HAIKU_MODEL,
        "max_tokens": 10,
        "messages": [{"role": "user", "content": prompt}],
    }
    resp = requests.post(ANTHROPIC_API_URL, headers=ANTHROPIC_HEADERS, json=payload)
    if not resp.ok:
        log(f"Haiku classify failed: {resp.status_code}: {resp.text[:300]}", indent=2)
        return "Other"

    answer = resp.json()["content"][0]["text"].strip()
    for valid in OBJECTION_CHOICES:
        if valid.lower() in answer.lower():
            return valid
    return "Other"


def haiku_summarize_concern(doubt_text):
    """Summarize the prospect's biggest concern in <=20 words."""
    if not doubt_text or len(doubt_text.strip()) < 20:
        return ""

    prompt = f"""Summarize the prospect's biggest concern from this sales call in 20 words or fewer. Be specific about what they actually doubt or worry about. Do not editorialize.

Doubt text:
{doubt_text[:3000]}

Respond with ONLY the summary, no preamble."""

    payload = {
        "model": HAIKU_MODEL,
        "max_tokens": 60,
        "messages": [{"role": "user", "content": prompt}],
    }
    resp = requests.post(ANTHROPIC_API_URL, headers=ANTHROPIC_HEADERS, json=payload)
    if not resp.ok:
        log(f"Haiku summarize failed: {resp.status_code}: {resp.text[:300]}", indent=2)
        return ""
    return resp.json()["content"][0]["text"].strip()


# ===== Custom Activity Type resolution =====
def find_custom_activity_type():
    """Resolve {id, fields_by_name} for our Custom Activity Type."""
    resp = close_get("/custom_activity/")
    if not resp.ok:
        raise Exception(
            f"Could not list custom activity types: {resp.status_code}: {resp.text[:300]}"
        )

    data = resp.json()
    for activity_type in data.get("data", []):
        if activity_type.get("name") == CUSTOM_ACTIVITY_TYPE_NAME:
            type_id = activity_type["id"]
            fields_list = (
                activity_type.get("fields")
                or activity_type.get("custom_fields")
                or activity_type.get("field_definitions")
                or []
            )
            field_ids = {}
            for field in fields_list:
                raw_name = field.get("name", "")
                normalized = normalize_field_name(raw_name)
                if not normalized:
                    continue
                field_ids[normalized] = field["id"]

            if not field_ids:
                log("WARNING: no fields found in Custom Activity Type response.", indent=1)
                log(f"Top-level keys returned: {list(activity_type.keys())}", indent=1)
                log("Sample (first 2000 chars):", indent=1)
                log(json.dumps(activity_type, indent=2)[:2000], indent=2)

            return {"id": type_id, "fields": field_ids}

    raise Exception(
        f"Custom Activity Type '{CUSTOM_ACTIVITY_TYPE_NAME}' not found in Close. "
        f"Verify it exists at Settings → Custom Activities."
    )


# ===== Close call iteration =====
def find_recent_close_calls(since_dt):
    """
    Iterate Close call activities newer than since_dt with a recording_url
    and duration >= MIN_DURATION. Newest-first.
    """
    eligible = []
    inspected = 0
    skipped_short = 0
    skipped_no_recording = 0
    skipped_no_lead = 0

    skip = 0
    pages = 0
    max_pages = 200
    while pages < max_pages:
        resp = close_get("/activity/call/", params={"_skip": skip, "_limit": 100})
        if not resp.ok:
            raise Exception(
                f"Close /activity/call/ returned {resp.status_code}: {resp.text[:300]}"
            )
        data = resp.json()
        items = data.get("data", [])
        if not items:
            break

        window_ended = False
        for call in items:
            inspected += 1

            # Parse date; stop once we're past the window
            date_str = call.get("date_created", "")
            try:
                call_dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue
            if call_dt < since_dt:
                window_ended = True
                break

            if not call.get("recording_url"):
                skipped_no_recording += 1
                continue
            if (call.get("duration") or 0) < MIN_DURATION:
                skipped_short += 1
                continue
            if not call.get("lead_id"):
                skipped_no_lead += 1
                continue

            eligible.append(call)

        if window_ended or not data.get("has_more"):
            break
        skip += 100
        pages += 1

    log(f"Inspected {inspected} Close calls in the {HOURS_BACK}h window")
    log(f"  Skipped (no recording):       {skipped_no_recording}")
    log(f"  Skipped (under {MIN_DURATION}s): {skipped_short}")
    log(f"  Skipped (no lead_id):         {skipped_no_lead}")
    log(f"  Eligible:                     {len(eligible)}")
    return eligible


# ===== Idempotency =====
def custom_activity_already_exists(lead_id, type_id, attention_uuid, attention_call_id_field_id):
    """Return True if a Custom Activity for this Attention conversation already exists on the lead."""
    resp = close_get(
        "/activity/custom/",
        params={"lead_id": lead_id, "custom_activity_type_id": type_id},
    )
    if not resp.ok:
        return False  # let the POST attempt surface the issue

    for activity in resp.json().get("data", []):
        existing = activity.get(f"custom.{attention_call_id_field_id}")
        if existing == attention_uuid:
            return True
    return False


# ===== Extracted intelligence helpers =====
def get_ei_value(ei_dict, target_title):
    """Find the EI entry whose title matches target_title (case-insensitive, trimmed)."""
    target = target_title.lower().strip()
    for key, val in ei_dict.items():
        if isinstance(val, dict):
            title = (val.get("title") or "").lower().strip()
            if title == target:
                return val.get("value", "") or ""
    return ""


# ===== Enrichment =====
def enrich_call(close_call, type_info):
    """
    Process one Close call: look up its Attention conversation by external_id,
    and if processed, create a Custom Activity on the matched lead.
    Returns the new Custom Activity ID, or None if skipped for any reason.
    """
    call_id = close_call["id"]
    duration = close_call.get("duration") or 0
    lead_id = close_call.get("lead_id")

    log(f"\n[{call_id}] duration={duration}s, lead={lead_id}")

    # 1. Look up Attention conversation by external_id
    attention_attrs = attention_get_conversation_by_external_id(call_id)
    if not attention_attrs:
        log("→ No Attention conversation yet (still importing or never imported), skip", indent=1)
        return None

    uuid = attention_attrs.get("uuid")
    title = attention_attrs.get("title", "")
    log(f"Attention conversation: {uuid}", indent=1)
    log(f"Title:                  {title!r}", indent=1)

    # 2. Check processing completeness
    sc = attention_attrs.get("scorecardResults") or []
    ei = attention_attrs.get("extractedIntelligence") or {}
    if not sc or not ei:
        log("→ Attention analysis not yet complete, skip (will retry next run)", indent=1)
        log(f"  scorecardResults: {len(sc)}, extractedIntelligence: {len(ei)}", indent=2)
        return None

    # 3. Idempotency check
    field_ids = type_info["fields"]
    attention_call_id_field_id = field_ids.get("Attention Call ID")
    if not attention_call_id_field_id:
        log("→ 'Attention Call ID' field not found in Custom Activity Type, abort", indent=1)
        return None

    if custom_activity_already_exists(lead_id, type_info["id"], uuid, attention_call_id_field_id):
        log("→ Custom Activity already exists for this conversation, skip", indent=1)
        return None

    # 4. Pull analysis fields
    qa_score = None
    if sc:
        summary = sc[0].get("summary") or {}
        qa_score = summary.get("averageScore")

    doubt_text = get_ei_value(ei, "Doubt")
    call_summary = get_ei_value(ei, "Call Summary")

    # 5. Claude Haiku enrichment
    log("Classifying Primary Objection (Haiku)...", indent=1)
    primary_objection = haiku_classify_objection(doubt_text)
    log(f"→ {primary_objection}", indent=2)

    log("Summarizing Key Concern (Haiku)...", indent=1)
    key_concern = haiku_summarize_concern(doubt_text)
    log(f"→ {key_concern[:120]}", indent=2)

    # 6. Build Custom Activity payload
    # NOTE: Close Custom Activity "Textarea" fields are parsed as HTML
    # server-side. Key Concern and Call Summary are Textarea-typed and must
    # be wrapped via html_wrap(); plain strings produce a 400 with
    # `"Start tag expected, '<' not found"`. The other fields are
    # Text/Number/Dropdown and take their values as-is.
    attention_link = f"https://app.attention.tech/conversations/{uuid}"
    field_mapping = {
        "Attention Call Link": attention_link,
        "Attention Call ID": uuid,
        "Attention Call Title": title,
        "QA Score": qa_score,
        "Primary Objection": primary_objection,
        "Key Concern": html_wrap(key_concern),
        "Call Summary": html_wrap(call_summary),
        "Call Duration": attention_attrs.get("mediaDuration") or duration,
        "Close Call Activity ID": call_id,
    }

    payload = {
        "custom_activity_type_id": type_info["id"],
        "lead_id": lead_id,
    }
    for name, value in field_mapping.items():
        if name not in field_ids:
            log(f"⚠️  Field '{name}' not present in Custom Activity Type; skipping that field", indent=1)
            continue
        if value is None or value == "":
            continue
        payload[f"custom.{field_ids[name]}"] = value

    if DRY_RUN:
        log("DRY_RUN — would POST payload:", indent=1)
        log(json.dumps(payload, indent=2)[:1500], indent=2)
        return None

    resp = close_post("/activity/custom/", payload)
    if not resp.ok:
        raise Exception(f"Failed to create Custom Activity: {resp.status_code}: {resp.text[:500]}")

    activity_id = resp.json().get("id")
    log(f"✅ Created Custom Activity {activity_id} on lead {lead_id}", indent=1)
    return activity_id


# ===== Main =====
def main():
    section(
        f"Attention → Close dialer call enrichment "
        f"(HOURS_BACK={HOURS_BACK}, MIN_DURATION={MIN_DURATION}s, DRY_RUN={DRY_RUN})"
    )

    # 1. Resolve the Custom Activity Type and its field IDs
    section("Resolving Close Custom Activity Type")
    type_info = find_custom_activity_type()
    log(f"Type:   {CUSTOM_ACTIVITY_TYPE_NAME}")
    log(f"ID:     {type_info['id']}")
    log(f"Fields ({len(type_info['fields'])}):")
    for name, field_id in sorted(type_info["fields"].items()):
        log(f"  {name}: {field_id}", indent=1)

    # 2. Find eligible recent Close calls
    since_dt = datetime.now(timezone.utc) - timedelta(hours=HOURS_BACK)
    section(f"Finding recent Close calls since {since_dt.isoformat()}")
    close_calls = find_recent_close_calls(since_dt)

    # 3. For each eligible Close call, look up Attention and enrich
    section("Enriching matched conversations")
    stats = {"enriched": 0, "skipped": 0, "failed": 0}

    for call in close_calls:
        try:
            activity_id = enrich_call(call, type_info)
            if activity_id:
                stats["enriched"] += 1
            else:
                stats["skipped"] += 1
        except Exception as e:
            stats["failed"] += 1
            log(f"❌ Error enriching {call.get('id')}: {e}", indent=1)

    section("Done")
    log(f"Enriched: {stats['enriched']}")
    log(f"Skipped:  {stats['skipped']}")
    log(f"Failed:   {stats['failed']}")

    sys.exit(0 if stats["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
