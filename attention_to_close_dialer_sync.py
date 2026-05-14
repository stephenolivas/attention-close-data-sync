#!/usr/bin/env python3
"""
Attention → Close dialer call enrichment.

Reads recent Attention conversations with applicationName == "close"
(i.e. imported by close_to_attention_sync.py) and writes their analysis
to the matched Close lead as a Custom Activity instance.

Why a separate sync from the existing Attention → Close reverse sync:
the existing sync writes to lead-level Attention fields that are
reserved for the official video call analysis. Dialer call analyses
land here instead as their own first-class Custom Activity entries on
the lead timeline, so they don't compete with or overwrite the video
call's fields.

For each eligible conversation:
  1. Find the matched Close lead via applicationExternalID (Close call
     activity ID) → activity.call.lead_id
  2. Check if a Custom Activity for this Attention conversation already
     exists on the lead (idempotency); skip if so
  3. Use Claude Haiku to classify Primary Objection and summarize Key
     Concern from extractedIntelligence.Doubt
  4. POST a new Custom Activity instance on the lead with all fields
     populated

Required GitHub secrets:
  CLOSE_API_KEY        Close API key (Basic auth)
  ATTENTION_API_KEY    Attention API key (header value, no "Bearer ")
  ANTHROPIC_API_KEY    Anthropic API key (for Claude Haiku)

Optional env vars:
  HOURS_BACK           Window of Attention conversations to consider (default: 24)
  DRY_RUN              If "1", log what would happen but don't write to Close
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

    Examples:
      "⚡ Attention Call Title"  -> "Attention Call Title"
      "⚡︎ Attention Call ID"     -> "Attention Call ID"
      "   QA Score"              -> "QA Score"
      "QA Score"                 -> "QA Score"
    """
    return re.sub(r"^[^a-zA-Z]+", "", name).strip()


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
            # Try multiple possible key names for the fields list — defensive
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
                log(f"Sample (first 2000 chars):", indent=1)
                log(json.dumps(activity_type, indent=2)[:2000], indent=2)

            return {"id": type_id, "fields": field_ids}

    raise Exception(
        f"Custom Activity Type '{CUSTOM_ACTIVITY_TYPE_NAME}' not found in Close. "
        f"Verify it exists at Settings → Custom Activities."
    )


# ===== Attention conversation fetching =====
def fetch_eligible_conversations(since_dt):
    """
    Iterate Attention conversations matching our dialer call title format
    that are fully processed (scorecard + extracted intelligence ready).

    We filter server-side by title rather than applicationName because the
    list endpoint doesn't reliably surface applicationName in its
    response (it lives in importMetadata for imported conversations and
    isn't returned consistently across list shapes). All our dialer
    imports use the title format "{lead_name} - Close Dialer Call", so a
    case-insensitive partial title match is precise enough.
    """
    eligible = []
    inspected = 0
    skipped_not_processed = 0
    sample_attrs = None

    page = 1
    # Attention's fromDateTime requires ISO 8601 with a Z suffix and no
    # microseconds (the default datetime.isoformat() format is rejected).
    since_str = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    while True:
        resp = attention_get(
            "/conversations",
            params={
                "fromDateTime": since_str,
                "page": page,
                "size": 50,
                # Server-side title filter narrows results to our dialer
                # imports only. Case-insensitive partial match per the
                # Attention API docs.
                "filter[title]": "Close Dialer Call",
            },
        )
        if not resp.ok:
            raise Exception(
                f"Could not list Attention conversations: {resp.status_code}: {resp.text[:300]}"
            )

        body = resp.json()
        items = body.get("data", [])
        if not items:
            break

        for item in items:
            inspected += 1
            attrs = item.get("attributes", item)

            if sample_attrs is None:
                sample_attrs = attrs

            # Need scorecard + extracted intelligence both populated for enrichment
            sc = attrs.get("scorecardResults") or []
            ei = attrs.get("extractedIntelligence") or {}
            if not sc or not ei:
                skipped_not_processed += 1
                continue

            eligible.append(attrs)

        meta = body.get("meta", {})
        page_count = meta.get("pageCount", 1)
        if page >= page_count:
            break
        page += 1

    log(f"Inspected {inspected} conversations matching title 'Close Dialer Call' in the window")
    log(f"  Skipped (not yet fully processed):    {skipped_not_processed}")
    log(f"  Eligible:                             {len(eligible)}")

    # Always emit diagnostic context when zero eligible
    if not eligible:
        log("")
        if not sample_attrs:
            log("DEBUG: No 'Close Dialer Call' titled conversations returned at all.", indent=1)
            log("Possible causes:", indent=1)
            log(f"- No dialer calls imported in the last {HOURS_BACK} hours", indent=2)
            log("- Imports use a different title format than 'Close Dialer Call'", indent=2)
            log("- Server-side title filter not actually working (check raw response)", indent=2)
        else:
            log("DEBUG: Conversations returned but none fully processed yet.", indent=1)
            log("Sample conversation attributes:", indent=1)
            log(f"  Title:           {sample_attrs.get('title')!r}", indent=2)
            log(f"  scorecardResults: {len(sample_attrs.get('scorecardResults') or [])} entries", indent=2)
            log(f"  extractedIntelligence: {len(sample_attrs.get('extractedIntelligence') or {})} fields", indent=2)
            log(f"  importStatus:    {sample_attrs.get('importStatus')!r}", indent=2)
            log(f"  transcriptStatus: {sample_attrs.get('transcriptStatus')!r}", indent=2)

    return eligible


# ===== Lead lookup =====
def get_close_lead_id(call_activity_id):
    """Look up the Close call activity to find its lead_id."""
    resp = close_get(f"/activity/call/{call_activity_id}/", params={"_fields": "lead_id"})
    if not resp.ok:
        return None
    return resp.json().get("lead_id")


# ===== Idempotency =====
def custom_activity_already_exists(lead_id, type_id, attention_uuid, attention_call_id_field_id):
    """Return True if a Custom Activity for this Attention conversation already exists on the lead."""
    resp = close_get(
        "/activity/custom/",
        params={"lead_id": lead_id, "custom_activity_type_id": type_id},
    )
    if not resp.ok:
        # Don't block — let the POST attempt either succeed or surface the issue
        return False

    for activity in resp.json().get("data", []):
        existing = activity.get(f"custom.{attention_call_id_field_id}")
        if existing == attention_uuid:
            return True
    return False


# ===== Helpers for extracted intelligence =====
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
def enrich_conversation(conv, type_info):
    """Process one Attention conversation. Returns the new Custom Activity ID, or None if skipped."""
    uuid = conv.get("uuid")
    title = conv.get("title", "")
    external_id = conv.get("applicationExternalID")
    duration = conv.get("mediaDuration", 0) or 0

    log(f"\n[{uuid}] '{title}'")
    log(f"  external_id: {external_id}", indent=1)
    log(f"  duration:    {duration}s", indent=1)

    if not external_id:
        log("→ No applicationExternalID, skip", indent=1)
        return None

    # Find the Close lead
    lead_id = get_close_lead_id(external_id)
    if not lead_id:
        log(f"→ Could not find Close lead for activity {external_id}, skip", indent=1)
        return None
    log(f"  lead_id:     {lead_id}", indent=1)

    # Idempotency
    field_ids = type_info["fields"]
    attention_call_id_field_id = field_ids.get("Attention Call ID")
    if not attention_call_id_field_id:
        log("→ 'Attention Call ID' field not found in Custom Activity Type, abort", indent=1)
        return None

    if custom_activity_already_exists(lead_id, type_info["id"], uuid, attention_call_id_field_id):
        log("→ Custom Activity already exists for this conversation, skip", indent=1)
        return None

    # Pull analysis fields
    scorecards = conv.get("scorecardResults") or []
    qa_score = None
    if scorecards:
        summary = scorecards[0].get("summary") or {}
        qa_score = summary.get("averageScore")

    ei = conv.get("extractedIntelligence") or {}
    doubt_text = get_ei_value(ei, "Doubt")
    call_summary = get_ei_value(ei, "Call Summary")

    # Claude Haiku enrichment
    log("Classifying Primary Objection (Haiku)...", indent=1)
    primary_objection = haiku_classify_objection(doubt_text)
    log(f"→ {primary_objection}", indent=2)

    log("Summarizing Key Concern (Haiku)...", indent=1)
    key_concern = haiku_summarize_concern(doubt_text)
    log(f"→ {key_concern[:120]}", indent=2)

    # Build the Custom Activity payload
    attention_link = f"https://app.attention.tech/conversations/{uuid}"

    field_mapping = {
        "Attention Call Link": attention_link,
        "Attention Call ID": uuid,
        "Attention Call Title": title,
        "QA Score": qa_score,
        "Primary Objection": primary_objection,
        "Key Concern": key_concern,
        "Call Summary": call_summary,
        "Call Duration": duration,
        "Close Call Activity ID": external_id,
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
        log(f"DRY_RUN — would POST payload:", indent=1)
        log(json.dumps(payload, indent=2)[:1000], indent=2)
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
        f"(HOURS_BACK={HOURS_BACK}, DRY_RUN={DRY_RUN})"
    )

    # 1. Resolve the Custom Activity Type and its field IDs
    section("Resolving Close Custom Activity Type")
    type_info = find_custom_activity_type()
    log(f"Type:   {CUSTOM_ACTIVITY_TYPE_NAME}")
    log(f"ID:     {type_info['id']}")
    log(f"Fields ({len(type_info['fields'])}):")
    for name, field_id in sorted(type_info["fields"].items()):
        log(f"  {name}: {field_id}", indent=1)

    # 2. Fetch eligible Attention conversations
    since_dt = datetime.now(timezone.utc) - timedelta(hours=HOURS_BACK)
    section(f"Fetching Attention conversations since {since_dt.isoformat()}")
    conversations = fetch_eligible_conversations(since_dt)

    # 3. Enrich each
    section("Enriching conversations")
    stats = {"enriched": 0, "skipped": 0, "failed": 0}

    for conv in conversations:
        try:
            activity_id = enrich_conversation(conv, type_info)
            if activity_id:
                stats["enriched"] += 1
            else:
                stats["skipped"] += 1
        except Exception as e:
            stats["failed"] += 1
            log(f"❌ Error enriching {conv.get('uuid')}: {e}", indent=1)

    section("Done")
    log(f"Enriched: {stats['enriched']}")
    log(f"Skipped:  {stats['skipped']}")
    log(f"Failed:   {stats['failed']}")

    sys.exit(0 if stats["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
