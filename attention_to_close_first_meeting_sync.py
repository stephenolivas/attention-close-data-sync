#!/usr/bin/env python3
"""
Attention → Close first-meeting analysis sync (Custom Activity edition).

Captures first sales calls — the same conversations that sync.py at :00
already processes into LEAD-LEVEL fields — and additionally writes them
to a Custom Activity of type "Attention - First Meeting Analysis".

This runs ALONGSIDE sync.py, not as a replacement. sync.py continues
populating lead-level QA Score / Attention Tier / Max Follow-up Touches /
First Touch Deadline / Primary Objection / Key Concern / Call Link /
Call ID / Meeting Title — those drive tier-based smart views and
workflows in Close. This sync adds a parallel Custom Activity record
per call so first-sale data has the same shape as dialer and
everything-else Custom Activities.

Filter (must satisfy ALL):
  - Title contains "vendingpren" (the first-sale marker)
  - Title does NOT contain any of the FIRST_SALE_EXCLUSION_KEYWORDS
    (follow-up, discovery, setter, next steps, rescheduled, etc.)
  This mirrors sync.py's is_valid_title() exactly.

Hourly schedule slot:
  :00 - sync.py                                  (first sales → lead fields)
  :05 - THIS                                     (first sales → Close CA)
  :15 - close_to_attention_sync.py               (Close dialer recordings → Attention)
  :30 - attention_to_close_dialer_sync.py        (dialer analyses → Close CA)
  :45 - attention_to_close_meeting_sync.py       (everything else → Close CA)

Required GitHub secrets:
  CLOSE_API_KEY        Close API key (Basic auth)
  ATTENTION_API_KEY    Attention API key (Bearer prefix for list endpoint)
  ANTHROPIC_API_KEY    Anthropic API key (for Claude Haiku enrichment)

Optional env vars:
  HOURS_BACK           Window of Attention conversations to consider (default: 24)
  DRY_RUN              If "1", log payloads without writing to Close
"""

import os
import sys
import re
import time
import json
import base64
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

CUSTOM_ACTIVITY_TYPE_NAME = "Attention - First Meeting Analysis"
HAIKU_MODEL = "claude-haiku-4-5-20251001"

INTERNAL_DOMAIN = "@modern-amenities.com"

# Keep in sync with sync.py's INVALID_TITLE_KEYWORDS — if a title has any
# of these, it's NOT a first sales call (it's a follow-up / discovery /
# setter / etc., handled by the :45 sync instead).
FIRST_SALE_EXCLUSION_KEYWORDS = (
    "quick discovery",
    "discovery call",
    "setter",
    "follow-up",
    "follow up",
    "rescheduled",
    "reschedule",
    "next steps",
)

# Substring that marks a title as a "first sales call" candidate. Combined
# with FIRST_SALE_EXCLUSION_KEYWORDS to mirror sync.py's is_valid_title().
FIRST_SALE_TITLE_MARKER = "vendingpren"

# Primary Objection dropdown values. Must match Close field config exactly.
OBJECTION_CHOICES = ("Timing", "Investment", "Fit", "Other")

CLOSE_REQUEST_DELAY = 0.5

# Auth setup
_close_auth_b64 = base64.b64encode(f"{CLOSE_API_KEY}:".encode()).decode()
CLOSE_HEADERS = {"Authorization": f"Basic {_close_auth_b64}"}
ATTENTION_LIST_HEADERS = {
    "Authorization": f"Bearer {ATTENTION_API_KEY}",
    "Content-Type": "application/json",
}
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


# ===== Text helpers =====
def normalize_field_name(name):
    """Strip leading decorative chars (emoji, whitespace) before the first ASCII letter."""
    return re.sub(r"^[^a-zA-Z]+", "", name).strip()


def clean_title(title):
    """Strip recording upload suffixes — mirrors sync.py."""
    if not title:
        return title
    return re.sub(
        r"\s*-\s*\d{4}[_\-]\d{2}[_\-]\d{2}[\s_]\d{2}[_\-]\d{2}.*$",
        "",
        title,
    ).strip()


def html_wrap(text):
    """
    Wrap plain text for Close Custom Activity Textarea fields.

    Close's "Textarea" type is parsed as XHTML server-side and specifically
    requires <body>...</body> wrapping and self-closing void elements
    (<br/>, not <br>). See the dialer enrichment script for the full
    debugging history of this format.
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
        f"<p>{p.replace(chr(10), '<br/>')}</p>" for p in paragraphs
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
def attention_list_conversations(since_dt):
    """
    Fetch Attention conversations finished after `since_dt`. Returns a list
    of {id, attributes} items. Mirrors sync.py's call pattern.
    """
    from_dt = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    url = f"{ATTENTION_API_BASE}/conversations"
    params = {
        "filter[hide_non_analyzed]": "true",
        "fromDateTime": from_dt,
    }
    log(f"Fetching Attention conversations since {from_dt}...")

    for attempt in range(3):
        resp = requests.get(url, params=params, headers=ATTENTION_LIST_HEADERS, timeout=60)
        if resp.status_code in (502, 503, 504):
            time.sleep(2 ** attempt)
            continue
        if not resp.ok:
            raise Exception(f"Attention list returned {resp.status_code}: {resp.text[:300]}")
        return resp.json().get("data", [])
    raise Exception("Attention list exhausted retries")


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


# ===== Title filter =====
def is_first_sale_title(title):
    """
    Mirror of sync.py's is_valid_title(). True if this is a first sales call.
    Keep in sync with sync.py if its filter ever changes.
    """
    if not title:
        return False
    lower = clean_title(title).lower()
    if FIRST_SALE_TITLE_MARKER not in lower:
        return False
    if any(kw in lower for kw in FIRST_SALE_EXCLUSION_KEYWORDS):
        return False
    return True


# ===== Attention data extraction =====
def get_ei_value(ei_dict, target_title):
    """Find an extractedIntelligence entry by its title field (case-insensitive)."""
    target = target_title.lower().strip()
    for val in ei_dict.values():
        if isinstance(val, dict):
            if (val.get("title") or "").lower().strip() == target:
                return val.get("value", "") or ""
    return ""


def get_prospect_email(participants):
    """First non-internal email from the participants list."""
    for p in participants or []:
        email = (p.get("email") or "").lower()
        if email and INTERNAL_DOMAIN not in email:
            return email
    return None


def extract_prospect_name_from_title(title):
    """
    First-sale titles follow the pattern "<Name> and Vendingpreneur(s) Consultation".
    Extract the prefix before "and vending..." as the candidate prospect name.
    Mirrors sync.py's titles_match() extraction logic.

    Examples:
      'Tara Fiddler and Vendingpreneurs Consultation' → 'Tara Fiddler'
      'Hutchison Heberer and Vendingprenuers Consultation' → 'Hutchison Heberer'
    """
    if not title:
        return None
    lower = clean_title(title).lower()
    for separator in (" and vending", "and vendingpren"):
        idx = lower.find(separator)
        if idx > 0:
            name = title[:idx].strip()
            if len(name) > 3:
                return name
    return None


# ===== Custom Activity Type resolution =====
def find_custom_activity_type():
    """Resolve {id, fields_by_name} for the Attention - First Meeting Analysis type."""
    resp = close_get("/custom_activity/")
    if not resp.ok:
        raise Exception(
            f"Could not list custom activity types: {resp.status_code}: {resp.text[:300]}"
        )

    for activity_type in resp.json().get("data", []):
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
                normalized = normalize_field_name(field.get("name", ""))
                if normalized:
                    field_ids[normalized] = field["id"]
            return {"id": type_id, "fields": field_ids}

    raise Exception(
        f"Custom Activity Type '{CUSTOM_ACTIVITY_TYPE_NAME}' not found in Close. "
        f"Verify it exists at Settings → Custom Activities."
    )


# ===== Close lead matching =====
def find_close_lead_by_email(email):
    """Search Close for a lead with this contact email."""
    if not email:
        return None
    resp = close_get(
        "/lead/",
        params={
            "query": f"email_address:{email}",
            "_fields": "id,display_name,contacts",
            "_limit": 5,
        },
    )
    if not resp.ok:
        return None
    leads = resp.json().get("data", [])
    if not leads:
        return None
    return leads[0]


def find_close_lead_by_title(title):
    """Title-only fallback: name extraction → Close lead search by display name."""
    name = extract_prospect_name_from_title(title)
    if not name:
        return None
    resp = close_get(
        "/lead/",
        params={
            "query": name,
            "_fields": "id,display_name,contacts",
            "_limit": 5,
        },
    )
    if not resp.ok:
        return None
    leads = resp.json().get("data", [])
    if not leads:
        return None
    name_lower = name.lower()
    for lead in leads:
        if name_lower in (lead.get("display_name") or "").lower():
            return lead
    if len(leads) == 1:
        return leads[0]
    return None


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
        if activity.get(f"custom.{attention_call_id_field_id}") == attention_uuid:
            return True
    return False


# ===== Enrichment =====
def process_conversation(conv, type_info):
    """
    Process one Attention conversation. Returns ('enriched', activity_id) on
    success, ('skipped', reason) when filtered out or unmatched.
    """
    attrs = conv.get("attributes", {})
    uuid = attrs.get("uuid") or conv.get("id", "")
    title = attrs.get("title", "")

    log(f"\n[{uuid}] '{title}'")

    # 1. Title filter — keep only first sales calls
    if not is_first_sale_title(title):
        log("→ Not a first sales call (handled by another sync), skip", indent=1)
        return ("skipped", "title-filter")

    # 2. Require completed analysis
    sc = attrs.get("scorecardResults") or []
    ei = attrs.get("extractedIntelligence") or {}
    if not sc or not ei:
        log(
            f"→ Attention analysis not yet complete (scorecard={len(sc)}, EI={len(ei)}), skip",
            indent=1,
        )
        return ("skipped", "not-analyzed")

    # 3. Resolve Close lead — email primary, "X and Vendingpreneurs" title fallback
    prospect_email = get_prospect_email(attrs.get("participants", []))
    matched_lead = None
    match_method = None
    if prospect_email:
        matched_lead = find_close_lead_by_email(prospect_email)
        if matched_lead:
            match_method = f"email ({prospect_email})"

    if not matched_lead:
        matched_lead = find_close_lead_by_title(title)
        if matched_lead:
            extracted = extract_prospect_name_from_title(title)
            match_method = f"title-name fallback ('{extracted}')"

    if not matched_lead:
        log(
            f"→ No Close lead found (email={prospect_email or 'none'}, title-extract failed), skip",
            indent=1,
        )
        return ("skipped", "no-match")

    lead_id = matched_lead["id"]
    lead_name = matched_lead.get("display_name", "Unknown")
    log(f"Matched lead: {lead_name} ({lead_id}) via {match_method}", indent=1)

    # 4. Idempotency
    field_ids = type_info["fields"]
    attention_call_id_field_id = field_ids.get("Attention Call ID")
    if not attention_call_id_field_id:
        log("→ 'Attention Call ID' field not found in Custom Activity Type, abort", indent=1)
        return ("skipped", "missing-field")

    if custom_activity_already_exists(lead_id, type_info["id"], uuid, attention_call_id_field_id):
        log("→ Custom Activity already exists for this conversation, skip", indent=1)
        return ("skipped", "duplicate")

    # 5. Pull analysis fields
    qa_score = None
    if sc:
        summary = sc[0].get("summary") or {}
        qa_score = summary.get("averageScore")

    doubt_text = get_ei_value(ei, "Doubt")
    call_summary = get_ei_value(ei, "Call Summary")

    # 6. Haiku enrichment
    log("Classifying Primary Objection (Haiku)...", indent=1)
    primary_objection = haiku_classify_objection(doubt_text)
    log(f"→ {primary_objection}", indent=2)

    log("Summarizing Key Concern (Haiku)...", indent=1)
    key_concern = haiku_summarize_concern(doubt_text)
    log(f"→ {key_concern[:120]}", indent=2)

    # 7. Build payload
    # Notes:
    # - Close Custom Activity "Textarea" fields require <body>...</body> XHTML;
    #   see html_wrap docstring. Key Concern and Call Summary need wrapping.
    # - "Close Call Activity ID" exists on the Custom Activity Type (mirrored
    #   from the dialer template) but doesn't apply to first-sale video calls.
    #   The "skip empty values" loop below omits None values from the payload.
    attention_link = f"https://app.attention.tech/conversations/{uuid}"
    field_mapping = {
        "Attention Call Link": attention_link,
        "Attention Call ID": uuid,
        "Attention Call Title": clean_title(title),
        "QA Score": qa_score,
        "Primary Objection": primary_objection,
        "Key Concern": html_wrap(key_concern),
        "Call Summary": html_wrap(call_summary),
        "Call Duration": attrs.get("mediaDuration"),
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
        return ("skipped", "dry-run")

    resp = close_post("/activity/custom/", payload)
    if not resp.ok:
        raise Exception(f"Failed to create Custom Activity: {resp.status_code}: {resp.text[:500]}")

    activity_id = resp.json().get("id")
    log(f"✅ Created Custom Activity {activity_id} on lead '{lead_name}'", indent=1)
    return ("enriched", activity_id)


# ===== Main =====
def main():
    section(
        f"Attention → Close first-meeting sync "
        f"(HOURS_BACK={HOURS_BACK}, DRY_RUN={DRY_RUN})"
    )

    section("Resolving Close Custom Activity Type")
    type_info = find_custom_activity_type()
    log(f"Type:   {CUSTOM_ACTIVITY_TYPE_NAME}")
    log(f"ID:     {type_info['id']}")
    log(f"Fields ({len(type_info['fields'])}):")
    for name, field_id in sorted(type_info["fields"].items()):
        log(f"  {name}: {field_id}", indent=1)

    since_dt = datetime.now(timezone.utc) - timedelta(hours=HOURS_BACK)
    section(f"Fetching Attention conversations since {since_dt.isoformat()}")
    conversations = attention_list_conversations(since_dt)
    log(f"Total returned: {len(conversations)}")

    section("Processing conversations")
    stats = {"enriched": 0, "skipped": 0, "failed": 0}
    skip_reasons = {}

    for conv in conversations:
        try:
            outcome, detail = process_conversation(conv, type_info)
            if outcome == "enriched":
                stats["enriched"] += 1
            else:
                stats["skipped"] += 1
                skip_reasons[detail] = skip_reasons.get(detail, 0) + 1
        except Exception as e:
            stats["failed"] += 1
            conv_id = conv.get("id") or conv.get("attributes", {}).get("uuid", "?")
            log(f"❌ Error processing {conv_id}: {e}", indent=1)

    section("Done")
    log(f"Enriched: {stats['enriched']}")
    log(f"Skipped:  {stats['skipped']}")
    for reason, count in sorted(skip_reasons.items(), key=lambda x: -x[1]):
        log(f"  ({reason}: {count})", indent=1)
    log(f"Failed:   {stats['failed']}")

    sys.exit(0 if stats["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
