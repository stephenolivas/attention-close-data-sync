#!/usr/bin/env python3
"""
Attention → Close QA Score Sync
Matches Attention calls to Close meetings by title + email, then writes QA scores,
call summary notes, and enrichment fields (tier, objection, key concern, call link, etc.)
"""

import os
import sys
import time
import json
import requests
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

# ─── CONFIG ───────────────────────────────────────────────────────────────────
ATTENTION_API_KEY = os.environ["ATTENTION_API_KEY"]
CLOSE_API_KEY     = os.environ["CLOSE_API_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

INTERNAL_DOMAIN   = "@modern-amenities.com"
LOOKBACK_HOURS    = 8
PACIFIC           = ZoneInfo("America/Los_Angeles")

# ─── CLOSE CUSTOM FIELD IDs ───────────────────────────────────────────────────
QA_FIELD_ID             = "custom.cf_kgYoaN7yLuoTTPQVd1xZsjFsfiyc76fpyjoryJ7ZJHq"
CALL_LINK_FIELD_ID      = "custom.cf_8fvkyAPLWZpKP0QGhcaqOYoAGGgJIy9aik26d6dY6ew"
PRIMARY_OBJECTION_ID    = "custom.cf_0oluAzEnKTCyMq55081ImESQBQY0uV6cnzZ0dFXdzgd"
KEY_CONCERN_ID          = "custom.cf_PPLSuz3PLKU8UB8czoLZYqeLGrFPE4uagM775nqdYgs"
ATTENTION_TIER_ID       = "custom.cf_qXoHdvbXLY0z8yZAofXheebmmNU0jpkqRJgCIYShxKX"
MAX_FOLLOWUP_ID         = "custom.cf_AqnJ9rNNdbSKNdIxXyJOCooaH7sPaSCjR89uUXMDzzG"
FIRST_TOUCH_DEADLINE_ID = "custom.cf_dzy8y9kY3V9xR1sygLb1cJkaKY8qvx3OT6QbB5IsiCA"
CALL_ID_FIELD_ID        = "custom.cf_o3rG6LXcHDGiX6x0j1ISjuytGrqM5UribVPhJMLRWbD"
MEETING_TITLE_FIELD_ID  = "custom.cf_IHSgCE1S61fMOF5kuHRS07oG7ZqfJitRaKXIYusRm5r"

ALL_CUSTOM_FIELDS = ",".join([
    QA_FIELD_ID, CALL_LINK_FIELD_ID, PRIMARY_OBJECTION_ID,
    KEY_CONCERN_ID, ATTENTION_TIER_ID, MAX_FOLLOWUP_ID, FIRST_TOUCH_DEADLINE_ID,
    CALL_ID_FIELD_ID, MEETING_TITLE_FIELD_ID,
])

# ─── TIER CONFIG ──────────────────────────────────────────────────────────────
# T3/T4 are no-show based and handled separately (no Attention call exists)
TIER_LABELS = {
    "T1": "T1 - Hot Lead",
    "T2": "T2 - Warm Lead",
    "T3": "T3 - Cool Lead",
    "T4": "T4 - Cold Lead",
}
TIER_TOUCHES      = {"T1": 5, "T2": 4}
TIER_WINDOW_HOURS = {"T1": 2, "T2": 4}  # hours after call end for first touch deadline

# ─── TITLE EXCLUSIONS ─────────────────────────────────────────────────────────
INVALID_TITLE_KEYWORDS = [
    "quick discovery",
    "discovery call",
    "setter",
    "follow-up",
    "follow up",
    "rescheduled",
    "reschedule",
    "next steps",
]

# ─── CLOSE API SESSION ────────────────────────────────────────────────────────
session = requests.Session()
session.auth = (CLOSE_API_KEY, "")
session.headers.update({"Content-Type": "application/json"})

def close_get(endpoint, params=None):
    time.sleep(0.5)
    url = f"https://api.close.com/api/v1/{endpoint}"
    for attempt in range(5):
        resp = session.get(url, params=params or {}, timeout=60)
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", 5))
            print(f"  Rate limited. Waiting {wait}s...", flush=True)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()

def close_put(endpoint, data):
    time.sleep(0.5)
    url = f"https://api.close.com/api/v1/{endpoint}"
    for attempt in range(5):
        resp = session.put(url, json=data, timeout=60)
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", 5))
            print(f"  Rate limited. Waiting {wait}s...", flush=True)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()

def close_post(endpoint, data):
    time.sleep(0.5)
    url = f"https://api.close.com/api/v1/{endpoint}"
    for attempt in range(5):
        resp = session.post(url, json=data, timeout=60)
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", 5))
            print(f"  Rate limited. Waiting {wait}s...", flush=True)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()

# ─── ANTHROPIC API ────────────────────────────────────────────────────────────
def claude_complete(prompt):
    """Single-turn Claude call. Returns text response."""
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 100,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["content"][0]["text"].strip()

def classify_objection(doubt_text):
    """Classify the primary objection into one of: timing, investment, fit, other."""
    if not doubt_text:
        return "other"
    prompt = (
        f"Classify the primary sales objection in the following text into EXACTLY one of these four categories: "
        f"Timing, Investment, Fit, Other.\n\n"
        f"Respond with only the single word category, nothing else.\n\n"
        f"Text:\n{doubt_text[:1500]}"
    )
    try:
        result = claude_complete(prompt).strip().capitalize()
        if result in ("Timing", "Investment", "Fit", "Other"):
            return result
        return "Other"
    except Exception as e:
        print(f"  ⚠️  Objection classification failed: {e}", flush=True)
        return "Other"

def generate_key_concern(doubt_text):
    """Summarize the prospect's main concern in 20 words or fewer."""
    if not doubt_text:
        return None
    prompt = (
        f"Summarize the prospect's single biggest concern from the following text in 20 words or fewer. "
        f"Be direct and specific. No preamble.\n\n"
        f"Text:\n{doubt_text[:1500]}"
    )
    try:
        return claude_complete(prompt)
    except Exception as e:
        print(f"  ⚠️  Key concern generation failed: {e}", flush=True)
        return None

# ─── ATTENTION DATA EXTRACTION ────────────────────────────────────────────────
def get_extracted_field(attrs, field_title):
    """Pull a value from extractedIntelligence by title (case-insensitive)."""
    intelligence = attrs.get("extractedIntelligence") or {}
    for item in intelligence.values():
        if item.get("title", "").strip().lower() == field_title.lower():
            return item.get("value")
    return None

def get_call_summary(attrs):
    return get_extracted_field(attrs, "call summary")

def get_doubt_text(attrs):
    return get_extracted_field(attrs, "doubt")

def get_prospect_email(participants):
    for p in participants or []:
        email = (p.get("email") or "").lower()
        if email and INTERNAL_DOMAIN not in email:
            return email
    return None

def get_tier(attendance, score):
    """Determine T1/T2 based on attendance label and score. T3/T4 handled separately."""
    if attendance != "Shown":
        return None
    if score >= 80:
        return "T1"
    if score >= 40:
        return "T2"
    return "T2"  # attended but very low score still gets T2

def get_first_touch_deadline(finished_at_str, tier):
    """Calculate first touch deadline: call end time + tier window."""
    if not finished_at_str or not tier or tier not in TIER_WINDOW_HOURS:
        return None
    try:
        finished = datetime.fromisoformat(finished_at_str.replace("Z", "+00:00"))
        deadline = finished + timedelta(hours=TIER_WINDOW_HOURS[tier])
        return deadline.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None

# ─── ATTENTION API ────────────────────────────────────────────────────────────
def get_attention_calls():
    """Fetch recent scored calls from Attention, filtered to sales call types."""
    from_dt = (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = "https://api.attention.tech/v2/conversations"
    params = {
        "filter[hide_non_analyzed]": "true",
        "fromDateTime": from_dt,
    }
    headers = {
        "Authorization": f"Bearer {ATTENTION_API_KEY}",
        "Content-Type": "application/json",
    }

    print(f"\nFetching Attention calls since {from_dt}...", flush=True)
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    calls = resp.json().get("data", [])
    print(f"  Total calls returned: {len(calls)}", flush=True)

    valid = []
    for call in calls:
        attrs = call.get("attributes", {})
        title = attrs.get("title", "")
        score_results = attrs.get("scorecardResults", [])
        score = score_results[0].get("summary", {}).get("averageScore") if score_results else None

        if not is_valid_title(title):
            print(f"  Skip (wrong type): {title}", flush=True)
            continue
        if score is None:
            print(f"  Skip (no score): {title}", flush=True)
            continue

        prospect_email = get_prospect_email(attrs.get("participants", []))
        if not prospect_email:
            print(f"  Skip (no external email): {title}", flush=True)
            continue

        # Extract attendance label
        labels = attrs.get("labels") or {}
        attendance = labels.get("Attendance", "")

        # Tier + derived fields
        tier = get_tier(attendance, score)
        finished_at = attrs.get("finishedAt")
        call_uuid = attrs.get("uuid") or call.get("id", "")
        call_link = f"https://app.attention.tech/conversations/all-calls/{call_uuid}" if call_uuid else None
        first_touch_deadline = get_first_touch_deadline(finished_at, tier)

        # AI-enriched fields from Doubt
        doubt_text = get_doubt_text(attrs)
        print(f"  🤖 Classifying objection + key concern for: {title[:60]}...", flush=True)
        primary_objection = classify_objection(doubt_text)
        key_concern = generate_key_concern(doubt_text)

        valid.append({
            "title": title,
            "clean_title": clean_title(title),
            "call_id": call_uuid,
            "score": score,
            "prospect_email": prospect_email,
            "call_summary": get_call_summary(attrs),
            "call_link": call_link,
            "tier": tier,
            "max_followup": TIER_TOUCHES.get(tier),
            "first_touch_deadline": first_touch_deadline,
            "primary_objection": primary_objection,
            "key_concern": key_concern,
        })
        print(f"  ✅ Valid call: \"{title}\" | score={score} | tier={tier} | objection={primary_objection}", flush=True)

    print(f"\n{len(valid)} valid Attention calls to process.", flush=True)
    return valid

# ─── CLOSE MEETINGS ───────────────────────────────────────────────────────────
def get_all_close_meetings():
    print("\nFetching all Close meetings (paginating)...", flush=True)
    meetings = []
    skip = 0
    limit = 100
    page = 0

    while True:
        page += 1
        data = close_get("activity/meeting/", params={
            "_skip": skip,
            "_limit": limit,
            "_fields": "id,lead_id,title,starts_at,activity_at,date_start",
        })
        batch = data.get("data", [])
        meetings.extend(batch)
        print(f"  Page {page}: {len(batch)} meetings (total so far: {len(meetings)})", flush=True)

        if not data.get("has_more"):
            break
        skip += limit

    print(f"  Total Close meetings fetched: {len(meetings)}", flush=True)
    return meetings

def filter_recent_meetings(meetings, hours=LOOKBACK_HOURS):
    cutoff = datetime.now(PACIFIC) - timedelta(hours=hours)
    recent = []

    for m in meetings:
        raw = m.get("starts_at") or m.get("activity_at") or m.get("date_start")
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(PACIFIC)
            if dt >= cutoff:
                recent.append(m)
        except Exception:
            continue

    print(f"  Meetings within last {hours}h: {len(recent)}", flush=True)
    return recent

# ─── MATCHING ─────────────────────────────────────────────────────────────────
import re as _re

def clean_title(title):
    """Strip recording upload suffixes e.g. ' - 2026_04_23 13_26 PDT - Recording.mp4'"""
    if not title:
        return title
    cleaned = _re.sub(r'\s*-\s*\d{4}[_\-]\d{2}[_\-]\d{2}[\s_]\d{2}[_\-]\d{2}.*$', '', title).strip()
    return cleaned

def is_valid_title(title):
    if not title:
        return False
    lower = clean_title(title).lower()
    if "vendingpren" not in lower:
        return False
    if any(kw in lower for kw in INVALID_TITLE_KEYWORDS):
        return False
    return True

def titles_match(attention_title, close_title):
    if not attention_title or not close_title:
        return False
    a = clean_title(attention_title).lower()
    c = close_title.lower()
    if a == c:
        return True
    name_part = None
    for separator in [" and vending", " and vendingprenu", "and vendingpren"]:
        if separator in a:
            name_part = a.split(separator)[0].strip()
            break
    if name_part and len(name_part) > 3:
        if name_part in c:
            return True
        parts = [p for p in name_part.split() if len(p) >= 3]
        if sum(1 for p in parts if p in c) >= 2:
            return True
    return False

def find_close_lead(attention_call, close_meetings, lead_cache):
    prospect_email = attention_call["prospect_email"]
    attn_title = attention_call["title"]

    title_matches = [m for m in close_meetings if titles_match(attn_title, m.get("title", ""))]
    print(f"  Title matches in Close: {len(title_matches)}", flush=True)

    for meeting in title_matches:
        lead_id = meeting.get("lead_id")
        if not lead_id:
            continue
        if lead_id not in lead_cache:
            lead_data = close_get(f"lead/{lead_id}", params={
                "_fields": f"id,display_name,contacts,{ALL_CUSTOM_FIELDS}"
            })
            lead_cache[lead_id] = lead_data
        lead = lead_cache[lead_id]
        lead_name = lead.get("display_name", "Unknown")
        for contact in lead.get("contacts", []):
            for email_obj in contact.get("emails", []):
                if email_obj.get("email", "").lower() == prospect_email:
                    print(f"  ✅ Matched lead: {lead_name} (email + title)", flush=True)
                    return lead_id, lead_name

    if title_matches:
        lead_id = title_matches[0].get("lead_id")
        if lead_id:
            if lead_id not in lead_cache:
                lead_data = close_get(f"lead/{lead_id}", params={
                    "_fields": f"id,display_name,contacts,{ALL_CUSTOM_FIELDS}"
                })
                lead_cache[lead_id] = lead_data
            lead_name = lead_cache[lead_id].get("display_name", "Unknown")
            print(f"  ⚠️  Title-only match (no email confirm): {lead_name}", flush=True)
            return lead_id, lead_name

    return None, None

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=== Attention → Close QA Sync ===", flush=True)
    print(f"Lookback: {LOOKBACK_HOURS} hours | {datetime.now(PACIFIC).strftime('%Y-%m-%d %H:%M %Z')}", flush=True)

    attention_calls = get_attention_calls()
    if not attention_calls:
        print("\nNo valid Attention calls to process. Done.", flush=True)
        return

    all_meetings = get_all_close_meetings()
    recent_meetings = filter_recent_meetings(all_meetings)

    if not recent_meetings:
        print("\nNo recent Close meetings found in lookback window. Done.", flush=True)
        return

    lead_cache = {}
    updated = 0
    skipped = 0
    errors = 0

    for call in attention_calls:
        print(f"\nProcessing: \"{call['title']}\"", flush=True)
        print(f"  Email: {call['prospect_email']} | Score: {call['score']} | Tier: {call['tier']}", flush=True)

        try:
            lead_id, lead_name = find_close_lead(call, recent_meetings, lead_cache)

            if not lead_id:
                print(f"  ❌ No matching Close lead found — skipping", flush=True)
                skipped += 1
                continue

            lead = lead_cache.get(lead_id, {})
            existing_score = lead.get(QA_FIELD_ID)
            score_changed = existing_score != call["score"]

            # Always write all fields on every match
            update_payload = {
                QA_FIELD_ID:             call["score"],
                CALL_LINK_FIELD_ID:      call["call_link"],
                ATTENTION_TIER_ID:       TIER_LABELS.get(call["tier"]) if call["tier"] else None,
                MAX_FOLLOWUP_ID:         call["max_followup"],
                FIRST_TOUCH_DEADLINE_ID: call["first_touch_deadline"],
                PRIMARY_OBJECTION_ID:    call["primary_objection"],
                KEY_CONCERN_ID:          call["key_concern"],
                CALL_ID_FIELD_ID:        call["call_id"],
                MEETING_TITLE_FIELD_ID:  call["clean_title"],
            }
            # Strip None values
            update_payload = {k: v for k, v in update_payload.items() if v is not None}

            # Try full payload; if 400, fall back field-by-field to find the culprit
            try:
                close_put(f"lead/{lead_id}/", update_payload)
            except Exception as put_err:
                if "400" in str(put_err):
                    print(f"  ⚠️  Full payload rejected (400) — trying fields individually...", flush=True)
                    for field_id, value in update_payload.items():
                        try:
                            close_put(f"lead/{lead_id}/", {field_id: value})
                            print(f"    ✅ {field_id[:40]} = {str(value)[:40]}", flush=True)
                        except Exception as fe:
                            print(f"    ❌ FAILED: {field_id[:40]} = {str(value)[:40]} → {fe}", flush=True)
                else:
                    raise
            print(f"  ✅ Updated \"{lead_name}\": score={call['score']} | tier={call['tier']} | objection={call['primary_objection']}", flush=True)
            updated += 1

            # Only create note when score is new (avoid duplicate notes on re-runs)
            if score_changed and call.get("call_summary"):
                note_body = f"📋 Attention Call Summary\n\n{call['call_summary']}"
                close_post("activity/note/", {"lead_id": lead_id, "note": note_body})
                print(f"  📝 Note created for \"{lead_name}\"", flush=True)
            elif score_changed:
                print(f"  ⚠️  No call summary available for \"{lead_name}\"", flush=True)

        except Exception as e:
            print(f"  ❌ Error: {e}", flush=True)
            errors += 1

    print("\n=== Sync Complete ===", flush=True)
    print(f"✅ Updated: {updated}", flush=True)
    print(f"⏭️  Skipped: {skipped}", flush=True)
    print(f"❌ Errors:  {errors}", flush=True)

if __name__ == "__main__":
    main()
