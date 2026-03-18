#!/usr/bin/env python3
"""
Attention → Close QA Score Backfill & Cleanup (One-Time Run)

- Fetches ALL Attention calls since Jan 22, 2026
- Fetches ALL Close meetings
- Matches by title + email, writes correct QA scores
- Clears QA scores from leads that have no matching Attention call
"""

import os
import sys
import time
import requests
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

# ─── CONFIG ───────────────────────────────────────────────────────────────────
ATTENTION_API_KEY = os.environ["ATTENTION_API_KEY"]
CLOSE_API_KEY     = os.environ["CLOSE_API_KEY"]

INTERNAL_DOMAIN   = "@modern-amenities.com"
QA_FIELD_ID       = "custom.cf_kgYoaN7yLuoTTPQVd1xZsjFsfiyc76fpyjoryJ7ZJHq"
PACIFIC           = ZoneInfo("America/Los_Angeles")

# Fetch all calls since go-live date
BACKFILL_FROM     = "2026-01-22T00:00:00Z"

VALID_TITLE_KEYWORDS = [
    "vendingpreneurs consultation",
    "vendingprenuers consultation",   # calendly misspelling
    "vending strategy call with vendingpreneurs",
    "vendingpreneurs strategy call",
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

# ─── ATTENTION API ────────────────────────────────────────────────────────────
def get_all_attention_calls():
    """Fetch all scored Attention calls since go-live date."""
    print(f"\nFetching ALL Attention calls since {BACKFILL_FROM}...", flush=True)

    url = "https://api.attention.tech/v2/conversations"
    params = {
        "filter[hide_non_analyzed]": "true",
        "fromDateTime": BACKFILL_FROM,
    }
    headers = {
        "Authorization": f"Bearer {ATTENTION_API_KEY}",
        "Content-Type": "application/json",
    }

    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    calls = resp.json().get("data", [])
    print(f"  Total Attention calls returned: {len(calls)}", flush=True)

    valid = []
    for call in calls:
        attrs = call.get("attributes", {})
        title = attrs.get("title", "")
        score_results = attrs.get("scorecardResults", [])
        score = score_results[0].get("summary", {}).get("averageScore") if score_results else None

        if not is_valid_title(title):
            continue
        if score is None:
            print(f"  Skip (no score yet): {title}", flush=True)
            continue

        prospect_email = get_prospect_email(attrs.get("participants", []))
        if not prospect_email:
            print(f"  Skip (no external email): {title}", flush=True)
            continue

        valid.append({
            "title": title,
            "score": score,
            "prospect_email": prospect_email,
        })

    print(f"  Valid scored sales calls: {len(valid)}", flush=True)
    return valid

# ─── CLOSE MEETINGS ───────────────────────────────────────────────────────────
def get_all_close_meetings():
    """Paginate ALL Close meetings."""
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
        print(f"  Page {page}: {len(batch)} meetings (total: {len(meetings)})", flush=True)

        if not data.get("has_more"):
            break
        skip += limit

    # Filter to only meetings since go-live
    from_dt = datetime(2026, 1, 22, tzinfo=PACIFIC)
    recent = []
    for m in meetings:
        raw = m.get("starts_at") or m.get("activity_at") or m.get("date_start")
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(PACIFIC)
            if dt >= from_dt:
                recent.append(m)
        except Exception:
            continue

    print(f"  Meetings since Jan 22 2026: {len(recent)}", flush=True)
    return recent

def get_all_leads_with_qa_scores():
    """Fetch all Close leads that currently have a QA score set."""
    print("\nFetching all Close leads with QA scores...", flush=True)
    leads = []
    skip = 0
    limit = 100
    page = 0

    while True:
        page += 1
        data = close_get("lead/", params={
            "_skip": skip,
            "_limit": limit,
            f"custom_fields[{QA_FIELD_ID}][exists]": "true",
            "_fields": f"id,display_name,contacts,{QA_FIELD_ID}",
        })
        batch = data.get("data", [])
        leads.extend(batch)
        print(f"  Page {page}: {len(batch)} leads (total: {len(leads)})", flush=True)

        if not data.get("has_more"):
            break
        skip += limit

    print(f"  Total leads with QA scores: {len(leads)}", flush=True)
    return leads

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def is_valid_title(title):
    if not title:
        return False
    lower = title.lower()
    return any(kw in lower for kw in VALID_TITLE_KEYWORDS)

def get_prospect_email(participants):
    for p in participants or []:
        email = (p.get("email") or "").lower()
        if email and INTERNAL_DOMAIN not in email:
            return email
    return None

def titles_match(attention_title, close_title):
    if not attention_title or not close_title:
        return False

    a = attention_title.lower()
    c = close_title.lower()

    if a == c:
        return True

    # Extract prospect name from Attention title (before 'and vending')
    name_part = None
    for separator in [" and vending", " and vendingpren"]:
        if separator in a:
            name_part = a.split(separator)[0].strip()
            break

    if name_part and len(name_part) > 3:
        if name_part in c:
            return True
        parts = [p for p in name_part.split() if len(p) >= 3]
        matches = sum(1 for p in parts if p in c)
        if matches >= 2:
            return True

    return False

def find_close_lead(attention_call, close_meetings, lead_cache):
    """Match Attention call to Close lead via title + email."""
    prospect_email = attention_call["prospect_email"]
    attn_title = attention_call["title"]

    title_matches = [m for m in close_meetings if titles_match(attn_title, m.get("title", ""))]

    for meeting in title_matches:
        lead_id = meeting.get("lead_id")
        if not lead_id:
            continue

        if lead_id not in lead_cache:
            lead_data = close_get(f"lead/{lead_id}", params={
                "_fields": f"id,display_name,contacts,{QA_FIELD_ID}"
            })
            lead_cache[lead_id] = lead_data

        lead = lead_cache[lead_id]

        # Verify email matches a contact on this lead
        for contact in lead.get("contacts", []):
            for email_obj in contact.get("emails", []):
                if email_obj.get("email", "").lower() == prospect_email:
                    return lead_id, lead.get("display_name", "Unknown")

    # Fallback: title match only
    if title_matches:
        lead_id = title_matches[0].get("lead_id")
        if lead_id:
            if lead_id not in lead_cache:
                lead_data = close_get(f"lead/{lead_id}", params={
                    "_fields": f"id,display_name,contacts,{QA_FIELD_ID}"
                })
                lead_cache[lead_id] = lead_data
            lead_name = lead_cache[lead_id].get("display_name", "Unknown")
            print(f"  ⚠️  Title-only match (no email confirm): {lead_name}", flush=True)
            return lead_id, lead_name

    return None, None

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=== Attention → Close QA Backfill & Cleanup ===", flush=True)
    print(f"Backfill from: {BACKFILL_FROM}", flush=True)
    print(f"Run time: {datetime.now(PACIFIC).strftime('%Y-%m-%d %H:%M %Z')}", flush=True)

    # Step 1: Get all valid Attention calls
    attention_calls = get_all_attention_calls()

    # Step 2: Get all Close meetings since go-live
    close_meetings = get_all_close_meetings()

    # Step 3: Get all leads that currently have a QA score (for cleanup)
    scored_leads = get_all_leads_with_qa_scores()
    scored_lead_ids = {l["id"] for l in scored_leads}

    lead_cache = {}
    legitimately_updated = set()  # lead_ids that get a valid score written

    updated = 0
    already_correct = 0
    skipped = 0
    errors = 0

    # ── PHASE 1: Write correct scores ────────────────────────────────────────
    print(f"\n{'='*50}", flush=True)
    print("PHASE 1: Writing correct QA scores", flush=True)
    print(f"{'='*50}", flush=True)

    for call in attention_calls:
        print(f"\nProcessing: \"{call['title']}\"", flush=True)
        print(f"  Email: {call['prospect_email']} | Score: {call['score']}", flush=True)

        try:
            lead_id, lead_name = find_close_lead(call, close_meetings, lead_cache)

            if not lead_id:
                print(f"  ❌ No matching Close lead found — skipping", flush=True)
                skipped += 1
                continue

            legitimately_updated.add(lead_id)

            existing_score = lead_cache.get(lead_id, {}).get(QA_FIELD_ID)
            if existing_score == call["score"]:
                print(f"  ⏭️  Score already correct ({existing_score})", flush=True)
                already_correct += 1
                continue

            close_put(f"lead/{lead_id}/", {QA_FIELD_ID: call["score"]})
            print(f"  ✅ Updated \"{lead_name}\": {existing_score} → {call['score']}", flush=True)
            updated += 1

        except Exception as e:
            print(f"  ❌ Error: {e}", flush=True)
            errors += 1

    # ── PHASE 2: Clear bad scores ─────────────────────────────────────────────
    print(f"\n{'='*50}", flush=True)
    print("PHASE 2: Clearing bad QA scores", flush=True)
    print(f"{'='*50}", flush=True)

    leads_to_clear = [l for l in scored_leads if l["id"] not in legitimately_updated]
    print(f"  Leads with scores that had no Attention match: {len(leads_to_clear)}", flush=True)

    cleared = 0
    for lead in leads_to_clear:
        lead_name = lead.get("display_name", "Unknown")
        bad_score = lead.get(QA_FIELD_ID)
        print(f"  Clearing \"{lead_name}\" (bad score: {bad_score})...", flush=True)
        try:
            close_put(f"lead/{lead['id']}/", {QA_FIELD_ID: None})
            print(f"  ✅ Cleared", flush=True)
            cleared += 1
        except Exception as e:
            print(f"  ❌ Error clearing: {e}", flush=True)
            errors += 1

    # ── SUMMARY ──────────────────────────────────────────────────────────────
    print(f"\n{'='*50}", flush=True)
    print("=== Backfill & Cleanup Complete ===", flush=True)
    print(f"✅ Scores written:     {updated}", flush=True)
    print(f"⏭️  Already correct:   {already_correct}", flush=True)
    print(f"🧹 Bad scores cleared: {cleared}", flush=True)
    print(f"⚠️  No match found:    {skipped}", flush=True)
    print(f"❌ Errors:             {errors}", flush=True)

if __name__ == "__main__":
    main()
