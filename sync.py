#!/usr/bin/env python3
"""
Attention → Close QA Score Sync
Matches Attention calls to Close meetings by title + email, then writes QA scores.
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

INTERNAL_DOMAIN   = "@modern-amenities.com"
QA_FIELD_ID       = "custom.cf_kgYoaN7yLuoTTPQVd1xZsjFsfiyc76fpyjoryJ7ZJHq"
LOOKBACK_HOURS    = 8
PACIFIC           = ZoneInfo("America/Los_Angeles")

# Match any title containing "vendingpren" (catches all Calendly spelling variants)
# and excluding setter/discovery calls
INVALID_TITLE_KEYWORDS = [
    "quick discovery",
    "discovery call",
    "setter",
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

        valid.append({
            "title": title,
            "score": score,
            "prospect_email": prospect_email,
        })
        print(f"  ✅ Valid call: \"{title}\" | {prospect_email} | score={score}", flush=True)

    print(f"\n{len(valid)} valid Attention calls to process.", flush=True)
    return valid

# ─── CLOSE MEETINGS ───────────────────────────────────────────────────────────
def get_all_close_meetings():
    """
    Paginate ALL Close meetings. Date filters are silently ignored by the API
    so we fetch everything and filter in Python.
    """
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
    """Filter meetings to those within the lookback window, converting UTC → Pacific."""
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
def is_valid_title(title):
    if not title:
        return False
    lower = title.lower()
    if "vendingpren" not in lower:
        return False
    if any(kw in lower for kw in INVALID_TITLE_KEYWORDS):
        return False
    return True

def get_prospect_email(participants):
    for p in participants or []:
        email = (p.get("email") or "").lower()
        if email and INTERNAL_DOMAIN not in email:
            return email
    return None

def titles_match(attention_title, close_title):
    """
    Check if two meeting titles refer to the same meeting.
    Strategy: extract the prospect name portion (before 'and Vending')
    and check if it appears in the other title.
    """
    if not attention_title or not close_title:
        return False

    a = attention_title.lower()
    c = close_title.lower()

    # Direct match
    if a == c:
        return True

    # Extract name from Attention title (everything before ' and vending')
    name_part = None
    for separator in [" and vending", " and vendingprenu", "and vendingpren"]:
        if separator in a:
            name_part = a.split(separator)[0].strip()
            break

    if name_part and len(name_part) > 3:
        # Check if the name appears in the Close title
        if name_part in c:
            return True
        # Try individual name parts (first + last at minimum)
        parts = [p for p in name_part.split() if len(p) >= 3]
        matches = sum(1 for p in parts if p in c)
        if matches >= 2:
            return True

    return False

def find_close_lead(attention_call, close_meetings, lead_cache):
    """
    Find the Close lead_id for an Attention call by:
    1. Matching meeting title
    2. Verifying prospect email matches a contact on that lead
    """
    prospect_email = attention_call["prospect_email"]
    attn_title = attention_call["title"]

    # Find Close meetings with matching title
    title_matches = [m for m in close_meetings if titles_match(attn_title, m.get("title", ""))]
    print(f"  Title matches in Close: {len(title_matches)}", flush=True)

    for meeting in title_matches:
        lead_id = meeting.get("lead_id")
        if not lead_id:
            continue

        # Fetch lead contacts (cached)
        if lead_id not in lead_cache:
            lead_data = close_get(f"lead/{lead_id}", params={"_fields": f"id,display_name,contacts,{QA_FIELD_ID}"})
            lead_cache[lead_id] = lead_data

        lead = lead_cache[lead_id]
        lead_name = lead.get("display_name", "Unknown")

        # Check if prospect email matches any contact on this lead
        for contact in lead.get("contacts", []):
            for email_obj in contact.get("emails", []):
                if email_obj.get("email", "").lower() == prospect_email:
                    print(f"  ✅ Matched lead: {lead_name} (email + title)", flush=True)
                    return lead_id, lead_name

    # Fallback: title match only (no email confirmation)
    if title_matches:
        lead_id = title_matches[0].get("lead_id")
        if lead_id:
            if lead_id not in lead_cache:
                lead_data = close_get(f"lead/{lead_id}", params={"_fields": f"id,display_name,contacts,{QA_FIELD_ID}"})
                lead_cache[lead_id] = lead_data
            lead_name = lead_cache[lead_id].get("display_name", "Unknown")
            print(f"  ⚠️  Title-only match (no email confirm): {lead_name}", flush=True)
            return lead_id, lead_name

    return None, None

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=== Attention → Close QA Sync ===", flush=True)
    print(f"Lookback: {LOOKBACK_HOURS} hours | {datetime.now(PACIFIC).strftime('%Y-%m-%d %H:%M %Z')}", flush=True)

    # Step 1: Get valid Attention calls
    attention_calls = get_attention_calls()
    if not attention_calls:
        print("\nNo valid Attention calls to process. Done.", flush=True)
        return

    # Step 2: Get all Close meetings and filter to recent
    all_meetings = get_all_close_meetings()
    recent_meetings = filter_recent_meetings(all_meetings)

    if not recent_meetings:
        print("\nNo recent Close meetings found in lookback window. Done.", flush=True)
        return

    # Step 3: Match and update
    lead_cache = {}
    updated = 0
    skipped = 0
    errors = 0

    for call in attention_calls:
        print(f"\nProcessing: \"{call['title']}\"", flush=True)
        print(f"  Email: {call['prospect_email']} | Score: {call['score']}", flush=True)

        try:
            lead_id, lead_name = find_close_lead(call, recent_meetings, lead_cache)

            if not lead_id:
                print(f"  ❌ No matching Close lead found — skipping", flush=True)
                skipped += 1
                continue

            # Check existing score to avoid unnecessary writes
            lead = lead_cache.get(lead_id, {})
            existing_score = lead.get(QA_FIELD_ID)
            if existing_score == call["score"]:
                print(f"  ⏭️  Score already up to date ({existing_score}) — skipping", flush=True)
                skipped += 1
                continue

            # Write QA score to Close
            close_put(f"lead/{lead_id}/", {QA_FIELD_ID: call["score"]})
            print(f"  ✅ Updated \"{lead_name}\": {existing_score} → {call['score']}", flush=True)
            updated += 1

        except Exception as e:
            print(f"  ❌ Error: {e}", flush=True)
            errors += 1

    print("\n=== Sync Complete ===", flush=True)
    print(f"✅ Updated: {updated}", flush=True)
    print(f"⏭️  Skipped: {skipped}", flush=True)
    print(f"❌ Errors:  {errors}", flush=True)

if __name__ == "__main__":
    main()
