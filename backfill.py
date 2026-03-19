#!/usr/bin/env python3
"""
Attention → Close QA Score Backfill & Cleanup

Phase 1: Clear any leads with QA score = 0 (bad data from Make)
Phase 2: Write scores to leads that are missing a QA score entirely
"""

import os
import time
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

# ─── CONFIG ───────────────────────────────────────────────────────────────────
ATTENTION_API_KEY = os.environ["ATTENTION_API_KEY"]
CLOSE_API_KEY     = os.environ["CLOSE_API_KEY"]

INTERNAL_DOMAIN   = "@modern-amenities.com"
QA_FIELD_ID       = "custom.cf_kgYoaN7yLuoTTPQVd1xZsjFsfiyc76fpyjoryJ7ZJHq"
QA_FIELD_SHORT    = "cf_kgYoaN7yLuoTTPQVd1xZsjFsfiyc76fpyjoryJ7ZJHq"
PACIFIC           = ZoneInfo("America/Los_Angeles")
BACKFILL_FROM     = "2026-01-22T00:00:00Z"

VALID_TITLE_KEYWORDS = [
    "vendingpreneurs consultation",
    "vendingprenuers consultation",
    "vending strategy call with vendingpreneurs",
    "vendingpreneurs strategy call",
]

# ─── CLOSE API ────────────────────────────────────────────────────────────────
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
    name_part = None
    for separator in [" and vending", " and vendingpren"]:
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

# ─── PHASE 1: CLEAR ZERO SCORES ───────────────────────────────────────────────
def clear_zero_scores():
    print("\n" + "="*50, flush=True)
    print("PHASE 1: Clearing leads with QA score = 0", flush=True)
    print("="*50, flush=True)

    leads = []
    skip = 0
    page = 0

    while True:
        page += 1
        data = close_get("lead/", params={
            "_skip": skip,
            "_limit": 100,
            f"custom_fields[{QA_FIELD_SHORT}]": 0,
            "_fields": f"id,display_name,{QA_FIELD_ID}",
        })
        batch = data.get("data", [])
        leads.extend(batch)
        print(f"  Page {page}: {len(batch)} leads (total: {len(leads)})", flush=True)
        if not data.get("has_more"):
            break
        skip += 100

    print(f"  Found {len(leads)} leads with score=0 to clear", flush=True)

    cleared = 0
    errors = 0
    for lead in leads:
        name = lead.get("display_name", "Unknown")
        try:
            close_put(f"lead/{lead['id']}/", {QA_FIELD_ID: None})
            print(f"  🧹 Cleared \"{name}\"", flush=True)
            cleared += 1
        except Exception as e:
            print(f"  ❌ Error clearing \"{name}\": {e}", flush=True)
            errors += 1

    print(f"\n  Done. Cleared: {cleared} | Errors: {errors}", flush=True)
    return cleared


# ─── PHASE 2: BACKFILL MISSING SCORES ─────────────────────────────────────────
def backfill_missing_scores():
    print("\n" + "="*50, flush=True)
    print("PHASE 2: Backfilling missing QA scores", flush=True)
    print("="*50, flush=True)

    # Step A: Fetch all Attention calls (paginated)
    print("\nFetching Attention calls...", flush=True)
    headers = {
        "Authorization": f"Bearer {ATTENTION_API_KEY}",
        "Content-Type": "application/json",
    }

    all_calls = []
    page = 1
    while True:
        resp = requests.get(
            "https://api.attention.tech/v2/conversations",
            params={
                "filter[hide_non_analyzed]": "true",
                "fromDateTime": BACKFILL_FROM,
                "page[size]": 100,
                "page[number]": page,
            },
            headers=headers,
            timeout=30
        )
        resp.raise_for_status()
        batch = resp.json().get("data", [])
        all_calls.extend(batch)
        print(f"  Attention page {page}: {len(batch)} calls (total: {len(all_calls)})", flush=True)
        if len(batch) < 100:
            break
        page += 1
        time.sleep(0.3)

    # Filter to valid scored sales calls
    valid_calls = []
    for call in all_calls:
        attrs = call.get("attributes", {})
        title = attrs.get("title", "")
        score_results = attrs.get("scorecardResults", [])
        score = score_results[0].get("summary", {}).get("averageScore") if score_results else None
        if not is_valid_title(title) or score is None:
            continue
        prospect_email = get_prospect_email(attrs.get("participants", []))
        if not prospect_email:
            continue
        valid_calls.append({"title": title, "score": score, "prospect_email": prospect_email})

    print(f"  Valid scored sales calls: {len(valid_calls)}", flush=True)

    # Step B: Fetch all Close meetings (paginated)
    print("\nFetching Close meetings...", flush=True)
    meetings = []
    skip = 0
    page = 0
    while True:
        page += 1
        data = close_get("activity/meeting/", params={
            "_skip": skip,
            "_limit": 100,
            "_fields": "id,lead_id,title,starts_at,activity_at,date_start",
        })
        batch = data.get("data", [])
        meetings.extend(batch)
        print(f"  Close page {page}: {len(batch)} (total: {len(meetings)})", flush=True)
        if not data.get("has_more"):
            break
        skip += 100

    # Filter to since go-live in Python
    from_dt = datetime(2026, 1, 22, tzinfo=PACIFIC)
    recent_meetings = []
    for m in meetings:
        raw = m.get("starts_at") or m.get("activity_at") or m.get("date_start")
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(PACIFIC)
            if dt >= from_dt:
                recent_meetings.append(m)
        except Exception:
            continue
    print(f"  Meetings since Jan 22: {len(recent_meetings)}", flush=True)

    # Step C: Match and update — ONLY leads missing a score
    lead_cache = {}
    updated = 0
    skipped_has_score = 0
    skipped_no_match = 0
    errors = 0

    print(f"\nProcessing {len(valid_calls)} calls...", flush=True)

    for i, call in enumerate(valid_calls, 1):
        print(f"\n[{i}/{len(valid_calls)}] \"{call['title']}\"", flush=True)
        print(f"  {call['prospect_email']} | score={call['score']}", flush=True)

        try:
            title_matches = [m for m in recent_meetings if titles_match(call["title"], m.get("title", ""))]
            lead_id = None
            lead_name = None
            skip_this = False

            for meeting in title_matches:
                mid = meeting.get("lead_id")
                if not mid:
                    continue
                if mid not in lead_cache:
                    lead_data = close_get(f"lead/{mid}", params={
                        "_fields": f"id,display_name,contacts,{QA_FIELD_ID}"
                    })
                    lead_cache[mid] = lead_data
                lead = lead_cache[mid]

                # Skip if already has a real non-zero score
                existing = lead.get(QA_FIELD_ID)
                if existing and existing != 0:
                    print(f"  ⏭️  \"{lead.get('display_name')}\" already has score {existing} — skipping", flush=True)
                    skipped_has_score += 1
                    skip_this = True
                    break

                # Verify email matches
                for contact in lead.get("contacts", []):
                    for email_obj in contact.get("emails", []):
                        if email_obj.get("email", "").lower() == call["prospect_email"]:
                            lead_id = mid
                            lead_name = lead.get("display_name", "Unknown")
                            break
                    if lead_id:
                        break
                if lead_id:
                    break

            if skip_this:
                continue

            # Fallback: title only if no email match found
            if not lead_id and title_matches:
                mid = title_matches[0].get("lead_id")
                if mid:
                    if mid not in lead_cache:
                        lead_data = close_get(f"lead/{mid}", params={
                            "_fields": f"id,display_name,contacts,{QA_FIELD_ID}"
                        })
                        lead_cache[mid] = lead_data
                    lead = lead_cache[mid]
                    existing = lead.get(QA_FIELD_ID)
                    if not existing or existing == 0:
                        lead_id = mid
                        lead_name = lead.get("display_name", "Unknown")
                        print(f"  ⚠️  Title-only match: {lead_name}", flush=True)

            if not lead_id:
                print(f"  ❌ No match found", flush=True)
                skipped_no_match += 1
                continue

            close_put(f"lead/{lead_id}/", {QA_FIELD_ID: call["score"]})
            if lead_id in lead_cache:
                lead_cache[lead_id][QA_FIELD_ID] = call["score"]
            print(f"  ✅ Updated \"{lead_name}\" → {call['score']}", flush=True)
            updated += 1

        except Exception as e:
            print(f"  ❌ Error: {e}", flush=True)
            errors += 1

    print(f"\n  Done. Updated: {updated} | Already had score: {skipped_has_score} | No match: {skipped_no_match} | Errors: {errors}", flush=True)
    return updated


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=== Attention → Close QA Backfill & Cleanup ===", flush=True)
    print(f"Run time: {datetime.now(PACIFIC).strftime('%Y-%m-%d %H:%M %Z')}", flush=True)

    cleared = clear_zero_scores()
    updated = backfill_missing_scores()

    print("\n" + "="*50, flush=True)
    print("=== All Done ===", flush=True)
    print(f"🧹 Zero scores cleared: {cleared}", flush=True)
    print(f"✅ Missing scores filled: {updated}", flush=True)

if __name__ == "__main__":
    main()
