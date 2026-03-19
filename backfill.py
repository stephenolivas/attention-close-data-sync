#!/usr/bin/env python3
"""
Attention → Close QA Score Backfill

ONLY writes scores to Close leads that:
1. Had a Vendingpreneurs-type meeting in Close since Jan 22 2026
2. Are currently missing a QA score (null/None only)
3. Have a matching scored call in Attention

Does NOT touch any other leads.
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
    return any(kw in title.lower() for kw in VALID_TITLE_KEYWORDS)

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

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=== Attention → Close QA Score Backfill ===", flush=True)
    print(f"Run time: {datetime.now(PACIFIC).strftime('%Y-%m-%d %H:%M %Z')}", flush=True)

    # ── STEP 1: Paginate all Close meetings, collect lead_ids with valid titles ──
    print("\n" + "="*50, flush=True)
    print("STEP 1: Finding Close leads with Vendingpreneurs meetings...", flush=True)
    print("="*50, flush=True)

    from_dt = datetime(2026, 1, 22, tzinfo=PACIFIC)
    lead_to_titles = {}  # lead_id -> list of meeting titles
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
        print(f"  Page {page}: {len(batch)} meetings fetched", flush=True)

        for m in batch:
            # Filter by date in Python (Close ignores date params)
            raw = m.get("starts_at") or m.get("activity_at") or m.get("date_start")
            if not raw:
                continue
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(PACIFIC)
            except Exception:
                continue
            if dt < from_dt:
                continue

            # Only keep valid sales call types
            title = m.get("title", "")
            if not is_valid_title(title):
                continue

            lead_id = m.get("lead_id")
            if not lead_id:
                continue

            if lead_id not in lead_to_titles:
                lead_to_titles[lead_id] = []
            lead_to_titles[lead_id].append(title)

        if not data.get("has_more"):
            break
        skip += 100

    print(f"\n  Found {len(lead_to_titles)} unique leads with Vendingpreneurs meetings", flush=True)

    if not lead_to_titles:
        print("  No leads found. Exiting.", flush=True)
        return

    # ── STEP 2: Check each of those leads — keep only ones missing a QA score ──
    print("\n" + "="*50, flush=True)
    print(f"STEP 2: Checking {len(lead_to_titles)} leads for missing QA scores...", flush=True)
    print("="*50, flush=True)

    leads_to_fill = []

    for i, (lead_id, meeting_titles) in enumerate(lead_to_titles.items(), 1):
        if i % 25 == 0:
            print(f"  Progress: {i}/{len(lead_to_titles)}", flush=True)

        data = close_get(f"lead/{lead_id}", params={
            "_fields": f"id,display_name,contacts,{QA_FIELD_ID}"
        })

        existing_score = data.get(QA_FIELD_ID)

        # ONLY proceed if score is truly null/None — skip zeros, skip existing scores
        if existing_score is not None:
            continue

        leads_to_fill.append({
            "lead_id": lead_id,
            "lead_name": data.get("display_name", "Unknown"),
            "contacts": data.get("contacts", []),
            "meeting_titles": meeting_titles,
        })

    print(f"\n  Leads with NULL QA score: {len(leads_to_fill)}", flush=True)

    if not leads_to_fill:
        print("  All leads already have scores. Nothing to do!", flush=True)
        return

    # ── STEP 3: Fetch all Attention calls (paginated) ──────────────────────────
    print("\n" + "="*50, flush=True)
    print("STEP 3: Fetching all Attention scored calls...", flush=True)
    print("="*50, flush=True)

    headers = {
        "Authorization": f"Bearer {ATTENTION_API_KEY}",
        "Content-Type": "application/json",
    }

    all_attention = []
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
        all_attention.extend(batch)
        print(f"  Page {page}: {len(batch)} calls (total: {len(all_attention)})", flush=True)
        if len(batch) < 100:
            break
        page += 1
        time.sleep(0.3)

    # Filter to valid scored sales calls only
    scored_calls = []
    for call in all_attention:
        attrs = call.get("attributes", {})
        title = attrs.get("title", "")
        score_results = attrs.get("scorecardResults", [])
        score = score_results[0].get("summary", {}).get("averageScore") if score_results else None
        if not is_valid_title(title) or score is None:
            continue
        prospect_email = get_prospect_email(attrs.get("participants", []))
        if not prospect_email:
            continue
        scored_calls.append({
            "title": title,
            "score": score,
            "prospect_email": prospect_email,
        })

    print(f"\n  Valid scored Attention calls: {len(scored_calls)}", flush=True)

    # ── STEP 4: Match each lead to an Attention call and write score ────────────
    print("\n" + "="*50, flush=True)
    print(f"STEP 4: Writing scores to {len(leads_to_fill)} leads...", flush=True)
    print("="*50, flush=True)

    updated = 0
    no_match = 0
    errors = 0

    for i, lead in enumerate(leads_to_fill, 1):
        lead_id   = lead["lead_id"]
        lead_name = lead["lead_name"]
        mtitles   = lead["meeting_titles"]
        contacts  = lead["contacts"]

        # Collect all emails on this lead
        lead_emails = set()
        for contact in contacts:
            for email_obj in contact.get("emails", []):
                e = email_obj.get("email", "").lower()
                if e:
                    lead_emails.add(e)

        print(f"\n[{i}/{len(leads_to_fill)}] \"{lead_name}\"", flush=True)

        # Primary: title + email match. Fallback: title only
        best_match = None
        fallback_match = None

        for acall in scored_calls:
            for close_title in mtitles:
                if titles_match(acall["title"], close_title):
                    if acall["prospect_email"] in lead_emails:
                        best_match = acall
                        break
                    elif fallback_match is None:
                        fallback_match = acall
            if best_match:
                break

        match = best_match or fallback_match

        if not match:
            print(f"  ❌ No Attention match found", flush=True)
            no_match += 1
            continue

        match_type = "email+title" if best_match else "title-only"
        print(f"  ✅ Match ({match_type}): score={match['score']}", flush=True)

        try:
            close_put(f"lead/{lead_id}/", {QA_FIELD_ID: match["score"]})
            print(f"  ✅ Updated \"{lead_name}\" → {match['score']}", flush=True)
            updated += 1
        except Exception as e:
            print(f"  ❌ Error: {e}", flush=True)
            errors += 1

    # ── SUMMARY ────────────────────────────────────────────────────────────────
    print("\n" + "="*50, flush=True)
    print("=== Backfill Complete ===", flush=True)
    print(f"✅ Scores written: {updated}", flush=True)
    print(f"❌ No match found: {no_match}", flush=True)
    print(f"⚠️  Errors:        {errors}", flush=True)

if __name__ == "__main__":
    main()
