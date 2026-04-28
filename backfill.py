#!/usr/bin/env python3
"""
Attention → Close QA Score Backfill

Writes all Attention enrichment fields to Close leads that:
1. Had a Vendingpreneurs-type meeting in Close since Jan 22 2026
2. Are missing the Attention Call ID field (new field = never been synced)
3. Have a matching scored call in Attention

Does NOT touch any other leads.
"""

import os
import re
import time
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ─── CONFIG ───────────────────────────────────────────────────────────────────
ATTENTION_API_KEY = os.environ["ATTENTION_API_KEY"]
CLOSE_API_KEY     = os.environ["CLOSE_API_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

INTERNAL_DOMAIN   = "@modern-amenities.com"
PACIFIC           = ZoneInfo("America/Los_Angeles")
BACKFILL_FROM     = "2026-01-22T00:00:00Z"

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
TIER_LABELS = {
    "T1": "T1 - Hot Lead",
    "T2": "T2 - Warm Lead",
    "T3": "T3 - Cool Lead",
    "T4": "T4 - Cold Lead",
}
TIER_TOUCHES      = {"T1": 5, "T2": 4}
TIER_WINDOW_HOURS = {"T1": 2, "T2": 4}

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
    if not doubt_text:
        return "Other"
    prompt = (
        f"Classify the primary sales objection in the following text into EXACTLY one of these four categories: "
        f"Timing, Investment, Fit, Other.\n\n"
        f"Respond with only the single word category, nothing else.\n\n"
        f"Text:\n{doubt_text[:1500]}"
    )
    try:
        result = claude_complete(prompt).strip().capitalize()
        return result if result in ("Timing", "Investment", "Fit", "Other") else "Other"
    except Exception as e:
        print(f"  ⚠️  Objection classification failed: {e}", flush=True)
        return "Other"

def generate_key_concern(doubt_text):
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

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def clean_title(title):
    """Strip recording upload suffixes e.g. ' - 2026_04_23 13_26 PDT - Recording.mp4'"""
    if not title:
        return title
    return re.sub(r'\s*-\s*\d{4}[_\-]\d{2}[_\-]\d{2}[\s_]\d{2}[_\-]\d{2}.*$', '', title).strip()

def is_valid_title(title):
    if not title:
        return False
    lower = clean_title(title).lower()
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

def get_extracted_field(attrs, field_title):
    intelligence = attrs.get("extractedIntelligence") or {}
    for item in intelligence.values():
        if item.get("title", "").strip().lower() == field_title.lower():
            return item.get("value")
    return None

def get_tier(attendance, score):
    if attendance != "Shown":
        return None
    return "T1" if score >= 80 else "T2"

def get_first_touch_deadline(finished_at_str, tier):
    if not finished_at_str or not tier or tier not in TIER_WINDOW_HOURS:
        return None
    try:
        finished = datetime.fromisoformat(finished_at_str.replace("Z", "+00:00"))
        deadline = finished + timedelta(hours=TIER_WINDOW_HOURS[tier])
        return deadline.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None

def titles_match(attention_title, close_title):
    if not attention_title or not close_title:
        return False
    a = clean_title(attention_title).lower()
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
    print("=== Attention → Close Full Backfill ===", flush=True)
    print(f"Run time: {datetime.now(PACIFIC).strftime('%Y-%m-%d %H:%M %Z')}", flush=True)

    # ── STEP 1: Paginate all Close meetings, collect lead_ids with valid titles ──
    print("\n" + "="*50, flush=True)
    print("STEP 1: Finding Close leads with Vendingpreneurs meetings...", flush=True)
    print("="*50, flush=True)

    from_dt = datetime(2026, 1, 22, tzinfo=PACIFIC)
    lead_to_titles = {}
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
            raw = m.get("starts_at") or m.get("activity_at") or m.get("date_start")
            if not raw:
                continue
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(PACIFIC)
            except Exception:
                continue
            if dt < from_dt:
                continue
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

    # ── STEP 2: Check leads — skip any already fully synced (Call ID present) ──
    print("\n" + "="*50, flush=True)
    print(f"STEP 2: Checking {len(lead_to_titles)} leads for missing Attention data...", flush=True)
    print("="*50, flush=True)

    leads_to_fill = []

    for i, (lead_id, meeting_titles) in enumerate(lead_to_titles.items(), 1):
        if i % 25 == 0:
            print(f"  Progress: {i}/{len(lead_to_titles)}", flush=True)

        try:
            data = close_get(f"lead/{lead_id}", params={
                "_fields": f"id,display_name,contacts,{ALL_CUSTOM_FIELDS}"
            })
        except Exception as e:
            if "404" in str(e):
                continue
            raise

        # Skip if Call ID already populated — means this lead was already fully synced
        if data.get(CALL_ID_FIELD_ID):
            continue

        leads_to_fill.append({
            "lead_id": lead_id,
            "lead_name": data.get("display_name", "Unknown"),
            "contacts": data.get("contacts", []),
            "meeting_titles": meeting_titles,
            "existing_qa_score": data.get(QA_FIELD_ID),
        })

    print(f"\n  Leads missing Attention data: {len(leads_to_fill)}", flush=True)
    if not leads_to_fill:
        print("  All leads already fully synced. Nothing to do!", flush=True)
        return

    # ── STEP 3: Fetch all Attention calls (paginated) ──────────────────────────
    print("\n" + "="*50, flush=True)
    print("STEP 3: Fetching all Attention scored calls...", flush=True)
    print("="*50, flush=True)

    attn_headers = {
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
            headers=attn_headers,
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

    # Filter and enrich valid scored calls
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

        labels = attrs.get("labels") or {}
        attendance = labels.get("Attendance", "")
        tier = get_tier(attendance, score)
        call_uuid = attrs.get("uuid") or call.get("id", "")
        finished_at = attrs.get("finishedAt")

        scored_calls.append({
            "title": title,
            "clean_title": clean_title(title),
            "call_id": call_uuid,
            "score": score,
            "prospect_email": prospect_email,
            "call_summary": get_extracted_field(attrs, "call summary"),
            "doubt_text": get_extracted_field(attrs, "doubt"),
            "call_link": f"https://app.attention.tech/conversations/all-calls/{call_uuid}" if call_uuid else None,
            "tier": tier,
            "max_followup": TIER_TOUCHES.get(tier),
            "first_touch_deadline": get_first_touch_deadline(finished_at, tier),
        })

    print(f"\n  Valid scored Attention calls: {len(scored_calls)}", flush=True)

    # ── STEP 4: Match each lead to an Attention call and write all fields ───────
    print("\n" + "="*50, flush=True)
    print(f"STEP 4: Writing data to {len(leads_to_fill)} leads...", flush=True)
    print("="*50, flush=True)

    updated = 0
    no_match = 0
    errors = 0

    for i, lead in enumerate(leads_to_fill, 1):
        lead_id   = lead["lead_id"]
        lead_name = lead["lead_name"]
        mtitles   = lead["meeting_titles"]
        contacts  = lead["contacts"]

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
        print(f"  ✅ Match ({match_type}): score={match['score']} | tier={match['tier']}", flush=True)

        # Run Claude enrichment for this call
        primary_objection = classify_objection(match.get("doubt_text"))
        key_concern = generate_key_concern(match.get("doubt_text"))

        update_payload = {
            QA_FIELD_ID:             match["score"],
            CALL_LINK_FIELD_ID:      match["call_link"],
            ATTENTION_TIER_ID:       TIER_LABELS.get(match["tier"]) if match["tier"] else None,
            MAX_FOLLOWUP_ID:         match["max_followup"],
            FIRST_TOUCH_DEADLINE_ID: match["first_touch_deadline"],
            PRIMARY_OBJECTION_ID:    primary_objection,
            KEY_CONCERN_ID:          key_concern,
            CALL_ID_FIELD_ID:        match["call_id"],
            MEETING_TITLE_FIELD_ID:  match["clean_title"],
        }
        # Strip None values
        update_payload = {k: v for k, v in update_payload.items() if v is not None}

        try:
            close_put(f"lead/{lead_id}/", update_payload)
            print(f"  ✅ Updated \"{lead_name}\" | objection={primary_objection}", flush=True)
            updated += 1

            # Create call summary note only for leads that didn't already have a QA score
            # (leads with existing scores already got their note from the hourly sync)
            if lead["existing_qa_score"] is None and match.get("call_summary"):
                note_body = f"📋 Attention Call Summary\n\n{match['call_summary']}"
                close_post("activity/note/", {"lead_id": lead_id, "note": note_body})
                print(f"  📝 Note created for \"{lead_name}\"", flush=True)

        except Exception as e:
            print(f"  ❌ Error: {e}", flush=True)
            errors += 1

    # ── SUMMARY ────────────────────────────────────────────────────────────────
    print("\n" + "="*50, flush=True)
    print("=== Backfill Complete ===", flush=True)
    print(f"✅ Records updated: {updated}", flush=True)
    print(f"❌ No match found:  {no_match}", flush=True)
    print(f"⚠️  Errors:         {errors}", flush=True)

if __name__ == "__main__":
    main()
