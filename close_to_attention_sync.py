#!/usr/bin/env python3
"""
Close → Attention dialer call sync.

Hourly: pulls Close native dialer calls from the last HOURS_BACK hours
that have a recording_url and duration >= MIN_DURATION, downloads each
MP3, uploads to Attention via the signed-upload + import pattern, and
sets applicationExternalID = call.id for idempotency.

Combined with the existing Attention → Close hourly sync, this closes
the loop:

  Close dialer call
    → import to Attention (this script)
    → Attention runs scorecard + extracted intelligence
    → existing Attention → Close sync picks it up next hour
    → writes QA Score, Tier, Primary Objection, Key Concern, Call
      Summary back to the Close lead

Idempotency: before importing, we check Attention for an existing
conversation with applicationExternalID == call.id (via Attention's
"Get Conversation by external_id" lookup). If one exists, we skip.

Required GitHub secrets:
  CLOSE_API_KEY        Close API key (Basic auth)
  ATTENTION_API_KEY    Attention API key (header value, no "Bearer ")

Optional env vars:
  HOURS_BACK           Window of recent calls to consider (default: 8)
  MIN_DURATION         Skip calls shorter than this in seconds (default: 180)
  DRY_RUN              If "1", log what would happen but don't import
"""

import os
import sys
import base64
import time
import json
import requests
from datetime import datetime, timezone, timedelta

# ===== Config =====
CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
ATTENTION_API_KEY = os.environ["ATTENTION_API_KEY"]
HOURS_BACK = int(os.environ.get("HOURS_BACK", "8"))
MIN_DURATION = int(os.environ.get("MIN_DURATION", "180"))
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

CLOSE_API_BASE = "https://api.close.com/api/v1"
ATTENTION_API_BASE = "https://api.attention.tech/v2"

APPLICATION_NAME = "close"
TITLE_TEMPLATE = "{lead_name} - Vendingpreneurs Close Dialer Call"
FALLBACK_TITLE_TEMPLATE = "Vendingpreneurs Close Dialer Call {call_id}"

CLOSE_REQUEST_DELAY = 0.5  # match existing reverse sync's pacing

# Auth setup
_close_auth_b64 = base64.b64encode(f"{CLOSE_API_KEY}:".encode()).decode()
CLOSE_HEADERS = {"Authorization": f"Basic {_close_auth_b64}"}
ATTENTION_HEADERS = {"Authorization": ATTENTION_API_KEY}


# ===== Helpers =====
def log(msg, indent=0):
    print(f"{'  ' * indent}{msg}", flush=True)


def section(label):
    print(f"\n{'=' * 60}\n{label}\n{'=' * 60}", flush=True)


# ===== Close API =====
def close_get(path, params=None):
    """GET with 429 retry + pacing."""
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


def close_iter_recent_calls(max_pages=200):
    """Iterate Close call activities newest-first. Stops at max_pages safety limit."""
    skip = 0
    pages = 0
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
        for item in items:
            yield item
        if not data.get("has_more"):
            break
        skip += 100
        pages += 1


def close_get_lead_name(lead_id):
    if not lead_id:
        return ""
    resp = close_get(f"/lead/{lead_id}/", params={"_fields": "display_name"})
    if resp.ok:
        return resp.json().get("display_name", "") or ""
    return ""


def close_get_user_email(user_id):
    if not user_id:
        return None
    resp = close_get(f"/user/{user_id}/", params={"_fields": "email,first_name,last_name"})
    if resp.ok:
        return resp.json().get("email")
    return None


# ===== Attention API =====
def attention_get(path, params=None):
    """GET with 5xx retry."""
    url = path if path.startswith("http") else f"{ATTENTION_API_BASE}{path}"
    for attempt in range(3):
        resp = requests.get(url, headers=ATTENTION_HEADERS, params=params)
        if resp.status_code in (502, 503, 504):
            time.sleep(2 ** attempt)
            continue
        return resp
    raise Exception(f"Attention GET {path} exhausted retries")


def attention_post(path, json_data):
    url = path if path.startswith("http") else f"{ATTENTION_API_BASE}{path}"
    headers = {**ATTENTION_HEADERS, "Content-Type": "application/json"}
    for attempt in range(3):
        resp = requests.post(url, headers=headers, json=json_data)
        if resp.status_code in (502, 503, 504):
            time.sleep(2 ** attempt)
            continue
        return resp
    raise Exception(f"Attention POST {path} exhausted retries")


def attention_build_user_map():
    """Return {email_lower: attention_uuid} for all Attention users in our org."""
    resp = attention_get("/users")
    if not resp.ok:
        raise Exception(
            f"Could not list Attention users: {resp.status_code}: {resp.text[:300]}"
        )
    data = resp.json()
    user_map = {}
    for u in data.get("data", []):
        attrs = u.get("attributes", u)
        email = (attrs.get("email") or "").lower().strip()
        uuid = attrs.get("uuid") or u.get("id")
        if email and uuid:
            user_map[email] = uuid
    return user_map


def attention_conversation_exists(external_id):
    """Check if a conversation with applicationExternalID == external_id already exists."""
    resp = attention_get(f"/conversations/{external_id}", params={"by": "external_id"})
    return resp.status_code == 200


def attention_get_signed_upload_url():
    resp = attention_get("/conversations/upload-url")
    if not resp.ok:
        raise Exception(
            f"Could not get signed upload URL: {resp.status_code}: {resp.text[:300]}"
        )
    return resp.json()


def attention_import_conversation(payload):
    resp = attention_post("/conversations/import", payload)
    if not resp.ok:
        raise Exception(f"Import failed: {resp.status_code}: {resp.text[:500]}")
    return resp.json().get("uuid")


# ===== Main sync logic =====
def find_eligible_calls(since_dt):
    """Return Close calls newer than since_dt with a recording and sufficient duration."""
    eligible = []
    inspected = 0
    skipped_short = 0
    skipped_no_recording = 0

    for call in close_iter_recent_calls():
        inspected += 1

        # Parse date and stop once we're past the window
        date_str = call.get("date_created", "")
        try:
            call_dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            continue
        if call_dt < since_dt:
            break

        # Filter
        if not call.get("recording_url"):
            skipped_no_recording += 1
            continue
        if (call.get("duration") or 0) < MIN_DURATION:
            skipped_short += 1
            continue

        eligible.append(call)

    log(f"Inspected {inspected} recent calls in the {HOURS_BACK}h window")
    log(f"  Skipped (no recording):       {skipped_no_recording}")
    log(f"  Skipped (under {MIN_DURATION}s): {skipped_short}")
    log(f"  Eligible:                     {len(eligible)}")
    return eligible


def import_call(call, user_map, user_email_cache):
    """
    Import one Close call to Attention. Returns the new Attention
    conversation UUID, or None if skipped (already imported, no owner
    mapping, etc.).
    """
    call_id = call["id"]
    duration = call.get("duration", "?")

    log(f"\n[{call_id}] duration={duration}s, lead={call.get('lead_id')}, user={call.get('user_id')}")

    # Idempotency
    if attention_conversation_exists(call_id):
        log("→ Already imported in Attention, skip", indent=1)
        return None

    # Resolve owner: Close user_id → Close email → Attention UUID
    close_user_id = call.get("user_id")
    if not close_user_id:
        log("→ No Close user_id on call, skip", indent=1)
        return None

    if close_user_id not in user_email_cache:
        user_email_cache[close_user_id] = close_get_user_email(close_user_id)
    user_email = user_email_cache[close_user_id]

    if not user_email:
        log(f"→ Could not resolve email for Close user {close_user_id}, skip", indent=1)
        return None

    attention_user_uuid = user_map.get(user_email.lower().strip())
    if not attention_user_uuid:
        log(f"→ No Attention user with email {user_email}, skip", indent=1)
        return None

    log(f"Owner: {user_email} → {attention_user_uuid}", indent=1)

    # Build title (must pass the existing reverse sync's is_valid_title filter)
    lead_name = close_get_lead_name(call.get("lead_id"))
    if lead_name:
        title = TITLE_TEMPLATE.format(lead_name=lead_name)
    else:
        title = FALLBACK_TITLE_TEMPLATE.format(call_id=call_id)
    log(f"Title: {title}", indent=1)

    if DRY_RUN:
        log("→ DRY_RUN, not actually importing", indent=1)
        return None

    # Download MP3 from Close
    resp = requests.get(call["recording_url"], headers=CLOSE_HEADERS)
    if not resp.ok:
        raise Exception(f"Recording download failed: {resp.status_code}")
    audio_bytes = resp.content
    content_type = resp.headers.get("Content-Type", "audio/mpeg")
    log(f"Downloaded MP3: {len(audio_bytes):,} bytes ({content_type})", indent=1)

    # Get Attention signed upload URL
    upload_info = attention_get_signed_upload_url()
    upload_url = upload_info["url"]
    download_url = upload_info["downloadUrl"]

    # PUT to signed URL
    put_resp = requests.put(upload_url, data=audio_bytes, headers={"Content-Type": content_type})
    if not put_resp.ok:
        raise Exception(f"Signed upload PUT failed: {put_resp.status_code}: {put_resp.text[:300]}")
    log("Audio uploaded to Attention signed URL", indent=1)

    # POST import
    started_at = call.get("date_created") or datetime.now(timezone.utc).isoformat()
    payload = {
        "mediaURL": download_url,
        "userID": attention_user_uuid,
        "conversationTitle": title,
        "conversationStartedAt": started_at,
        "applicationName": APPLICATION_NAME,
        "applicationExternalID": call_id,
    }

    conversation_uuid = attention_import_conversation(payload)
    log(f"✅ Imported as Attention conversation {conversation_uuid}", indent=1)
    log(f"   https://app.attention.tech/conversations/{conversation_uuid}", indent=1)
    return conversation_uuid


def main():
    section(
        f"Close → Attention sync "
        f"(HOURS_BACK={HOURS_BACK}, MIN_DURATION={MIN_DURATION}s, DRY_RUN={DRY_RUN})"
    )

    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=HOURS_BACK)
    log(f"Looking for Close calls created since {since.isoformat()}")

    # Build email → Attention UUID map (cached for this run)
    section("Building Attention user map")
    user_map = attention_build_user_map()
    log(f"Loaded {len(user_map)} Attention users")
    for email, uuid in sorted(user_map.items()):
        log(f"  {email} → {uuid}", indent=1)

    # Find eligible Close calls
    section("Finding eligible Close calls")
    calls = find_eligible_calls(since)

    # Import each
    section("Importing calls")
    user_email_cache = {}  # Close user_id → email
    stats = {"imported": 0, "skipped": 0, "failed": 0}

    for call in calls:
        try:
            uuid = import_call(call, user_map, user_email_cache)
            if uuid:
                stats["imported"] += 1
            else:
                stats["skipped"] += 1
        except Exception as e:
            stats["failed"] += 1
            log(f"❌ Error importing {call.get('id')}: {e}", indent=1)

    section("Done")
    log(f"Imported: {stats['imported']}")
    log(f"Skipped:  {stats['skipped']}")
    log(f"Failed:   {stats['failed']}")

    # Non-zero exit if anything failed, so the Actions run is marked failed
    sys.exit(0 if stats["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
