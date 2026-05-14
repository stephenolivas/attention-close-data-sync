#!/usr/bin/env python3
"""
End-to-end validation test: Close dialer call -> Attention.

Pulls one Close call with a recording (filtered by minimum duration to
avoid short voicemails/hangups that Attention discards), uploads it to
Attention via the signed-upload + import API pattern, and prints the
resulting Attention conversation UUID + URL.

After ~30-60 minutes, run check_attention_conversation.py with the UUID
to verify processing succeeded.

Required env vars:
  CLOSE_API_KEY        Close CRM API key (Basic auth)
  ATTENTION_API_KEY    Attention API key (header value)
  ATTENTION_USER_ID    UUID of the Attention user to own the conversation
                       (find via: curl -H "Authorization: $ATTENTION_API_KEY" \
                                  https://api.attention.tech/v2/users)

Optional env vars:
  CLOSE_CALL_ID        Specific Close call ID to test. If omitted, picks
                       the most recent call with a recording_url that is
                       at least MIN_DURATION seconds long.
  MIN_DURATION         Minimum call duration in seconds when auto-picking
                       (default: 180). Calls shorter than this are skipped
                       because Attention discards short imports.
"""

import os
import sys
import base64
import json
import requests
from datetime import datetime, timezone

# ===== Config =====
CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY")
ATTENTION_API_KEY = os.environ.get("ATTENTION_API_KEY")
ATTENTION_USER_ID = os.environ.get("ATTENTION_USER_ID")
CLOSE_CALL_ID = os.environ.get("CLOSE_CALL_ID")  # optional
MIN_DURATION = int(os.environ.get("MIN_DURATION", "180"))  # 3 min default

CLOSE_API_BASE = "https://api.close.com/api/v1"
ATTENTION_API_BASE = "https://api.attention.tech/v2"


# ===== Helpers =====
def die(msg, code=1):
    print(f"\n❌ ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def section(label):
    print(f"\n{'=' * 60}\n{label}\n{'=' * 60}")


# ===== Validate config =====
missing = []
if not CLOSE_API_KEY:
    missing.append("CLOSE_API_KEY")
if not ATTENTION_API_KEY:
    missing.append("ATTENTION_API_KEY")
if not ATTENTION_USER_ID:
    missing.append("ATTENTION_USER_ID")
if missing:
    die(f"Missing required env vars: {', '.join(missing)}")

close_auth_b64 = base64.b64encode(f"{CLOSE_API_KEY}:".encode()).decode()
close_headers = {"Authorization": f"Basic {close_auth_b64}"}
attention_headers = {"Authorization": ATTENTION_API_KEY}


# ===== Step 1: Find a Close call with a recording =====
section("Step 1: Find Close call with recording")
print(f"  Minimum duration filter: {MIN_DURATION}s")

call = None
skipped_short = 0

if CLOSE_CALL_ID:
    print(f"Using specified call ID: {CLOSE_CALL_ID}")
    resp = requests.get(
        f"{CLOSE_API_BASE}/activity/call/{CLOSE_CALL_ID}/", headers=close_headers
    )
    if not resp.ok:
        die(f"Could not fetch call {CLOSE_CALL_ID}: {resp.status_code} - {resp.text[:300]}")
    call = resp.json()
    if not call.get("recording_url"):
        die(f"Call {CLOSE_CALL_ID} has no recording_url")
    if (call.get("duration") or 0) < MIN_DURATION:
        print(f"  ⚠️  Warning: specified call duration is {call.get('duration')}s")
        print(f"     (below MIN_DURATION={MIN_DURATION}). Attention may discard it.")
else:
    print(f"No CLOSE_CALL_ID specified - finding most recent call with recording and duration >= {MIN_DURATION}s...")
    skip = 0
    while skip < 500:
        resp = requests.get(
            f"{CLOSE_API_BASE}/activity/call/",
            headers=close_headers,
            params={"_skip": skip, "_limit": 50},
        )
        if not resp.ok:
            die(f"Could not list calls: {resp.status_code} - {resp.text[:300]}")
        data = resp.json()
        for c in data.get("data", []):
            if not c.get("recording_url"):
                continue
            if (c.get("duration") or 0) < MIN_DURATION:
                skipped_short += 1
                continue
            call = c
            break
        if call:
            break
        if not data.get("has_more"):
            break
        skip += 50
    if not call:
        die(
            f"No Close calls with recording_url and duration >= {MIN_DURATION}s "
            f"found in last 500 calls (skipped {skipped_short} short calls). "
            f"Try lowering MIN_DURATION or set CLOSE_CALL_ID explicitly."
        )

print()
print(f"  Call ID:           {call['id']}")
print(f"  Date:              {call.get('date_created')}")
print(f"  Duration:          {call.get('duration')}s")
print(f"  Lead ID:           {call.get('lead_id')}")
print(f"  Close User ID:     {call.get('user_id')}")
print(f"  Recording URL:     {call['recording_url']}")
if skipped_short:
    print(f"  (Skipped {skipped_short} calls under {MIN_DURATION}s)")


# ===== Step 2: Download MP3 from Close =====
section("Step 2: Download MP3 from Close")

resp = requests.get(call["recording_url"], headers=close_headers)
if not resp.ok:
    die(f"Could not download recording: {resp.status_code} - {resp.text[:300]}")
audio_bytes = resp.content
content_type = resp.headers.get("Content-Type", "")

print(f"  Status:        {resp.status_code}")
print(f"  Content-Type:  {content_type}")
print(f"  Size:          {len(audio_bytes):,} bytes ({len(audio_bytes) / 1024:.1f} KB)")
print(f"  Magic bytes:   {audio_bytes[:4].hex()}")

if not content_type.startswith("audio/"):
    die(f"Expected audio/* Content-Type, got: {content_type}")


# ===== Step 3: Get Attention signed upload URL =====
section("Step 3: Get Attention signed upload URL")

resp = requests.get(f"{ATTENTION_API_BASE}/conversations/upload-url", headers=attention_headers)
if not resp.ok:
    die(f"Could not get signed upload URL: {resp.status_code} - {resp.text[:500]}")
upload_info = resp.json()
upload_url = upload_info["url"]
download_url = upload_info["downloadUrl"]
upload_key = upload_info.get("key")

print(f"  Status:        {resp.status_code}")
print(f"  Upload URL:    {upload_url[:80]}...")
print(f"  Download URL:  {download_url[:80]}...")
print(f"  Key:           {upload_key}")


# ===== Step 4: PUT audio to signed URL =====
section("Step 4: Upload MP3 to Attention's signed URL")

put_resp = requests.put(upload_url, data=audio_bytes, headers={"Content-Type": content_type})
if not put_resp.ok:
    die(f"Upload to signed URL failed: {put_resp.status_code} - {put_resp.text[:500]}")
print(f"  Status:    {put_resp.status_code}")
print(f"  ✅ Audio uploaded successfully")


# ===== Step 5: Pull lead name for title (best-effort) =====
lead_name = ""
if call.get("lead_id"):
    try:
        lead_resp = requests.get(
            f"{CLOSE_API_BASE}/lead/{call['lead_id']}/",
            headers=close_headers,
            params={"_fields": "display_name"},
        )
        if lead_resp.ok:
            lead_name = lead_resp.json().get("display_name", "") or ""
    except Exception as e:
        print(f"  (Could not fetch lead name: {e})")


# ===== Step 6: POST Import Conversation =====
section("Step 5: Import conversation to Attention")

started_at = call.get("date_created") or datetime.now(timezone.utc).isoformat()
title = (
    f"{lead_name} - Close Dialer Call".strip(" -")
    if lead_name
    else f"Close Dialer Call {call['id']}"
)

import_payload = {
    "mediaURL": download_url,
    "userID": ATTENTION_USER_ID,
    "conversationTitle": title,
    "conversationStartedAt": started_at,
    "applicationName": "close",
    "applicationExternalID": call["id"],
}

print(f"  Title:           {title}")
print(f"  Started at:      {started_at}")
print(f"  External ID:     {call['id']}")
print(f"  Application:     close")
print(f"  Owner user ID:   {ATTENTION_USER_ID}")
print(f"  Call duration:   {call.get('duration')}s")
print()
print(f"  Payload: {json.dumps(import_payload, indent=2)}")

import_resp = requests.post(
    f"{ATTENTION_API_BASE}/conversations/import",
    headers={**attention_headers, "Content-Type": "application/json"},
    json=import_payload,
)
if not import_resp.ok:
    die(f"Import failed: {import_resp.status_code} - {import_resp.text[:1000]}")

result = import_resp.json()
conversation_uuid = result.get("uuid")


# ===== Done =====
section("✅ Import Submitted")
print(f"  Attention conversation UUID:")
print(f"    {conversation_uuid}")
print()
print(f"  Open in Attention UI (after processing):")
print(f"    https://app.attention.tech/conversations/{conversation_uuid}")
print()
print(f"  === NEXT STEP ===")
print(f"  Wait 30-60 minutes, then run the check_attention_conversation workflow")
print(f"  with this UUID:")
print(f"    {conversation_uuid}")
print()
print(f"  That will report importStatus, transcriptStatus, and content quality.")
print(f"  If importStatus = DISCARDED, the call is still being filtered out -")
print(f"  try increasing MIN_DURATION further or selecting a known-good call.")
