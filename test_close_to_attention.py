#!/usr/bin/env python3
"""
End-to-end validation test: Close dialer call -> Attention.

Pulls one Close call with a recording, uploads it to Attention via the
signed-upload + import API pattern, and prints the resulting Attention
conversation UUID + URL.

After ~30-60 minutes, manually verify in Attention's UI that:
  - Transcript is clean and accurate (not riddled with errors)
  - Speakers are correctly diarized (rep vs prospect)
  - Scorecard ran and produced a reasonable score
  - extractedIntelligence fields populated (Doubt, Trust, Call Summary, etc.)

If all 4 pass -> green light to build the full hourly sync.
If any fail -> audio quality is the silent killer; reconsider before
building further.

Required env vars:
  CLOSE_API_KEY        Close CRM API key (Basic auth)
  ATTENTION_API_KEY    Attention API key (header value)
  ATTENTION_USER_ID    UUID of the Attention user to own the conversation

Optional env vars:
  CLOSE_CALL_ID        Specific Close call ID to test. If omitted, picks
                       the most recent call with a recording_url.
"""

import os
import sys
import base64
import json
import requests
from datetime import datetime, timezone

# ===== Config =====
CLOSE_API_KEY     = os.environ.get("CLOSE_API_KEY")
ATTENTION_API_KEY = os.environ.get("ATTENTION_API_KEY")
ATTENTION_USER_ID = os.environ.get("ATTENTION_USER_ID")
CLOSE_CALL_ID     = os.environ.get("CLOSE_CALL_ID")  # optional

CLOSE_API_BASE    = "https://api.close.com/api/v1"
ATTENTION_API_BASE = "https://api.attention.tech/v2"


# ===== Helpers =====
def die(msg, code=1):
    print(f"\n❌ ERROR: {msg}", file=sys.stderr, flush=True)
    sys.exit(code)

def section(label):
    print(f"\n{'=' * 60}\n{label}\n{'=' * 60}", flush=True)


# ===== Validate config =====
missing = []
if not CLOSE_API_KEY:     missing.append("CLOSE_API_KEY")
if not ATTENTION_API_KEY: missing.append("ATTENTION_API_KEY")
if not ATTENTION_USER_ID: missing.append("ATTENTION_USER_ID")
if missing:
    die(f"Missing required env vars: {', '.join(missing)}")

close_auth_b64   = base64.b64encode(f"{CLOSE_API_KEY}:".encode()).decode()
close_headers    = {"Authorization": f"Basic {close_auth_b64}"}
attention_headers = {"Authorization": ATTENTION_API_KEY}


# ===== Step 1: Find a Close call with a recording =====
section("Step 1: Find Close call with recording")

call = None
if CLOSE_CALL_ID:
    print(f"Using specified call ID: {CLOSE_CALL_ID}", flush=True)
    resp = requests.get(
        f"{CLOSE_API_BASE}/activity/call/{CLOSE_CALL_ID}/", headers=close_headers
    )
    if not resp.ok:
        die(f"Could not fetch call {CLOSE_CALL_ID}: {resp.status_code} - {resp.text[:300]}")
    call = resp.json()
    if not call.get("recording_url"):
        die(f"Call {CLOSE_CALL_ID} has no recording_url")
else:
    print("No CLOSE_CALL_ID specified - finding most recent call with recording...", flush=True)
    skip = 0
    while skip < 300:
        resp = requests.get(
            f"{CLOSE_API_BASE}/activity/call/",
            headers=close_headers,
            params={"_skip": skip, "_limit": 50},
        )
        if not resp.ok:
            die(f"Could not list calls: {resp.status_code} - {resp.text[:300]}")
        data = resp.json()
        for c in data.get("data", []):
            if c.get("recording_url"):
                call = c
                break
        if call:
            break
        if not data.get("has_more"):
            break
        skip += 50
    if not call:
        die("No Close calls with recording_url found in last 300 calls")

print(f"  Call ID:       {call['id']}", flush=True)
print(f"  Date:          {call.get('date_created')}", flush=True)
print(f"  Duration:      {call.get('duration')}s", flush=True)
print(f"  Recording URL: {call['recording_url']}", flush=True)


# ===== Step 2: Download MP3 from Close =====
section("Step 2: Download MP3 from Close")

resp = requests.get(call["recording_url"], headers=close_headers)
if not resp.ok:
    die(f"Could not download recording: {resp.status_code} - {resp.text[:300]}")
audio_bytes  = resp.content
content_type = resp.headers.get("Content-Type", "")

print(f"  Status:        {resp.status_code}", flush=True)
print(f"  Content-Type:  {content_type}", flush=True)
print(f"  Size:          {len(audio_bytes):,} bytes ({len(audio_bytes) / 1024:.1f} KB)", flush=True)
print(f"  Magic bytes:   {audio_bytes[:4].hex()}", flush=True)

if not content_type.startswith("audio/"):
    die(f"Expected audio/* Content-Type, got: {content_type}")


# ===== Step 3: Get Attention signed upload URL =====
section("Step 3: Get Attention signed upload URL")

resp = requests.get(f"{ATTENTION_API_BASE}/conversations/upload-url", headers=attention_headers)
if not resp.ok:
    die(f"Could not get signed upload URL: {resp.status_code} - {resp.text[:500]}")
upload_info  = resp.json()
upload_url   = upload_info["url"]
download_url = upload_info["downloadUrl"]
upload_key   = upload_info.get("key")

print(f"  Status:        {resp.status_code}", flush=True)
print(f"  Upload URL:    {upload_url[:80]}...", flush=True)
print(f"  Download URL:  {download_url[:80]}...", flush=True)
print(f"  Key:           {upload_key}", flush=True)


# ===== Step 4: PUT audio bytes to signed URL =====
section("Step 4: Upload MP3 to Attention signed URL")

put_resp = requests.put(upload_url, data=audio_bytes, headers={"Content-Type": content_type})
if not put_resp.ok:
    die(f"Upload to signed URL failed: {put_resp.status_code} - {put_resp.text[:500]}")
print(f"  Status:    {put_resp.status_code}", flush=True)
print(f"  ✅ Audio uploaded successfully", flush=True)


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
        print(f"  (Could not fetch lead name: {e})", flush=True)


# ===== Step 6: POST Import Conversation =====
section("Step 6: Import conversation to Attention")

started_at = call.get("date_created") or datetime.now(timezone.utc).isoformat()
title = (
    f"{lead_name} - Close Dialer Call".strip(" -")
    if lead_name
    else f"Close Dialer Call {call['id']}"
)

import_payload = {
    "mediaURL":               download_url,
    "userID":                 ATTENTION_USER_ID,
    "conversationTitle":      title,
    "conversationStartedAt":  started_at,
    "applicationName":        "close",
    "applicationExternalID":  call["id"],
}

print(f"  Title:         {title}", flush=True)
print(f"  Started at:    {started_at}", flush=True)
print(f"  Payload: {json.dumps(import_payload, indent=2)}", flush=True)

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
section("✅ Import Successful")
print(f"  Attention conversation UUID: {conversation_uuid}", flush=True)
print(f"\n  Open in Attention UI (after ~30-60 min processing):", flush=True)
print(f"    https://app.attention.tech/c/{conversation_uuid}", flush=True)
print(f"\n  === MANUAL VERIFICATION CHECKLIST ===", flush=True)
print(f"  [ ] Transcript is clean and accurate", flush=True)
print(f"  [ ] Speakers correctly diarized (rep vs prospect)", flush=True)
print(f"  [ ] Scorecard ran and produced a reasonable score", flush=True)
print(f"  [ ] extractedIntelligence fields populated (Doubt, Trust, Call Summary, etc.)", flush=True)
print(f"\n  All 4 pass -> green light for full sync build.", flush=True)
print(f"  Any fail   -> audio quality issue; reconsider before building.", flush=True)
