#!/usr/bin/env python3
"""
Diagnostic: Verify Close recording_url returns actual MP3 audio bytes.
Fetches the most recent call with a recording_url, downloads it,
and reports file size, content-type, and magic bytes.
"""

import os
import base64
import requests

CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]

auth_header = "Basic " + base64.b64encode(f"{CLOSE_API_KEY}:".encode()).decode()
session = requests.Session()
session.auth = (CLOSE_API_KEY, "")

print("=== Close Recording Format Check ===\n", flush=True)

# ── Step 1: Find a recent call with a recording_url ───────────────────────────
print("Fetching recent calls to find one with a recording...", flush=True)
resp = session.get(
    "https://api.close.com/api/v1/activity/call/",
    params={"_limit": 50, "_order_by": "-date_created"},
    timeout=30,
)
resp.raise_for_status()
calls = resp.json().get("data", [])

recording_call = next((c for c in calls if c.get("recording_url")), None)

if not recording_call:
    print("❌ No calls with a recording_url found in the last 50 calls.", flush=True)
    exit(1)

recording_url = recording_call["recording_url"]
print(f"✅ Found call with recording:", flush=True)
print(f"   ID:               {recording_call.get('id')}", flush=True)
print(f"   Date:             {recording_call.get('date_created')}", flush=True)
print(f"   Duration:         {recording_call.get('duration')}s", flush=True)
print(f"   recording_url:    {recording_url}", flush=True)
print(f"   recording_expires_at: {recording_call.get('recording_expires_at')}", flush=True)

# ── Step 2: Download the recording ────────────────────────────────────────────
print(f"\nDownloading recording...", flush=True)
dl = requests.get(
    recording_url,
    headers={"Authorization": auth_header},
    allow_redirects=True,
    timeout=60,
)

content_type = dl.headers.get("Content-Type", "unknown")
content_length = len(dl.content)
magic_bytes = dl.content[:16].hex()
first_bytes_raw = dl.content[:4]

# Save to disk for artifact upload
with open("recording.mp3", "wb") as f:
    f.write(dl.content)
print(f"Saved to recording.mp3", flush=True)

print(f"\n=== Results ===", flush=True)
print(f"HTTP Status:      {dl.status_code}", flush=True)
print(f"Content-Type:     {content_type}", flush=True)
print(f"File size:        {content_length:,} bytes ({content_length / 1024:.1f} KB)", flush=True)
print(f"Magic bytes (hex): {magic_bytes}", flush=True)

# ── Step 3: Identify file type from magic bytes ───────────────────────────────
print(f"\n=== File Type Analysis ===", flush=True)

if first_bytes_raw[:3] == b'ID3' or (first_bytes_raw[0] == 0xFF and first_bytes_raw[1] & 0xE0 == 0xE0):
    print(f"✅ RESULT: Valid MP3 audio file detected!", flush=True)
    print(f"   This is importable audio — Ronnie is wrong.", flush=True)
elif first_bytes_raw[:4] == b'RIFF':
    print(f"✅ RESULT: Valid WAV audio file detected!", flush=True)
    print(f"   This is importable audio — Ronnie is wrong.", flush=True)
elif b'<html' in dl.content[:200].lower() or b'<!doctype' in dl.content[:200].lower():
    print(f"❌ RESULT: HTML page returned, not an audio file.", flush=True)
    print(f"   Ronnie is correct — this URL serves a player page, not raw audio.", flush=True)
    print(f"\nFirst 500 chars of response:", flush=True)
    print(dl.content[:500].decode("utf-8", errors="replace"), flush=True)
else:
    print(f"⚠️  RESULT: Unknown file type.", flush=True)
    print(f"   Content-Type header: {content_type}", flush=True)
    print(f"   First 32 bytes (hex): {dl.content[:32].hex()}", flush=True)
    print(f"   First 200 chars: {dl.content[:200]}", flush=True)
