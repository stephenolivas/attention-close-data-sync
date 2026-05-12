#!/usr/bin/env python3
"""
Diagnostic: Fetch a specific Close call activity and display all fields,
redacting PII. Specifically surfaces recording_url and any other URL-shaped fields.
"""

import os
import re
import json
import requests

CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
CALL_ID = "acti_FOemY0qUcOhyfUGgdkAw4YmnmQu2BLIzUNhRh3vYqVr"

session = requests.Session()
session.auth = (CLOSE_API_KEY, "")

print(f"=== Close Call Activity: {CALL_ID} ===\n", flush=True)

resp = session.get(
    f"https://api.close.com/api/v1/activity/call/{CALL_ID}/",
    timeout=30,
)
resp.raise_for_status()
call = resp.json()

# ── Redact PII ────────────────────────────────────────────────────────────────
PII_FIELDS = ["phone", "local_phone", "remote_phone", "local_phone_formatted",
              "remote_phone_formatted", "note", "transcript", "user_name",
              "updated_by_name", "transferred_from"]

def redact(obj):
    if isinstance(obj, dict):
        return {
            k: "[REDACTED]" if k in PII_FIELDS else redact(v)
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [redact(i) for i in obj]
    return obj

redacted = redact(call)

# ── Surface URL-shaped fields specifically ────────────────────────────────────
print("=== URL-shaped fields ===", flush=True)
for k, v in call.items():
    if isinstance(v, str) and ("http" in v or "url" in k.lower()):
        print(f"  {k}: {v}", flush=True)

print("\n=== Key fields ===", flush=True)
key_fields = ["id", "source", "direction", "duration", "recording_url",
              "voicemail_url", "recording_duration", "recording_expires_at",
              "dialer_id", "parent_meeting_id"]
for f in key_fields:
    print(f"  {f}: {call.get(f)}", flush=True)

print("\n=== Full redacted response ===", flush=True)
print(json.dumps(redacted, indent=2), flush=True)
