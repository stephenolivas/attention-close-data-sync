#!/usr/bin/env python3
"""
Diagnostic: Check Close CRM call recording URL availability
Fetches the 3 most recent call activities and prints the full raw response
so we can see if recording_url is present and accessible.
"""

import os
import json
import requests

CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]

session = requests.Session()
session.auth = (CLOSE_API_KEY, "")

print("=== Close CRM Call Recording URL Check ===\n", flush=True)

resp = session.get(
    "https://api.close.com/api/v1/activity/call/",
    params={
        "_limit": 3,
        "_order_by": "-date_created",
    },
    timeout=30,
)
resp.raise_for_status()
calls = resp.json().get("data", [])

print(f"Found {len(calls)} recent calls\n", flush=True)

for i, call in enumerate(calls, 1):
    print(f"--- Call {i} ---", flush=True)
    print(f"ID:            {call.get('id')}", flush=True)
    print(f"Date:          {call.get('date_created')}", flush=True)
    print(f"Duration:      {call.get('duration')}s", flush=True)
    print(f"Direction:     {call.get('direction')}", flush=True)
    print(f"recording_url: {call.get('recording_url')}", flush=True)
    print(f"voicemail_url: {call.get('voicemail_url')}", flush=True)
    print(f"\nFull raw response:", flush=True)
    print(json.dumps(call, indent=2), flush=True)
    print(flush=True)
