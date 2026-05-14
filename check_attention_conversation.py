#!/usr/bin/env python3
"""
Diagnostic check: fetch an Attention conversation by UUID and report
whether it exists, whether it processed successfully, and which of
the four validation criteria passed.

Run after importing a conversation via test_close_to_attention.py
to verify Attention actually processed it correctly.

Required env vars:
  ATTENTION_API_KEY            Attention API key
  ATTENTION_CONVERSATION_UUID  UUID of the conversation to check
"""

import os
import sys
import json
import requests

ATTENTION_API_KEY = os.environ.get("ATTENTION_API_KEY")
ATTENTION_CONVERSATION_UUID = os.environ.get("ATTENTION_CONVERSATION_UUID")
ATTENTION_API_BASE = "https://api.attention.tech/v2"


def die(msg, code=1):
    print(f"\n❌ ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def section(label):
    print(f"\n{'=' * 60}\n{label}\n{'=' * 60}")


# ===== Validate config =====
missing = []
if not ATTENTION_API_KEY:
    missing.append("ATTENTION_API_KEY")
if not ATTENTION_CONVERSATION_UUID:
    missing.append("ATTENTION_CONVERSATION_UUID")
if missing:
    die(f"Missing required env vars: {', '.join(missing)}")


# ===== Fetch conversation =====
section(f"Fetching conversation {ATTENTION_CONVERSATION_UUID}")

headers = {"Authorization": ATTENTION_API_KEY}
url = f"{ATTENTION_API_BASE}/conversations/{ATTENTION_CONVERSATION_UUID}"
resp = requests.get(url, headers=headers)

print(f"  URL:         {url}")
print(f"  HTTP Status: {resp.status_code}")


# ===== Handle 404: conversation never existed =====
if resp.status_code == 404:
    section("❌ Conversation NOT FOUND")
    print("  The UUID does not exist in Attention.")
    print()
    print("  This means the import call returned a UUID but the conversation")
    print("  was never actually created. Possible causes:")
    print("    - The PUT to the signed upload URL didn't land")
    print("    - Attention rejected the conversation downstream after returning")
    print("      a UUID (validation, format, metadata, etc.)")
    print("    - The audio at downloadUrl was inaccessible to Attention's pipeline")
    print()
    print("  Next steps:")
    print("    - Re-run the import test with verbose response logging")
    print("    - Check the full body of the /conversations/import response")
    print("    - Try fetching the signed downloadUrl directly to confirm the")
    print("      audio actually persisted there")
    sys.exit(2)


# ===== Handle other errors =====
if not resp.ok:
    print(f"  Body: {resp.text[:1500]}")
    die(f"Unexpected status {resp.status_code}")


# ===== Parse response =====
body = resp.json()

# Attention follows JSON:API: { data: { type, id, attributes: {...} } }
if isinstance(body, dict) and "data" in body and isinstance(body["data"], dict):
    conv = body["data"]
    attrs = conv.get("attributes", {})
else:
    # Defensive fallback
    attrs = body if isinstance(body, dict) else {}


# ===== Top-level fields =====
section("✅ Conversation Found")
print(f"  UUID:            {attrs.get('uuid', 'N/A')}")
print(f"  Title:           {attrs.get('title', 'N/A')}")
print(f"  Started At:      {attrs.get('startedAt') or attrs.get('finishedAt', 'N/A')}")
print(f"  Application:     {attrs.get('applicationName', 'N/A')}")
print(f"  External ID:     {attrs.get('applicationExternalID', 'N/A')}")
print(f"  Analysis Status: {attrs.get('analysisStatus') or attrs.get('status', 'N/A')}")

# Check participants
participants = attrs.get("participants") or []
if participants:
    print(f"  Participants:    {len(participants)}")
    for p in participants[:5]:
        if isinstance(p, dict):
            email = p.get("email", "?")
            name = p.get("name", "?")
            access = p.get("accessType", "?")
            print(f"    - {name} <{email}> ({access})")


# ===== Validation checks =====
section("Validation Criteria")

checks = {}

# 1. Transcript
transcript = attrs.get("transcript")
transcript_ok = False
transcript_segments = 0
if transcript:
    if isinstance(transcript, dict):
        v1 = transcript.get("v1", {})
        final = v1.get("final", []) if isinstance(v1, dict) else []
        transcript_segments = len(final)
        transcript_ok = transcript_segments > 0
checks["Transcript present and non-empty"] = transcript_ok

# 2. Speaker diarization (multiple distinct speakers in transcript)
diarization_ok = False
if transcript_ok:
    speakers = set()
    v1 = transcript.get("v1", {})
    for seg in v1.get("final", []):
        if seg.get("speaker"):
            speakers.add(seg["speaker"])
    diarization_ok = len(speakers) >= 2
    print(f"  Distinct speakers detected: {len(speakers)}")
checks["Diarization (2+ distinct speakers)"] = diarization_ok

# 3. Scorecard ran
scorecards = attrs.get("scorecardResults") or []
scorecard_ok = len(scorecards) > 0
checks["Scorecard ran"] = scorecard_ok

# 4. Extracted intelligence populated
ei = attrs.get("extractedIntelligence") or {}
ei_ok = len(ei) > 0
checks["Extracted intelligence populated"] = ei_ok

# Print checklist
print()
all_passed = True
for label, passed in checks.items():
    icon = "✅" if passed else "❌"
    print(f"  {icon} {label}")
    if not passed:
        all_passed = False


# ===== Detail dumps =====
if transcript_ok:
    section("Transcript Preview (first 5 segments)")
    v1 = transcript.get("v1", {})
    for seg in v1.get("final", [])[:5]:
        speaker = seg.get("speaker", "?")
        text = seg.get("sentence", "")
        start = seg.get("startTimestamp", 0)
        print(f"  [{start:6.2f}s] {speaker}: {text}")
    print(f"  ... ({transcript_segments} total segments)")

if scorecard_ok:
    section("Scorecard Results")
    for sc in scorecards:
        title = sc.get("scorecardTitle") or sc.get("title", "?")
        avg = sc.get("averageScore", "?")
        print(f"  - {title}: average score = {avg}")

if ei_ok:
    section("Extracted Intelligence")
    for key, val in ei.items():
        if isinstance(val, dict):
            title = val.get("title", key)
            value = val.get("value", "")
        else:
            title = key
            value = val
        value_str = str(value).replace("\n", " ")[:200]
        if len(str(value)) > 200:
            value_str += "..."
        print(f"  - {title}: {value_str}")


# ===== Final verdict =====
section("Verdict")
if all_passed:
    print("  ✅ ALL CHECKS PASSED")
    print()
    print("  Close audio is producing useful output through Attention's pipeline.")
    print("  Green light to build the full Close -> Attention hourly sync.")
    sys.exit(0)
else:
    print("  ⚠️  SOME CHECKS FAILED")
    print()
    print("  Review the failing items above. Common causes:")
    print("    - No transcript: audio failed to process or was unreadable")
    print("    - No diarization: mono audio without speaker separation, or all")
    print("      speech assigned to one channel")
    print("    - No scorecard: scorecard config may need to apply to imported calls")
    print("    - No extracted intelligence: pipeline didn't run, or specific fields")
    print("      not configured for this conversation type")
    print()
    print("  If transcript quality is the issue, the project is moot regardless")
    print("  of who builds the integration.")
    sys.exit(1)
