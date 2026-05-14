#!/usr/bin/env python3
"""
Diagnostic check: fetch an Attention conversation by UUID and report
its processing status across all stages (import, media storage, video,
transcript, scorecard, extracted intelligence).

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


def status_icon(value, good_values=("completed", "ready", "exported", "success")):
    if not value:
        return "⚠️ "
    v = str(value).lower()
    if v in good_values:
        return "✅"
    if v in ("failed", "error"):
        return "❌"
    if v in ("pending", "processing", "in_progress"):
        return "⏳"
    return "ℹ️ "


# ===== Validate config =====
missing = []
if not ATTENTION_API_KEY:
    missing.append("ATTENTION_API_KEY")
if not ATTENTION_CONVERSATION_UUID:
    missing.append("ATTENTION_CONVERSATION_UUID")
if missing:
    die(f"Missing required env vars: {', '.join(missing)}")


# ===== Fetch =====
section(f"Fetching conversation {ATTENTION_CONVERSATION_UUID}")

headers = {"Authorization": ATTENTION_API_KEY}
url = f"{ATTENTION_API_BASE}/conversations/{ATTENTION_CONVERSATION_UUID}"
resp = requests.get(url, headers=headers)
print(f"  URL:         {url}")
print(f"  HTTP Status: {resp.status_code}")


# ===== Handle 404 =====
if resp.status_code == 404:
    section("❌ Conversation NOT FOUND")
    print("  The UUID does not exist in Attention.")
    print("  Re-run the import test and inspect the full /conversations/import response.")
    sys.exit(2)


# ===== Other errors =====
if not resp.ok:
    print(f"  Body: {resp.text[:1500]}")
    die(f"Unexpected status {resp.status_code}")


# ===== Parse =====
body = resp.json()
# Schema (per Attention docs): { type, id, attributes: {...}, links: {...} }
attrs = body.get("attributes", {}) if isinstance(body, dict) else {}


# ===== Identity =====
section("✅ Conversation Found")
print(f"  UUID:            {attrs.get('uuid', 'N/A')}")
print(f"  Title:           {attrs.get('title', 'N/A')}")
print(f"  Finished At:     {attrs.get('finishedAt', 'N/A')}")
print(f"  Media Duration:  {attrs.get('mediaDuration', 'N/A')}s")
print(f"  User UUID:       {attrs.get('userUUID', 'N/A')}")
print(f"  Team UUID:       {attrs.get('teamUUID', 'N/A')}")
print(f"  Is Empty:        {attrs.get('isEmpty', 'N/A')}")
print(f"  Archived:        {attrs.get('archived', 'N/A')}")


# ===== Pipeline Status =====
section("Processing Status (pipeline stages)")

status_fields = [
    ("Import",          "importStatus"),
    ("Media Storage",   "mediaStorageStatus"),
    ("Video",           "videoStatus"),
    ("Transcript",      "transcriptStatus"),
    ("CRM Export",      "crmExportStatus"),
]

for label, field in status_fields:
    value = attrs.get(field)
    icon = status_icon(value)
    print(f"  {icon} {label:<15} {value or 'N/A'}")


# ===== Content =====
section("Content")

transcript = attrs.get("transcript") or {}
participants = attrs.get("participants") or []
scorecards = attrs.get("scorecardResults") or []
ei = attrs.get("extractedIntelligence") or {}
labels = attrs.get("labels") or {}

transcript_segments = 0
distinct_speakers = set()
if isinstance(transcript, dict):
    v1 = transcript.get("v1", {})
    final = v1.get("final", []) if isinstance(v1, dict) else []
    transcript_segments = len(final)
    for seg in final:
        if isinstance(seg, dict) and seg.get("speaker"):
            distinct_speakers.add(seg["speaker"])

print(f"  Transcript segments:       {transcript_segments}")
print(f"  Distinct speakers:         {len(distinct_speakers)}")
print(f"  Participants:              {len(participants)}")
print(f"  Scorecard results:         {len(scorecards)}")
print(f"  Extracted intelligence:    {len(ei)} field(s)")
print(f"  Labels:                    {len(labels)} key(s)")


# ===== Participants =====
if participants:
    section("Participants")
    for p in participants[:10]:
        if isinstance(p, dict):
            name = p.get("name", "?")
            email = p.get("email", "?")
            organizer = "(organizer)" if p.get("organizer") else ""
            print(f"  - {name} <{email}> {organizer}")


# ===== Transcript preview =====
if transcript_segments:
    section("Transcript Preview (first 5 segments)")
    for seg in transcript.get("v1", {}).get("final", [])[:5]:
        speaker = seg.get("speaker", "?")
        text = seg.get("sentence", "")
        start = seg.get("startTimestamp", 0)
        print(f"  [{start:6.2f}s] {speaker}: {text}")


# ===== Scorecards =====
if scorecards:
    section("Scorecard Results")
    for sc in scorecards:
        title = sc.get("title", "?")
        summary = sc.get("summary") or {}
        avg = summary.get("averageScore", "N/A")
        summary_text = summary.get("summaryText", "")
        print(f"  - {title}")
        print(f"    Average score: {avg}")
        if summary_text:
            print(f"    Summary: {summary_text[:200]}")


# ===== Extracted intelligence =====
if ei:
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


# ===== Raw response =====
section("Raw Response (truncated to 5000 chars)")
print(json.dumps(body, indent=2)[:5000])


# ===== Verdict =====
section("Verdict")

import_ok            = str(attrs.get("importStatus", "")).lower() == "completed"
transcript_ok        = str(attrs.get("transcriptStatus", "")).lower() == "completed"
media_ok             = str(attrs.get("mediaStorageStatus", "")).lower() == "ready"
has_transcript       = transcript_segments > 0
has_diarization      = len(distinct_speakers) >= 2
has_scorecard        = len(scorecards) > 0
has_intelligence     = len(ei) > 0

checks = [
    ("Import status = completed",           import_ok),
    ("Media storage status = ready",        media_ok),
    ("Transcript status = completed",       transcript_ok),
    ("Transcript has content",              has_transcript),
    ("Diarization (2+ distinct speakers)",  has_diarization),
    ("Scorecard ran",                       has_scorecard),
    ("Extracted intelligence populated",    has_intelligence),
]

for label, ok in checks:
    icon = "✅" if ok else "❌"
    print(f"  {icon} {label}")

all_ok = all(ok for _, ok in checks)
print()
if all_ok:
    print("  ✅ ALL CHECKS PASSED")
    print("  Close audio produces useful output through Attention's pipeline.")
    print("  Green light to build the full Close -> Attention hourly sync.")
    sys.exit(0)
else:
    print("  ⚠️  SOME CHECKS FAILED")
    print()
    print("  Diagnostic guide based on which checks failed:")
    if not import_ok:
        print("    - importStatus not completed -> Attention couldn't ingest the audio.")
        print("      Most likely: signed-upload PUT didn't land, or downloadUrl wasn't")
        print("      accessible when Attention's pipeline tried to fetch from it.")
    if not media_ok:
        print("    - mediaStorageStatus not ready -> Audio file storage problem on")
        print("      Attention's side. Possibly format/encoding rejection.")
    if not transcript_ok or not has_transcript:
        print("    - Transcript missing/incomplete -> Speech-to-text didn't run or")
        print("      produced empty output. Could be audio quality, format, or")
        print("      bitrate (Close calls are ~30 kbps mono).")
    if has_transcript and not has_diarization:
        print("    - No diarization -> Mono audio without speaker separation. Common")
        print("      for telephony recordings. May need different transcription")
        print("      settings (e.g. transcriptionSettings.gladia.enableDiarization).")
    if not has_scorecard:
        print("    - Scorecard didn't run -> May need scorecard config that targets")
        print("      imported conversations specifically, or the scorecard depends")
        print("      on transcript data which isn't available.")
    if not has_intelligence:
        print("    - Extracted intelligence empty -> Pipeline didn't run, or the")
        print("      configured intelligence fields don't apply to imported calls.")
    sys.exit(1)
