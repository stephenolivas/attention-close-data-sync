#!/usr/bin/env python3
"""
Diagnostic: dump the structure of a single Attention conversation.

Use this to inspect what any Attention conversation's full API response
looks like — especially useful for understanding what NON-STANDARD
scorecards (e.g. the "ScoreCard - VP Setters Script") emit under the
hood, so we can adapt the sync scripts to extract useful narrative
content (Call Summary equivalents) from them.

The script prints a structured summary to stdout AND writes the full raw
JSON to `attention_diagnostic_<uuid>.json` in the current directory, so
you can share the file or diff it against the standard scorecard's shape.

Usage:
    export ATTENTION_API_KEY="..."
    python3 diagnose_attention_conversation.py <conversation_uuid>

Example:
    python3 diagnose_attention_conversation.py d6bd25ee-7935-4bee-a834-9e2b0cfc6807

The script tries both auth styles (`Authorization: Bearer <key>` and
`Authorization: <key>`) since Attention's API is inconsistent between
list and get-by-id endpoints.
"""

import os
import sys
import json
import requests

ATTENTION_API_BASE = "https://api.attention.tech/v2"

# Top-level attributes that MIGHT contain narrative summary content on
# non-standard scorecards. We check for these explicitly at the end.
NARRATIVE_CANDIDATE_KEYS = (
    "coaching",
    "coachingResults",
    "coachingFeedback",
    "coachingInsights",
    "insights",
    "summary",
    "callSummary",
    "notes",
    "highlights",
    "keyMoments",
    "keyPoints",
    "actionItems",
    "feedback",
    "observations",
)


def die(msg, code=1):
    print(msg, file=sys.stderr)
    sys.exit(code)


def summarize_value(v, max_len=200):
    """Compact one-line representation of a value for the structured summary."""
    if v is None:
        return "None"
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        if len(v) > max_len:
            return f"{v[:max_len]!r}... ({len(v)} chars total)"
        return repr(v)
    if isinstance(v, list):
        if not v:
            return "[]"
        first_type = type(v[0]).__name__
        return f"[list of {len(v)} × {first_type}]"
    if isinstance(v, dict):
        return f"{{dict with {len(v)} keys: {list(v.keys())}}}"
    return f"<{type(v).__name__}>"


def dump_nested(d, current_depth=0, max_depth=4):
    """Recursive structured dump — shows keys and value shapes at each level."""
    prefix = "  " * current_depth
    if not isinstance(d, dict):
        print(f"{prefix}{summarize_value(d)}")
        return
    if not d:
        print(f"{prefix}(empty)")
        return
    for k, v in d.items():
        if isinstance(v, dict) and current_depth < max_depth:
            print(f"{prefix}{k}:")
            dump_nested(v, current_depth + 1, max_depth)
        elif (
            isinstance(v, list)
            and v
            and isinstance(v[0], dict)
            and current_depth < max_depth
        ):
            print(f"{prefix}{k}: [list of {len(v)} dicts]")
            print(f"{prefix}  [0]:")
            dump_nested(v[0], current_depth + 2, max_depth)
            if len(v) > 1:
                print(f"{prefix}  ... {len(v) - 1} more entries with similar shape")
        else:
            print(f"{prefix}{k}: {summarize_value(v)}")


def try_fetch(url, params, api_key):
    """
    Attention's API uses inconsistent auth styles across endpoints. Try
    Bearer first, then raw, so this diagnostic works regardless.
    """
    last_resp = None
    for auth_style in ("Bearer", "raw"):
        header_value = f"Bearer {api_key}" if auth_style == "Bearer" else api_key
        headers = {"Authorization": header_value}
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=60)
        except requests.RequestException as e:
            die(f"Network error: {e}")
        last_resp = resp
        if resp.ok:
            print(f"Auth style that worked: {auth_style}")
            return resp
        if resp.status_code not in (401, 403):
            # Not an auth error; different auth won't help.
            return resp
    return last_resp


def main():
    if len(sys.argv) < 2:
        die("Usage: python3 diagnose_attention_conversation.py <conversation_uuid>")

    api_key = os.environ.get("ATTENTION_API_KEY")
    if not api_key:
        die("ATTENTION_API_KEY environment variable required")

    uuid = sys.argv[1]
    url = f"{ATTENTION_API_BASE}/conversations/{uuid}"
    params = {"detailedTranscript": "true"}

    print(f"Fetching {url} ...")
    resp = try_fetch(url, params, api_key)
    if not resp.ok:
        die(f"HTTP {resp.status_code}: {resp.text[:500]}")

    body = resp.json()

    # Save the full raw JSON for sharing / deeper inspection.
    out_path = f"attention_diagnostic_{uuid}.json"
    with open(out_path, "w") as f:
        json.dump(body, f, indent=2)
    print(f"Full JSON saved to: {out_path}\n")

    # -- Top-level shape --
    print("=" * 60)
    print("TOP-LEVEL KEYS")
    print("=" * 60)
    for k, v in body.items():
        print(f"  {k}: {summarize_value(v)}")

    # Attention responses come back as either
    #   {id, type, attributes: {...}}
    # or sometimes wrapped in {data: {id, ..., attributes: {...}}}
    attrs = None
    if isinstance(body.get("attributes"), dict):
        attrs = body["attributes"]
    elif isinstance(body.get("data"), dict):
        attrs = body["data"].get("attributes")

    if not attrs:
        print("\n(!) Could not locate `attributes` in the response.")
        print("Everything is in the JSON file for manual inspection.")
        return

    print("\n" + "=" * 60)
    print("ATTRIBUTES — top-level shape")
    print("=" * 60)
    for k, v in attrs.items():
        print(f"  {k}: {summarize_value(v)}")

    # -- Labels --
    labels = attrs.get("labels") or {}
    print("\n" + "=" * 60)
    print(f"LABELS (count: {len(labels)})")
    print("=" * 60)
    if labels:
        for k, v in labels.items():
            print(f"  {k}: {summarize_value(v)}")
    else:
        print("  (empty)")

    # -- scorecardResults --
    sc = attrs.get("scorecardResults") or []
    print("\n" + "=" * 60)
    print(f"scorecardResults (count: {len(sc)})")
    print("=" * 60)
    for i, entry in enumerate(sc):
        print(f"\n[{i}]:")
        if isinstance(entry, dict):
            dump_nested(entry, current_depth=1, max_depth=5)
        else:
            print(f"  {summarize_value(entry)}")

    # -- extractedIntelligence --
    ei = attrs.get("extractedIntelligence") or {}
    print("\n" + "=" * 60)
    print(f"extractedIntelligence (count: {len(ei)})")
    print("=" * 60)
    if not ei:
        print("  (empty — this is the signature of setter/non-standard scorecards)")
    else:
        for key, entry in ei.items():
            print(f"\n  {key}:")
            if isinstance(entry, dict):
                for k, v in entry.items():
                    print(f"    {k}: {summarize_value(v, max_len=400)}")
            else:
                print(f"    {summarize_value(entry, max_len=400)}")

    # -- Narrative-summary candidates elsewhere in attributes --
    print("\n" + "=" * 60)
    print("NARRATIVE-SUMMARY CANDIDATES")
    print("=" * 60)
    print("Checking common attribute-level keys that might contain")
    print("readable coaching/summary/insight content for setter calls:\n")
    found_any = False
    for candidate in NARRATIVE_CANDIDATE_KEYS:
        if candidate in attrs:
            found_any = True
            v = attrs[candidate]
            print(f"  ✓ attrs['{candidate}']: {summarize_value(v, max_len=500)}")
    if not found_any:
        print(
            "  (none of the guessed candidate keys found — narrative content may")
        print(
            "   only live inside scorecardResults; check the dump above for")
        print(
            "   fields with substantial string content)")

    print("\n" + "=" * 60)
    print("SUGGESTED NEXT STEPS")
    print("=" * 60)
    print(f"1. Skim the scorecardResults dump above for string fields with")
    print(f"   real narrative content (rationales, feedback, observations).")
    print(f"2. Share {out_path} back to plan a concrete extraction strategy.")
    print(f"3. Consider running this against a STANDARD sales-scorecard call")
    print(f"   too, to compare shapes side-by-side.")


if __name__ == "__main__":
    main()
