#!/usr/bin/env python3
import argparse
import json
import os
import time
import xml.etree.ElementTree as ET

def parse_junit(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    tests = int(root.attrib.get("tests", 0))
    failures = int(root.attrib.get("failures", 0))
    return {"total": tests, "failures": failures}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", nargs="+", required=True, help="JUnit XML files")
    parser.add_argument("--output", required=True, help="Path to cumulative JSON")
    parser.add_argument("--branch", required=True)
    parser.add_argument("--run", required=True)
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    # Load existing results
    if os.path.exists(args.output):
        with open(args.output) as f:
            data = json.load(f)
    else:
        data = []

    # Deduplicate by run_id
    if any(r["run_id"] == args.run for r in data):
        print(f"Run {args.run} already recorded, skipping.")
        return

    # Aggregate results from all XMLs
    total_tests = 0
    total_failures = 0
    for xml_file in args.input:
        parsed = parse_junit(xml_file)
        total_tests += parsed["total"]
        total_failures += parsed["failures"]

    # Append new entry
    entry = {
        "run_id": args.run,
        "branch": args.branch,
        "total": total_tests,
        "failures": total_failures,
        "timestamp": int(time.time())
    }
    data.append(entry)

    # Trim to last N runs
    data = sorted(data, key=lambda r: r["timestamp"], reverse=True)[:args.limit]

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    
    with open(args.output, "w") as f:
        json.dump(data, f, indent=2)

if __name__ == "__main__":
    main()
