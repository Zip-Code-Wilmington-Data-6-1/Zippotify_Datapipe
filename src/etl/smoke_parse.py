import json, statistics, argparse
from collections import Counter

PATH = "data/sample/events_small_combined.jsonl"
MAX_LINES = 2000

def run(path, max_lines):
    fields = Counter()
    durations = []
    with open(path) as f:
        for i, line in enumerate(f, 1):
            if not line.strip():
                continue
            o = json.loads(line)
            for k in o:
                fields[k] += 1
            d = o.get("duration")
            if isinstance(d, (int, float)):
                durations.append(d)
            if i >= max_lines:
                break
    print(f"Scanned lines: {min(i, max_lines)}")
    print("Field counts:", dict(fields))
    if durations:
        print("Duration seconds (min/median/max):",
              min(durations), statistics.median(durations), max(durations))
    missing = [k for k,c in fields.items() if c < min(i, max_lines)]
    if missing:
        print("Warning: some fields missing in subset:", missing)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", default=PATH)
    ap.add_argument("--max-lines", type=int, default=MAX_LINES)
    args = ap.parse_args()
    run(args.path, args.max_lines)

if __name__ == "__main__":
    main()