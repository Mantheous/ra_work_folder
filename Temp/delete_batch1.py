import os
import re

# Same root resolution as missed_downloader.py
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT   = os.path.abspath(os.path.join(_SCRIPT_DIR, ".."))
RESULTS_ROOT = os.path.join(_REPO_ROOT, "Civil_Status", "Results")

# Filename pattern: {dept}_{r_type}_{commune}_{batch}_{page_idx}.jpg
# We want to match files where the batch segment == 1
BATCH1_RE = re.compile(r'^.+_1_\d+\.jpg$')

def main():
    deleted = []
    skipped = []

    for dirpath, _dirnames, filenames in os.walk(RESULTS_ROOT):
        for filename in filenames:
            if BATCH1_RE.match(filename):
                full_path = os.path.join(dirpath, filename)
                try:
                    os.remove(full_path)
                    deleted.append(full_path)
                    print(f"Deleted | {full_path}")
                except Exception as e:
                    skipped.append((full_path, str(e)))
                    print(f"ERROR   | {full_path} | {e}")

    print(f"\nDone. {len(deleted)} file(s) deleted, {len(skipped)} error(s).")
    if skipped:
        print("\nFailed to delete:")
        for path, err in skipped:
            print(f"  {path} — {err}")

if __name__ == "__main__":
    main()
