import csv

def analyze():
    data = []
    with open('Creuse.csv', 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split('|')
            if len(parts) >= 3:
                page = int(parts[1])
                index = int(parts[2])
                data.append((page, index))

    split_idx = len(data)
    for i in range(1, len(data)):
        if data[i][0] < data[i-1][0]:
            split_idx = i
            break

    def check_run(run_data, run_name):
        print(f"--- {run_name} ---")
        if not run_data:
            print("Empty run.")
            return
            
        pages = {}
        for p, idx in run_data:
            if p not in pages:
                pages[p] = []
            pages[p].append(idx)
        
        max_page = max(pages.keys())
        for p in range(1, max_page + 1):
            if p not in pages:
                print(f"Missing entire Page {p}")
                continue
            
            indices = sorted(pages[p])
            if p == max_page:
                expected = list(range(1, max(indices) + 1))
            else:
                expected = list(range(1, 101))
                
            missing = set(expected) - set(indices)
            if missing:
                print(f"Page {p} missing index/indices: {sorted(list(missing))}")
                
        # Also check for duplicates
        duplicate_count = 0
        for p, idx_list in pages.items():
            if len(idx_list) != len(set(idx_list)):
                import collections
                dupes = [item for item, count in collections.Counter(idx_list).items() if count > 1]
                print(f"Page {p} has duplicate indices: {dupes}")

    run1 = data[:split_idx]
    run2 = data[split_idx:]
    
    check_run(run1, "Run 1")
    if run2:
        check_run(run2, "Run 2")
    else:
        print("Run 2 not found! The data did not start over.")

if __name__ == '__main__':
    analyze()
