import sys

substring_counts = {}

for line in sys.stdin:
    substring, count = line.strip().split('\t', 1)
    try:
        count = int(count)
        if substring in substring_counts:
            substring_counts[substring] += count
        else:
            substring_counts[substring] = count
    except ValueError:
        continue

sorted_substrings = sorted(substring_counts.items(), key=lambda item: item[1], reverse=True)[:100]

for substring, count in sorted_substrings:
    print(f'{substring}\t{count}')