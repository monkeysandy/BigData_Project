#!/usr/bin/env python3

import sys

# used to sort the multiple reducers output
data = []

for line in sys.stdin:
    line = line.strip()
    if line:
        # split the line into word and count
        word, count = line.split('\t')
        # convert count (currently a string) to int
        data.append((word, int(count)))

# sort the word-count pairs by count (descending order)
data.sort(key=lambda x: x[1], reverse=True)

# print the top 100 words
for word, count in data[:100]:
    print ('%s\t%s' % (word, count))