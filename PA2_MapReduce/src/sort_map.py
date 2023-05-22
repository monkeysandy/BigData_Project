#!/usr/bin/env python3
"""used to sort the output of multiple reducers"""

import sys
from collections import Counter

for line in sys.stdin:
    line = line.strip()
    if line:
        # split the line into word and count
        word = line.split()[0]
        count = line.split()[1]
        print ('%s\t%s' % (word, count))