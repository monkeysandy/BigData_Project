#!/usr/bin/env python3
"""reducer.py"""

import sys
from collections import defaultdict
from heapq import nlargest

def read_mapper_output(file, separator=' '):
    for line in file:
        yield line.rstrip().split(separator, 1)

def main(separator=' '):
    data = read_mapper_output(sys.stdin, separator=separator)
    # using defaultdict to initialize a dictionary with default value 0
    count_dict = defaultdict(int)
    for word, count in data:
        # increment the count of word by count
        count_dict[word] += int(count)
    # nlargest() function returns the specified number of items from the
    # iterable in a list. The item with the highest count is returned first.
    # The key argument is used to specify a function to be called on each
    # list element prior to making comparisons.
    top_100_words = nlargest(100, count_dict.items(), key=lambda x: x[1])
    for word, count in top_100_words:
        # format print output
        print('{:<15} {:<10}'.format(word, count))

if __name__ == "__main__":
    main()