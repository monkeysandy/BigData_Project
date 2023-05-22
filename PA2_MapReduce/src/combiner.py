#!/usr/bin/env python3
"""combiner.py"""

import sys
from collections import defaultdict

# used to combine the output of multiple mappers
def combine_counts(data):
    # using defaultdict to initialize a dictionary with default value 0
    count_dict = defaultdict(int)
    for word, count in data:
        # increment the count of word by count
        count_dict[word] += int(count)
    return count_dict.items()

# used to read the output of multiple mappers
def read_mapper_output(file, separator=' '):
    for line in file:
        # yield each line as a tuple of (word, count)
        yield line.rstrip().split(separator, 1)

def main(separator=' '):
    # read data from STDIN (standard input) and separate the word and count
    data = read_mapper_output(sys.stdin, separator=separator)
    # combine the word counts from multiple mappers
    combined_counts = combine_counts(data)
    # write the combined word counts to STDOUT (standard output)
    for word, count in combined_counts:
        print('%s %s' % (word, count))

if __name__ == "__main__":
    main()
