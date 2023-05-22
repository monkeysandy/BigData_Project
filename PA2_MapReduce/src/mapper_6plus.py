#!/usr/bin/env python3
"""mapper.py"""

import sys
from collections import Counter
                            
def read_input(file):
    for line in file:
        # split the line into words
        yield line.split()

# read stopwords from stopword.txt
def read_stopwords(file):
    for line in file:
        yield line.strip()

def main(separator=' '):
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin)
    stopwords = read_stopwords(open('stopword.txt', 'r'))
    # put stopwords into a set, which is faster to search
    stop_words = set(stopwords)
    for words in data:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        word_count = Counter()
        for word in words:
            word = word.lower()
            # only count words with length greater than 6 and not in stopwords
            if word not in stop_words and len(word) > 6:
                # set the count of word to 1
                print ('%s %d' % (word, 1))

if __name__ == "__main__":
    main()
