import os
import sys
import time
import logging
import argparse
from gensim.models import KeyedVectors

from helpers import mkdir_if_not_exists
from preprocess import clean_single_sentence

def process(input_filename, output_filename):
    with open(input_filename, 'r', 65536) as input:
        with open(output_filename, 'w', 65536) as output:
            input.readline() # ignore header line
            for line in input:
                columns = line.split(';')
                columns[1:3] = [clean_single_sentence(c) for c in columns[1:3]]
                output.write(';'.join(columns))

def main():
    logging.basicConfig(format='%(levelname)s: %(message)s')
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    parser.add_argument("input", help="input_filename")
    parser.add_argument("-o", "--output", default="./processed.csv",
                        help="output_filename")

    args = parser.parse_args()
    output_filename = args.output
    mkdir_if_not_exists(os.path.dirname(output_filename))
    extract_start = time.perf_counter()

    logging.info("Processing ...")
    process(args.input, output_filename)

    extract_duration = time.perf_counter() - extract_start
    logging.info("elapsed %f", extract_duration)


if __name__ == '__main__':
    main()


# python ./scripts/remove-contexts.py ./data/contexts/ -m ./data/models/word2vec/word2vec-s100-w5-m2-sg0.bin -o ./data/contexts2/
