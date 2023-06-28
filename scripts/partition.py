#!/usr/bin/env python3

from argparse import ArgumentParser, SUPPRESS
from random import randint

"""
Generates a set of random pairs representing a relation with columns (row_id, payload),
and then partitions that set of pairs similarly to the partitioned hash join algorithm.

The purpose of this script is to create easily verifiable results for randomized datasets,
so we can test the implementation of the corresponding partitionining function written in C.

Usage:
    ./partition [--repeat] [--tuples <int>] [--bits1 <int>] [--bits2 <int>] [--maxvalue <int>]

Args:
    repeat: specifies that the partitioning will happen in two passes.
    tuples: number of tuples to generate (default: 10).
    bits1: number of rightmost bits to extract from a payload as a hash function for the first pass (default: 2).
    bits2: number of rightmost bits to extract from a payload (right-shifted by bits1) as a hash function for
        the second pass (default: 2).
    maxvalue: the maximum value a payload can have (default: 1000).

Prints:
	A string formatted as

		<bits1> ", " <bits2> ", " <num_passes> ", " <num_tuples> ", " <zipped_tuples> "\n",

	where <zipped_tuples> is formatted as

		"[" ( "((" <row_id1> ", " <payload1> "), (" <row_id2> ", " <payload2> "))" ", "? )+ "]".

	Here, row_id1 refers to the row ID of a tuple in the original tuples list, while row_id2 refers to the row ID
	of the tuple in the partitioned tuples list at the corresponding index.

Example:
	>>> ./partition.py --tuples=4 --maxval=50 --bits1=2
	2, 2, 1, 4, [((0, 10), (3, 21)), ((1, 31), (0, 10)), ((2, 3), (1, 31)), ((3, 21), (2, 3))]
"""

def generate_test_data(repeat, ntuples, nbits1, nbits2, maxvalue):
	lsbits = lambda value, nbits: value & ~(~0 << nbits)

	tuples = [
		(row_id, payload)
		for row_id, payload in zip(
			range(ntuples),
			[randint(0, maxvalue) for i in range(ntuples)]
		)
	]

	partitions = {
		hashval: [tup for tup in tuples if lsbits(tup[1], nbits1) == hashval]
		for hashval in range(2 ** nbits1)
	}

	if repeat:
		for hashval1, partition in partitions.items():
			sub_partitions = {
				hashval2: [tup for tup in partition if lsbits(tup[1] >> nbits1, nbits2) == hashval2]
				for hashval2 in range(2 ** nbits2)
			}

			partitions[hashval1] = [t for ts in sorted(sub_partitions.items()) for t in ts[1]]

	partitioned_tuples = [t for ts in sorted(partitions.items()) for t in ts[1]]
	print(f"{nbits1}, {nbits2}, {int(repeat) + 1}, {ntuples}, {list(zip(tuples, partitioned_tuples))}")

if __name__ == "__main__":
	parser = ArgumentParser()
	parser.add_argument("--repeat", action="store_true", default=False)
	parser.add_argument("--tuples", default=10)
	parser.add_argument("--bits1", default=2)
	parser.add_argument("--bits2", default=2)
	parser.add_argument("--maxvalue", default=1000)

	args = parser.parse_args()
	generate_test_data(args.repeat, int(args.tuples), int(args.bits1), int(args.bits2), int(args.maxvalue))
