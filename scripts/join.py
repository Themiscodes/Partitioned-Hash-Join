#!/usr/bin/env python3

from argparse import ArgumentParser
from random import randint

"""
Generates two sets of random pairs representing relations with columns (row_id, payload),
and then joins them using a brute-force approach to produce a new relation with columns
(row_id1, row_id2).

The purpose of this script is to create easily verifiable results for randomized datasets, so
we can test the implementation of the corresponding partitioned hash join function written in C.

Usage:
    ./join [--ntuples <int>] [--mtuples <int>] [--maxvalue <int>]

Args:
    ntuples: number of tuples to generate for the first relation (default: 10).
    mtuples: number of tuples to generate for the second relation (default: 15).
    maxvalue: the maximum value a payload can have (default: 12).

Prints:
	A string formatted as

		<ntuples> ", " <tuples1> "\n" <mtuples> ", " <tuples2> "\n" <njoined_tuples> ", " <joined_tuples> "\n",

	where all tuple lists are formatted as

		"[" ( "(" <row_id> ", " <payload> ")" ", "? )+ "]".

	Here, row_id refers to the row ID of a tuple in the original tuple lists. For the "joined_tuples" part,
	<payload> is just another row ID because the result represents pairs of joined rows using their IDs.

Example:
	>>> ./join.py --ntuples=2 --mtuples=2 --maxval=50
	2, [(0, 5), (1, 10)]
	2, [(0, 35), (1, 5)]
	1, [(0, 1)]
"""

def generate_test_data(ntuples, mtuples, maxvalue):
	tuples_R = [
		(row_id, payload)
		for row_id, payload in zip(
			range(ntuples),
			[randint(0, maxvalue) for i in range(ntuples)]
		)
	]

	tuples_S = [
		(row_id, payload)
		for row_id, payload in zip(
			range(mtuples),
			[randint(0, maxvalue) for i in range(mtuples)]
		)
	]

	# Brute-force algorithm for join
	result = []
	for row_id1, payload1 in tuples_R:
		for row_id2, payload2 in tuples_S:
			if payload1 == payload2:
				result.append((row_id1, row_id2))

	print(f"{ntuples}, {tuples_R}\n{mtuples}, {tuples_S}\n{len(result)}, {result}")

if __name__ == "__main__":
	parser = ArgumentParser()
	parser.add_argument("--ntuples", default=10)
	parser.add_argument("--mtuples", default=15)
	parser.add_argument("--maxvalue", default=12)

	args = parser.parse_args()

	generate_test_data(int(args.ntuples), int(args.mtuples), int(args.maxvalue))
