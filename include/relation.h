#ifndef RELATION_H
#define RELATION_H

#include <stdint.h>

// This is a hard-coded value that represents the number of relations in the *small* SIGMOD workload.
#define NUM_RELATIONS 14

// in public they are 9 so change accordingly
// #define NUM_RELATIONS 9

// Represents (row_ID, column_value) or (row_ID, row_ID), depending on the context.
typedef struct tuple {
  uint32_t key;
  uint32_t payload;
} Tuple;

// Temporary structure used to avoid fully materializing intermediate join results.
typedef struct join_relation {
  Tuple *tuples;
  uint32_t num_tuples;
} JoinRelation;

// This is a wrapper around a relation that's been mapped to the process' address space.
// Leaving these as uint64_t as they are in the original
typedef struct relation {
  uint64_t **columns;
  uint64_t num_tuples;
  uint64_t num_columns;
} Relation;

// Loads a relation that corresponds to a given filename.
//
// Args:
//     filename: the name of the relation's file (expected to be in binary format).
//
// Returns:
//     A new, heap-allocated Relation object that represents the relation.

Relation *loadRelation(char *filename);

// Reclaims all memory used by a JoinRelation object.
void destroyJoinRelation(JoinRelation *join_relation);

#endif  // RELATION_H