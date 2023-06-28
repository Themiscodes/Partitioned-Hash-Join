#ifndef QUERY_H
#define QUERY_H

#include <stdint.h>
#include <stdio.h>

#include "helpers.h"
#include "relation.h"
#include "scheduler.h"

// These upper bounds are reasonable assumptions, given that we're expecting at most 4 relations per query
#define MAX_JOINS 16
#define MAX_FILTERS 16
#define MAX_PROJECTIONS 16

// An example input query is: "0 2 4|0.1=1.2&1.0=2.1&0.1>3000|0.0 1.1". This translates to:
//
//     SELECT SUM("0".c0), SUM("1".c1)
//     FROM r0 "0", r2 "1", r4 "2"
//     WHERE 0.c1 = 1.c2 AND 1.c0 = 2.c1 AND 0.c1 > 3000
//
// The former is what we're interested in parsing, so the following definitions will allow
// us to convert it into a representation we can easily handle.

typedef enum { LT, GT, EQ } Operator;

typedef struct column {
  uint32_t table;  // The actual table, e.g. in the above example "r4", represented by the value 4
  uint32_t alias;  // The alias of the table in the context of a query, e.g. the value 2 for "r4"
  uint32_t index;  // The column's index, exactly as it appears in the query
} Column;

// Represents expressions like 0.1 > 3000
typedef struct filter_predicate {
  Column column;
  uint32_t value;
  Operator operator;
} FilterPredicate;

// Represents expressions like 0.1 = 1.1
typedef struct join_predicate {
  Column left;
  Column right;
} JoinPredicate;

typedef struct query {
  uint32_t num_relations;
  uint32_t num_joins;
  uint32_t num_filters;
  uint32_t num_projections;

  JoinPredicate joins[MAX_JOINS];
  FilterPredicate filters[MAX_FILTERS];
  Column projections[MAX_PROJECTIONS];
} Query;

// Parses the next query in the stream fp and returns a new, heap-allocated Query object that represents it.
Query *parseQuery(FILE *fp);

// -------------------
// Note: in the following functions, "inters" refers to intermediate results produced by either a filter or a join.

// Applies a number of filters to a given relation (filter_inters needs to be allocated beforehand).
//
// Args:
//     relations: the source relations, as obtained by the call to mmap in loadRelation.
//     filter_inters: the current intermediate filter results.
//     query: the source query that contains the filters to be applied.
//     empty_result: a write-only flag that's used to propagate nullability information to the caller.
//
// Returns:
//     The updated intermediate results produced after applying each filter.

RowIDs **applyFilters(Relation **relations, RowIDs **filter_inters, Query *query, bool *empty_result);

// Applies a number of joins to a given relation (filter_inters and join_inters need to be allocated beforehand).
//
// Args:
//     relations: the source relations, as obtained by the call to mmap in loadRelation.
//     join_inters: the current intermediate join results.
//     filter_inters: the current intermediate filter results.
//     query: the source query that contains the joins to be applied.
//     empty_result: a write-only flag that's used to propagate nullability information to the caller.
//     scheduler: the job scheduler to be used for multi-threading purposes.
//
// Returns:
//     The updated intermediate results produced after applying each join.

RowIDs **applyJoins(Relation **relations,
                    RowIDs **join_inters,
                    RowIDs **filter_inters,
                    Query *query,
                    bool *empty_result,
                    JobScheduler *scheduler);

// Converts a sequence of row IDs into a JoinRelation object that can be used as a phjoin argument.
//
// Args:
//     joined_row_ids: the current intermediate join results.
//     filtered_row_ids: the current intermediate filter results.
//     relation: the source relation to read for building the JoinRelation object.
//     column: the index of the column of interest.
//
// Returns:
//     A new, heap-allocated JoinRelation object with the corresponding row IDs and values for the given column.

JoinRelation *buildJoinRelation(RowIDs *joined_row_ids, RowIDs *filtered_row_ids, Relation *relation, uint32_t column);

// Computes the target checksums (projections) for a given query.
//
// Args:
//     join_inters: the current intermediate join results.
//     relations: the source relations, as obtained by the call to mmap in loadRelation.
//     query: the query of interest.
//     empty_result: a flag that's used to determine whether we should print NULL for all checksums or not.
//
// Returns:
//     An array of all the computed checksums, in the same order as in query. A NULL checksum is represented by 0,
//     so it's silently assumed that we'll never have a checksum that's actually equal to this value.

uint64_t *calculateChecksums(RowIDs **join_inters, Relation **relations, Query *query, bool empty_result);

// Writes a sequence of checksums in stream, separated by newlines, according to the SIGMOD format.
// More info about the format can be found here: https://db.in.tum.de/sigmod18contest/task.shtml

void printChecksums(FILE *stream, uint64_t *checksums, uint32_t size);

// Reclaims all memory used by a an intermediate result object (array of RowIDs).
void destroyInters(RowIDs **inters, uint32_t num_relations);

#endif  // QUERY_H