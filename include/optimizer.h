#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include <stdint.h>
#include <stdio.h>

#include "helpers.h"
#include "query.h"
#include "relation.h"

// 5 million as suggested in the handout
#define MAX_COUNT 5000000

typedef struct column_statistics {
  uint32_t min;       // Minimum value of this column
  uint32_t max;       // Maximum value of this column
  uint32_t count;     // Number of tuples
  uint32_t distinct;  // Number of distinct values of tuples
} ColumnStats;

typedef struct relation_statistics {
  ColumnStats* column_stats;

  uint32_t count;  // Number of columns of relation
} RelationStats;

// Function that collects statistcs of a Relation
RelationStats* gatherStatistics(Relation* relation);

// Function that transforms the query based on the statistics of the relations
void optimizeQuery(Query* query_original, RelationStats** data_statistics, uint32_t num_relations, bool dynamic);

// Creates a deep copy of Relation Statistics
void copyStats(RelationStats** copy, RelationStats** original, uint32_t num_relations);

// Frees Relation Statistics
void destroyStats(RelationStats** stats, uint32_t num_relations);

#endif  // OPTIMIZER_H