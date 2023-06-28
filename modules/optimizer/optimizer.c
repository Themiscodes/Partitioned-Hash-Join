#include "optimizer.h"

#include <assert.h>
#include <inttypes.h>
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "helpers.h"
#include "query.h"
#include "relation.h"

// Function that gathers the statistics of a relation
RelationStats *gatherStatistics(Relation *relation) {
  RelationStats *relation_stats = memAlloc(sizeof(RelationStats), 1, false, NULL);
  relation_stats->column_stats = memAlloc(sizeof(ColumnStats), (uint32_t)relation->num_columns, false, NULL);
  relation_stats->count = (uint32_t)relation->num_columns;
  for (uint32_t i = 0; i < (uint32_t)relation->num_columns; i++) {
    // Initialise the variables
    relation_stats->column_stats[i].min = UINT32_MAX;
    relation_stats->column_stats[i].max = 0;
    relation_stats->column_stats[i].count = (uint32_t)relation->num_tuples;

    for (uint32_t j = 0; j < (uint32_t)relation->num_tuples; j++) {
      // Update the min and max when needed
      if ((uint32_t)relation->columns[i][j] > relation_stats->column_stats[i].max) {
        relation_stats->column_stats[i].max = (uint32_t)relation->columns[i][j];
      }
      if ((uint32_t)relation->columns[i][j] < relation_stats->column_stats[i].min) {
        relation_stats->column_stats[i].min = (uint32_t)relation->columns[i][j];
      }
    }

    // Find the distinct count of the values. Here beware pass them as they are not casted
    uint64_t length = (relation_stats->column_stats[i].count > MAX_COUNT ? MAX_COUNT : relation->num_tuples);
    relation_stats->column_stats[i].distinct = distinctCount(relation->columns[i], length);
  }

  return relation_stats;
}

// The transformer determines whether it is helpful to change the form of the query
static bool transform(Query *query_original) {
  // If there are less than 2 joins no reason for optimisation since
  // we're working under the assumption we don't reorder filters
  if (query_original->num_joins < 2) {
    return false;
  }

  // The case where it's two but one of the firsts' table are in the future inters
  if (query_original->num_joins == 2) {
    for (uint32_t i = 0; i < query_original->num_filters; i++) {
      if (((query_original->joins[0].left.alias == query_original->filters[i].column.alias) ||
           (query_original->joins[0].right.alias == query_original->filters[i].column.alias)) &&
          // and it is not the same table
          (query_original->joins[0].left.table != query_original->joins[0].right.table)) {
        return false;
      }
    }
  }

  // Otherwise perform query transformation
  return true;
}

// Estimates the cost of executing a join(whether handled as filter or actual join) and updates relevant stats
static uint32_t estimateJoinCost(uint32_t left_relation,
                                 uint32_t left_column,
                                 uint32_t left_alias,
                                 uint32_t right_relation,
                                 uint32_t right_column,
                                 uint32_t right_alias,
                                 RelationStats **stats) {
  // In the case the relation's alias is the same
  if (left_alias == right_alias) {
    // These are the same for both sub-cases
    uint32_t n = stats[left_relation]->column_stats[left_column].max - stats[left_relation]->column_stats[left_column].min + 1;
    uint32_t oldcount = stats[left_relation]->column_stats[left_column].count;

    // If the columns are also the same we have a self join
    if (left_column == right_column) {
      // Max, min and distinct stay the same only the count changes
      uint32_t new_count = (oldcount * oldcount) / n;
      stats[left_relation]->column_stats[left_column].count = new_count;

      // Informing the costs of the rest of the columns where the count is the only thing changing
      for (uint32_t j = 0; j < stats[left_relation]->count; j++) {
        if (left_column != j) {
          stats[left_relation]->column_stats[j].count = new_count;
        }
      }

      // Finally return the cost
      return new_count;
    }
    // Beware: Alias is handled as a filter (as per Piazza) not the actual table
    else {
      // Also here careful we put the smaller(min) for the max
      if (compareUints(&stats[left_relation]->column_stats[left_column].max,
                       &stats[left_relation]->column_stats[right_column].max) > 0) {
        stats[left_relation]->column_stats[left_column].max = stats[left_relation]->column_stats[right_column].max;
      } else {
        stats[left_relation]->column_stats[right_column].max = stats[left_relation]->column_stats[left_column].max;
      }

      // And here we put the bigger(max) of the min
      if (compareUints(&stats[left_relation]->column_stats[left_column].min,
                       &stats[left_relation]->column_stats[right_column].min) > 0) {
        stats[left_relation]->column_stats[right_column].min = stats[left_relation]->column_stats[left_column].min;
      } else {
        stats[left_relation]->column_stats[left_column].min = stats[left_relation]->column_stats[right_column].min;
      }

      uint32_t newcount = oldcount / n;
      stats[left_relation]->column_stats[left_column].count = newcount;
      stats[left_relation]->column_stats[right_column].count = newcount;

      stats[left_relation]->column_stats[left_column].distinct =
          stats[left_relation]->column_stats[left_column].distinct *
          (1 - (1 - pow((double)((double)newcount / (double)oldcount),
                        (double)oldcount / (double)stats[left_relation]->column_stats[left_column].distinct)));

      stats[left_relation]->column_stats[right_column].distinct = stats[left_relation]->column_stats[left_column].distinct;

      // Informing the costs of the rest of the columns
      for (uint32_t j = 0; j < stats[left_relation]->count; j++) {
        if (left_column != j && right_column != j) {
          stats[left_relation]->column_stats[j].distinct =
              stats[left_relation]->column_stats[j].distinct *
              ((double)1 -
               pow(((double)1 - ((double)((double)newcount / (double)oldcount))),
                   (double)stats[left_relation]->column_stats[j].count / (double)stats[left_relation]->column_stats[j].distinct));

          // This after distinct always! We need the old count above
          stats[left_relation]->column_stats[j].count = newcount;
        }
      }

      return newcount;
    }
  }

  // Finally, the Join case between different "relations" (aliases)
  if (compareUints(&stats[left_relation]->column_stats[left_column].max, &stats[right_relation]->column_stats[right_column].max) >
      0) {
    stats[left_relation]->column_stats[left_column].max = stats[right_relation]->column_stats[right_column].max;
  } else {
    stats[right_relation]->column_stats[right_column].max = stats[left_relation]->column_stats[left_column].max;
  }

  // And here we put the bigger(max) of the min
  if (compareUints(&stats[left_relation]->column_stats[left_column].min, &stats[right_relation]->column_stats[right_column].min) >
      0) {
    stats[right_relation]->column_stats[right_column].min = stats[left_relation]->column_stats[left_column].min;
  } else {
    stats[left_relation]->column_stats[left_column].min = stats[right_relation]->column_stats[right_column].min;
  }

  // This here and not before since it depends on these values having changed above
  uint32_t n = stats[left_relation]->column_stats[left_column].max - stats[left_relation]->column_stats[left_column].min + 1;
  uint32_t newcount =
      (stats[left_relation]->column_stats[left_column].count * stats[right_relation]->column_stats[right_column].count) / n;
  uint32_t old_distinct_left = stats[left_relation]->column_stats[left_column].distinct;
  uint32_t old_distinct_right = stats[right_relation]->column_stats[right_column].distinct;

  // Update distincts
  stats[left_relation]->column_stats[left_column].distinct *= stats[right_relation]->column_stats[right_column].distinct;
  stats[right_relation]->column_stats[right_column].distinct = stats[left_relation]->column_stats[left_column].distinct;

  // Needed to update distincts of others
  double frac_left = (double)stats[left_relation]->column_stats[left_column].distinct / (double)old_distinct_left;
  double frac_right = (double)stats[right_relation]->column_stats[right_column].distinct / (double)old_distinct_right;

  // Informing the counts of all and distincts of left relation
  for (uint32_t j = 0; j < stats[left_relation]->count; j++) {
    if (left_column != j) {
      stats[left_relation]->column_stats[j].distinct =
          stats[left_relation]->column_stats[j].distinct *
          ((double)1 - pow(((double)1 - frac_left), (double)stats[left_relation]->column_stats[j].count /
                                                        (double)stats[left_relation]->column_stats[j].distinct));
    }

    // This after distinct cause we need the old count above
    stats[left_relation]->column_stats[j].count = newcount;
  }

  // Informing the counts of all and distincts of right relation
  for (uint32_t j = 0; j < stats[right_relation]->count; j++) {
    if (right_column != j) {
      stats[right_relation]->column_stats[j].distinct =
          stats[right_relation]->column_stats[j].distinct *
          ((double)1 - pow(((double)1 - frac_right), (double)stats[right_relation]->column_stats[j].count /
                                                         (double)stats[right_relation]->column_stats[j].distinct));
    }

    // This after distinct cause we need the old count above
    stats[right_relation]->column_stats[j].count = newcount;
  }

  return newcount;
}

// Check whether this permutation is left deep and worth considering
// As the paper suggests: If we are only interested in left-deep join trees with
// no cross products, we have to require that each R is connected in preceeding S.
static bool noCrossProduct(Query *query, uint32_t *perms, uint32_t index, uint32_t count) {
  // Parse the query (in the permutation order)
  for (uint32_t j = 1; j < count; j++) {
    // J-1 join index
    uint32_t join_idx_1 = perms[j - 1 + index * count];

    // The join's left relation table alias
    uint32_t left_relation_1 = query->joins[join_idx_1].left.alias;

    // The join's right relation table alias
    uint32_t right_relation_1 = query->joins[join_idx_1].right.alias;

    // J join index
    uint32_t join_idx_2 = perms[j + index * count];

    // The join's left relation table alias
    uint32_t left_relation_2 = query->joins[join_idx_2].left.alias;

    // The join's right relation table alias
    uint32_t right_relation_2 = query->joins[join_idx_2].right.alias;

    // If there is no connecting relations
    if (!(left_relation_1 == left_relation_2 || left_relation_1 == right_relation_2 || right_relation_1 == left_relation_2 ||
          right_relation_1 == right_relation_2)) {
      // This permutation is not left deep
      return false;
    }
  }

  // If it is left-deep this permutation is worth considering
  return true;
}

// Function to calculate factorial recursively
static uint32_t factorial(uint32_t n) {
  // If the number is 0 or 1, the factorial is 1
  if (n == 0 || n == 1)
    return 1;

  // Recursively calculate the factorial
  return n * factorial(n - 1);
}

// Function to generate all permutations of a given array of unsigned integers
static void generatePermutations(uint32_t *arr, uint32_t left, uint32_t right, uint32_t *perms, uint32_t *count) {
  // Base case: If the length of the array is 1, there is only one permutation
  if (left == right) {
    // Add the permutation
    for (uint32_t i = 0; i <= right; i++) {
      perms[i + (*count)] = arr[i];
    }

    // Increase the count that is used for the indices
    *count = *count + right + 1;
  } else {
    // Recursively generate permutations for the rest of the array
    for (uint32_t i = left; i <= right; i++) {
      // Swap the elements at indices left and i
      uint32_t temp = arr[left];
      arr[left] = arr[i];
      arr[i] = temp;

      // Generate permutations for the rest of the array
      generatePermutations(arr, left + 1, right, perms, count);

      // Swap the elements back to their original positions
      temp = arr[left];
      arr[left] = arr[i];
      arr[i] = temp;
    }
  }
}

// Creates a deep copy of Relation Statistics
void copyStats(RelationStats **copy, RelationStats **original, uint32_t num_relations) {
  for (uint32_t i = 0; i < num_relations; i++) {
    copy[i] = memAlloc(sizeof(RelationStats), 1, false, NULL);
    copy[i]->count = original[i]->count;
    copy[i]->column_stats = memAlloc(sizeof(ColumnStats), copy[i]->count, false, NULL);
    memcpy(copy[i]->column_stats, original[i]->column_stats, copy[i]->count * sizeof(ColumnStats));
  }
}

// Frees Relation Statistics
void destroyStats(RelationStats **stats, uint32_t num_relations) {
  for (uint32_t i = 0; i < num_relations; i++) {
    if (stats[i] != NULL) {
      if (stats[i]->column_stats) {
        free(stats[i]->column_stats);
      }
      free(stats[i]);
    }
  }
}

void optimizeQuery(Query *query_original, RelationStats **relation_stats, uint32_t num_relations, bool dynamic) {
  // Create a copy so we don't alter original statistics needed for other queries
  RelationStats *data_statistics[num_relations];
  copyStats(data_statistics, relation_stats, num_relations);

  // Determine whether it is helpful to change the form of the query
  if (transform(query_original)) {
    uint32_t best_cost = UINT32_MAX;

    // First calculate cost after filters are applied
    for (uint32_t i = 0; i < query_original->num_filters; i++) {
      // Table and indices
      uint32_t filter_idx = query_original->filters[i].column.table;
      uint32_t column_idx = query_original->filters[i].column.index;
      uint32_t op = query_original->filters[i].operator;
      uint32_t value = query_original->filters[i].value;

      // Needed later in the calculations
      uint32_t oldcount = data_statistics[filter_idx]->column_stats[column_idx].count;

      // If it's already not zero
      if (data_statistics[filter_idx]->column_stats[column_idx].count != 0) {
        if (op == LT) {
          if (data_statistics[filter_idx]->column_stats[column_idx].min < value) {
            double frac = data_statistics[filter_idx]->column_stats[column_idx].max > value
                              ? ((double)(value - data_statistics[filter_idx]->column_stats[column_idx].min) /
                                 (double)(data_statistics[filter_idx]->column_stats[column_idx].max -
                                          data_statistics[filter_idx]->column_stats[column_idx].min))
                              : 1;
            data_statistics[filter_idx]->column_stats[column_idx].count =
                frac * data_statistics[filter_idx]->column_stats[column_idx].count;
            data_statistics[filter_idx]->column_stats[column_idx].distinct =
                frac * data_statistics[filter_idx]->column_stats[column_idx].distinct;
          } else {
            data_statistics[filter_idx]->column_stats[column_idx].count = 0;
            data_statistics[filter_idx]->column_stats[column_idx].distinct = 0;
          }
          data_statistics[filter_idx]->column_stats[column_idx].max = value;
        } else if (op == GT) {
          if (data_statistics[filter_idx]->column_stats[column_idx].max > value) {
            double frac = data_statistics[filter_idx]->column_stats[column_idx].min < value
                              ? ((double)(data_statistics[filter_idx]->column_stats[column_idx].max - value) /
                                 (double)(data_statistics[filter_idx]->column_stats[column_idx].max -
                                          data_statistics[filter_idx]->column_stats[column_idx].min))
                              : 1;
            data_statistics[filter_idx]->column_stats[column_idx].count =
                frac * data_statistics[filter_idx]->column_stats[column_idx].count;
            data_statistics[filter_idx]->column_stats[column_idx].distinct =
                frac * data_statistics[filter_idx]->column_stats[column_idx].distinct;
          } else {
            data_statistics[filter_idx]->column_stats[column_idx].count = 0;
            data_statistics[filter_idx]->column_stats[column_idx].distinct = 0;
          }
          data_statistics[filter_idx]->column_stats[column_idx].min = value;
        } else {
          if (data_statistics[filter_idx]->column_stats[column_idx].max >= value &&
              data_statistics[filter_idx]->column_stats[column_idx].min <= value) {
            data_statistics[filter_idx]->column_stats[column_idx].count =
                oldcount / data_statistics[filter_idx]->column_stats[column_idx].distinct;
            data_statistics[filter_idx]->column_stats[column_idx].distinct = 1;
          } else {
            data_statistics[filter_idx]->column_stats[column_idx].count = 0;
            data_statistics[filter_idx]->column_stats[column_idx].distinct = 0;
          }
          data_statistics[filter_idx]->column_stats[column_idx].max = value;
          data_statistics[filter_idx]->column_stats[column_idx].min = value;
        }

        // Informing the costs of the rest of the columns
        for (uint32_t j = 0; j < data_statistics[filter_idx]->count; j++) {
          if (column_idx != j) {
            data_statistics[filter_idx]->column_stats[j].distinct =
                data_statistics[filter_idx]->column_stats[j].distinct *
                ((double)1 -
                 pow(((double)1 - (double)((double)data_statistics[filter_idx]->column_stats[j].count / (double)oldcount)),
                     (double)data_statistics[filter_idx]->column_stats[j].count /
                         (double)data_statistics[filter_idx]->column_stats[j].distinct));
            data_statistics[filter_idx]->column_stats[j].count = data_statistics[filter_idx]->column_stats[j].count;
          }
        }
      }

      // Short circuit here after filters are calculated in case cost is 1 already or 0
      if (data_statistics[filter_idx]->column_stats[0].count < 2) {
        best_cost = 0;
      }
    }

    uint32_t best_plan = 0;

    // If no filter gives an empty result begin Join Enumeration
    if (best_cost != 0) {
      // Firstly create all the possible permutations
      uint32_t arr[query_original->num_joins];
      for (uint32_t i = 0; i < query_original->num_joins; i++) {
        arr[i] = i;
      }
      uint32_t fact = factorial(query_original->num_joins);
      uint32_t permutations[query_original->num_joins * fact];
      uint32_t count = 0;
      generatePermutations(arr, 0, query_original->num_joins - 1, permutations, &count);

      // Dynamic greedy implementation
      if (dynamic == 1) {
        // Finding the costs of initial joins
        count = query_original->num_joins;
        uint32_t best_cost_one = UINT32_MAX;
        uint32_t best_one = 0;

        for (uint32_t ok = 0; ok < count; ok++) {
          RelationStats *temp_stats[num_relations];
          copyStats(temp_stats, data_statistics, num_relations);

          // The join's left relation information
          uint32_t left_alias = query_original->joins[ok].left.alias;
          uint32_t left_relation = query_original->joins[ok].left.table;
          uint32_t left_column = query_original->joins[ok].left.index;

          // The join's right relation information
          uint32_t right_alias = query_original->joins[ok].right.alias;
          uint32_t right_relation = query_original->joins[ok].right.table;
          uint32_t right_column = query_original->joins[ok].right.index;

          // Get the join cost and add it
          uint32_t cost =
              estimateJoinCost(left_relation, left_column, left_alias, right_relation, right_column, right_alias, temp_stats);

          if (cost < best_cost_one) {
            best_cost_one = cost;
            best_one = ok;
          }

          destroyStats(temp_stats, num_relations);
        }

        // Try all permutations of the joins and pick the lowest cost one
        for (uint32_t i = 0; i < fact; i++) {
          // Check whether this permutation is left deep and worth considering
          if ((permutations[i * count] == best_one) && (noCrossProduct(query_original, permutations, i, count))) {
            uint32_t cost = best_cost_one;

            // Create helper temporary stats
            RelationStats *temp_stats[num_relations];
            copyStats(temp_stats, data_statistics, num_relations);

            for (uint32_t j = 0; j < count; j++) {
              // Which join to estimate the cost of
              uint32_t join_idx = permutations[j + i * count];
              uint32_t joincost = 0;

              // The join's left relation information
              uint32_t left_alias = query_original->joins[join_idx].left.alias;
              uint32_t left_relation = query_original->joins[join_idx].left.table;
              uint32_t left_column = query_original->joins[join_idx].left.index;

              // The join's right relation information
              uint32_t right_alias = query_original->joins[join_idx].right.alias;
              uint32_t right_relation = query_original->joins[join_idx].right.table;
              uint32_t right_column = query_original->joins[join_idx].right.index;

              // Get the join cost and add it
              joincost =
                  estimateJoinCost(left_relation, left_column, left_alias, right_relation, right_column, right_alias, temp_stats);

              if (joincost == 0) {
                break;
              }

              cost += joincost;
            }

            // If this permutation was less costly keep this one
            if (cost < best_cost) {
              best_cost = cost;
              best_plan = i;
            }

            // Destroy the helper temporary stats
            destroyStats(temp_stats, num_relations);
          }
        }
      }
      // Simple implementation
      else {
        count = query_original->num_joins;
        // Try all permutations of the joins and pick the lowest cost one
        for (uint32_t i = 0; i < fact; i++) {
          // Check whether this permutation is left deep and worth considering
          if (noCrossProduct(query_original, permutations, i, count)) {
            uint32_t cost = 0;

            // Create helper temporary stats
            RelationStats *temp_stats[num_relations];
            copyStats(temp_stats, data_statistics, num_relations);

            for (uint32_t j = 0; j < count; j++) {
              // Which join to estimate the cost of
              uint32_t join_idx = permutations[j + i * count];
              uint32_t joincost = 0;

              // The join's left relation information
              uint32_t left_alias = query_original->joins[join_idx].left.alias;
              uint32_t left_relation = query_original->joins[join_idx].left.table;
              uint32_t left_column = query_original->joins[join_idx].left.index;

              // The join's right relation information
              uint32_t right_alias = query_original->joins[join_idx].right.alias;
              uint32_t right_relation = query_original->joins[join_idx].right.table;
              uint32_t right_column = query_original->joins[join_idx].right.index;

              // Get the join cost and add it
              joincost =
                  estimateJoinCost(left_relation, left_column, left_alias, right_relation, right_column, right_alias, temp_stats);

              if (joincost == 0) {
                break;
              }

              cost += joincost;
            }

            // If this permutation was less costly keep this one
            if (cost < best_cost) {
              best_cost = cost;
              best_plan = i;
            }

            // Destroy the helper temporary stats
            destroyStats(temp_stats, num_relations);
          }
        }
      }

      // If it isn't the original arrangement
      if (best_plan != 0) {
        // Create a temporary order
        uint32_t count = query_original->num_joins;
        JoinPredicate new_order[count];

        // Add the transformed joins
        for (uint32_t k = 0; k < count; k++) {
          uint32_t join_idx = permutations[k + best_plan * count];
          new_order[k] = query_original->joins[join_idx];
        }

        // Then on the original query
        for (uint32_t k = 0; k < count; k++) {
          query_original->joins[k] = new_order[k];
        }
      }
    }
  }
  destroyStats(data_statistics, num_relations);
}