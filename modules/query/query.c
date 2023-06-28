#include "query.h"

#include <assert.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "helpers.h"
#include "phjoin.h"
#include "relation.h"

static void setFilter(
    FilterPredicate *filter, uint32_t table, uint32_t alias, uint32_t index, uint32_t value, Operator operator) {
  filter->column.table = table;
  filter->column.alias = alias;
  filter->column.index = index;
  filter->value = value;
  filter->operator= operator;
}

Query *parseQuery(FILE *fp) {
  Query *query = memAlloc(sizeof(Query), 1, true, NULL);

  uint32_t num_relations = 0;
  uint32_t aliases[NUM_RELATIONS];

  // Scan relation IDs
  do {
    assert(fscanf(fp, "%" SCNu32, &aliases[num_relations++]) == 1);
  } while (fgetc(fp) != '|');

  query->num_relations = num_relations;

  uint32_t ch;
  uint32_t table1, index1, table2, index2, value;

  // Scan join / filter predicates
  do {
    assert(fscanf(fp, "%" SCNu32 ".%" SCNu32, &table1, &index1) == 2);
    switch (ch = fgetc(fp)) {
      case '>':
        // Fall-through
      case '<':
        assert(fscanf(fp, "%" SCNu32, &value) == 1);
        setFilter(&query->filters[query->num_filters++], aliases[table1], table1, index1, value, ch == '>' ? GT : LT);
        break;

      default:
        assert(fscanf(fp, "%" SCNu32, &table2) == 1);
        if ((ch = fgetc(fp)) == '.') {
          assert(fscanf(fp, "%" SCNu32, &index2) == 1);
          query->joins[query->num_joins].left.table = aliases[table1];
          query->joins[query->num_joins].left.alias = table1;
          query->joins[query->num_joins].left.index = index1;
          query->joins[query->num_joins].right.table = aliases[table2];
          query->joins[query->num_joins].right.alias = table2;
          query->joins[query->num_joins].right.index = index2;
          query->num_joins++;
        } else {
          ungetc(ch, fp);
          setFilter(&query->filters[query->num_filters++], aliases[table1], table1, index1, value = table2, EQ);
        }
        break;
    }
  } while (fgetc(fp) != '|');

  // Scan projections
  do {
    assert(fscanf(fp, "%" SCNu32 ".%" SCNu32, &table1, &index1) == 2);
    query->projections[query->num_projections].table = aliases[table1];
    query->projections[query->num_projections].index = index1;
    query->projections[query->num_projections].alias = table1;
    query->num_projections++;
  } while (fgetc(fp) != '\n');

  return query;
}

static bool predicateHolds(Operator op, uint32_t column, uint32_t value) {
  if (op == LT) {
    return column < value;
  } else if (op == GT) {
    return column > value;
  } else {
    return column == value;
  }
}

RowIDs **applyFilters(Relation **relations, RowIDs **filter_inters, Query *query, bool *empty_result) {
  for (uint32_t filter = 0; filter < query->num_filters; filter++) {
    // The relation to be filtered
    uint32_t relation = query->filters[filter].column.table;
    uint32_t relation_alias = query->filters[filter].column.alias;

    // The relation's column that's referenced in the filter
    uint64_t *column = relations[relation]->columns[query->filters[filter].column.index];

    // Case: first time we're applying a filter to this relation
    if (filter_inters[relation_alias] == NULL) {
      for (uint32_t row_id = 0; row_id < (uint32_t)relations[relation]->num_tuples; row_id++) {
        if (predicateHolds(query->filters[filter].operator,(uint32_t) column[row_id], query->filters[filter].value)) {
          addRowID(row_id, &filter_inters[relation_alias]);
        }
      }

      if (filter_inters[relation_alias] == NULL) {
        *empty_result = true;
        return filter_inters;
      }

      // Case: this relation has been filtered before
    } else {
      // The new filtered array
      RowIDs *filtered_RowIDs = NULL;

      // Instead of the relation we scan only the filtered row IDs
      for (uint32_t index = 0; index < filter_inters[relation_alias]->count; index++) {
        // The actual row ID of the relation
        uint32_t row_id = filter_inters[relation_alias]->ids[index];

        // The same filtering as the trivial case and adding to the new array
        if (predicateHolds(query->filters[filter].operator,(uint32_t) column[row_id], query->filters[filter].value)) {
          addRowID(row_id, &filtered_RowIDs);
        }
      }

      if (filtered_RowIDs == NULL) {
        *empty_result = true;
        return filter_inters;
      }

      free(filter_inters[relation_alias]);
      filter_inters[relation_alias] = filtered_RowIDs;
    }
  }

  return filter_inters;
}

JoinRelation *buildJoinRelation(RowIDs *joined_row_ids, RowIDs *filtered_row_ids, Relation *relation, uint32_t column) {
  uint64_t *column_ = relation->columns[column];

  JoinRelation *join_rel = memAlloc(sizeof(JoinRelation), 1, false, NULL);
  RowIDs *row_ids = (joined_row_ids == NULL) ? filtered_row_ids : joined_row_ids;

  // Case: relation not in intermediate results => build a JoinRelation from scratch
  if (row_ids == NULL) {
    join_rel->num_tuples = relation->num_tuples;
    join_rel->tuples = memAlloc(sizeof(Tuple), join_rel->num_tuples, false, NULL);

    for (uint32_t row_id = 0; row_id < (uint32_t)relation->num_tuples; row_id++) {
      join_rel->tuples[row_id].key = row_id;
      join_rel->tuples[row_id].payload = (uint32_t)column_[row_id];
    }

    // Case: relation in intermediate results => build a JoinRelation using the corresponding row IDs
  } else {
    join_rel->num_tuples = row_ids->count;
    join_rel->tuples = memAlloc(sizeof(Tuple), join_rel->num_tuples, false, NULL);

    for (uint32_t i = 0; i < row_ids->count; i++) {
      join_rel->tuples[i].key = i;
      join_rel->tuples[i].payload = (uint32_t)column_[row_ids->ids[i]];
    }
  }

  return join_rel;
}

RowIDs **applyJoins(Relation **relations,
                    RowIDs **join_inters,
                    RowIDs **filter_inters,
                    Query *query,
                    bool *empty_result,
                    JobScheduler *scheduler) {
  for (uint32_t join = 0; join < query->num_joins; join++) {
    // The left relation
    uint32_t left_relation_table = query->joins[join].left.table;
    uint32_t left_relation_alias = query->joins[join].left.alias;
    uint32_t left_column = query->joins[join].left.index;

    // The right relation
    uint32_t right_relation_table = query->joins[join].right.table;
    uint32_t right_relation_alias = query->joins[join].right.alias;
    uint32_t right_column = query->joins[join].right.index;

    // Case: both relations have been joined before, so we actually apply a filter on the intermediate results
    if (join_inters[left_relation_alias] != NULL && join_inters[right_relation_alias] != NULL) {
      RowIDs **new_join_inters = memAlloc(sizeof(RowIDs *), query->num_relations, true, NULL);

      uint64_t *left_column_ = relations[left_relation_table]->columns[left_column];
      uint64_t *right_column_ = relations[right_relation_table]->columns[right_column];

      bool none_remaining = true;
      for (uint32_t inters_index = 0; inters_index < join_inters[left_relation_alias]->count; inters_index++) {
        uint32_t row_id_left = join_inters[left_relation_alias]->ids[inters_index];
        uint32_t row_id_right = join_inters[right_relation_alias]->ids[inters_index];

        if (predicateHolds(EQ, (uint32_t)left_column_[row_id_left], (uint32_t)right_column_[row_id_right])) {
          for (uint32_t relation_alias = 0; relation_alias < query->num_relations; relation_alias++) {
            if (join_inters[relation_alias] != NULL) {
              none_remaining = false;
              addRowID(join_inters[relation_alias]->ids[inters_index], &new_join_inters[relation_alias]);
            }
          }
        }
      }

      if (none_remaining) {
        destroyInters(new_join_inters, query->num_relations);
        *empty_result = true;
        return join_inters;
      }

      destroyInters(join_inters, query->num_relations);
      join_inters = new_join_inters;

      // Case: either relation doesn't appear in the intermediate results, so we need to execute a join
    } else {
      // Prepare for calling phjoin by creating the appropriate JoinRelation objects
      JoinRelation *join_left_relation = buildJoinRelation(join_inters[left_relation_alias], filter_inters[left_relation_alias],
                                                           relations[left_relation_table], left_column);

      JoinRelation *join_right_relation = buildJoinRelation(
          join_inters[right_relation_alias], filter_inters[right_relation_alias], relations[right_relation_table], right_column);

      JoinRelation *join_results = phjoin(join_left_relation, join_right_relation, scheduler);

      destroyJoinRelation(join_left_relation);
      destroyJoinRelation(join_right_relation);

      if (join_results->num_tuples == 0) {
        destroyJoinRelation(join_results);
        *empty_result = true;
        return join_inters;
      }

      // Case: neither relation exists in the intermediate results
      if (join_inters[left_relation_alias] == NULL && join_inters[right_relation_alias] == NULL) {
        for (uint32_t i = 0; i < join_results->num_tuples; i++) {
          if (filter_inters[left_relation_alias] == NULL) {
            addRowID(join_results->tuples[i].key, &join_inters[left_relation_alias]);
          } else {
            addRowID(filter_inters[left_relation_alias]->ids[join_results->tuples[i].key], &join_inters[left_relation_alias]);
          }
        }

        for (uint32_t i = 0; i < join_results->num_tuples; i++) {
          if (filter_inters[right_relation_alias] == NULL) {
            addRowID(join_results->tuples[i].payload, &join_inters[right_relation_alias]);
          } else {
            addRowID(filter_inters[right_relation_alias]->ids[join_results->tuples[i].payload],
                     &join_inters[right_relation_alias]);
          }
        }

        // Case: one of the two relations exists in the intermediate results
      } else {
        // Remember which of the two it is
        uint32_t old_relation_alias = join_inters[right_relation_alias] == NULL ? left_relation_alias : right_relation_alias;
        uint32_t new_relation_alias = join_inters[left_relation_alias] == NULL ? left_relation_alias : right_relation_alias;

        RowIDs **new_inters = memAlloc(sizeof(RowIDs *), query->num_relations, true, NULL);

        // Move those row ids of the current intermediate results to the new ones, only if the corresponding entry for the
        // relation in the results was included in the join table (the "new" relation that didn't appear in the intermediate
        // results before will be updated in the code after the inner for loop)

        for (uint32_t join_rel_index = 0; join_rel_index < join_results->num_tuples; join_rel_index++) {
          for (uint32_t relation = 0; relation < query->num_relations; relation++) {
            if (join_inters[relation] != NULL && relation != new_relation_alias) {
              if (old_relation_alias == left_relation_alias) {
                addRowID(join_inters[relation]->ids[join_results->tuples[join_rel_index].key], &new_inters[relation]);
              } else {
                addRowID(join_inters[relation]->ids[join_results->tuples[join_rel_index].payload], &new_inters[relation]);
              }
            }
          }

          uint32_t target_index = old_relation_alias == left_relation_alias ? join_results->tuples[join_rel_index].payload
                                                                            : join_results->tuples[join_rel_index].key;
          if (filter_inters[new_relation_alias] == NULL) {
            addRowID(target_index, &new_inters[new_relation_alias]);
          } else {
            addRowID(filter_inters[new_relation_alias]->ids[target_index], &new_inters[new_relation_alias]);
          }
        }

        destroyInters(join_inters, query->num_relations);
        join_inters = new_inters;
      }

      destroyJoinRelation(join_results);
    }
  }
  return join_inters;
}

uint64_t *calculateChecksums(RowIDs **join_inters, Relation **relations, Query *query, bool empty_result) {
  uint64_t *checksums = memAlloc(sizeof(uint64_t), query->num_projections, true, NULL);

  if (!empty_result) {
    for (uint32_t projection = 0; projection < query->num_projections; projection++) {
      Relation *current_relation = relations[query->projections[projection].table];
      uint64_t *column = current_relation->columns[query->projections[projection].index];

      for (uint32_t row_id = 0; row_id < join_inters[query->projections[projection].alias]->count; row_id++) {
        checksums[projection] += column[join_inters[query->projections[projection].alias]->ids[row_id]];
      }
    }
  }

  return checksums;
}

void printChecksums(FILE *stream, uint64_t *checksums, uint32_t size) {
  for (uint32_t i = 0; i < size; i++) {
    if (checksums[i] == 0) {
      fprintf(stream, "NULL%s", i == size - 1 ? "\n" : " ");
    } else {
      fprintf(stream, "%" PRIu64 "%s", checksums[i], (i == size - 1) ? "\n" : " ");
    }
  }
}

void destroyInters(RowIDs **inters, uint32_t num_relations) {
  for (uint32_t i = 0; i < num_relations; i++) {
    destroyRowIDs(inters[i]);
  }
  free(inters);
}