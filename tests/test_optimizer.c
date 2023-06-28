#include <assert.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

#include "acutest.h"
#include "helpers.h"
#include "optimizer.h"
#include "phjoin.h"
#include "query.h"
#include "relation.h"

uint32_t l2size;

uint8_t nbits1 = 6;
uint8_t nbits2 = 4;

// Since its different to
uint32_t num_relations = 14;

// Tests the distinctCount function
void testDistinctCount(void) {
  uint64_t array[6];

  array[0] = 1;
  array[1] = 1;
  array[2] = 55;
  array[3] = 77;
  array[4] = 12;
  array[5] = 155;
  TEST_ASSERT(5 == distinctCount(array, 6));

  array[0] = 1;
  array[1] = 1;
  array[2] = 2;
  array[3] = 3;
  TEST_ASSERT(3 == distinctCount(array, 4));

  array[0] = 1;
  array[1] = 2;
  array[2] = 1;
  array[3] = 1;
  TEST_ASSERT(2 == distinctCount(array, 4));

  uint64_t unique_array[100000];
  for (uint64_t i = 0; i < 100000; i++) {
    unique_array[i] = 8829;
  }
  TEST_ASSERT(1 == distinctCount(unique_array, 100000));
}

// Tests the gatherStatistics function
void testGatherStatistics(void) {
  Relation *relation = loadRelation("./fixtures/relation");

  RelationStats *relation_stats = gatherStatistics(relation);

  // Check that the metadata is correct
  TEST_ASSERT(relation_stats->count == 3);
  TEST_ASSERT(relation_stats->column_stats[0].count == 1561);
  TEST_ASSERT(relation_stats->column_stats[1].count == 1561);
  TEST_ASSERT(relation_stats->column_stats[2].count == 1561);

  // Check that the columns have correct min, max
  TEST_ASSERT(relation_stats->column_stats[0].min == 1);
  TEST_ASSERT(relation_stats->column_stats[1].max == 10262);

  // Check that the distinct values are also correct
  TEST_ASSERT(relation_stats->column_stats[0].distinct == 1561);
  TEST_ASSERT(relation_stats->column_stats[1].distinct == 1365);
  TEST_ASSERT(relation_stats->column_stats[2].distinct == 1431);

  free(relation_stats->column_stats);
  free(relation_stats);
  free(relation->columns);
  free(relation);
}

// Tests the copyStats function
void testCopyStats(void) {
  FILE *infp = fopen("../programs/sigmod/workloads/small.init", "r");
  assert(infp != NULL);

  Relation *relations[num_relations];
  RelationStats *data_statistics[num_relations];

  char path[128];
  char relation_filename[1024];

  strcpy(path, "../programs/sigmod/workloads/");
  char *path_end = path + strlen(path);

  // Read all relation file names (note: this **WILL BREAK** if we get more than num_relations file names)
  for (uint32_t i = 0; fgets(relation_filename, sizeof(relation_filename), infp) != NULL;) {
    relation_filename[strlen(relation_filename) - 1] = '\0';  // Get rid of the trailing newline

    if (strlen(relation_filename) > 0) {
      strcpy(path_end, relation_filename);
      relations[i++] = loadRelation(path);
      data_statistics[i - 1] = gatherStatistics(relations[i - 1]);
    }
  }
  fclose(infp);

  // Make a new RelationStats array to copy
  RelationStats *relation_stats[num_relations];
  copyStats(relation_stats, data_statistics, num_relations);

  // Check that the metadata is correct
  TEST_ASSERT(relation_stats[0]->count == 3);
  TEST_ASSERT(relation_stats[0]->column_stats[0].count == 1561);
  TEST_ASSERT(relation_stats[0]->column_stats[1].count == 1561);
  TEST_ASSERT(relation_stats[0]->column_stats[2].count == 1561);

  // Check that the columns have correct min, max
  TEST_ASSERT(relation_stats[0]->column_stats[0].min == 1);
  TEST_ASSERT(relation_stats[0]->column_stats[1].max == 10262);

  // Check that the distinct values are also correct
  TEST_ASSERT(relation_stats[0]->column_stats[0].distinct == 1561);
  TEST_ASSERT(relation_stats[0]->column_stats[1].distinct == 1365);
  TEST_ASSERT(relation_stats[0]->column_stats[2].distinct == 1431);

  // Check different parts of the stats
  TEST_ASSERT(relation_stats[2]->count == data_statistics[2]->count);
  TEST_ASSERT(relation_stats[3]->count == data_statistics[3]->count);
  TEST_ASSERT(relation_stats[1]->column_stats[0].distinct == data_statistics[1]->column_stats[0].distinct);
  TEST_ASSERT(relation_stats[5]->column_stats[1].max == data_statistics[5]->column_stats[1].max);
  TEST_ASSERT(relation_stats[5]->column_stats[0].min == data_statistics[5]->column_stats[0].min);
  TEST_ASSERT(relation_stats[10]->column_stats[1].count == data_statistics[10]->column_stats[1].count);

  destroyStats(relation_stats, num_relations);
  destroyStats(data_statistics, num_relations);

  for (uint64_t rel = 0; rel < num_relations; rel++) {
    if (relations[rel] != NULL) {
      free(relations[rel]->columns);
      free(relations[rel]);
    }
  }
}

// Tests the optimise query function
void testOptimizeQuery(void) {
  l2size = getL2CacheSize();
  FILE *infp = fopen("../programs/sigmod/workloads/small.init", "r");
  assert(infp != NULL);

  Relation *relations[num_relations];
  RelationStats *relation_stats[num_relations];

  char path[128];
  char relation_filename[1024];

  strcpy(path, "../programs/sigmod/workloads/");
  char *path_end = path + strlen(path);

  // Read all relation file names (note: this **WILL BREAK** if we get more than num_relations file names)
  for (uint32_t i = 0; fgets(relation_filename, sizeof(relation_filename), infp) != NULL;) {
    relation_filename[strlen(relation_filename) - 1] = '\0';  // Get rid of the trailing newline

    if (strlen(relation_filename) > 0) {
      strcpy(path_end, relation_filename);
      relations[i++] = loadRelation(path);
      relation_stats[i - 1] = gatherStatistics(relations[i - 1]);
    }
  }
  fclose(infp);

  infp = fopen("./fixtures/queries.txt", "r");
  assert(infp != NULL);

  int query_index = 0;

  // Then, read all query batches ('F' is used to separate each batch)
  for (int ch = fgetc(infp); ch != EOF; ch = fgetc(infp)) {
    ungetc(ch, infp);
    Query *query = parseQuery(infp);

    // With simple implementation
    optimizeQuery(query, relation_stats, num_relations, false);

    // This should be
    if (query_index == 0) {
      TEST_ASSERT(query->joins[0].right.alias == 2);
      TEST_ASSERT(query->joins[0].right.index == 1);
    }

    // It performed the right optimization and reordered correctly the joins of second query
    if (query_index == 1) {
      TEST_ASSERT(query->joins[0].right.alias == 1);
      TEST_ASSERT(query->joins[0].right.index == 2);
      TEST_ASSERT(query->joins[1].right.alias == 0);
      TEST_ASSERT(query->joins[1].right.index == 0);
    }

    // This should also alter thus
    if (query_index == 5) {
      TEST_ASSERT(query->joins[0].right.alias == 2);
      TEST_ASSERT(query->joins[0].right.index == 1);
      TEST_ASSERT(query->joins[1].right.alias == 1);
      TEST_ASSERT(query->joins[1].right.index == 0);
    }

    // This should change this way
    if (query_index == 11) {
      TEST_ASSERT(query->joins[0].right.alias == 3);
      TEST_ASSERT(query->joins[0].right.index == 1);
      TEST_ASSERT(query->joins[1].right.alias == 1);
      TEST_ASSERT(query->joins[1].right.index == 0);
    }

    free(query);
    query_index++;
  }
  fclose(infp);

  destroyStats(relation_stats, num_relations);
  for (uint32_t rel = 0; rel < num_relations; rel++) {
    if (relations[rel] != NULL) {
      free(relations[rel]->columns);
      free(relations[rel]);
    }
  }
}

// Tests the optimise query function with dynamic greedy implementation
void testOptimizeQueryDynamic(void) {
  l2size = getL2CacheSize();
  FILE *infp = fopen("../programs/sigmod/workloads/small.init", "r");
  assert(infp != NULL);

  Relation *relations[num_relations];
  RelationStats *relation_stats[num_relations];

  char path[128];
  char relation_filename[1024];

  strcpy(path, "../programs/sigmod/workloads/");
  char *path_end = path + strlen(path);

  // Read all relation file names (note: this **WILL BREAK** if we get more than num_relations file names)
  for (uint32_t i = 0; fgets(relation_filename, sizeof(relation_filename), infp) != NULL;) {
    relation_filename[strlen(relation_filename) - 1] = '\0';  // Get rid of the trailing newline

    if (strlen(relation_filename) > 0) {
      strcpy(path_end, relation_filename);
      relations[i++] = loadRelation(path);
      relation_stats[i - 1] = gatherStatistics(relations[i - 1]);
    }
  }
  fclose(infp);

  infp = fopen("./fixtures/queries.txt", "r");
  assert(infp != NULL);

  int query_index = 0;

  // Then, read all query batches ('F' is used to separate each batch)
  for (int ch = fgetc(infp); ch != EOF; ch = fgetc(infp)) {
    ungetc(ch, infp);
    Query *query = parseQuery(infp);

    // With greedy implementation
    optimizeQuery(query, relation_stats, num_relations, true);

    // This should stay unaltered
    if (query_index == 0) {
      TEST_ASSERT(query->joins[0].right.alias == 1);
      TEST_ASSERT(query->joins[0].right.index == 0);
    }

    // It performed the right optimization and reordered correctly the joins of second query
    if (query_index == 1) {
      TEST_ASSERT(query->joins[0].right.alias == 2);
      TEST_ASSERT(query->joins[0].right.index == 1);
      TEST_ASSERT(query->joins[1].right.alias == 1);
      TEST_ASSERT(query->joins[1].right.index == 2);
    }

    // This should also alter thus
    if (query_index == 5) {
      TEST_ASSERT(query->joins[0].right.alias == 2);
      TEST_ASSERT(query->joins[0].right.index == 1);
      TEST_ASSERT(query->joins[1].right.alias == 1);
      TEST_ASSERT(query->joins[1].right.index == 0);
    }

    // This should change this way
    if (query_index == 11) {
      TEST_ASSERT(query->joins[0].right.alias == 3);
      TEST_ASSERT(query->joins[0].right.index == 1);
      TEST_ASSERT(query->joins[1].right.alias == 1);
      TEST_ASSERT(query->joins[1].right.index == 0);
    }

    free(query);
    query_index++;
  }
  fclose(infp);

  destroyStats(relation_stats, num_relations);
  for (uint32_t rel = 0; rel < num_relations; rel++) {
    if (relations[rel] != NULL) {
      free(relations[rel]->columns);
      free(relations[rel]);
    }
  }
}

TEST_LIST = {{"testDistinctCount", testDistinctCount},
             {"testGatherStatistics", testGatherStatistics},
             {"testCopyStats", testCopyStats},
             {"testOptimizeQuery", testOptimizeQuery},
             {"testOptimizeQueryDynamic", testOptimizeQueryDynamic},
             {NULL, NULL}};