#include <assert.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

#include "acutest.h"
#include "helpers.h"
#include "phjoin.h"
#include "query.h"
#include "relation.h"
#include "scheduler.h"

uint32_t l2size;

uint8_t nbits1 = 8;
uint8_t nbits2 = 8;

void _compareFiles(const char *file1, const char *file2) {
  FILE *fp1 = fopen(file1, "r");
  FILE *fp2 = fopen(file2, "r");

  assert(fp1 != NULL && fp2 != NULL);

  for (int ch1, ch2; true;) {
    ch1 = fgetc(fp1);
    ch2 = fgetc(fp2);
    TEST_ASSERT(ch1 == ch2);

    if (ch1 == EOF || ch2 == EOF) {
      break;
    }
  }

  fclose(fp1);
  fclose(fp2);
}

Relation *_parseRelation(FILE *infp) {
  Relation *relation = memAlloc(sizeof(Relation), 1, false, NULL);

  assert(fscanf(infp, "%" SCNu64 ", ", &relation->num_tuples) == 1);
  assert(fscanf(infp, "%" SCNu64 ", [", &relation->num_columns) == 1);

  relation->columns = memAlloc(sizeof(uint64_t *), relation->num_columns, false, NULL);
  for (uint64_t i = 0; i < relation->num_columns; i++) {
    relation->columns[i] = memAlloc(sizeof(uint64_t *), relation->num_tuples, false, NULL);

    assert(fscanf(infp, "[") == 0);
    for (uint64_t j = 0; j < relation->num_tuples; j++) {
      assert(fscanf(infp, "%" SCNu64, &relation->columns[i][j]) == 1);
      assert(fscanf(infp, j + 1 == relation->num_tuples ? "]" : ", ") == 0);
    }

    assert(fscanf(infp, i + 1 == relation->num_columns ? "]" : ", ") == 0);
  }

  assert(fscanf(infp, "\n") == 0);

  return relation;
}

void testQueryParsing(void) {
  FILE *infp = fopen("./fixtures/query.txt", "r");
  assert(infp != NULL);

  Query *query = parseQuery(infp);
  fclose(infp);

  // Check metadata
  TEST_ASSERT(query->num_joins == 2);
  TEST_ASSERT(query->num_filters == 1);
  TEST_ASSERT(query->num_projections == 2);

  // Check first join
  TEST_ASSERT(query->joins[0].left.table == 0);
  TEST_ASSERT(query->joins[0].left.index == 1);
  TEST_ASSERT(query->joins[0].right.table == 2);
  TEST_ASSERT(query->joins[0].right.index == 2);

  // Check second join
  TEST_ASSERT(query->joins[1].left.table == 2);
  TEST_ASSERT(query->joins[1].left.index == 0);
  TEST_ASSERT(query->joins[1].right.table == 4);
  TEST_ASSERT(query->joins[1].right.index == 1);

  // Check filter
  TEST_ASSERT(query->filters[0].column.table == 0);
  TEST_ASSERT(query->filters[0].column.index == 1);
  TEST_ASSERT(query->filters[0].value == 3000);
  TEST_ASSERT(query->filters[0].operator== GT);

  // Check checksums
  TEST_ASSERT(query->projections[0].table == 0);
  TEST_ASSERT(query->projections[0].index == 0);
  TEST_ASSERT(query->projections[1].table == 2);
  TEST_ASSERT(query->projections[1].index == 1);

  free(query);
}

void testBuildJoinRelation(void) {
  FILE *infp = fopen("./fixtures/relation.txt", "r");
  assert(infp != NULL);

  Relation *relation = _parseRelation(infp);
  fclose(infp);

  // Case 1: relation isn't in intermediate results
  RowIDs **filter_inters = memAlloc(sizeof(RowIDs *), 1, true, NULL);
  RowIDs **join_inters = memAlloc(sizeof(RowIDs *), 1, true, NULL);

  JoinRelation *join_rel = buildJoinRelation(join_inters[0], filter_inters[0], relation, 0);

  TEST_ASSERT(join_rel->num_tuples == 3);
  TEST_ASSERT(join_rel->tuples[0].key == 0 && join_rel->tuples[0].payload == 19);
  TEST_ASSERT(join_rel->tuples[1].key == 1 && join_rel->tuples[1].payload == 44444);
  TEST_ASSERT(join_rel->tuples[2].key == 2 && join_rel->tuples[2].payload == 30001);

  destroyJoinRelation(join_rel);

  // Case 2: some IDs of relation exist in intermediate results
  filter_inters[0] = memAlloc(sizeof(RowIDs), 1, false, NULL);
  filter_inters[0]->capacity = filter_inters[0]->count = 2;
  filter_inters[0]->ids = memAlloc(sizeof(uint32_t), 2, false, NULL);
  filter_inters[0]->ids[0] = 2;
  filter_inters[0]->ids[1] = 1;

  join_rel = buildJoinRelation(join_inters[0], filter_inters[0], relation, 0);

  TEST_ASSERT(join_rel->num_tuples == 2);
  TEST_ASSERT(join_rel->tuples[0].key == 0 && join_rel->tuples[0].payload == 30001);
  TEST_ASSERT(join_rel->tuples[1].key == 1 && join_rel->tuples[1].payload == 44444);

  destroyJoinRelation(join_rel);

  join_inters[0] = memAlloc(sizeof(RowIDs), 1, false, NULL);
  join_inters[0]->capacity = join_inters[0]->count = 2;
  join_inters[0]->ids = memAlloc(sizeof(uint32_t), 2, false, NULL);
  join_inters[0]->ids[0] = 0;
  join_inters[0]->ids[1] = 1;

  join_rel = buildJoinRelation(join_inters[0], filter_inters[0], relation, 3);
  TEST_ASSERT(join_rel->num_tuples == 2);
  TEST_ASSERT(join_rel->tuples[0].key == 0 && join_rel->tuples[0].payload == 45);
  TEST_ASSERT(join_rel->tuples[1].key == 1 && join_rel->tuples[1].payload == 34);

  destroyJoinRelation(join_rel);

  destroyInters(filter_inters, 1);
  destroyInters(join_inters, 1);

  for (uint64_t column = 0; column < relation->num_columns; column++) {
    free(relation->columns[column]);
  }
  free(relation->columns);
  free(relation);
}

void testSigmodHarness(void) {
  l2size = getL2CacheSize();

  FILE *infp = fopen("../programs/sigmod/workloads/small.init", "r");
  assert(infp != NULL);

  Relation *relations[NUM_RELATIONS];

  char path[128];
  char relation_filename[1024];

  strcpy(path, "../programs/sigmod/workloads/");
  char *path_end = path + strlen(path);

  // Read all relation file names (note: this **WILL BREAK** if we get more than NUM_RELATIONS file names)
  for (uint32_t i = 0; fgets(relation_filename, sizeof(relation_filename), infp) != NULL;) {
    relation_filename[strlen(relation_filename) - 1] = '\0';  // Get rid of the trailing newline

    if (strlen(relation_filename) > 0) {
      strcpy(path_end, relation_filename);
      relations[i++] = loadRelation(path);
    }
  }

  fclose(infp);
  assert((infp = fopen("../programs/sigmod/workloads/small.work", "r")) != NULL);

  FILE *outfp = fopen("checksums.txt", "w");
  assert(outfp != NULL);

  JobScheduler *scheduler = initializeScheduler(4);

  for (int character; ((character = fgetc(infp)) != EOF);) {
    ungetc(character, infp);
    for (int ch; ((ch = fgetc(infp)) != 'F');) {
      ungetc(ch, infp);

      Query *query = parseQuery(infp);

      RowIDs **filter_inters = memAlloc(sizeof(RowIDs *), query->num_relations, true, NULL);
      RowIDs **join_inters = memAlloc(sizeof(RowIDs *), query->num_relations, true, NULL);

      bool empty_result = false;

      filter_inters = applyFilters(relations, filter_inters, query, &empty_result);
      if (empty_result == false) {
        join_inters = applyJoins(relations, join_inters, filter_inters, query, &empty_result, scheduler);
      }

      uint64_t *checksums = calculateChecksums(join_inters, relations, query, empty_result);

      printChecksums(outfp, checksums, query->num_projections);

      free(checksums);

      destroyInters(join_inters, query->num_relations);
      destroyInters(filter_inters, query->num_relations);
      free(query);
    }
  }

  destroyScheduler(scheduler);

  fclose(outfp);
  fclose(infp);

  _compareFiles("checksums.txt", "../programs/sigmod/workloads/small.result");
  assert((remove("checksums.txt")) != -1);

  for (uint32_t rel = 0; rel < NUM_RELATIONS; rel++) {
    if (relations[rel] != NULL) {
      free(relations[rel]->columns);
      free(relations[rel]);
    }
  }
}

TEST_LIST = {{"testQueryParsing", testQueryParsing},
             {"testBuildJoinRelation", testBuildJoinRelation},
             {"testSigmodHarness", testSigmodHarness},
             {NULL, NULL}};
