#include <assert.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "acutest.h"
#include "helpers.h"
#include "phjoin.h"
#include "relation.h"
#include "scheduler.h"

uint32_t l2size;

uint8_t nbits1;
uint8_t nbits2;

// Input file format is explained in scripts/partition.py.
void _parseTestCase(FILE* infp, JoinRelation* relation, Tuple** partitioned_tuples) {
  uint8_t num_passes;

  assert(fscanf(infp, "%" SCNu8 ", ", &nbits1) == 1);
  assert(fscanf(infp, "%" SCNu8 ", ", &nbits2) == 1);
  assert(fscanf(infp, "%" SCNu8 ", ", &num_passes) == 1);
  assert(fscanf(infp, "%" SCNu32 ", [", &relation->num_tuples) == 1);

  relation->tuples = memAlloc(sizeof(Tuple), relation->num_tuples, false, NULL);
  *partitioned_tuples = memAlloc(sizeof(Tuple), relation->num_tuples, false, NULL);

  for (uint32_t i = 0; i < relation->num_tuples; i++) {
    assert(fscanf(infp, "((%" SCNu32 ", %" SCNu32 "), ", &relation->tuples[i].key, &relation->tuples[i].payload) == 2);
    assert(fscanf(infp, "(%" SCNu32 ", %" SCNu32 "))", &(*partitioned_tuples)[i].key, &(*partitioned_tuples)[i].payload) == 2);
    assert(fscanf(infp, i + 1 == relation->num_tuples ? "]\n" : ", ") == 0);
  }

  // Adapt the L2 cache's size as needed so that we get the corresponding number of passes in partition
  l2size = num_passes == 1 ? (uint32_t)-1 : 0;
}

// Returns true iff the next line to read is commented (starts with '#').
bool _consumedComment(FILE* infp) {
  int ch = fgetc(infp);

  if (ch == '#') {
    while ((ch = fgetc(infp)) != '\n')
      ;
    return true;
  } else if (!feof(infp)) {
    ungetc(ch, infp);
  }

  return false;
}

void testPartition(void) {
  FILE* infp = fopen("./fixtures/partition.txt", "r");
  assert(infp != NULL);

  Tuple* partitioned_tuples = NULL;
  JoinRelation relation = {.tuples = NULL, .num_tuples = 0};

  JobScheduler* scheduler = initializeScheduler(4);

  while (!feof(infp)) {
    if (_consumedComment(infp) || feof(infp)) {
      continue;
    }

    _parseTestCase(infp, &relation, &partitioned_tuples);

    uint8_t num_partition_passes;

    JoinRelation* partitioned_relation = partition(&relation, true, false, &num_partition_passes, scheduler);

    TEST_ASSERT(partitioned_relation->num_tuples == relation.num_tuples);
    TEST_ASSERT(l2size == 0 ? num_partition_passes == 2 : num_partition_passes == 1);

    for (uint32_t i = 0; i < partitioned_relation->num_tuples; i++) {
      TEST_ASSERT(partitioned_relation->tuples[i].key == partitioned_tuples[i].key);
      TEST_ASSERT(partitioned_relation->tuples[i].payload == partitioned_tuples[i].payload);
    }

    destroyJoinRelation(partitioned_relation);
    free(partitioned_tuples);
    free(relation.tuples);
  }

  destroyScheduler(scheduler);

  fclose(infp);
}

TEST_LIST = {{"testPartition", testPartition}, {NULL, NULL}};