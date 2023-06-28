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

uint8_t nbits1 = 4;
uint8_t nbits2 = 8;

void _parseRelation(FILE* infp, JoinRelation* target) {
  assert(fscanf(infp, "%" SCNu32 ", [", &target->num_tuples) == 1);

  target->tuples = memAlloc(sizeof(Tuple), target->num_tuples, false, NULL);

  for (uint32_t i = 0; i < target->num_tuples; i++) {
    assert(fscanf(infp, "(%" SCNu32 ", %" SCNu32 ")", &target->tuples[i].key, &target->tuples[i].payload) == 2);
    assert(fscanf(infp, i + 1 == target->num_tuples ? "]\n" : ", ") == 0);
  }
}

// Input file format is explained in scripts/join.py.
void _parseTestCase(FILE* infp, JoinRelation* relation_R, JoinRelation* relation_S, JoinRelation* expected_relation) {
  _parseRelation(infp, relation_R);
  _parseRelation(infp, relation_S);
  _parseRelation(infp, expected_relation);
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

void _testPhjoin(void) {
  FILE* infp = fopen("./fixtures/join.txt", "r");
  assert(infp != NULL);

  JoinRelation relation_R, relation_S, expected_relation;

  JobScheduler* scheduler = initializeScheduler(4);

  while (!feof(infp)) {
    if (_consumedComment(infp) || feof(infp)) {
      continue;
    }

    _parseTestCase(infp, &relation_R, &relation_S, &expected_relation);

    JoinRelation* join_results = phjoin(&relation_R, &relation_S, scheduler);
    for (uint32_t i = 0; i < join_results->num_tuples; i++) {
      bool found = false;

      for (uint32_t j = 0; j < expected_relation.num_tuples; j++) {
        if (join_results->tuples[i].key == expected_relation.tuples[j].key &&
            join_results->tuples[i].payload == expected_relation.tuples[j].payload) {
          found = true;
          break;
        }
      }

      TEST_ASSERT(found == true);
    }

    free(relation_R.tuples);
    free(relation_S.tuples);
    free(expected_relation.tuples);
    destroyJoinRelation(join_results);
  }

  destroyScheduler(scheduler);

  fclose(infp);
}

void testPhjoinTwoPasses(void) {
  l2size = 0;
  _testPhjoin();
}

void testPhjoinNoPartitioning(void) {
  l2size = (uint32_t)-1;
  _testPhjoin();
}

void testPhjoinArbitraryL2Size(void) {
  l2size = 1000;
  _testPhjoin();
}

TEST_LIST = {{"testPhjoinTwoPasses", testPhjoinTwoPasses},
             {"testPhjoinNoPartitioning", testPhjoinNoPartitioning},
             {"testPhjoinArbitraryL2Size", testPhjoinArbitraryL2Size},
             {NULL, NULL}};