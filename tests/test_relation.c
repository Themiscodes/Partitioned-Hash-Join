#include <stdlib.h>

#include "acutest.h"
#include "helpers.h"
#include "relation.h"

// Tests whether we can ingest the relation r0 used in the SIGMOD harness correctly.
void testLoadRelation(void) {
  Relation *relation = loadRelation("./fixtures/relation");

  // Check that the metadata is correct
  TEST_ASSERT(relation->num_tuples == 1561);
  TEST_ASSERT(relation->num_columns == 3);

  // Check that each column points to the right place
  TEST_ASSERT(relation->columns[0][0] == 1);
  TEST_ASSERT(relation->columns[1][0] == 8463);
  TEST_ASSERT(relation->columns[2][0] == 582);

  // Look at some subsequent values too
  TEST_ASSERT(*(relation->columns[2] + 1) == 6962);
  TEST_ASSERT(*(relation->columns[0] + 2) == 7);

  free(relation->columns);
  free(relation);
}

TEST_LIST = {{"testLoadRelation", testLoadRelation}, {NULL, NULL}};
