#include <stdint.h>
#include <stdlib.h>

#include "acutest.h"
#include "helpers.h"

#define MAX_POW2 25

void testHelpers(void) {
  for (uint32_t i = 0, pow = 1; i < MAX_POW2; i++, pow *= 2) {
    TEST_ASSERT(POW2(i) == pow);
    TEST_ASSERT(NTH_BIT(pow, i + 1) == 1);
  }

  for (uint32_t i = 0, pow = 1, lim = POW2(MAX_POW2); i < lim; i++) {
    TEST_ASSERT(LSBITS(i, MAX_POW2, 0) == i);
    TEST_ASSERT(gtePow2(i) == pow);

    if (i == pow) {
      pow *= 2;
    }
  }
}

TEST_LIST = {{"testHelpers", testHelpers}, {NULL, NULL}};
