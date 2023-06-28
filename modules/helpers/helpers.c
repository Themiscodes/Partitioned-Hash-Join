#include "helpers.h"

#include <assert.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

void *memAlloc(uint32_t size, uint32_t count, bool set_zeros, void *source) {
  // set_zeros is incompatible with a non-NULL source; a user can't pass both
  assert(!(set_zeros && source != NULL));

  void *mem = NULL;
  if (set_zeros) {
    mem = calloc(count, size);
  } else if (source != NULL) {
    mem = realloc(source, count * size);
  } else {
    mem = malloc(count * size);
  }

  assert(mem != NULL);
  return mem;
}

void addRowID(uint32_t id, RowIDs **row_ids) {
  // If this is the first row ID to be added, allocate space just for it
  if (*row_ids == NULL) {
    *row_ids = memAlloc(sizeof(RowIDs), 1, false, NULL);
    (*row_ids)->count = 0;
    (*row_ids)->capacity = 512;
    (*row_ids)->ids = memAlloc(sizeof(uint32_t), (*row_ids)->capacity, false, NULL);

    // Else, we'll double the array's size if it needs to be grown
  } else if ((*row_ids)->capacity == (*row_ids)->count) {
    (*row_ids)->capacity *= 2;
    (*row_ids)->ids = memAlloc(sizeof(uint32_t), (*row_ids)->capacity, false, (*row_ids)->ids);
  }

  (*row_ids)->ids[(*row_ids)->count++] = id;
}

uint32_t gtePow2(uint32_t number) {
  // If the number is already a power of two, return it
  if (number != 0 && (number & (number - 1)) == 0)
    return number;

  uint32_t pow = 0;
  while (number != 0) {
    number >>= 1;
    pow++;
  }

  return POW2(pow);
}

uint32_t getL2CacheSize(void) {
#if defined(__linux__)
  return (uint32_t)sysconf(_SC_LEVEL2_CACHE_SIZE);
#elif defined(__APPLE__) && defined(__MACH__)
  assert(system("sysctl -a | grep hw.l2 | cut -d' ' -f 2 > cachesize.txt") != -1);

  FILE *fp = fopen("cachesize.txt", "r");
  assert(fp != NULL);

  uint32_t size = 0;
  assert(fscanf(fp, "%" SCNu32, &size) == 1);

  fclose(fp);
  assert(system("rm cachesize.txt") != -1);

  return size;
#else
  // Default to 256KB if the current system is neither Linux nor MacOS
  return 256 * (1 << 10);
#endif
}

uint32_t maxArray(uint32_t *array, uint32_t length) {
  uint32_t max = 0;

  for (uint32_t i = 0; i < length; i++) {
    max = array[i] > max ? array[i] : max;
  }

  return max;
}

void destroyRowIDs(RowIDs *row_ids) {
  if (row_ids != NULL) {
    if (row_ids->count > 0) {
      free(row_ids->ids);
    }
    free(row_ids);
  }
}

int compareUints(const void *a, const void *b) {
  return (*(uint64_t *)a - *(uint64_t *)b);
}

// Distinct function with nlogn instead of n^2 complexity
uint32_t distinctCount(uint64_t *column, uint64_t count) {
  uint64_t *unique_array = malloc(count * sizeof(uint64_t));
  for (uint64_t i = 0; i < count; i++) {
    unique_array[i] = column[i];
  }
  uint64_t distinct_count = 1;
  qsort(unique_array, count, sizeof(uint64_t), compareUints);
  for (uint64_t i = 1; i < count; i++) {
    if (unique_array[i] != unique_array[i - 1]) {
      distinct_count++;
    }
  }
  free(unique_array);
  return (uint32_t)distinct_count;
}