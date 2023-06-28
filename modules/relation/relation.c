#include "relation.h"

#include <assert.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "helpers.h"

Relation* loadRelation(char* filename) {
  int infd;
  struct stat sb;
  off_t size;
  char* data;

  // Open the relation's file and memory map it to this process' address space.
  // Note: the file needs to contain at least 16 bytes to have a valid header.

  assert((infd = open(filename, O_RDONLY)) != -1);
  assert(fstat(infd, &sb) != -1);
  assert((size = sb.st_size) > 16);
  assert((data = mmap(NULL, size, PROT_READ, MAP_PRIVATE, infd, 0U)) != MAP_FAILED);

  Relation* relation = memAlloc(sizeof(Relation), 1, false, NULL);

  memcpy(&relation->num_tuples, data, sizeof(uint64_t));
  memcpy(&relation->num_columns, data + sizeof(uint64_t), sizeof(uint64_t));

  data += 2 * sizeof(uint64_t);
  relation->columns = memAlloc(sizeof(uint64_t*), relation->num_columns, false, NULL);

  // In the following snippet, we're making the assumption that the relation files will always
  // contain valid data; that is their sizes will, at least, be multiples of 8 (according to the
  // given ABI). We're also taking for granted that sizeof(uint64_t) will yield 8 bytes. These
  // assumptions need to be made if we want to cast the char * into a uint64_t * in order to
  // reinterpret the payloads correctly. Since we're compiling with optimizations activated, we
  // could also encounter strict aliasing related problems, so the project should also be compiled
  // with the -fno-strict-aliasing flag to prohibit any related optimizations for extra safety.

  uint64_t bytes_per_column = relation->num_tuples * sizeof(uint64_t);
  for (uint64_t col = 0; col < relation->num_columns; col++) {
    relation->columns[col] = (uint64_t*)(data + col * bytes_per_column);
  }

  return relation;
}

void destroyJoinRelation(JoinRelation* join_relation) {
  if (join_relation != NULL) {
    free(join_relation->tuples);
    free(join_relation);
  }
}
