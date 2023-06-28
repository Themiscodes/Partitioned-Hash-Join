#include "hash.h"

#include <stdint.h>

uint64_t ranHash(uint64_t value) {
  uint64_t hash = value;

  hash *= (uint64_t)3935559000370003845;
  hash += (uint64_t)2691343689449507681;
  hash ^= hash >> 21;
  hash ^= hash << 37;
  hash ^= hash >> 4;
  hash *= (uint64_t)4768777513237032717;
  hash ^= hash << 20;
  hash ^= hash >> 41;
  hash ^= hash << 5;

  return hash;
}
