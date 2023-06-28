#ifndef HASH_H
#define HASH_H

#include <stdint.h>

// Produces the hash value of a 64-bit unsigned integer (source: Numerical Recipes book).
uint64_t ranHash(uint64_t value);

#endif  // HASH_H
