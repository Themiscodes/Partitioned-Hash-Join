#ifndef HELPERS_H
#define HELPERS_H

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#include "relation.h"

// Wrapper around a dynamic array of row IDs and its associated metadata.
typedef struct row_ids {
  uint32_t count;
  uint32_t capacity;
  uint32_t* ids;
} RowIDs;

// Computes 2^pow.
#define POW2(pow) (((uint32_t)1) << (pow))

// Extracts the pos-th bit of number (for instance, use pos = 1 to extract its least-significant bit).
#define NTH_BIT(number, pos) (((number) >> ((pos)-1)) & 1)

// Extracts the nbits least-significant bits of number right-shifted by shamt.
#define LSBITS(number, nbits, shamt) (((number) >> (shamt)) & ~(~((uint32_t)0) << (nbits)))

// Allocates a chunk of memory, possibly zero-initialized or containing the contents of a previously
// allocated chunk. This is essentially a "safe" version of malloc, calloc and realloc: it asserts
// that we'll never get NULL back from them. Mainly used to avoid repeating the same assert checks.
//
// Args:
//     size: the size of a single object of the type we want to allocate, in bytes.
//     count: the number of such objects to allocate.
//     set_zeros: whether to zero-initialize the resulting chunk (calloc).
//     source: whether to use a source chunk or not, if NULL is present (realloc).
//
// Returns:
//     A pointer to a newly-allocated memory chunk of size * count bytes.

void* memAlloc(uint32_t size, uint32_t count, bool set_zeros, void* source);

// Appends a row ID to the corresponding array of a RowIDs object.
//
// Args:
//     id: the row ID to append.
//     row_ids: the target RowIDs object. If the dereferenced pointer is NULL, it will be allocated
//         accordingly. Additionally, if there's not enough space, the array will be grown as needed.

void addRowID(uint32_t id, RowIDs** row_ids);

// Returns the nearest power of 2 that's greater than or equal to number.
uint32_t gtePow2(uint32_t number);

// Returns the L2 cache's size (tested in Linux and MacOS).
uint32_t getL2CacheSize(void);

// Returns the maximum element in an array.
uint32_t maxArray(uint32_t* array, uint32_t length);

// Reclaims all memory used by a RowIDs object.
void destroyRowIDs(RowIDs* row_ids);

// Compares uints returns their difference
int compareUints(const void* a, const void* b);

// Returns the count of distinct (unique) values in a column
uint32_t distinctCount(uint64_t* column, uint64_t count);

#endif  // HELPERS_H