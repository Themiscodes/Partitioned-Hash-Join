#ifndef HOPSCOTCH_H
#define HOPSCOTCH_H

#include <stdint.h>

#include "helpers.h"
#include "relation.h"

typedef struct bucket {
  uint32_t key;      // What the payload hashes to
  uint32_t payload;  // The column value that's used in the join condition

  // Informs us which buckets in the neighbourhood are occupied by payloads of the same key
  uint64_t bitmap;

  // Used as a "chain" for the row IDs of duplicate payload values
  RowIDs *row_ids;
} Bucket;

typedef struct hash_table {
  Bucket *buckets;

  uint32_t size;                // Number of payloads in the table
  uint32_t capacity;            // Number of total buckets in the hash table
  uint32_t neighbourhood_size;  // Number of buckets that consitute a neighbourhood
} HashTable;

// Creates and returns a new hopscotch hash table.
//
// Args:
//     capacity: the initial capacity of the hash table (might be changed due to rehashing).
//     neighbourhood_size: number of buckets that constitute a neighbourhood.
//
// Returns:
//     A pointer to a new, heap-allocated hopscotch hash table, initialized as needed.

HashTable *createHashTable(uint32_t capacity, uint32_t neighbourhood_size);

// Reclaims all memory used by a HashTable object.
void destroyHashTable(HashTable *table);

// Inserts tuple into table.
uint32_t insert(HashTable *table, Tuple *tuple);

// Returns an array of row IDs that correspond to matching rows in the relation the table was built from.
//
// Args:
//     table: the table to search in.
//     value: the value to search for.
//
// Returns:
//     A pointer to a new, heap-allocated RowIDs object that contains all matched row ids. If no
//     match was found, NULL is returned instead.

RowIDs *search(HashTable *table, uint32_t value);

#endif  // HOPSCOTCH_H