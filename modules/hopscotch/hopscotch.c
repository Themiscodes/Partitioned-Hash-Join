#include "hopscotch.h"

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "hash.h"
#include "helpers.h"
#include "relation.h"

static uint32_t min2(uint32_t a, uint32_t b) {
  return a < b ? a : b;
}

static uint32_t bucketDistance(uint32_t smaller_index, uint32_t larger_index, uint32_t total_buckets) {
  if (smaller_index > larger_index) {
    larger_index += total_buckets;
  }

  return larger_index - smaller_index;
}

static uint32_t numPayloads(RowIDs *row_ids) {
  return row_ids != NULL ? row_ids->count : 0;
}

// Returns the offset of the first zero bit in bitmap
uint32_t emptySpace(uint64_t bitmap, uint32_t neighbourhood_size) {
  // Check if the neighbourhood is full (all bits are activated) to skip the loop entirely
  if ((((uint64_t)1 << neighbourhood_size) - 1) == bitmap) {
    return neighbourhood_size;
  }

  // If not, find the first zero bit (right to left)
  for (uint32_t j = neighbourhood_size; j > 0; j--) {
    if (!NTH_BIT(bitmap, j)) {
      // Index of the first zero in bitmap (from the "left")
      return neighbourhood_size - j;
    }
  }

  // This part won't be reached by any execution path
  assert(false);
  return neighbourhood_size;
}

uint32_t linearProbe(HashTable *table, uint32_t curr_index) {
  uint32_t num_hops = 0;

  while (true) {
    if (numPayloads(table->buckets[curr_index].row_ids) == 0) {
      return curr_index;  // The bucket's empty, so we found the slot
    }

    // Find the next bucket index to examine. We've already checked if the bucket is empty, so if 0 is returned
    // by emptySpace it's a false positive, which is why we're adding one instead to "hop" over it

    uint32_t new_index = emptySpace(table->buckets[curr_index].bitmap, table->neighbourhood_size);
    new_index = (curr_index + (new_index != 0 ? new_index : 1)) % table->capacity;

    num_hops += bucketDistance(curr_index, new_index, table->capacity);

    // Check if the total number of hops made up to this point result in a circle
    if (num_hops >= table->capacity - 1) {
      return table->capacity + 1;  // Sentinel value to inform the caller that the search has failed
    }

    curr_index = new_index;
  }
}

static void rehash(HashTable *table) {
  Bucket *old_buckets = table->buckets;
  uint32_t old_capacity = table->capacity;

  table->size = 0;       // Reset the size so that insert updates it accordingly
  table->capacity *= 2;  // Double the number of buckets upon rehashing

  table->buckets = memAlloc(sizeof(Bucket), table->capacity, true, NULL);
  for (uint32_t i = 0; i < old_capacity; i++) {
    uint32_t num_payloads = numPayloads(old_buckets[i].row_ids);

    for (uint32_t j = 0; j < num_payloads; j++) {
      Tuple tuple = {.key = old_buckets[i].row_ids->ids[j], .payload = old_buckets[i].payload};
      insert(table, &tuple);
    }

    if (num_payloads > 0) {
      free(old_buckets[i].row_ids->ids);
    }
    free(old_buckets[i].row_ids);  // No problem here; if row_ids is NULL then this is a no-op
  }

  free(old_buckets);
}

HashTable *createHashTable(uint32_t capacity, uint32_t neighbourhood_size) {
  HashTable *table = memAlloc(sizeof(HashTable), 1, false, NULL);
  table->buckets = memAlloc(sizeof(Bucket), capacity, true, NULL);

  table->size = 0;
  table->capacity = capacity;
  table->neighbourhood_size = neighbourhood_size;

  return table;
}

void destroyHashTable(HashTable *table) {
  for (uint32_t i = 0; i < table->capacity; i++) {
    if (numPayloads(table->buckets[i].row_ids) > 0) {
      free(table->buckets[i].row_ids->ids);
    }

    free(table->buckets[i].row_ids);  // No problem here; if row_ids is NULL then this is a no-op
  }

  free(table->buckets);
  free(table);
}

// Swapping around buckets and informing the appropriate bitmaps
static void swap(HashTable *table, uint32_t empty_slot) {
  // Examine slot: A bucket that contains something
  // Empty slot: The empty bucket to be swapped

  // First place to examine is the furthest possible bucket
  uint32_t examine_slot = bucketDistance(table->neighbourhood_size, empty_slot, table->capacity) + 1;
  examine_slot %= table->capacity;

  while (examine_slot != empty_slot) {
    uint32_t bucket_distance = bucketDistance(table->buckets[examine_slot].key, empty_slot, table->capacity);

    // If key is within the range of the neighbourhood of the empty slot
    if (bucket_distance < table->neighbourhood_size) {
      // Fill the empty bucket
      table->buckets[empty_slot].key = table->buckets[examine_slot].key;
      table->buckets[empty_slot].payload = table->buckets[examine_slot].payload;
      table->buckets[empty_slot].row_ids = table->buckets[examine_slot].row_ids;

      // Inform the bitmap with the relative offset of the full bucket
      uint32_t relative = bucketDistance(table->buckets[examine_slot].key, examine_slot, table->capacity) + 1;
      table->buckets[table->buckets[examine_slot].key].bitmap ^= (uint64_t)1 << (table->neighbourhood_size - relative);

      // Inform the bitmap as well for the empty bucket
      relative = bucket_distance + 1;
      table->buckets[table->buckets[examine_slot].key].bitmap ^= (uint64_t)1 << (table->neighbourhood_size - relative);

      // For house keeping "empty" the bucket information
      table->buckets[examine_slot].key = 0;
      table->buckets[examine_slot].row_ids = NULL;

      break;
    }

    // Else go one step closer to the empty slot
    examine_slot = (examine_slot + 1) % table->capacity;
  }

  // If we ended up without having performed the swap a rehash needs to happen
  if (examine_slot == empty_slot) {
    rehash(table);
  }
}

// Check for duplicates. If none rehash, otherwise merge duplicates in this neighbourhood
void merge_or_rehash(HashTable *table, uint32_t key) {
  bool found_duplicate = false;

  // The loop checks each bucket in the neighbourhood except the last one, since the invariant of
  // this hashing scheme is that if we find a duplicate it will be in the same neighbourhood

  for (uint32_t i = 0; i < table->neighbourhood_size - 1; i++) {
    uint32_t bucket_i_index = (key + i) % table->capacity;

    // If the bucket still contains a value
    if (numPayloads(table->buckets[bucket_i_index].row_ids) > 0) {
      // For each subsequent bucket
      for (uint32_t j = i + 1; j < table->neighbourhood_size; j++) {
        uint32_t bucket_j_index = (key + j) % table->capacity;
        uint32_t num_payloads = numPayloads(table->buckets[bucket_j_index].row_ids);

        // If the subsequent bucket contains the same payload as the current bucket
        if (num_payloads > 0 && table->buckets[bucket_i_index].payload == table->buckets[bucket_j_index].payload) {
          found_duplicate = true;

          // Iterate over the second bucket's row ids and add move them over to the current bucket
          for (uint32_t rowid = 0; rowid < num_payloads; rowid++) {
            addRowID(table->buckets[bucket_j_index].row_ids->ids[rowid], &table->buckets[bucket_i_index].row_ids);
          }

          destroyRowIDs(table->buckets[bucket_j_index].row_ids);
          table->buckets[bucket_j_index].row_ids = NULL;

          // Inform the bitmap accordingly by marking this bucket as empty
          table->buckets[table->buckets[bucket_j_index].key].bitmap ^= (uint64_t)1 << (table->neighbourhood_size - j - 1);
        }
      }
    }
  }

  // If no duplicate values were found, all values in the neighourhood were identical, so rehash
  if (!found_duplicate) {
    rehash(table);
  }
}

uint32_t insert(HashTable *table, Tuple *tuple) {
  // This simply requires a little casting to make it work with the 64 func
  uint32_t key = (uint32_t)(ranHash((uint64_t)tuple->payload) % table->capacity);
  // uint32_t key = tuple->payload % table->capacity;
  uint32_t num_payloads = numPayloads(table->buckets[key].row_ids);

  // Case: empty bucket => insert the tuple in it
  if (num_payloads == 0) {
    addRowID(tuple->key, &table->buckets[key].row_ids);

    table->size++;
    table->buckets[key].key = key;
    table->buckets[key].payload = tuple->payload;
    table->buckets[key].bitmap ^= (uint64_t)1 << (table->neighbourhood_size - 1);

    return key;
  }

  // Case: full neighbourhood => check for duplicates, if none rehash; otherwise merge them in a single bucket
  if (table->buckets[key].bitmap == ((uint64_t)1 << table->neighbourhood_size) - 1) {
    merge_or_rehash(table, key);
    return insert(table, tuple);
  }

  // Otherwise, there might exist an empty space so we need to search for it
  uint32_t empty_bucket_index = linearProbe(table, key);

  // If no empty space was found, rehash and try again
  if (empty_bucket_index == table->capacity + 1) {
    rehash(table);
    return insert(table, tuple);
  }

  uint32_t bucket_distance = bucketDistance(key, empty_bucket_index, table->capacity);

  // If there's a space within the neighourhood, insert the tuple in it
  if (bucket_distance < table->neighbourhood_size) {
    addRowID(tuple->key, &table->buckets[empty_bucket_index].row_ids);

    table->size++;
    table->buckets[empty_bucket_index].key = key;
    table->buckets[empty_bucket_index].payload = tuple->payload;
    table->buckets[key].bitmap ^= ((uint64_t)1 << (table->neighbourhood_size - bucket_distance - 1));

    return empty_bucket_index;
  }

  // Finally, if possible swap the space and try again
  swap(table, empty_bucket_index);
  return insert(table, tuple);
}

RowIDs *search(HashTable *table, uint32_t value) {
  RowIDs *matches = NULL;

  uint32_t initial_index = (uint32_t)(ranHash((uint64_t)value) % table->capacity);
  // uint32_t initial_index = value % table->capacity;
  uint32_t limit = initial_index + min2(table->neighbourhood_size, table->capacity);

  // Hopscotch hashing guarantees that a match may occur only within the same neighbourhood
  for (uint32_t i = initial_index; i < limit; i++) {
    Bucket *bucket = &table->buckets[i % table->capacity];
    uint32_t num_payloads = numPayloads(bucket->row_ids);

    if (bucket->payload == value) {
      for (uint32_t j = 0; j < num_payloads; j++) {
        addRowID(bucket->row_ids->ids[j], &matches);
      }
    }
  }

  return matches;
}