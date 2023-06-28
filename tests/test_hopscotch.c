#include <stdint.h>
#include <stdio.h>

#include "acutest.h"
#include "hash.h"
#include "helpers.h"
#include "hopscotch.h"
#include "relation.h"

// Tests the hash function.
void testComputeKey(void) {
  // Checking with different neighbourhoods
  TEST_ASSERT(11 == ranHash(4) % 16);
  TEST_ASSERT(3 == ranHash(0) % 16);
  TEST_ASSERT(15 == ranHash(1028) % 16);
  TEST_ASSERT(4 == ranHash(36) % 8);
  TEST_ASSERT(1 == ranHash(552) % 2);
}

// Tests the hash table's initialization.
void testInit(void) {
  uint32_t size = 0;
  uint32_t capacity = 16;
  uint32_t neighbourhood_size = 4;

  HashTable *table = createHashTable(capacity, neighbourhood_size);

  for (uint32_t i = 0; i < table->capacity; i++) {
    TEST_ASSERT(0 == table->buckets[i].key);
    TEST_ASSERT(0 == table->buckets[i].payload);
    TEST_ASSERT(0 == table->buckets[i].bitmap);
    TEST_ASSERT(NULL == table->buckets[i].row_ids);
  }

  TEST_ASSERT(size == table->size);
  TEST_ASSERT(capacity == table->capacity);
  TEST_ASSERT(neighbourhood_size == table->neighbourhood_size);

  destroyHashTable(table);
}

void _testBasicInsert(HashTable *table, uint32_t capacity) {
  for (uint32_t i = 0; i < capacity; i++) {
    Tuple tuple = {.key = i, .payload = i};
    uint32_t insertion_location = insert(table, &tuple);
    TEST_ASSERT(table->buckets[insertion_location].payload == i);
  }

  TEST_ASSERT(table->size == capacity);
}

// Tests the hash table's insert operation.
void testInsert(void) {
  uint32_t capacity = 16;
  uint32_t neighbourhood_size = 4;

  HashTable *table = createHashTable(capacity, neighbourhood_size);
  _testBasicInsert(table, capacity);

  // This checks if we get reasonable behavior when we overflow a neighbourhood with tuples
  for (uint32_t i = 0; i < neighbourhood_size + 2; i++) {
    Tuple tuple = {.key = i, .payload = 1908};
    uint32_t insertion_location = insert(table, &tuple);
    TEST_ASSERT(table->buckets[insertion_location].payload == 1908);
  }

  TEST_ASSERT(table->size == capacity + neighbourhood_size + 2);

  // Adding some more tuples in...
  for (uint32_t i = 0; i < neighbourhood_size; i++) {
    Tuple tuple = {.key = i, .payload = 1355};
    insert(table, &tuple);
  }

  TEST_ASSERT(table->size == capacity + neighbourhood_size + neighbourhood_size + 2);

  // This checks that the buckets where the initial tuples were inserted have a consistent state
  for (uint32_t i = 0; i < capacity; i++) {
    RowIDs *row_ids = search(table, i);

    TEST_ASSERT(row_ids->count == 1);
    TEST_ASSERT(row_ids->ids[0] == i);

    destroyRowIDs(row_ids);
  }

  destroyHashTable(table);
}

// Tests collisions caused by unique values with identical hash values.
void testCollisions(void) {
  uint32_t count = 16;
  uint32_t collision[count];

  // The number it should hash to is 2
  uint32_t num = 2;

  // Produces a collection of numbers that hash to the same bucket for a neighbourhood of size 4
  for (uint32_t i = 0; count != 0; num++) {
    if (ranHash(num) % 16 == 5) {
      collision[i++] = num;
      count -= 1;
    }
  }

  uint32_t capacity = 8;
  uint32_t neighbourhood_size = 4;

  HashTable *table = createHashTable(capacity, neighbourhood_size);

  for (uint32_t i = 0; i < 16; i++) {
    Tuple tuple = {.key = i, .payload = collision[i]};
    insert(table, &tuple);
  }

  TEST_ASSERT(table->size == 16);

  for (uint32_t i = 0; i < 16; i++) {
    RowIDs *row_ids = search(table, collision[i]);

    TEST_ASSERT(row_ids->count == 1);
    TEST_ASSERT(row_ids->ids[0] == i);

    destroyRowIDs(row_ids);
  }

  destroyHashTable(table);
}

void _testRehash(uint32_t _capacity, uint32_t _neighbourhood_size, uint32_t _payload) {
  uint32_t capacity = _capacity;
  uint32_t neighbourhood_size = _neighbourhood_size;

  uint32_t insertion_location;

  HashTable *table = createHashTable(capacity, neighbourhood_size);
  _testBasicInsert(table, capacity);

  // Insert 1000 duplicate values
  for (uint32_t i = 0; i < 1000; i++) {
    Tuple tuple = {.key = i, .payload = _payload};
    insertion_location = insert(table, &tuple);
    TEST_ASSERT(table->buckets[insertion_location].payload == _payload);
  }

  TEST_ASSERT(table->size == capacity + 1000);

  RowIDs *row_ids = search(table, _payload);
  TEST_ASSERT(row_ids->count == 1000);

  // Add a million more to force many resizes
  for (uint32_t i = 0; i < 1000000; i++) {
    Tuple tuple = {.key = i, .payload = i};
    insertion_location = insert(table, &tuple);
    TEST_ASSERT(table->buckets[insertion_location].payload == i);
  }

  TEST_ASSERT(table->size == capacity + 1001000);

  destroyRowIDs(row_ids);
  destroyHashTable(table);
}

// Tests the hash table's rehashing functionality for various configurations.
void testRehash(void) {
  _testRehash(16, 4, 88888);
  _testRehash(32, 32, 2323);
}

// Tests the hash table's search operation.
void testSearch(void) {
  uint32_t capacity = 16;
  uint32_t neighbourhood_size = 4;

  uint32_t insertion_location;

  HashTable *table = createHashTable(capacity, neighbourhood_size);
  _testBasicInsert(table, capacity);

  // Insert a specific value & check that it was inserted correctly
  Tuple tuple = {.key = 2, .payload = 3000};
  insertion_location = insert(table, &tuple);
  TEST_ASSERT(table->buckets[insertion_location].payload == 3000);

  TEST_ASSERT(table->size == capacity + 1);

  // Check that searching for it yields the correct results
  RowIDs *row_ids = search(table, 3000);

  TEST_ASSERT(row_ids->count == 1);
  TEST_ASSERT(row_ids->ids[0] == 2);

  uint32_t expected_row_ids[10];

  // Add 10 duplicate values
  for (uint32_t i = 0; i < 10; i++) {
    expected_row_ids[i] = i;
    Tuple tuple = {.key = i, .payload = 99};
    insertion_location = insert(table, &tuple);
    TEST_ASSERT(table->buckets[insertion_location].payload == 99);
  }

  TEST_ASSERT(table->size == capacity + 11);

  // Search for these 10 values
  RowIDs *row_ids2 = search(table, 99);
  TEST_ASSERT(row_ids2->count == 10);

  // Check that all the row IDs are present
  for (uint32_t i = 0; i < row_ids2->count; i++) {
    for (uint32_t h = 0; h < 10; h++) {
      if (expected_row_ids[h] == row_ids2->ids[i]) {
        expected_row_ids[h] = 77;
        break;
      }
    }
  }

  // Now the array should only contain 77s
  uint32_t count_ids = 0;
  for (uint32_t h = 0; h < 10; h++) {
    if (expected_row_ids[h] == 77) {
      count_ids += 1;
    }
  }

  // Check that all ids were unique and found
  TEST_ASSERT(count_ids == 10);

  destroyRowIDs(row_ids);
  destroyRowIDs(row_ids2);
  destroyHashTable(table);
}

TEST_LIST = {{"testComputeKey", testComputeKey},
             {"testInit", testInit},
             {"testInsert", testInsert},
             {"testCollisions", testCollisions},
             {"testRehash", testRehash},
             {"testSearch", testSearch},
             {NULL, NULL}};