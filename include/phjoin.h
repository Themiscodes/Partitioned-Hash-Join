#ifndef PHJOIN_H
#define PHJOIN_H

#include <stdbool.h>
#include <stdint.h>

#include "hopscotch.h"
#include "relation.h"
#include "scheduler.h"

// The L2 cache's size, measured in bytes.
extern uint32_t l2size;

// Number of least-significant bits to be extracted from a payload as its hash value for the
// partitioning phase. If we use two passes, the 2nd time we'll right-shift the payload by nbits1
// bits and then extract the nbits2 least-significant bits to produce the corresponding hash value.

extern uint8_t nbits1;
extern uint8_t nbits2;

// Joins two relations on their tuple's "payload" field.
//
// Args:
//     relation_R: the left relation.
//     relation_S: the right relation.
//     scheduler: the job scheduler to be used for multi-threading purposes.
//
// Returns:
//     A new, heap-allocated relation that represents the join result for relation_R and relation_S.

JoinRelation *phjoin(JoinRelation *relation_R, JoinRelation *relation_S, JobScheduler *scheduler);

// Partitions a relation so that tuples with same hash values are contiguous.
//
// Args:
//     relation: the relation to be partitioned.
//     is_smallest: whether we're currently partitioning the smallest relation of the two.
//     two_passes: whether to use two passes, if we're not partitioning the smallest relation.
//     num_partition_passes: this is written to in order to return the number of passes done.
//     scheduler: the job scheduler to be used for multi-threading purposes.
//
// Returns:
//     A pointer to a new, heap-allocated partitioned relation.

JoinRelation *partition(
    JoinRelation *relation, bool is_smallest, bool two_passes, uint8_t *num_partition_passes, JobScheduler *scheduler);

// -------------------
// Note: the following declarations are needed for the job scheduler.

// Histogram job
typedef struct histogram_job_args {
  Tuple *tuples;
  uint32_t start;
  uint32_t end;
  uint32_t nbits;
  uint8_t shamt;
  uint32_t **hist;
} HistogramJobArgs;

typedef void (*HistogramJob)(void *args);

// Creates a histogram.
void histogramJob(void *args);

// Merges a number of histograms into one by adding their frequencies.
uint32_t *mergeHistograms(uint32_t **histograms, uint32_t num_threads, uint8_t nbits);

// Building job
typedef struct building_job_args {
  HashTable *index;
  Tuple *tuples;
  uint32_t start;
  uint32_t end;
} BuildingJobArgs;

typedef void (*BuildingJob)(void *args);

void buildingJob(void *args);

// Join job
typedef struct join_job_args {
  JoinRelation *result;
  JoinRelation *largest_rel;
  HashTable *table;
  uint32_t start;
  uint32_t end;
  uint32_t *joined_rows_capacity;
  bool relation_R_is_smallest;
} JoinJobArgs;

typedef void (*JoinJob)(void *args);

void joinJob(void *args);

JoinRelation *mergeResults(JoinRelation **results, uint32_t num_results);

#endif  // PHJOIN_H
