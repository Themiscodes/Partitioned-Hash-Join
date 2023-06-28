#include "phjoin.h"

#include <assert.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "helpers.h"
#include "hopscotch.h"
#include "relation.h"
#include "scheduler.h"

#define NEIGHBOURHOOD_SIZE 48  // Parameter used for the hopscotch tables

static uint8_t _partition(Tuple *tuples,              // The original relation's tuples
                          Tuple *partitioned_tuples,  // The resulting (partitioned) tuples
                          uint32_t start,             // Starting index of sub-array we want to partition
                          uint32_t end,               // Ending index of sub-array we want to partition
                          bool called_recursively,    // Whether this function has been called recursively or not
                          bool is_smallest,           // Whether we're currently partitioning the smallest relation
                          bool two_passes,            // Whether to use two passes or not, if not partitioning smallest
                          JobScheduler *scheduler     // The job scheduler to be used for multi-threading purposes
) {
  // Number of bits to use for hashing in the current partitioning pass
  uint8_t nbits = called_recursively ? nbits2 : nbits1;

  // How much the payload should be right-shifted before extracting its least-significant bits as its hash value
  uint8_t shamt = called_recursively ? nbits1 : 0;

  // How many tuples we're currently partitioning
  uint32_t num_tuples = end - start;

  // This is needed to determine whether we need to partition the relation using two passes or one
  uint32_t max_tuples_in_partition = 0;

  // Number of possible hash values for the given "nbits" parameter
  uint32_t hash_value_count = POW2(nbits);

  // Step 1: build a histogram for counting hash value frequencies
  uint32_t **histograms = memAlloc(sizeof(uint32_t *), scheduler->execution_threads, false, NULL);

  uint32_t tuples_per_thread = num_tuples / scheduler->execution_threads;

  for (uint32_t i = 0; i < scheduler->execution_threads; i++) {
    uint32_t start_ = i * tuples_per_thread;
    uint32_t end_ = (i + 1 == scheduler->execution_threads) ? num_tuples : (i + 1) * tuples_per_thread;

    HistogramJobArgs *args = memAlloc(sizeof(HistogramJobArgs), 1, false, NULL);

    args->tuples = tuples;
    args->start = start_;
    args->end = end_;
    args->nbits = nbits;
    args->shamt = shamt;
    args->hist = &histograms[i];

    JobInfo *job_info = memAlloc(sizeof(JobInfo), 1, false, NULL);
    job_info->job = histogramJob;
    job_info->args = args;
    job_info->kind = HISTOGRAM_JOB;

    submitJob(scheduler, job_info);
  }

  executeAllJobs(scheduler);
  waitAllJobs(scheduler);

  uint32_t *hist = mergeHistograms(histograms, scheduler->execution_threads, nbits);

  max_tuples_in_partition = maxArray(hist, hash_value_count);

  // This check takes care of two things:
  // 1. Use the correct number of passes for the smallest relation, depending on the L2 cache's size
  // 2. Use the same number of passes for the largest relation as we did for the smallest one
  bool should_partition = is_smallest ? max_tuples_in_partition * sizeof(Tuple) > l2size : two_passes;

  // Step 2: convert the histogram to the corresponding prefix sum array (in-place)
  uint32_t *psum = hist;
  for (uint32_t counter = 0, i = 0; i < hash_value_count; i++) {
    uint32_t hist_val = hist[i];
    psum[i] = counter;
    counter += hist_val;
  }

  // This will be used if we need to partition using two passes instead of one
  uint32_t *psum_copy = NULL;

  if (should_partition) {
    psum_copy = memAlloc(sizeof(uint32_t), hash_value_count, false, NULL);
    memcpy(psum_copy, psum, sizeof(uint32_t) * hash_value_count);
  }

  // Step 3: partition the tuples with respect to their hash values
  // Note: psum determines the offset of the next available position for a tuple in each partition
  for (uint32_t i = 0; i < num_tuples; i++) {
    uint32_t hash_val = LSBITS(tuples[i].payload, nbits, shamt);
    partitioned_tuples[start + psum[hash_val]++] = tuples[i];
  }

  // Step 4: recursively partition all partitions if needed (but don't do more than two passes)
  if (should_partition && !called_recursively) {
    for (uint32_t i = 0; i < hash_value_count; i++) {
      uint32_t partition_end = i + 1 == hash_value_count ? end : psum_copy[i + 1];

      if (psum_copy[i] == partition_end) {
        continue;  // The current partition is empty
      }

      uint32_t partition_num_tuples = partition_end - psum_copy[i];

      // This is used so that the internal ordering in each partition is as determined in the first pass
      Tuple *partitioned_tuples_copy = memAlloc(sizeof(Tuple), partition_num_tuples, false, NULL);
      memcpy(partitioned_tuples_copy, partitioned_tuples + psum_copy[i], sizeof(Tuple) * partition_num_tuples);

      _partition(partitioned_tuples_copy, partitioned_tuples, psum_copy[i], partition_end, true, is_smallest, two_passes,
                 scheduler);
      free(partitioned_tuples_copy);
    }
  }

  free(hist);
  if (psum_copy != NULL) {
    free(psum_copy);
  }

  return should_partition + 1;
}

JoinRelation *partition(
    JoinRelation *relation, bool is_smallest, bool two_passes, uint8_t *num_partition_passes, JobScheduler *scheduler) {
  JoinRelation *partitioned_relation = memAlloc(sizeof(JoinRelation), 1, false, NULL);
  partitioned_relation->num_tuples = relation->num_tuples;
  partitioned_relation->tuples = memAlloc(sizeof(Tuple), partitioned_relation->num_tuples, false, NULL);

  *num_partition_passes = _partition(relation->tuples, partitioned_relation->tuples, 0, relation->num_tuples, false, is_smallest,
                                     two_passes, scheduler);

  return partitioned_relation;
}

JoinRelation *phjoin(JoinRelation *relation_R, JoinRelation *relation_S, JobScheduler *scheduler) {
  uint8_t num_partition_passes = 0;

  // Step 1: (possibly) partition the smallest relation to build an index out of it
  JoinRelation *smallest_rel = relation_R->num_tuples > relation_S->num_tuples ? relation_S : relation_R;
  JoinRelation *largest_rel = relation_R->num_tuples <= relation_S->num_tuples ? relation_S : relation_R;

  bool relation_R_is_smallest = smallest_rel == relation_R;

  // Only partition if the smallest relation doesn't fit in the L2 cache
  if (smallest_rel->num_tuples * sizeof(Tuple) > l2size) {
    smallest_rel = partition(smallest_rel, true, false, &num_partition_passes, scheduler);
  }

  // How many bits we've used for partitioning the smallest relation
  uint8_t total_nbits = (num_partition_passes != 0) * nbits1 + (num_partition_passes == 2) * nbits2;

  // Step 2: reconstruct the histogram for the smallest relation only if it was partitioned
  uint32_t *hist_smallest_rel = NULL;

  if (num_partition_passes != 0) {
    hist_smallest_rel = memAlloc(sizeof(uint32_t), 1 << total_nbits, true, NULL);

    for (uint32_t i = 0; i < smallest_rel->num_tuples; i++) {
      uint32_t new_hash_val = LSBITS(smallest_rel->tuples[i].payload, total_nbits, 0);
      hist_smallest_rel[new_hash_val]++;
    }
  }

  // Step 3: create an index of hash tables from the smallest relation
  uint32_t num_htables = 1 << total_nbits;
  HashTable **index = memAlloc(sizeof(HashTable *), num_htables, true, NULL);

  for (uint32_t i = 0; i < num_htables; i++) {
    // Create a hash table only for existing partitions (or the whole relation)
    if (num_partition_passes == 0 || hist_smallest_rel[i] != 0) {
      index[i] = createHashTable(gtePow2(smallest_rel->num_tuples), NEIGHBOURHOOD_SIZE);
    }
  }

  uint32_t start = 0, end = 0;
  for (uint32_t i = 0; i < num_htables; i++, start = end) {
    if (num_partition_passes == 0) {
      end = smallest_rel->num_tuples;
    } else {
      if (hist_smallest_rel[i] == 0) {
        continue;
      }

      end += hist_smallest_rel[i];
    }

    BuildingJobArgs *args = memAlloc(sizeof(BuildingJobArgs), 1, false, NULL);

    args->index = index[i];
    args->tuples = smallest_rel->tuples;
    args->start = start;
    args->end = end;

    JobInfo *job_info = memAlloc(sizeof(JobInfo), 1, false, NULL);
    job_info->job = buildingJob;
    job_info->args = args;
    job_info->kind = BUILDING_JOB;

    submitJob(scheduler, job_info);
  }

  executeAllJobs(scheduler);
  waitAllJobs(scheduler);

  uint32_t *hist_largest_rel = NULL;

  // Step 5: (possibly) partition the largest relation and reconstruct its histogram
  if (num_partition_passes != 0) {
    largest_rel = partition(largest_rel, false, num_partition_passes == 2, &num_partition_passes, scheduler);

    hist_largest_rel = memAlloc(sizeof(uint32_t), 1 << total_nbits, true, NULL);

    for (uint32_t i = 0; i < largest_rel->num_tuples; i++) {
      uint32_t new_hash_val = LSBITS(largest_rel->tuples[i].payload, total_nbits, 0);
      hist_largest_rel[new_hash_val]++;
    }
  }

  // Step 6: probing phase
  uint32_t *joined_rows_capacity = memAlloc(sizeof(uint32_t), num_htables, true, NULL);
  JoinRelation **results = memAlloc(sizeof(JoinRelation *), num_htables, true, NULL);

  for (uint32_t i = 0; i < num_htables; i++) {
    results[i] = memAlloc(sizeof(JoinRelation), 1, true, NULL);
    joined_rows_capacity[i] = (index[i] == NULL) ? 0 : index[i]->size;
    results[i]->tuples = memAlloc(sizeof(Tuple), joined_rows_capacity[i], true, NULL);
  }

  start = end = 0;
  for (uint32_t i = 0; i < num_htables; i++, start = end) {
    if (num_partition_passes == 0) {
      end = largest_rel->num_tuples;
    } else {
      if (hist_largest_rel[i] == 0) {
        continue;
      }
      end += hist_largest_rel[i];
    }

    JoinJobArgs *args = memAlloc(sizeof(JoinJobArgs), 1, false, NULL);

    args->result = results[i];
    args->largest_rel = largest_rel;
    args->table = index[i];
    args->start = start;
    args->end = end;
    args->joined_rows_capacity = &joined_rows_capacity[i];
    args->relation_R_is_smallest = relation_R_is_smallest;

    JobInfo *job_info = memAlloc(sizeof(JobInfo), 1, false, NULL);

    job_info->args = args;
    job_info->job = joinJob;
    job_info->kind = JOIN_JOB;

    submitJob(scheduler, job_info);
  }

  executeAllJobs(scheduler);
  waitAllJobs(scheduler);

  free(joined_rows_capacity);

  JoinRelation *result = mergeResults(results, num_htables);

  if (num_partition_passes != 0) {
    free(hist_smallest_rel);
    free(hist_largest_rel);
    destroyJoinRelation(smallest_rel);
    destroyJoinRelation(largest_rel);
  }

  for (uint32_t i = 0; i < num_htables; i++) {
    if (index[i] != NULL) {
      destroyHashTable(index[i]);
    }
  }

  free(index);

  return result;
}