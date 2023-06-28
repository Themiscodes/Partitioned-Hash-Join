#include <assert.h>
#include <stdint.h>
#include <stdlib.h>

#include "helpers.h"
#include "inttypes.h"
#include "phjoin.h"
#include "relation.h"

void histogramJob(void *args_) {
  HistogramJobArgs *args = args_;

  *(args->hist) = memAlloc(sizeof(uint32_t), POW2(args->nbits), true, NULL);

  for (uint32_t i = args->start; i < args->end; i++) {
    uint32_t hash_val = LSBITS(args->tuples[i].payload, args->nbits, args->shamt);
    (*(args->hist))[hash_val]++;
  }
}

uint32_t *mergeHistograms(uint32_t **histograms, uint32_t num_threads, uint8_t nbits) {
  uint32_t hist_size = POW2((uint32_t)nbits);
  uint32_t *hist = memAlloc(sizeof(uint32_t), hist_size, true, NULL);

  for (uint32_t thread = 0; thread < num_threads; thread++) {
    for (uint32_t i = 0; i < hist_size; i++) {
      hist[i] += histograms[thread][i];
    }
  }

  for (uint32_t i = 0; i < num_threads; i++) {
    free(histograms[i]);
  }

  free(histograms);

  return hist;
}

void buildingJob(void *args_) {
  BuildingJobArgs *args = args_;

  for (uint32_t i = args->start; i < args->end; i++) {
    Tuple tuple = {.key = args->tuples[i].key, .payload = args->tuples[i].payload};

    insert(args->index, &tuple);
  }
}

void joinJob(void *args_) {
  JoinJobArgs *args = args_;
  if (args->table == NULL) {
    return;
  }

  for (uint32_t i = args->start; i < args->end; i++) {
    RowIDs *matches = search(args->table, args->largest_rel->tuples[i].payload);
    if (matches != NULL) {
      if (args->result->num_tuples + matches->count >= *(args->joined_rows_capacity)) {
        *(args->joined_rows_capacity) *= 2;
        args->result->tuples = memAlloc(sizeof(Tuple), *(args->joined_rows_capacity), false, args->result->tuples);
      }

      for (uint32_t j = 0; j < matches->count; j++, args->result->num_tuples++) {
        // Make sure we save the row ID of the left (R) relation in the key field of the result's tuples
        if (args->relation_R_is_smallest) {
          args->result->tuples[args->result->num_tuples].key = matches->ids[j];
          args->result->tuples[args->result->num_tuples].payload = args->largest_rel->tuples[i].key;
        } else {
          args->result->tuples[args->result->num_tuples].key = args->largest_rel->tuples[i].key;
          args->result->tuples[args->result->num_tuples].payload = matches->ids[j];
        }
      }
      free(matches->ids);
      free(matches);
    }
  }
}

JoinRelation *mergeResults(JoinRelation **results, uint32_t num_results) {
  uint32_t total_tuples = 0;

  for (uint32_t i = 0; i < num_results; i++) {
    total_tuples += results[i]->num_tuples;
  }

  JoinRelation *result = memAlloc(sizeof(JoinRelation), 1, true, NULL);
  result->tuples = memAlloc(sizeof(Tuple), total_tuples, false, NULL);

  for (uint32_t i = 0; i < num_results; i++) {
    for (uint32_t j = 0; j < results[i]->num_tuples; j++, result->num_tuples++) {
      result->tuples[result->num_tuples].key = results[i]->tuples[j].key;
      result->tuples[result->num_tuples].payload = results[i]->tuples[j].payload;
    }

    free(results[i]->tuples);
    free(results[i]);
  }
  free(results);

  return result;
}
