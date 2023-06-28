#include <assert.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "helpers.h"
#include "optimizer.h"
#include "phjoin.h"
#include "query.h"
#include "relation.h"
#include "scheduler.h"

#define WORKLOADS_DIR "./workloads/"
#define PUBLIC_DIR "./public/"

#define MAX_RESULTS 15
#define MAX_THREADS 3
#define JOB_THREADS 3

// Wrapper around the checksums of a batch
typedef struct results {
  uint64_t projections;
  uint64_t *checksums;
} Results;

// Mutex for queue of queries
pthread_mutex_t pool_mutex = PTHREAD_MUTEX_INITIALIZER;

// Condition variable if queue is empty
pthread_cond_t empty_pool = PTHREAD_COND_INITIALIZER;

// Condition variable if queue is full
pthread_cond_t full_pool = PTHREAD_COND_INITIALIZER;

// Query job to be passed in args
typedef struct query_job {
  // The query to be handled
  Query *query;

  // Where the checksum results should go
  uint8_t index;

} QueryJob;

// The queue to be used for queries
typedef struct {
  QueryJob **pool;
  int front;
  int rear;
  int count;
} queue;

void initQueue(queue *q) {
  q->pool = memAlloc(sizeof(QueryJob *), MAX_THREADS, true, NULL);
  q->front = 0;
  q->rear = -1;
  q->count = 0;
}

void enQueue(queue *q, QueryJob *job) {
  if (q->count >= MAX_THREADS) {
    fprintf(stderr, "Overflow o'clock\n");
  } else {
    q->rear = (q->rear + 1) % MAX_THREADS;
    q->pool[q->rear] = job;
    q->count++;
  }
}

QueryJob *deQueue(queue *q) {
  QueryJob *job;
  if (q->count <= 0) {
    fprintf(stderr, "Underflow o'clock\n");
    return NULL;
  } else {
    job = q->pool[q->front];
    q->front = (q->front + 1) % MAX_THREADS;
    q->count--;
    return job;
  }
}

queue thread_pool;

RelationStats *data_statistics[NUM_RELATIONS];
Relation *relations[NUM_RELATIONS];
Results *batch_results[MAX_RESULTS];
uint8_t threads;

uint32_t l2size;
uint8_t nbits1 = 8;
uint8_t nbits2 = 10;

static void *query_job(void *args) {
  uint8_t query_count = *(uint8_t *)args;
  Query *query;

  while (1) {
    pthread_mutex_lock(&pool_mutex);

    // Wait if there is no jobs yet
    while (threads >= MAX_THREADS)
      pthread_cond_wait(&empty_pool, &pool_mutex);

    // Find the current query_count that you need to put the results
    QueryJob *job = deQueue(&thread_pool);
    query_count = job->index;
    query = job->query;

    // Used to know whether to short-circuit in case we get a NULL in the output
    bool empty_result = false;

    // Apply filters
    RowIDs **filter_inters = memAlloc(sizeof(RowIDs *), query->num_relations, true, NULL);
    filter_inters = applyFilters(relations, filter_inters, query, &empty_result);

    // Apply joins
    RowIDs **join_inters = memAlloc(sizeof(RowIDs *), query->num_relations, true, NULL);
    if (!empty_result) {
      // Run through the transformer and optimizer
      optimizeQuery(query, data_statistics, NUM_RELATIONS, true);
      JobScheduler *scheduler = initializeScheduler(JOB_THREADS);
      join_inters = applyJoins(relations, join_inters, filter_inters, query, &empty_result, scheduler);
      destroyScheduler(scheduler);
    }

    batch_results[query_count]->checksums = calculateChecksums(join_inters, relations, query, empty_result);
    batch_results[query_count]->projections = query->num_projections;

    if (join_inters != NULL) {
      for (uint32_t i = 0; i < query->num_relations; i++) {
        if (join_inters[i] != NULL) {
          free(join_inters[i]->ids);
          free(join_inters[i]);
        }
      }
      free(join_inters);
    }

    if (filter_inters) {
      for (uint32_t i = 0; i < query->num_relations; i++) {
        if (filter_inters[i] != NULL) {
          free(filter_inters[i]->ids);
          free(filter_inters[i]);
        }
      }
      free(filter_inters);
    }
    free(query);

    // Make that thread available again
    threads++;

    // Send signal full pool
    pthread_cond_signal(&full_pool);
    pthread_mutex_unlock(&pool_mutex);
  }

  return NULL;
}

int main(void) {
  l2size = getL2CacheSize() / JOB_THREADS;

  initQueue(&thread_pool);

  char path[128];
  char *path_end;

  strcpy(path, WORKLOADS_DIR);
  path_end = path + strlen(path);  // Keep a ref. to the path's end so that we can update it

  // Read all relation file names (note: this **WILL BREAK** if we get more than NUM_RELATIONS file names)
  for (uint32_t i = 0; true; i++) {
    char relation_filename[1024];
    assert(fgets(relation_filename, sizeof(relation_filename), stdin) != NULL);

    relation_filename[strlen(relation_filename) - 1] = '\0';  // Get rid of the trailing newline

    if (strcmp(relation_filename, "Done") == 0) {
      break;
    }

    strcpy(path_end, relation_filename);

    // Load the relation
    relations[i] = loadRelation(path);

    // Gather the statistics in less than 1 second
    data_statistics[i] = gatherStatistics(relations[i]);
  }

  for (int i = 0; i < MAX_RESULTS; i++) {
    batch_results[i] = malloc(sizeof(Results *));
  }

  // Variables used in the loop
  threads = MAX_THREADS;
  uint8_t query_count = 0;

  pthread_t *thread_handles;
  int thread;
  thread_handles = (pthread_t *)malloc(MAX_THREADS * sizeof(pthread_t));
  pthread_mutex_init(&pool_mutex, NULL);

  for (thread = 0; thread < MAX_THREADS; thread++)
    pthread_create(&thread_handles[thread], NULL, query_job, (void *)&thread);

  // Then, read all query batches ('F' is used to separate each batch)
  for (int ch = fgetc(stdin); ch != EOF; ch = fgetc(stdin)) {
    if (ch == 'F' || ch == '\n') {
      pthread_mutex_lock(&pool_mutex);

      // Wait if queue isn't full meaning the threads haven't finished
      while (threads < MAX_THREADS)
        pthread_cond_wait(&full_pool, &pool_mutex);

      pthread_mutex_unlock(&pool_mutex);

      for (int i = 0; i < query_count; i++) {
        printChecksums(stdout, batch_results[i]->checksums, batch_results[i]->projections);
        free(batch_results[i]->checksums);
      }
      query_count = 0;
      fflush(stdout);
      continue;
    }

    // We push the read character back to the input stream because we need it to parse the query
    ungetc(ch, stdin);

    pthread_mutex_lock(&pool_mutex);

    // Wait if there are no available threads
    while (threads < 1)
      pthread_cond_wait(&full_pool, &pool_mutex);

    // Add the new query in the queue
    QueryJob *job = malloc(sizeof(QueryJob));
    job->query = parseQuery(stdin);
    job->index = query_count;
    query_count++;
    enQueue(&thread_pool, job);

    threads--;

    pthread_cond_signal(&empty_pool);
    pthread_mutex_unlock(&pool_mutex);
  }

  fflush(stdout);

  // Free allocated memory
  for (int i = 0; i < MAX_RESULTS; i++) {
    free(batch_results[i]);
  }
  free(thread_pool.pool);
  destroyStats(data_statistics, NUM_RELATIONS);
  for (uint32_t i = 0; i < NUM_RELATIONS; i++) {
    if (relations[i] != NULL) {
      free(relations[i]->columns);
      free(relations[i]);
    }
  }

  return 0;
}
