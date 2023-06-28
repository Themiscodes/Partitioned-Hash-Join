#include "scheduler.h"

#include <assert.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "helpers.h"
#include "phjoin.h"

#define INITIAL_CAPACITY 1024

static void initializeQueue(JobQueue* queue, uint64_t capacity) {
  queue->jobs = memAlloc(sizeof(JobInfo*), capacity, true, NULL);
  queue->capacity = capacity;
  queue->front = queue->back = 0;
}

static JobInfo* dequeue(JobQueue* queue) {
  JobInfo* ret = queue->jobs[queue->front];

  if (ret != NULL) {
    queue->jobs[queue->front] = NULL;
    queue->front = (queue->front + 1) % queue->capacity;
  }

  return ret;
}

static void enqueue(JobQueue* queue, JobInfo* job_info) {
  int64_t num_jobs = queue->back - queue->front + 1;

  if (num_jobs < 0) {
    num_jobs = -num_jobs;
  }

  if ((uint64_t)num_jobs >= queue->capacity) {
    queue->capacity *= 2;
    queue->jobs = memAlloc(sizeof(JobInfo*), queue->capacity, false, queue->jobs);

    memset(&queue->jobs[queue->back + 1], 0, num_jobs);
  }

  uint64_t insert_index = queue->back;
  queue->back = (queue->back + 1) % queue->capacity;
  queue->jobs[insert_index] = job_info;
}

static void* schedulerLoop(void* scheduler_) {
  JobScheduler* scheduler = scheduler_;

  while (true) {
    pthread_mutex_lock(&scheduler->queue_mutex);
    while (!scheduler->is_available && !scheduler->terminate) {
      pthread_cond_wait(&scheduler->queue_available, &scheduler->queue_mutex);
    }

    if (scheduler->terminate) {
      pthread_mutex_unlock(&scheduler->queue_mutex);
      pthread_cond_broadcast(&scheduler->queue_available);

      break;
    }

    JobInfo* job_info = dequeue(&scheduler->jobs);
    bool empty_queue = job_info == NULL;

    if (empty_queue) {
      scheduler->is_available = false;
    }

    pthread_mutex_unlock(&scheduler->queue_mutex);
    pthread_cond_broadcast(&scheduler->queue_available);

    if (!empty_queue) {
      switch (job_info->kind) {
        case HISTOGRAM_JOB:
          ((HistogramJob)job_info->job)(job_info->args);
          break;

        case BUILDING_JOB:
          ((BuildingJob)job_info->job)(job_info->args);
          break;

        case JOIN_JOB:
          ((JoinJob)job_info->job)(job_info->args);
          break;

        default:
          assert(false);  // This shouldn't be called
      }

      pthread_mutex_lock(&scheduler->job_count_mutex);

      if (--(scheduler->job_count) == 0) {
        scheduler->is_available = false;
      }

      pthread_mutex_unlock(&scheduler->job_count_mutex);
      pthread_cond_signal(&scheduler->jobs_completed);
    }

    if (job_info != NULL) {
      free(job_info->args);
      free(job_info);
    }
  }

  return NULL;
}

JobScheduler* initializeScheduler(uint64_t execution_threads) {
  JobScheduler* scheduler = memAlloc(sizeof(JobScheduler), 1, false, NULL);
  scheduler->execution_threads = execution_threads;
  scheduler->thread_ids = memAlloc(sizeof(pthread_t), execution_threads, true, NULL);

  initializeQueue(&scheduler->jobs, INITIAL_CAPACITY);

  scheduler->job_count = 0;

  scheduler->terminate = false;
  scheduler->is_available = false;

  pthread_mutex_init(&scheduler->queue_mutex, NULL);
  pthread_cond_init(&scheduler->queue_available, NULL);

  pthread_mutex_init(&scheduler->job_count_mutex, NULL);
  pthread_cond_init(&scheduler->jobs_completed, NULL);

  for (uint64_t i = 0; i < execution_threads; i++) {
    pthread_create(&scheduler->thread_ids[i], NULL, schedulerLoop, scheduler);
  }

  return scheduler;
}

void submitJob(JobScheduler* scheduler, JobInfo* job_info) {
  scheduler->job_count++;
  enqueue(&scheduler->jobs, job_info);
}

void executeAllJobs(JobScheduler* scheduler) {
  scheduler->is_available = true;
  pthread_cond_broadcast(&scheduler->queue_available);
}

void waitAllJobs(JobScheduler* scheduler) {
  pthread_mutex_lock(&scheduler->job_count_mutex);
  while (scheduler->job_count > 0) {
    pthread_cond_wait(&scheduler->jobs_completed, &scheduler->job_count_mutex);
  }

  scheduler->job_count = 0;
  scheduler->is_available = false;
  scheduler->jobs.front = scheduler->jobs.back = 0;

  pthread_mutex_unlock(&scheduler->job_count_mutex);
}

void destroyScheduler(JobScheduler* scheduler) {
  scheduler->terminate = true;

  pthread_cond_broadcast(&scheduler->queue_available);

  // Let every thread in the pool know that they can stop working
  for (uint64_t i = 0; i < scheduler->execution_threads; i++) {
    pthread_join(scheduler->thread_ids[i], NULL);
  }

  pthread_mutex_destroy(&scheduler->queue_mutex);
  pthread_cond_destroy(&scheduler->queue_available);

  pthread_mutex_destroy(&scheduler->job_count_mutex);
  pthread_cond_destroy(&scheduler->jobs_completed);

  free((scheduler->jobs).jobs);
  free(scheduler->thread_ids);
  free(scheduler);
}
