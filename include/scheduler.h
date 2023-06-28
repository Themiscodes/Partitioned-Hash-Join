#ifndef SCHEDULER_H
#define SCHEDULER_H

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

typedef void (*Job)(void* args);

typedef enum { HISTOGRAM_JOB, BUILDING_JOB, JOIN_JOB } JobKind;

typedef struct job_info {
  Job job;
  void* args;
  JobKind kind;
} JobInfo;

typedef struct job_queue {
  JobInfo** jobs;
  uint64_t capacity;
  uint64_t front;
  uint64_t back;
} JobQueue;

typedef struct job_scheduler {
  JobQueue jobs;
  uint64_t execution_threads;
  pthread_t* thread_ids;

  // This tracks the number of jobs submitted before execute_all_jobs is called.
  // It's useful for synchronizing the threads upon calling wait_all_tasks.
  uint64_t job_count;

  // When set to true, all threads will unblock and terminate their execution.
  bool terminate;

  // Signifies that a thread may enter the critical section and process a batch of jobs.
  bool is_available;

  // Queue synchronization
  pthread_mutex_t queue_mutex;
  pthread_cond_t queue_available;

  // Job count synchronization
  pthread_mutex_t job_count_mutex;
  pthread_cond_t jobs_completed;
} JobScheduler;

JobScheduler* initializeScheduler(uint64_t execution_threads);

void submitJob(JobScheduler* scheduler, JobInfo* job_info);

void executeAllJobs(JobScheduler* scheduler);

void waitAllJobs(JobScheduler* scheduler);

void destroyScheduler(JobScheduler* scheduler);

#endif  // SCHEDULER_H
