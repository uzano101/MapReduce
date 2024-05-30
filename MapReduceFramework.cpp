#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <algorithm>
#include <vector>
#include <iostream>
#include "Barrier/Barrier.h"

// Structure to hold thread-specific context
typedef struct ThreadContext {
    int tid;
    pthread_t thread;
    IntermediateVec *intermediate;
    void *jobContext;
} ThreadContext;

// Structure to hold job-specific context
typedef struct JobContext {
    std::atomic<uint64_t> *stageAndProgress;
    InputVec input;
    OutputVec *output;
    std::vector<IntermediateVec> queue;
    std::vector<ThreadContext *> threads;
    const MapReduceClient *client;
    pthread_mutex_t *emitMutex;
    pthread_mutex_t *stateMutex;
    pthread_mutex_t *reduceMutex;
    int multiThreadLevel;
    std::atomic<bool> *isWaiting;
    Barrier *barrier;
} JobContext;

// Function to update the stage and progress of the job
void updateStage(std::atomic<uint64_t> *stageAndProgress, stage_t stage, int totalItems) {
    uint64_t num = stageAndProgress->load();
    int current_stage = num >> 62;
    if (current_stage != int(stage)) {
        uint64_t newStageAndProgress = 0;
        newStageAndProgress += static_cast<int>(stage);
        newStageAndProgress = newStageAndProgress << 31;
        newStageAndProgress += totalItems;
        newStageAndProgress = newStageAndProgress << 31;
        *stageAndProgress = newStageAndProgress;
    }
}

// Function to perform the map operation
void map(ThreadContext *threadContext) {
    JobContext *jobContext = static_cast<JobContext *>(threadContext->jobContext);

    pthread_mutex_lock(jobContext->stateMutex);
    updateStage(jobContext->stageAndProgress, MAP_STAGE, jobContext->input.size());
    pthread_mutex_unlock(jobContext->stateMutex);

    uint64_t i = (*jobContext->stageAndProgress)++;
    i = (i << 33) >> 33;
    while (i < jobContext->input.size()) {
        InputPair inputPair = jobContext->input.at(i);
        jobContext->client->map(inputPair.first, inputPair.second, threadContext);
        i = (*jobContext->stageAndProgress)++;
        i = (i << 33) >> 33;
    }
}

// Function to find the maximum key in the intermediate data
K2 *findMaxKey(JobContext *job) {
    K2 *maxKey = nullptr;
    for (int i = 0; i < job->multiThreadLevel; i++) {
        if (!job->threads[i]->intermediate->empty()) {
            K2 *key = job->threads[i]->intermediate->back().first;
            if (maxKey == nullptr || *maxKey < *key) {
                maxKey = key;
            }
        }
    }
    return maxKey;
}

// Function to perform the shuffle operation
void shuffle(JobContext *job) {
    pthread_mutex_lock(job->stateMutex);
    int numItems = 0;
    for (int i = 0; i < job->multiThreadLevel; i++) {
        numItems += job->threads[i]->intermediate->size();
    }
    updateStage(job->stageAndProgress, SHUFFLE_STAGE, numItems);
    pthread_mutex_unlock(job->stateMutex);

    std::vector<IntermediatePair> sequence;
    uint64_t i = (*job->stageAndProgress)++;
    i = (i << 33) >> 33;
    while (static_cast<int>(i) < numItems) {
        K2 *maxKey = findMaxKey(job);
        for (int j = 0; j < job->multiThreadLevel; j++) {
            while (!job->threads[j]->intermediate->empty() &&
                   !(job->threads[j]->intermediate->back().first->operator<(*maxKey))) {
                IntermediatePair value = job->threads[j]->intermediate->back();
                job->threads[j]->intermediate->pop_back();
                sequence.push_back(value);
                i = (*job->stageAndProgress)++;
                i = (i << 33) >> 33;
            }
        }
        job->queue.push_back(sequence);
        sequence.clear();
    }
}

// Function to perform the reduce operation
void reduce(JobContext *job) {
    pthread_mutex_lock(job->stateMutex);
    updateStage(job->stageAndProgress, REDUCE_STAGE, job->queue.size());
    pthread_mutex_unlock(job->stateMutex);

    uint64_t i = (*job->stageAndProgress)++;
    i = (i << 33) >> 33;
    while (i < job->queue.size()) {
        pthread_mutex_lock(job->reduceMutex);
        IntermediateVec intermediateVec = job->queue.at(i);
        pthread_mutex_unlock(job->reduceMutex);
        job->client->reduce(&intermediateVec, job);
        i = (*job->stageAndProgress)++;
        i = (i << 33) >> 33;
    }
}

// Comparator function to compare two intermediate pairs
bool compare(IntermediatePair p1, IntermediatePair p2) {
    return p1.first->operator<(*p2.first);
}

// Function to run the thread
void *runThread(void *context) {
    auto *threadContext = static_cast<ThreadContext *>(context);
    auto *jobContext = static_cast<JobContext *>(threadContext->jobContext);

    map(threadContext);

    std::sort(threadContext->intermediate->begin(), threadContext->intermediate->end(), compare);

    jobContext->barrier->barrier();

    if (threadContext->tid == 0) {
        shuffle(jobContext);
    }
    jobContext->barrier->barrier();

    reduce(jobContext);
    return nullptr;
}

// Function to emit intermediate key-value pairs
void emit2(K2 *key, V2 *value, void *context) {
    auto *threadContext = static_cast<ThreadContext *>(context);
    threadContext->intermediate->emplace_back(key, value);
}

// Function to emit final key-value pairs
void emit3(K3 *key, V3 *value, void *context) {
    auto *jobContext = static_cast<JobContext *>(context);
    pthread_mutex_lock(jobContext->emitMutex);
    jobContext->output->emplace_back(key, value);
    pthread_mutex_unlock(jobContext->emitMutex);
}

// Function to start the MapReduce job
JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel) {
    auto *job = new JobContext();
    job->input = inputVec;
    job->output = &outputVec;
    job->multiThreadLevel = multiThreadLevel;
    job->stageAndProgress = new std::atomic<uint64_t>(0);
    job->threads = std::vector<ThreadContext *>(multiThreadLevel);
    job->client = &client;
    job->isWaiting = new std::atomic<bool>(false);
    job->emitMutex = new pthread_mutex_t(PTHREAD_MUTEX_INITIALIZER);
    job->stateMutex = new pthread_mutex_t(PTHREAD_MUTEX_INITIALIZER);
    job->reduceMutex = new pthread_mutex_t(PTHREAD_MUTEX_INITIALIZER);
    job->barrier = new Barrier(multiThreadLevel);
    for (int i = 0; i < multiThreadLevel; i++) {
        job->threads[i] = new ThreadContext();
        job->threads[i]->tid = i;
        job->threads[i]->intermediate = new IntermediateVec();
        job->threads[i]->jobContext = job;
        int res = pthread_create(&job->threads[i]->thread, nullptr, runThread, job->threads[i]);
        if (res != 0) {
            std::cerr << "system error: pthread create failed" << std::endl;
            exit(1);
        }
    }
    return static_cast<JobHandle>(job);
}

// Function to wait for the job to complete
void waitForJob(JobHandle job) {
    auto *jobContext = static_cast<JobContext *>(job);
    if (!jobContext->isWaiting->load()) {
        jobContext->isWaiting->store(true);
        for (int i = 0; i < jobContext->multiThreadLevel; i++) {
            int res = pthread_join(jobContext->threads[i]->thread, nullptr);
            if (res != 0) {
                std::cerr << "system error: pthread join failed" << std::endl;
                exit(1);
            }
        }
    }
}

// Function to get the state of the job
void getJobState(JobHandle job, JobState *state) {
    auto *jobContext = static_cast<JobContext *>(job);
    uint64_t number = jobContext->stageAndProgress->load();
    int stage = number >> 62;
    state->stage = static_cast<stage_t>(stage);
    uint64_t firstBlock = (number >> 31) << 31;
    uint64_t secondBlock = (number >> 62) << 62;
    float progress = number - firstBlock;
    float totalItems = (firstBlock - secondBlock) >> 31;
    float percentage = totalItems == 0 ? 0 : (progress / totalItems) * 100;
    state->percentage = std::min(percentage, 100.0f);
}

// Function to close the job handle and clean up resources
void closeJobHandle(JobHandle job) {
    waitForJob(job);
    auto *jobContext = static_cast<JobContext *>(job);
    delete jobContext->barrier;
    for (int i = 0; i < jobContext->multiThreadLevel; i++) {
        delete jobContext->threads[i]->intermediate;
        delete jobContext->threads[i];
    }
    delete jobContext->stageAndProgress;
    delete jobContext->isWaiting;
    pthread_mutex_destroy(jobContext->emitMutex);
    delete jobContext->emitMutex;
    pthread_mutex_destroy(jobContext->stateMutex);
    delete jobContext->stateMutex;
    pthread_mutex_destroy(jobContext->reduceMutex);
    delete jobContext->reduceMutex;
    delete jobContext;
}
