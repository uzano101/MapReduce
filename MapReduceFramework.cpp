#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <algorithm>
#include "vector"
#include "iostream"
#include "Barrier.h"


typedef struct ThreadContext{
    int tid;
    pthread_t thread;
    IntermediateVec* _intermediate;
    void * jobContext;
}ThreadContext;

typedef struct JobContext{
    std::atomic<uint64_t>* _stageAndProgress;
    InputVec _input;
    OutputVec* _output;
    std::vector<IntermediateVec> _queue;
    std::vector<ThreadContext*> _threads;
    const MapReduceClient* _client;
    pthread_mutex_t* _emitMutex;
    pthread_mutex_t* _stateMutex;
    pthread_mutex_t* _reduceMutex;
    int _multiThreadLevel;
    std::atomic<bool>* _isWaiting;
    Barrier* _barrier;
}JobContext;

void updateStage(std::atomic<uint64_t>* _stageAndProgress, stage_t stage, int totalItems){
    uint64_t num = _stageAndProgress->load();
    int current_stage = num >> 62;
    if(current_stage != int(stage)){
        uint64_t newStageAndProgress = 0;
        newStageAndProgress += (int)stage;
        newStageAndProgress = newStageAndProgress << 31;
        newStageAndProgress += totalItems;
        newStageAndProgress = newStageAndProgress << 31;
        _stageAndProgress->operator=(newStageAndProgress);
    }
}

void map(ThreadContext* threadContext){
    JobContext* jobContext = (JobContext*)threadContext->jobContext;
    if(pthread_mutex_lock(jobContext->_stateMutex) != 0){
        std::cout << "system error: pthread mutex lock failed" << std::endl;
        exit(1);
    }
    updateStage(jobContext->_stageAndProgress, MAP_STAGE, jobContext->_input.size());
    if(pthread_mutex_unlock(jobContext->_stateMutex) != 0){
        std::cout << "system error: pthread mutex unlock failed" << std::endl;
        exit(1);
    }
    uint64_t i = (*(jobContext->_stageAndProgress))++;
    i = (i << 33) >> 33;
    while(i < jobContext->_input.size()){
        InputPair inputPair = jobContext->_input.at(i);
        jobContext->_client->map(inputPair.first, inputPair.second, threadContext);
        i = (*(jobContext->_stageAndProgress))++;
        i = (i << 33) >> 33;
    }
}

K2* findMaxKey(JobContext* job){
    K2* maxKey = nullptr;
    for (int i = 0; i < job->_multiThreadLevel; i++) {
        if (!job->_threads[i]->_intermediate->empty()){
            K2* key = job->_threads[i]->_intermediate->back().first;
            if(maxKey == nullptr || maxKey->operator<(*key)){
                maxKey = key;
            }
        }
    }
    return maxKey;
}

void shuffle(JobContext* job){
    if(pthread_mutex_lock(job->_stateMutex) != 0){
        std::cout << "system error: pthread mutex lock failed" << std::endl;
        exit(1);
    }
    int numItems= 0;
    for (int i = 0; i < job->_multiThreadLevel; i++) {
        numItems+= job->_threads[i]->_intermediate->size();
    }
    updateStage(job->_stageAndProgress, SHUFFLE_STAGE, numItems);
    if(pthread_mutex_unlock(job->_stateMutex) != 0){
        std::cout << "system error: pthread mutex unlock failed" << std::endl;
        exit(1);
    }
    std::vector<IntermediatePair> sequence;
    uint64_t i = (*(job->_stageAndProgress))++;
    i = (i << 33) >> 33;
    while ((int)i < numItems){
        K2* maxKey = findMaxKey(job);
        for (int j = 0; j < job->_multiThreadLevel; j++) {
            while (!job->_threads[j]->_intermediate->empty()
            && !(job->_threads[j]->_intermediate->back().first->operator<(*maxKey))){
                IntermediatePair value = job->_threads[j]->_intermediate->back();
                job->_threads[j]->_intermediate->pop_back();
                sequence.push_back(value);
                i = (*(job->_stageAndProgress))++;
                i = (i << 33) >> 33;
            }
        }
        job->_queue.push_back(sequence);
        sequence.clear();
    }
}

void reduce(JobContext* job){
    if(pthread_mutex_lock(job->_stateMutex) != 0){
        std::cout << "system error: pthread mutex lock failed" << std::endl;
        exit(1);
    }
    updateStage(job->_stageAndProgress, REDUCE_STAGE, job->_queue.size());
    if(pthread_mutex_unlock(job->_stateMutex) != 0){
        std::cout << "system error: pthread mutex unlock failed" << std::endl;
        exit(1);
    }
    uint64_t i = (*(job->_stageAndProgress))++;
    i = (i << 33) >> 33;
    while (i < job->_queue.size()){
        if(pthread_mutex_lock(job->_reduceMutex) != 0){
            std::cout << "system error: pthread mutex lock failed" << std::endl;
            exit(1);
        }
        IntermediateVec intermediateVec = job->_queue.at(i);
        if(pthread_mutex_unlock(job->_reduceMutex) != 0){
            std::cout << "system error: pthread mutex unlock failed" << std::endl;
            exit(1);
        }
        job->_client->reduce(&intermediateVec, job);
        i = (*(job->_stageAndProgress))++;
        i = (i << 33) >> 33;
    }
}

bool compare(IntermediatePair p1, IntermediatePair p2){
    return p1.first->operator<(*p2.first);
}

void *runThread(void* context) {
    ThreadContext * threadContext = (ThreadContext*)context;
    JobContext * jobContext = (JobContext*)threadContext->jobContext;

    map(threadContext);

    std::sort(threadContext->_intermediate->begin(), threadContext->_intermediate->end(), compare);

    jobContext->_barrier->barrier();

    if(threadContext->tid == 0){
        shuffle(jobContext);
    }
    jobContext->_barrier->barrier();

    reduce(jobContext);
    return nullptr;
}

void emit2 (K2* key, V2* value, void* context){
    ThreadContext* threadContext = (ThreadContext*)context;
    threadContext->_intermediate->push_back(IntermediatePair(key, value));
}

void emit3 (K3* key, V3* value, void* context){
    JobContext* jobContext = (JobContext*) context;
    if(pthread_mutex_lock(jobContext->_emitMutex) != 0){
        std::cout << "system error: pthread mutex lock failed" << std::endl;
        exit(1);
    }
    jobContext->_output->push_back(OutputPair(key, value));
    if(pthread_mutex_unlock(jobContext->_emitMutex) != 0){
        std::cout << "system error: pthread mutex lock failed" << std::endl;
        exit(1);
    }
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    JobContext* job = new JobContext();
    job->_input = inputVec;
    job->_output = &outputVec;
    job->_multiThreadLevel = multiThreadLevel;
    job->_stageAndProgress= new std::atomic<uint64_t>(0);
    job->_threads = std::vector<ThreadContext*>(multiThreadLevel);
    job->_client = &client;
    job->_isWaiting= new std::atomic<bool>(false);
    job->_emitMutex =  new pthread_mutex_t(PTHREAD_MUTEX_INITIALIZER);
    job->_stateMutex =  new pthread_mutex_t(PTHREAD_MUTEX_INITIALIZER);
    job->_reduceMutex =  new pthread_mutex_t(PTHREAD_MUTEX_INITIALIZER);
    job->_barrier = new Barrier(multiThreadLevel);
    for(int i = 0 ; i < multiThreadLevel ; i++){
        job->_threads[i] = new ThreadContext();
        job->_threads[i]->tid = i;
        job->_threads[i]->_intermediate = new IntermediateVec();
        job->_threads[i]->jobContext = job;
        int res = pthread_create(&job->_threads[i]->thread, nullptr, runThread, job->_threads[i]);
        if(res != 0){
            std::cout << "system error: pthread create failed"<< std::endl;
            exit(1);
        }
    }
    return static_cast<JobHandle>(job);
}

void waitForJob(JobHandle job){
    JobContext* jobContext = (JobContext*)job;
    if(!jobContext->_isWaiting->load()) {
        jobContext->_isWaiting->operator=(true);
        for (int i=0; i < jobContext->_multiThreadLevel; i++){
            int res = pthread_join(jobContext->_threads[i]->thread, nullptr);
            if (res != 0) {
                std::cout << "system error: pthread join failed"<< std::endl;
                exit(1);
            }
        }
    }
}

void getJobState(JobHandle job, JobState* state){
    JobContext* jobContext = (JobContext*)job;
    uint64_t number = jobContext->_stageAndProgress->load();
    int stage = number >> 62;
    state->stage = (stage_t)stage;
    uint64_t firstBlock = (number>> 31) <<31;
    uint64_t secondBlock = (number >> 62) <<62;
    float progress = number - firstBlock;
    float totalItems = (firstBlock-secondBlock) >> 31;
    float percentage;
    if(totalItems == 0){
        percentage = 0;
    }else{
        percentage = (progress/totalItems)*100;
    }
    state->percentage = percentage > 100 ? 100 : percentage;
}

void closeJobHandle(JobHandle job){
    waitForJob(job);
    JobContext* jobContext = (JobContext*)job;
    delete jobContext->_barrier;
    for (int i = 0; i< jobContext->_multiThreadLevel; i++){
        delete jobContext->_threads[i]->_intermediate;
        delete jobContext->_threads[i];
    }
    delete jobContext->_stageAndProgress;
    delete jobContext->_isWaiting;
    pthread_mutex_destroy(jobContext->_emitMutex);
    delete jobContext->_emitMutex;
    pthread_mutex_destroy(jobContext->_stateMutex);
    delete jobContext->_stateMutex;
    pthread_mutex_destroy(jobContext->_reduceMutex);
    delete jobContext->_reduceMutex;
    delete (JobContext*) job;
}



