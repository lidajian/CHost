#include "threadPool.hpp"

namespace ch {

    // Launch nThreads number of threads
    void ThreadPool::launchThreads(size_t nThreads) {
        while (nThreads--) {
            std::shared_ptr<std::atomic_bool> stopFlag = std::make_shared<std::atomic_bool>(false);
            stopFlags.push_back(stopFlag);
            auto f = [this, stopFlag]() {
                std::function<void()> * taskPtr;
                bool hasTask = this->q.pop(taskPtr);
                std::atomic_bool & _stopFlag = *stopFlag;
                while (true) {
                    while (hasTask) {
                        std::unique_ptr<std::function<void()> > task{taskPtr};
                        (*task)();
                        if (_stopFlag) {
                            return;
                        } else {
                            hasTask = this->q.pop(taskPtr);
                        }
                    }
                    std::unique_lock<std::mutex> holder{this->lock};
                    ++this->nIdleThreads;
                    this->cv.wait(holder, [this, &taskPtr, &hasTask, &_stopFlag](){
                            hasTask = this->q.pop(taskPtr);
                            return hasTask || this->isTerminated || _stopFlag;
                            });
                    --this->nIdleThreads;
                    if (!hasTask) {
                        return;
                    }
                }
            };
            threads.emplace_back(new std::thread{f});
        }
    }

    // Clear the task queue
    void ThreadPool::clearQueue() {
        std::function<void()> * task;
        while (q.pop(task)) {
            delete task;
        }
    }

    // Default constructor
    ThreadPool::ThreadPool(): nIdleThreads{0}, isTerminated{false} {}

    // From value
    ThreadPool::ThreadPool(size_t nThreads): nIdleThreads{0}, isTerminated{false} {
        launchThreads(nThreads);
    }

    // Destructor
    ThreadPool::~ThreadPool() {
        stop();
    }

    // Resize the thread pool
    bool ThreadPool::resize(size_t nThreads) {
        if (!isTerminated) {
            size_t oldNThreads = threads.size();
            if (oldNThreads < nThreads) {
                launchThreads(nThreads - oldNThreads);
            } else if (oldNThreads > nThreads) {
                for (size_t i = nThreads; i < oldNThreads; ++i) {
                    *(stopFlags[i]) = true;
                    threads[i]->detach();
                }
                {
                    std::lock_guard<std::mutex> holder{lock};
                    cv.notify_all(); // noexcept
                }
                threads.resize(nThreads);
                stopFlags.resize(nThreads);
            }
            return true;
        } else {
            return false;
        }
    }

    // Stop the thread pool
    void ThreadPool::stop() {
        if (isTerminated) {
            return;
        }
        isTerminated = true;
        {
            std::lock_guard<std::mutex> holder{lock};
            cv.notify_all(); // noexcept
        }
        for (size_t i = 0; i < threads.size(); ++i) {
            if (threads[i]->joinable()) {
                threads[i]->join();
            }
        }
        clearQueue();
        threads.clear();
        stopFlags.clear();
    }
}
