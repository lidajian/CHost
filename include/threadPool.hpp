/*
 * Thread pool
 */

#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <mutex> // std::mutex
#include <condition_variable> // condition_variable
#include <future> // std::future
#include <functional> // function

#include "blockedQueue.hpp" // BlockedQueue

namespace ch {

    /********************************************
     ************** Declaration *****************
    ********************************************/

    class ThreadPool {
        protected:
            std::atomic_uint nIdleThreads; // Number of idle threads in the thread pool
            std::atomic_bool isTerminated;      // True if all threads are terminated

            std::vector<std::unique_ptr<std::thread> > threads; // Threads in the pool
            std::vector<std::shared_ptr<std::atomic_bool> > stopFlags; // True if we are to stop a thread
            BlockedQueue<std::function<void(void)> *> q; // Functions to be executed

            std::mutex lock;
            std::condition_variable cv;

            // Launch nThreads number of threads
            void launchThreads(unsigned int nThreads);

            // Clear the task queue
            void clearQueue();

        public:
            // Default constructor
            ThreadPool();

            // Copy constructor (deleted)
            ThreadPool(const ThreadPool &) = delete;

            // Move constructor (deleted)
            ThreadPool(ThreadPool &&) = delete;

            // From value
            ThreadPool(unsigned int nThreads);

            // Destructor
            ~ThreadPool();

            // Resize the thread pool
            bool resize(unsigned int nThreads);

            // Stop the thread pool
            void stop();

            // Add a task without parameter
            template <typename FUNCTION>
            auto addTask(FUNCTION && f) -> std::future<decltype(f())>;

            // Add a task with parameter
            template <typename FUNCTION, typename... ARGS>
            auto addTask(FUNCTION && f, ARGS && ... args) -> std::future<decltype(f(args...))>;
    };

    /********************************************
     ************ Implementation ****************
    ********************************************/

    // Launch nThreads number of threads
    void ThreadPool::launchThreads(unsigned int nThreads) {
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
            threads.emplace_back(new std::thread(f));
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
    ThreadPool::ThreadPool(unsigned int nThreads): nIdleThreads{0}, isTerminated{false} {
        launchThreads(nThreads);
    }

    // Destructor
    ThreadPool::~ThreadPool() {
        stop();
    }

    // Resize the thread pool
    bool ThreadPool::resize(unsigned int nThreads) {
        if (!isTerminated) {
            unsigned int oldNThreads = threads.size();
            if (oldNThreads < nThreads) {
                launchThreads(nThreads - oldNThreads);
            } else if (oldNThreads > nThreads) {
                for (unsigned int i = nThreads; i < oldNThreads; ++i) {
                    *(stopFlags[i]) = true;
                    threads[i]->detach();
                }
                {
                    std::lock_guard<std::mutex> holder(lock);
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
            std::lock_guard<std::mutex> holder(lock);
            cv.notify_all(); // noexcept
        }
        for (unsigned int i = 0; i < threads.size(); ++i) {
            if (threads[i]->joinable()) {
                threads[i]->join();
            }
        }
        clearQueue();
        threads.clear();
        stopFlags.clear();
    }

    // Add a task without parameter
    template <typename FUNCTION>
    auto ThreadPool::addTask(FUNCTION && f) -> std::future<decltype(f())> {
        auto task = std::make_shared<std::packaged_task<decltype(f())()> >(std::forward<FUNCTION>(f));
        q.push(new std::function<void()>([task](){
                (*task)();
                }));
        std::lock_guard<std::mutex> holder{lock};
        cv.notify_one();
        return task->get_future();
    }

    // Add a task with parameter
    template <typename FUNCTION, typename... ARGS>
    auto ThreadPool::addTask(FUNCTION && f, ARGS && ... args) -> std::future<decltype(f(args...))> {
        auto task = std::make_shared<std::packaged_task<decltype(f(args...))()> >(
                std::bind(std::forward<FUNCTION>(f), std::forward<ARGS>(args)...)
                );
        q.push(new std::function<void()>([task](){
                (*task)();
                }));
        std::lock_guard<std::mutex> holder{lock};
        cv.notify_one();
        return task->get_future();
    }

}

#endif
