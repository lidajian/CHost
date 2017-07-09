/*
 * Thread pool
 */

#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <atomic>             // atomic_xxx
#include <mutex>              // mutex, unique_lock
#include <condition_variable> // condition_variable
#include <future>             // future, packaged_task
#include <functional>         // function, bind
#include <thread>             // thread
#include <memory>             // unique_ptr, shared_ptr

#include "blockedQueue.hpp"   // BlockedQueue

namespace ch {

    /********************************************
     ************** Declaration *****************
    ********************************************/

    class ThreadPool {

        protected:

            // Number of idle threads in the thread pool
            std::atomic_uint nIdleThreads;

            // True if all threads are terminated
            std::atomic_bool isTerminated;

            // Threads in the pool
            std::vector<std::unique_ptr<std::thread> > threads;

            // True if we are to stop a thread
            std::vector<std::shared_ptr<std::atomic_bool> > stopFlags;

            // Functions to be executed
            BlockedQueue<std::function<void(void)> *> q;

            // Mutex for the Condition variable
            std::mutex lock;

            // Condition variable
            std::condition_variable cv;

            // Launch nThreads number of threads
            void launchThreads(size_t nThreads);

            // Clear the task queue
            void clearQueue();

        public:

            // Default constructor
            ThreadPool();

            // Copy constructor (deleted)
            ThreadPool(const ThreadPool &) = delete;

            // Move constructor (deleted)
            ThreadPool(ThreadPool &&) = delete;

            // Copy assignment (deleted)
            ThreadPool & operator = (const ThreadPool &) = delete;

            // Move assignment (deleted)
            ThreadPool & operator = (ThreadPool &&) = delete;

            // From value
            ThreadPool(size_t nThreads);

            // Destructor
            ~ThreadPool();

            // Resize the thread pool
            bool resize(size_t nThreads);

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

    // Add a task without parameter
    template <typename FUNCTION>
    auto ThreadPool::addTask(FUNCTION && f) -> std::future<decltype(f())> {

        auto task = std::make_shared<std::packaged_task<decltype(f())()> >
                                                    (std::forward<FUNCTION>(f));

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
