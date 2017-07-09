/*
 * Queue with mutex
 */

#ifndef BLOCKEDQUEUE_H
#define BLOCKEDQUEUE_H

#include <queue> // queue
#include <mutex> // mutex

namespace ch {

    /********************************************
     ************** Declaration *****************
    ********************************************/

    template <typename T>
    class BlockedQueue {

        protected:

            // Data structure using STL queue
            std::queue<T> q;

            // Lock for the queue
            std::mutex lock;

        public:

            // Constructor
            BlockedQueue();

            // Copy constructor (deleted)
            BlockedQueue(const BlockedQueue<T> &) = delete;

            // Move constructor (deleted)
            BlockedQueue(BlockedQueue<T> &&) = delete;

            // Copy assignment (deleted)
            BlockedQueue<T> & operator = (const BlockedQueue<T> &) = delete;

            // Move assignment (deleted)
            BlockedQueue<T> & operator = (BlockedQueue<T> &&) = delete;

            // Destructor
            ~BlockedQueue();

            // Copy push
            void push(const T & v);

            // Move push (rvalue)
            void push(T && v);

            // Pop
            bool pop(T & v);

            // True if queue is empty
            bool empty() const;

            // Clear the queue
            void clear();
    };

    /********************************************
     ************ Implementation ****************
    ********************************************/

    // Constructor
    template <typename T>
    BlockedQueue<T>::BlockedQueue() {}

    // Destructor
    template <typename T>
    BlockedQueue<T>::~BlockedQueue() {}

    // Copy push
    template <typename T>
    void BlockedQueue<T>::push(const T & v) {

        std::lock_guard<std::mutex> holder{lock};

        q.push(v);

    }

    // Move push (rvalue)
    template <typename T>
    void BlockedQueue<T>::push(T && v) {

        std::lock_guard<std::mutex> holder{lock};

        q.push(std::move(v));

    }

    // Pop
    template <typename T>
    bool BlockedQueue<T>::pop(T & v) {

        std::lock_guard<std::mutex> holder{lock};

        if (q.empty()) {
            return false;
        }

        v = q.front();
        q.pop();
        return true;

    }

    // True if queue is empty
    template <typename T>
    bool BlockedQueue<T>::empty() const {

        std::lock_guard<std::mutex> holder{lock};

        return q.empty();

    }

    // Clear the queue
    template <typename T>
    void BlockedQueue<T>::clear() {

        std::lock_guard<std::mutex> holder{lock};

        while (!q.empty()) {
            q.pop();
        }

    }
}

#endif
