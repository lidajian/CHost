/*
 * Merge sort stream (External sort)
 */

#ifndef SORTEDSTREAM_H
#define SORTEDSTREAM_H

#include <unistd.h> // unlink

#include <vector>   // vector
#include <queue>    // priority_queue
#include <string>   // string
#include <fstream>  // ifstream
#include <memory>   // shared_ptr

namespace ch {

    /********************************************
     ************** Declaration *****************
    ********************************************/

    template <typename DataType>
    class SortedStream {

        private:
            struct PairGreaterComparator {
                bool operator()(const std::pair<DataType, std::shared_ptr<std::ifstream> > & l,
                                const std::pair<DataType, std::shared_ptr<std::ifstream> > & r) {
                    return l.first > r.first;
                }
            };

        protected:

            // Files it manages
            std::vector<std::string> _files;

            // Min heap for streams
            std::priority_queue<std::pair<DataType, std::shared_ptr<std::ifstream> >,
                std::vector<std::pair<DataType, std::shared_ptr<std::ifstream> > >,
                PairGreaterComparator> minHeap;

        public:

            // Constructor
            SortedStream(std::vector<std::string> && files);

            // Constructor with iterator
            template <typename FileIter_T>
            SortedStream(const FileIter_T & begin, const FileIter_T & end);

            // Copy constructor (deleted)
            SortedStream(const SortedStream<DataType> & ) = delete;

            // Move constructor
            SortedStream(SortedStream<DataType> && o);

            // Copy assignment (deleted)
            SortedStream<DataType> & operator = (const SortedStream<DataType> &) = delete;

            // Move assignment
            SortedStream<DataType> & operator = (SortedStream<DataType> && o);

            // Destructor
            ~SortedStream();

            // Open stream
            bool open();

            // True if the stream has data
            bool isValid() const;

            // Get data from stream
            bool get(DataType & ret);
    };

    /********************************************
     ************ Implementation ****************
    ********************************************/

    // Constructor
    template <typename DataType>
    SortedStream<DataType>::SortedStream(std::vector<std::string> && files)
    : _files{std::move(files)} {

        files.clear();

    }

    // Constructor with iterator
    template <typename DataType>
    template <typename FileIter_T>
    SortedStream<DataType>::SortedStream(const FileIter_T & begin, const FileIter_T & end) {

        FileIter_T it = begin;

        while (it < end) {
            _files.push_back(std::move(*it));
            ++it;
        }

    }

    // Move constructor
    template <typename DataType>
    SortedStream<DataType>::SortedStream(SortedStream<DataType> && o)
    : _files{std::move(o._files)} {

        o._files.clear();
        o.minHeap.swap(minHeap);

    }

    // Move assignment
    template <typename DataType>
    SortedStream<DataType> & SortedStream<DataType>::operator = (SortedStream<DataType> && o) {

        _files = std::move(o._files);
        o.minHeap.swap(minHeap);
        return *this;

    }

    // Destructor
    template <typename DataType>
    SortedStream<DataType>::~SortedStream() {

        while (!minHeap.empty()) {
            minHeap.pop();
        }

        for (const std::string & file: _files) {
            unlink(file.c_str());
        }

    }

    // Open stream
    template <typename DataType>
    bool SortedStream<DataType>::open() {

        DataType temp;

        for (const std::string & file: _files) {
            std::shared_ptr<std::ifstream> is{new std::ifstream{file}};
            if ((*is) && ((*is) >> temp)) {
                minHeap.push(std::make_pair<DataType, std::shared_ptr<std::ifstream> >
                                (
                                    std::move(temp),
                                    std::move(is)
                                )
                            );
            }
        }

        return isValid();

    }

    // True if the stream has data
    template <typename DataType>
    inline bool SortedStream<DataType>::isValid() const {

        return (minHeap.size() != 0);

    }

    // Get data from stream
    template <typename DataType>
    bool SortedStream<DataType>::get(DataType & ret) {

        if (!isValid()) {
            return false;
        }

        std::pair<DataType, std::shared_ptr<std::ifstream> > top{std::move(minHeap.top())};
        minHeap.pop();
        ret = std::move(top.first);

        if((*(top.second)) >> top.first) {
            minHeap.push(std::move(top));
        }

        return true;

    }

}

#endif
