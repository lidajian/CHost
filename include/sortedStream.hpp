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

    /*
     * pairComparator: comparator template for pair
     */
    template <typename DataType_1, typename DataType_2, bool greater>
    struct pairComparator {
        bool operator()(const std::pair<DataType_1, DataType_2> & l,
                        const std::pair<DataType_1, DataType_2> & r) {

            if (greater) {
                return l.first > r.first;
            } else {
                return l.first < r.first;
            }

        }
    };

    template <typename DataType>
    class SortedStream {

        protected:

            // Files it manages
            std::vector<std::string> _files;

            // Min heap for streams
            std::priority_queue<std::pair<DataType, std::shared_ptr<std::ifstream> >,
                std::vector<std::pair<DataType, std::shared_ptr<std::ifstream> > >,
                pairComparator<DataType, std::shared_ptr<std::ifstream>, true> > minHeap;

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

    }

    // Constructor with iterator
    template <typename DataType>
    template <typename FileIter_T>
    SortedStream<DataType>::SortedStream(const FileIter_T & begin, const FileIter_T & end) {

        DataType temp;
        FileIter_T it = begin;

        while (it < end) {
            std::shared_ptr<std::ifstream> is{new std::ifstream{*it}};
            _files.push_back(std::move(*it));
            if ((*is) && ((*is) >> temp)) {
                minHeap.push(std::make_pair<DataType, std::shared_ptr<std::ifstream> >
                                (
                                    std::move(temp),
                                    std::move(is)
                                )
                            );
            }
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
