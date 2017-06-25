/*
 * Merge sort stream (External sort)
 */

#ifndef SORTEDSTREAM_H
#define SORTEDSTREAM_H

#include <unistd.h> // unlink

#include <vector> // vector
#include <queue> // priority_queue
#include <string> // string
#include <fstream> // ifstream

namespace ch {

    /********************************************
     ************** Declaration *****************
    ********************************************/

    /*
     * pairComparator: comparator template for pair
     */
    template <class DataType_1, class DataType_2, bool greater>
    struct pairComparator {
        bool operator()(const std::pair<DataType_1, DataType_2> & l, const std::pair<DataType_1, DataType_2> & r) {
            if (greater) {
                return l.first > r.first;
            } else {
                return l.first < r.first;
            }
        }
    };

    template <class DataType>
    class SortedStream {
        protected:
            // Files it manages
            std::vector<std::string> _files;

            // Min heap for streams
            std::priority_queue<std::pair<DataType, std::ifstream *>, std::vector<std::pair<DataType, std::ifstream *> >, pairComparator<DataType, std::ifstream *, true> > minHeap;
        public:

            // Move constructor
            SortedStream(std::vector<std::string> && files);

            // Constructor with iterator
            template <class FileIter_T>
            SortedStream(const FileIter_T & begin, const FileIter_T & end);

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

    // Move constructor
    template <class DataType>
    SortedStream<DataType>::SortedStream(std::vector<std::string> && files): _files{std::move(files)} {
        files.clear();
        DataType temp;
        for (const std::string & file: _files) {
            std::ifstream * is = new std::ifstream(file);
            if ((*is) && ((*is) >> temp)) {
                minHeap.push(std::make_pair<DataType, std::ifstream *>(std::move(temp), std::move(is)));
            } else {
                is->close();
                delete is;
            }
        }
    }

    // Constructor with iterator
    template <class DataType>
    template <class FileIter_T>
    SortedStream<DataType>::SortedStream(const FileIter_T & begin, const FileIter_T & end) {
        DataType temp;
        FileIter_T it = begin;
        while (it < end) {
            std::ifstream * is = new std::ifstream(*it);
            _files.push_back(std::move(*it));
            if ((*is) && ((*is) >> temp)) {
                minHeap.push(std::make_pair<DataType, std::ifstream *>(std::move(temp), std::move(is)));
            } else {
                is->close();
                delete is;
            }
            ++it;
        }
    }

    // Destructor
    template <class DataType>
    SortedStream<DataType>::~SortedStream() {
        while (minHeap.size()) {
            std::ifstream * is = minHeap.top().second;
            is->close();
            delete is;
            minHeap.pop();
        }
        for (const std::string & file: _files) {
            unlink(file.c_str());
        }
    }

    // True if the stream has data
    template <class DataType>
    inline bool SortedStream<DataType>::isValid() const {
        return (minHeap.size() != 0);
    }

    // Get data from stream
    template <class DataType>
    bool SortedStream<DataType>::get(DataType & ret) {
        if (!isValid()) {
            return false;
        }
        std::pair<DataType, std::ifstream *> top(std::move(minHeap.top()));
        minHeap.pop();
        ret = std::move(top.first);
        std::ifstream * is = top.second;
        if((*is) >> top.first) {
            minHeap.push(std::move(top));
        } else {
            is->close();
            delete is;
        }
        return true;
    }

}

#endif
