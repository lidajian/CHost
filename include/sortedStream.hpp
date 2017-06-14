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

    template <class DataType_1, class DataType_2, bool greater>
    struct pair_comparator {
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
            std::vector<std::string> _files;
            std::priority_queue<std::pair<DataType, std::ifstream *>, std::vector<std::pair<DataType, std::ifstream *> >, pair_comparator<DataType, std::ifstream *, true> > minHeap;
        public:
            ~SortedStream() {
                while (minHeap.size()) {
                    std::ifstream * is = minHeap.top().second;
                    is->close();
                    delete is;
                    minHeap.pop();
                }
                for (const std::string & file: _files) {
                    D("sorted: " << file << " deleted.\n");
                    unlink(file.c_str());
                }
            }
            SortedStream(std::vector<std::string> & files): _files(files) {
                DataType temp;
                for (const std::string & file: files) {
                    std::ifstream * is = new std::ifstream(file);
                    if ((*is) && ((*is) >> temp)) {
                        minHeap.push(std::pair<DataType, std::ifstream *>(temp, is));
                    } else {
                        is->close();
                        delete is;
                    }
                }
                files.clear();
            }
            inline bool isValid() const {
                return (minHeap.size() != 0);
            }
            bool get(DataType & ret) {
                if (minHeap.empty()) {
                    return false;
                }
                std::pair<DataType, std::ifstream *> top = minHeap.top();
                ret = top.first;
                minHeap.pop();
                std::ifstream * is = top.second;
                if((*is) >> top.first) {
                    minHeap.push(top);
                } else {
                    is->close();
                    delete is;
                }
                return true;
            }
    };

}

#endif
