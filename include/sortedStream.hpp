#ifndef SORTEDSTREAM_H
#define SORTEDSTREAM_H

#include <vector>
#include <queue>
#include <string>
#include <fstream>
#include <unistd.h> // unlink

namespace ch {

    template <class T1, class T2, bool greater>
    struct pair_comparator {
        bool operator()(const std::pair<T1, T2> & l, const std::pair<T1, T2> & r) {
            if (greater) {
                return l.first > r.first;
            } else {
                return l.first < r.first;
            }
        }
    };

    template <class T>
    class SortedStream {
        protected:
            std::vector<std::string> _files;
            std::priority_queue<std::pair<T, std::ifstream *>, std::vector<std::pair<T, std::ifstream *> >, pair_comparator<T, std::ifstream *, true> > minHeap;
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
                T temp;
                for (const std::string & file: files) {
                    std::ifstream * is = new std::ifstream(file);
                    if ((*is) && ((*is) >> temp)) {
                        minHeap.push(std::pair<T, std::ifstream *>(temp, is));
                    } else {
                        is->close();
                        delete is;
                    }
                }
                files.clear();
            }
            inline bool isValid() {
                return (minHeap.size() != 0);
            }
            bool get(T & ret) {
                if (minHeap.empty()) {
                    return false;
                }
                std::pair<T, std::ifstream *> top = minHeap.top();
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
