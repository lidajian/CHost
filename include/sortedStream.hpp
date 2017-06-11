#ifndef SORTEDSTREAM_H
#define SORTEDSTREAM_H

#include <vector>
#include <queue>
#include <string>
#include <unistd.h> // unlink
#include <fcntl.h> // close

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
            std::priority_queue<std::pair<T, int>, std::vector<std::pair<T, int> >, pair_comparator<T, int, true> > minHeap;
        public:
            ~SortedStream() {
                while (minHeap.size()) {
                    close(minHeap.top().second);
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
                    int fd = open(file.c_str(), O_RDONLY);
                    if (fd < 0) {
                        D(file << " cannot be opened. " << strerror(errno) << "\n");
                    } else if (temp.read(fd)) {
                        D("sorted: " << fd << " opened.\n");
                        minHeap.push(std::pair<T, int>(temp, fd));
                    } else {
                        D(file << " is empty!\n");
                        close(fd);
                    }
                }
            }
            inline bool isValid() {
                return (minHeap.size() != 0);
            }
            bool get(T & ret) {
                if (minHeap.empty()) {
                    return false;
                }
                std::pair<T, int> top(std::move(minHeap.top()));
                ret = top.first;
                minHeap.pop();
                if(top.first.read(top.second)) {
                    minHeap.push(top);
                } else {
                    close(top.second);
                }
                return true;
            }
    };

}

#endif
