#ifndef UNSORTEDSTREAM_H
#define UNSORTEDSTREAM_H

#include <vector>
#include <queue>
#include <string>
#include <unistd.h> // unlink
#include <fcntl.h> // close

namespace ch {

    template <class T>
    class UnsortedStream {
        protected:
            std::vector<std::string> _files;
            size_t i;
            int fd;
        public:
            ~UnsortedStream() {
                for (const std::string & file: _files) {
                    D("unsorted: " << file << " deleted.\n");
                    unlink(file.c_str());
                }
            }
            UnsortedStream(std::vector<std::string> & files): _files(files) {
                fd = INVALID;
                i = 0;
                while (!isValid() && i < files.size()) {
                    fd = open(_files[i++].c_str(), O_RDONLY);
                }
            }
            inline bool isValid() {
                return fd >= 0;
            }
            bool get(T & ret) {

                while (!(isValid() && ret.read(fd))) {
                    if (isValid()) {
                        close(fd);
                    }
                    if (i < _files.size()) {
                        fd = open(_files[i++].c_str(), O_RDONLY);
                    } else {
                        fd = INVALID;
                        return false;
                    }
                }
                return true;
            }
    };

}

#endif
