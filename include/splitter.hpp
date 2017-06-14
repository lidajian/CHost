#ifndef SPLITTER_H
#define SPLITTER_H

#include <stdio.h> // fopen, fread, ...

#include <mutex> // mutex

namespace ch {

    class Splitter {
        protected:
            FILE * _fd;
            char buffer[DATA_BLOCK_SIZE + 1];
            std::mutex readLock;
            size_t bufferedLength; // byte cached in buffer

        public:
            Splitter(): _fd(NULL), bufferedLength(0) {
                buffer[DATA_BLOCK_SIZE] = '\0';
            }

            ~Splitter() {
                setFd(NULL);
            }

            bool open(const char * file) {
                setFd(NULL);
                _fd = fopen(file, "r");
                return isValid();
            }

            inline bool isValid() const {
                return (_fd != NULL);
            }

            void setFd(FILE * fd) {
                if (isValid()) {
                    fclose(_fd);
                }
                _fd = fd;
                bufferedLength = 0;
            }

            bool next(std::string & res) {
                std::lock_guard<std::mutex> holder(readLock);
                res.clear();
                if (!isValid()) {
                    return false;
                }
                size_t rv = fread(buffer + bufferedLength, sizeof(char), DATA_BLOCK_SIZE - bufferedLength, _fd);
                if (rv == 0) { // EOF
                    if (bufferedLength == 0) {
                        setFd(NULL);
                        return false;
                    } else {
                        res.append(buffer, bufferedLength);
                        res.push_back('\n');
                        setFd(NULL);
                        return true;
                    }
                } else {
                    size_t totalLength = rv + bufferedLength;
                    size_t cursor = totalLength - 1;
                    char c;
                    for (cursor = totalLength - 1; cursor >= 0; --cursor) {
                        c = buffer[cursor];
                        if (IS_ESCAPER(c)) {
                            int returnLength = cursor + 1;
                            res.append(buffer, returnLength);
                            bufferedLength = totalLength - returnLength;
                            memmove(static_cast<void *>(buffer), static_cast<void *>(buffer + returnLength), bufferedLength);
                            return true;
                        }
                    }
                    D("Line out of buffer.\n");
                    setFd(NULL);
                    return false;
                }
            }
    };
}

#endif
