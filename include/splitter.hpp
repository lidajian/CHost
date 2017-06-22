/*
 * Splitter: buffer to get splits of data from file
 */

#ifndef SPLITTER_H
#define SPLITTER_H

#include <stdio.h> // fopen, fread, ...

#include <mutex> // mutex

namespace ch {

    class Splitter {
        protected:
            // The file descriptor it holds
            FILE * _fd;

            // The buffer (ends with '\0')
            char buffer[DATA_BLOCK_SIZE + 1];

            // The data lock that disallow file read by multiple threads
            std::mutex readLock;

            // Number of bytes cached in buffer
            size_t bufferedLength;

        public:
            // Default constructor
            Splitter(): _fd{nullptr}, bufferedLength{0} {
                buffer[DATA_BLOCK_SIZE] = '\0';
            }

            // Destructor
            ~Splitter() {
                setFd(nullptr);
            }

            // Open the file
            bool open(const char * file) {
                setFd(fopen(file, "r"));
                return isValid();
            }

            // True if the file is opened and there are data remain
            inline bool isValid() const {
                return (_fd != nullptr);
            }

            // Set file descriptor
            // close the previous file
            void setFd(FILE * fd) {
                if (isValid()) {
                    fclose(_fd);
                }
                _fd = fd;
                bufferedLength = 0;
            }

            // Get next split of data
            bool next(std::string & res) {
                res.clear();
                std::lock_guard<std::mutex> holder(readLock);
                if (!isValid()) {
                    return false;
                }
                size_t rv = fread(buffer + bufferedLength, sizeof(char), DATA_BLOCK_SIZE - bufferedLength, _fd);
                if (rv == 0) { // EOF
                    if (bufferedLength == 0) {
                        setFd(nullptr);
                        return false;
                    } else {
                        res.append(buffer, bufferedLength);
                        res.push_back('\n');
                        setFd(nullptr);
                        return true;
                    }
                } else {
                    const size_t totalLength = rv + bufferedLength;
                    size_t cursor;
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
                    E("(Splitter) Line out of buffer. File is not consumed completely.\n");
                    I("Long line is not supported for the moment.\n");
                    setFd(nullptr);
                    return false;
                }
            }
    };
}

#endif
