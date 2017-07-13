#include "splitter.hpp"

namespace ch {
    // Default constructor
    Splitter::Splitter(): _fd{nullptr}, bufferedLength{0} {

        buffer[DATA_BLOCK_SIZE] = '\0';

    }

    // Destructor
    Splitter::~Splitter() {

        setFd(nullptr);

    }

    // Open the file
    bool Splitter::open(const char * file) {

        setFd(fopen(file, "r"));
        return isValid();

    }

    // True if the file is opened and there are data remain
    inline bool Splitter::isValid() const {

        return (_fd != nullptr);

    }

    // Set file descriptor
    // close the previous file
    void Splitter::setFd(FILE * fd) {

        if (isValid()) {
            fclose(_fd);
        }

        _fd = fd;
        bufferedLength = 0;

    }

    // Get next split of data
    bool Splitter::next(std::string & res) {

        res.clear();
        std::lock_guard<std::mutex> holder{readLock};

        if (!isValid()) {
            return false;
        }

        size_t rv = Fread(_fd, buffer + bufferedLength, DATA_BLOCK_SIZE - bufferedLength);

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
            int returnLength;

            for (cursor = totalLength - 1; cursor > 0; --cursor) {
                c = buffer[cursor];
                if (IS_ESCAPER(c)) {
                    returnLength = cursor + 1;
                    res.append(buffer, returnLength);
                    bufferedLength = totalLength - returnLength;
                    memmove(static_cast<void *>(buffer),
                            static_cast<void *>(buffer + returnLength), bufferedLength);
                    return true;
                }
            }

            if (cursor == 0) {
                c = buffer[cursor];
                if (IS_ESCAPER(c)) {
                    returnLength = cursor + 1;
                    res.append(buffer, returnLength);
                    bufferedLength = totalLength - returnLength;
                    memmove(static_cast<void *>(buffer),
                            static_cast<void *>(buffer + returnLength), bufferedLength);
                    return true;
                }
            }
            E("(Splitter) Line out of buffer. File is not consumed completely.\n");
            I("Long line is not supported for the moment.\n");
            setFd(nullptr);
            return false;
        }

    }

}
