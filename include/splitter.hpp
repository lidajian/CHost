/*
 * Splitter: buffer to get splits of data from file
 */

#ifndef SPLITTER_H
#define SPLITTER_H

#include <stdio.h> // fopen, fread, ...

#include <mutex> // mutex
#include <string.h> // memmove

#include "def.hpp"

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
            Splitter();

            // Copy constructor (deleted)
            Splitter(const Splitter &) = delete;

            // Move constructor (deleted)
            Splitter(Splitter &&) = delete;

            // Copy assignment (deleted)
            Splitter & operator = (const Splitter &) = delete;

            // Move assignment (deleted)
            Splitter & operator = (Splitter &&) = delete;

            // Destructor
            ~Splitter();

            // Open the file
            bool open(const char * file);

            // True if the file is opened and there are data remain
            bool isValid() const;

            // Set file descriptor
            // close the previous file
            void setFd(FILE * fd);

            // Get next split of data
            bool next(std::string & res);
    };
}

#endif
