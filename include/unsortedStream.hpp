/*
 * Unsorted stream over bunch of files, read files one by one
 */

#ifndef UNSORTEDSTREAM_H
#define UNSORTEDSTREAM_H

#include <unistd.h> // unlink

#include <vector> // vector
#include <string> // string
#include <fstream> // ifstream

namespace ch {

    template <class DataType>
    class UnsortedStream {
        protected:
            // Files it manages
            const std::vector<std::string> _files;

            // Index of next file
            size_t i;

            // Input stream of current file
            std::ifstream is;
        public:

            // Move constructor
            UnsortedStream(std::vector<std::string> && files): _files(std::move(files)) {
                i = 0;
                while (!isValid() && i < _files.size()) {
                    is.close();
                    is.open(_files[i++]);
                }
                files.clear();
            }

            // Destructor
            ~UnsortedStream() {
                for (const std::string & file: _files) {
                    unlink(file.c_str());
                }
            }

            // True if stream is good
            inline bool isValid() {
                return bool(is);
            }

            // Get data from stream
            bool get(DataType & ret) {

                while (!(isValid() && (is >> ret))) {
                    is.close();
                    if (i < _files.size()) {
                        is.open(_files[i++]);
                    } else {
                        return false;
                    }
                }
                return true;
            }
    };

}

#endif
