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

    /********************************************
     ************** Declaration *****************
    ********************************************/

    template <typename DataType>
    class UnsortedStream {
        protected:
            // Files it manages
            const std::vector<std::string> _files;

            // Index of next file
            size_t i;

            // Input stream of current file
            std::ifstream is;
        public:

            // Constructor
            UnsortedStream(std::vector<std::string> && files);

            // Copy constructor (deleted)
            UnsortedStream(const UnsortedStream<DataType> &) = delete;

            // Move constructor
            UnsortedStream(UnsortedStream<DataType> && o);

            // Destructor
            ~UnsortedStream();

            // True if stream is good
            bool isValid();

            // Get data from stream
            bool get(DataType & ret);
    };

    /********************************************
     ************ Implementation ****************
    ********************************************/

    // Constructor
    template <typename DataType>
    UnsortedStream<DataType>::UnsortedStream(std::vector<std::string> && files): _files(std::move(files)) {
        i = 0;
        while (!isValid() && i < _files.size()) {
            is.close();
            is.open(_files[i++]);
        }
        files.clear();
    }

    // Move constructor
    template <typename DataType>
    UnsortedStream<DataType>::UnsortedStream(UnsortedStream<DataType> && o): _files{std::move(o._files)}, i{o.i}, is{std::move(o.is)} {}

    // Destructor
    template <typename DataType>
    UnsortedStream<DataType>::~UnsortedStream() {
        for (const std::string & file: _files) {
            unlink(file.c_str());
        }
    }

    // True if stream is good
    template <typename DataType>
    inline bool UnsortedStream<DataType>::isValid() {
        return bool(is);
    }

    // Get data from stream
    template <typename DataType>
    bool UnsortedStream<DataType>::get(DataType & ret) {

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

}

#endif
