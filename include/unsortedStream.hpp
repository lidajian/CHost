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
            std::vector<std::string> _files;
            size_t i;
            std::ifstream is;
        public:
            ~UnsortedStream() {
                for (const std::string & file: _files) {
                    D("unsorted: " << file << " deleted.\n");
                    unlink(file.c_str());
                }
            }
            UnsortedStream(std::vector<std::string> & files): _files(files) {
                i = 0;
                while (!isValid() && i < _files.size()) {
                    is.close();
                    is.open(_files[i++]);
                }
                files.clear();
            }
            inline bool isValid() {
                return bool(is);
            }
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
