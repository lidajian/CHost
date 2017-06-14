/*
 * Manager temporary files in local machine
 */

#ifndef LOCALFILEMANAGER_H
#define LOCALFILEMANAGER_H

#include <unistd.h> // unlink

#include <vector> // vector
#include <string> // string
#include <fstream> // ofstream
#include <random> // random_device, default_random_engine, uniform_int_distribution
#include <functional> // std::bind

#include "def.hpp" // RANDOM_FILE_NAME_LENGTH
#include "sortedStream.hpp" // SortedStream
#include "unsortedStream.hpp" // UnsortedStream

namespace ch {

    template <class DataType>
    class LocalFileManager {
        protected:
            // file paths it manages
            std::string dumpFileDir;
            std::vector<std::string> dumpFiles;
        public:
            void clear() {
                for (const std::string & file: dumpFiles) {
                    D(file << " deleted.\n");
                    unlink(file.c_str());
                }
                dumpFiles.clear();
            }
            LocalFileManager(std::string & dir): dumpFileDir(dir) {}
            ~LocalFileManager() {
                clear();
            }
            bool dumpToFile(std::vector<DataType *> & received) {
                // generate random file name
                std::random_device d;
                std::default_random_engine generator(d());
                std::uniform_int_distribution<char> distribution('a', 'z');
                auto char_dice = std::bind(distribution, generator);
                dumpFiles.emplace_back(dumpFileDir);
                std::string & fullPath = dumpFiles.back();
                fullPath.append("/.", sizeof("/.") - 1); // exclude the '\0'
                for (int i = 0; i < RANDOM_FILE_NAME_LENGTH; ++i) {
                    fullPath.push_back((char)char_dice());
                }
                std::ofstream os(fullPath);
                D(fullPath << " created.\n");
                if (os) {
                    for (int i = 0, l = received.size(); i < l; ++i) {
                        if (!(os << *(received[i]))) {
                            D("Maybe no hard disk space?\n");
                            for (; i < l; ++i) {
                                delete received[i];
                            }
                            received.clear();
                            os.close();
                            return false;
                        }
                        delete received[i];
                    }
                }
                os.close();
                received.clear();
                return true;
            }
            SortedStream<DataType> * getSortedStream() {
                SortedStream<DataType> * ret = new SortedStream<DataType>(dumpFiles);
                if (ret->isValid()) {
                    return ret;
                } else {
                    delete ret;
                    return NULL;
                }
            }
            UnsortedStream<DataType> * getUnsortedStream() {
                UnsortedStream<DataType> * ret = new UnsortedStream<DataType>(dumpFiles);
                if (ret->isValid()) {
                    return ret;
                } else {
                    delete ret;
                    return NULL;
                }
            }
    };

}

#endif
