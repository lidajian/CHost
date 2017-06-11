#ifndef LOCALFILEMANAGER_H
#define LOCALFILEMANAGER_H

#include <vector>
#include <string>
#include <fcntl.h>
#include <random>
#include <unistd.h> // unlink
#include <functional> // std::bind
#include "def.hpp" // RANDOM_FILE_NAME_LENGTH
#include "sortedStream.hpp" // SortedStream
#include "unsortedStream.hpp" // UnsortedStream

namespace ch {

    template <class T>
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
            bool dumpToFile(std::vector<T *> & received) {
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
                int fd = open(fullPath.c_str(), O_WRONLY | O_CREAT, S_CREAT_DEFAULT);
                D(fullPath << ", " << fd << " created.\n");
                if (fd < 0) {
                    L("Cannot open file to dump.\n");
                    return false;
                }
                for (int i = 0, l = received.size(); i < l; ++i) {
                    if (!(received[i]->write(fd))) {
                        D("Maybe no hard disk space?\n");
                        for (; i < l; ++i) {
                            delete received[i];
                        }
                        received.clear();
                        close(fd);
                        return false;
                    }
                    delete received[i];
                }
                received.clear();
                close(fd);
                return true;
            }
            SortedStream<T> * getSortedStream() {
                SortedStream<T> * ret = new SortedStream<T>(dumpFiles);
                dumpFiles.clear();
                if (ret->isValid()) {
                    return ret;
                } else {
                    delete ret;
                    return NULL;
                }
            }
            UnsortedStream<T> * getUnsortedStream() {
                UnsortedStream<T> * ret = new UnsortedStream<T>(dumpFiles);
                dumpFiles.clear();
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
