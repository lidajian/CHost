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

    typedef std::vector<std::string>::reverse_iterator VecStrIterR;

    template <class DataType>
    class LocalFileManager {
        protected:
            // file paths it manages
            std::string dumpFileDir;
            std::vector<std::string> dumpFiles;

            // no greater than MERGE_SORT_WAY files
            void unitMergeSort(const VecStrIterR & begin, const VecStrIterR & end) {
                SortedStream<DataType> stm(begin, end);
                std::ofstream os;
                getStream(os);
                if (os) {
                    DataType temp;
                    while (stm.get(temp)) {
                        os << temp;
                    }
                } else {
                    L("Fatal: merge sort may missing one file!\n");
                }
                os.close();
            }
            // less than K * K files, greater than K files
            void gridMergeSort (const VecStrIterR & begin, const VecStrIterR & end) {
                int l = end - begin;
                l -= MERGE_SORT_WAY; // reserve for all slot
                int fullSlots = l / MERGE_SORT_WAY;
                int remain = l % MERGE_SORT_WAY;
                VecStrIterR it = begin;
                VecStrIterR next_it;
                for (int i = 0; i < fullSlots; i++) {
                    next_it = it + MERGE_SORT_WAY;
                    unitMergeSort(it, next_it);
                    it = next_it;
                }
                if (remain > 0) {
                    next_it = it + remain;
                    unitMergeSort(it, next_it);
                    it = next_it;
                }
                while (it < end) {
                    dumpFiles.push_back(*it);
                    ++it;
                }
            }
            // no less than MERGE_SORT_WAY * MERGE_SORT_WAY files
            void fullMergeSort (const VecStrIterR & begin, const VecStrIterR & end) {
                VecStrIterR it = end;
                VecStrIterR next_it = end - MERGE_SORT_WAY;
                while (next_it > begin) { // loop invariant: index increase -> size decrease
                    unitMergeSort(next_it, it);
                    it = next_it;
                    next_it = it - MERGE_SORT_WAY;
                }
                unitMergeSort(begin, it);
            }
            void doMergeSort () {
                size_t l = dumpFiles.size();

                // full merge sort first
                while (l >= MERGE_SORT_WAY * MERGE_SORT_WAY) { // loop invariant: index increase -> size decrease
                    std::vector<std::string> files = dumpFiles;
                    dumpFiles.clear();

                    VecStrIterR begin = files.rbegin();
                    VecStrIterR end = files.rend();
                    fullMergeSort(begin, end);

                    l = dumpFiles.size();
                }

                // grid merge sort (minimize data read!)
                if (l > MERGE_SORT_WAY) { // assert: index increase -> size decrease
                    std::vector<std::string> files = dumpFiles;
                    dumpFiles.clear();

                    VecStrIterR begin = files.rbegin();
                    VecStrIterR end = files.rend();
                    gridMergeSort(begin, end);

                    l = dumpFiles.size();
                }
            }
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
            void getStream(std::ofstream & os) {
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
                os.open(fullPath);
                if (os) {
                    D(fullPath << " created.\n");
                } else {
                    dumpFiles.pop_back();
                    return getStream(os);
                }
            }
            bool dumpToFile(std::vector<DataType *> & received) {
                std::ofstream os;
                getStream(os);
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
                doMergeSort();
                SortedStream<DataType> * ret = new SortedStream<DataType>(dumpFiles);
                dumpFiles.clear();
                if (ret->isValid()) {
                    return ret;
                } else {
                    delete ret;
                    return NULL;
                }
            }
            UnsortedStream<DataType> * getUnsortedStream() {
                UnsortedStream<DataType> * ret = new UnsortedStream<DataType>(dumpFiles);
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
