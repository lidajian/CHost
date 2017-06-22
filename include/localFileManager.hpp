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
#include <thread> // sleep_for
#include <chrono> // seconds

#include "def.hpp" // RANDOM_FILE_NAME_LENGTH
#include "sortedStream.hpp" // SortedStream
#include "unsortedStream.hpp" // UnsortedStream

namespace ch {

    typedef std::vector<std::string>::reverse_iterator FileIterR;

    template <class DataType>
    class LocalFileManager {
        protected:
            // Directory of dump file
            const std::string dumpFileDir;

            // All dump files it holds
            std::vector<std::string> dumpFiles;

            // Sort data if there are no greater than MERGE_SORT_WAY files
            bool unitMergeSort(const FileIterR & begin, const FileIterR & end) {
                SortedStream<DataType> stm(begin, end);
                std::ofstream os;
                getStream(os);
                if (os) {
                    DataType temp;
                    while (stm.get(temp)) {
                        os << temp;
                        if (!os) {
                            E("(LocalFileManager) Cannot write to file while merge sort.\n");
                            I("Check if there is no space.\n");
                            return false;
                        }
                    }
                } else {
                    E("(LocalFileManager) Cannot open file stream to write.\n");
                    I("Check if there is no space.\n");
                    return false;
                }
                return true;
            }

            // Sort data if there are less than MERGE_SORT_WAY * MERGE_SORT_WAY files
            // and greater than MERGE_SORT_WAY files
            bool gridMergeSort (const FileIterR & begin, const FileIterR & end) {
                const int l = end - begin - MERGE_SORT_WAY; // reserve for all slot
                const int fullSlots = l / (MERGE_SORT_WAY - 1); // number of full merges in Step 1
                const int remain = l % (MERGE_SORT_WAY - 1) + 1; // number of files to merge in Step 2
                FileIterR it = begin;
                FileIterR next_it;

                // Step 1: MERGE_SORT_WAY full merge
                for (int i = 0; i < fullSlots; i++) {
                    next_it = it + MERGE_SORT_WAY;
                    if (!unitMergeSort(it, next_it)) {
                        return false;
                    }
                    it = next_it;
                }

                // Step 2:
                if (remain > 0) {
                    next_it = it + remain;
                    if (unitMergeSort(it, next_it)) {
                        return false;
                    }
                    it = next_it;
                }

                // Step 3: single files without a need to merge
                while (it < end) {
                    dumpFiles.push_back(*it);
                    ++it;
                }
                return true;
            }

            // Sort data if there are no less than MERGE_SORT_WAY * MERGE_SORT_WAY files
            bool fullMergeSort (const FileIterR & begin, const FileIterR & end) {
                FileIterR it = end;
                FileIterR next_it = end - MERGE_SORT_WAY;
                while (next_it > begin) { // loop invariant: index increase -> size decrease
                    if (!unitMergeSort(next_it, it)) {
                        return false;
                    }
                    it = next_it;
                    next_it = it - MERGE_SORT_WAY;
                }
                return unitMergeSort(begin, it);
            }

            // Entry that actually do merge sort
            bool doMergeSort () {
                size_t l = dumpFiles.size();

                // full merge sort first
                while (l >= MERGE_SORT_WAY * MERGE_SORT_WAY) { // loop invariant: index increase -> size decrease
                    std::vector<std::string> files(std::move(dumpFiles));
                    dumpFiles.clear(); // dumpFiles unspecified, clear dumpFiles

                    if (!fullMergeSort(files.rbegin(), files.rend())) {
                        return false;
                    }

                    l = dumpFiles.size();
                }

                // grid merge sort (minimize data read!)
                if (l > MERGE_SORT_WAY) { // assert: index increase -> size decrease
                    std::vector<std::string> files(std::move(dumpFiles));
                    dumpFiles.clear(); // dumpFiles unspecified, clear dumpFiles

                    if (!gridMergeSort(files.rbegin(), files.rend())) {
                        return false;
                    }

                    l = dumpFiles.size();
                }
                return true;
            }
        public:
            // Constructor
            LocalFileManager(const std::string & dir): dumpFileDir{dir} {}

            // Destructor
            ~LocalFileManager() {
                clear();
            }

            // Remove all temporary files
            void clear() {
                for (const std::string & file: dumpFiles) {
                    unlink(file.c_str());
                }
                dumpFiles.clear();
            }

            // Get output file stream of a new temporary file
            void getStream(std::ofstream & os) {
                // generate random file name
                std::random_device d;
                std::default_random_engine generator(d());
                std::uniform_int_distribution<char> distribution('a', 'z');
                auto char_dice = std::bind(distribution, generator);
                dumpFiles.emplace_back(dumpFileDir);
                std::string & fullPath = dumpFiles.back();
                fullPath.append("/.", LENGTH_CONST_CHAR_ARRAY("/."));
                for (int i = 0; i < RANDOM_FILE_NAME_LENGTH; ++i) {
                    fullPath.push_back(char_dice());
                }
                os.open(fullPath);
                if (!os) {
                    dumpFiles.pop_back();
                    E("(LocalFileManager) Fail to create temporary file.\n");
                    I("Check if there is no space.\n");
                    std::this_thread::sleep_for(std::chrono::seconds(OPEN_FILESTREAM_RETRY_INTERVAL));
                    getStream(os);
                }
            }

            // Dump data to file
            bool dumpToFile(std::vector<const DataType *> & data) {
                std::ofstream os;
                getStream(os);
                if (os) {
                    for (size_t i = 0, l = data.size(); i < l; ++i) {
                        if (!(os << *(data[i]))) {
                            E("(LocalFileManager) Fail to write data to file.\n");
                            I("Check if there is no space.\n");
                            for (; i < l; ++i) {
                                delete data[i];
                            }
                            data.clear();
                            os.close();
                            return false;
                        }
                        delete data[i];
                    }
                } else {
                    E("(LocalFileManager) Fail to opening file to write.\n");
                    I("Check if there is no space.\n");
                    return false;
                }
                os.close();
                data.clear();
                return true;
            }

            // Get sorted stream with all files
            SortedStream<DataType> * getSortedStream() {
                if (!doMergeSort()) {
                    return nullptr;
                }
                SortedStream<DataType> * ret = new SortedStream<DataType>(std::move(dumpFiles));
                if (ret->isValid()) {
                    return ret;
                } else {
                    delete ret;
                    return nullptr;
                }
            }

            // Get unsorted stream with all files
            UnsortedStream<DataType> * getUnsortedStream() {
                UnsortedStream<DataType> * ret = new UnsortedStream<DataType>(std::move(dumpFiles));
                if (ret->isValid()) {
                    return ret;
                } else {
                    delete ret;
                    return nullptr;
                }
            }
    };

}

#endif
