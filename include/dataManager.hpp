/*
 * Manager intermediate data
 */

#ifndef DATAMANAGER_H
#define DATAMANAGER_H

#include <vector> // vector
#include <mutex> // mutex
#include <algorithm> // sort

#include "localFileManager.hpp" // LocalFileManager
#include "sortedStream.hpp" // SortedStream
#include "unsortedStream.hpp" // UnsortedStream

namespace ch {
    template <class DataType>
    class DataManager {
        protected:
            // Sort the data before dumping to file
            bool _presort;

            // Dump to file if data length exceed the threshold
            const size_t _maxDataSize;

            // Data it manages
            std::vector<const DataType *> _data;

            // Data lock
            std::mutex _dataLock;

            // Manages temporary files
            LocalFileManager<DataType> fileManager;

            // Clear the data manager
            void clear() {
                _dataLock.lock();
                _data.clear();
                _dataLock.unlock();
                fileManager.clear();
            }

            // Dereference pointer and compare
            static bool pointerComp (const DataType * l, const DataType * r) {
                return (*l) < (*r);
            }
        public:
            // Constructor
            explicit DataManager (const std::string & dir, size_t maxDataSize = DEFAULT_MAX_DATA_SIZE, bool presort = true): _presort{presort}, _maxDataSize{maxDataSize}, fileManager{dir} {}

            // Destructor
            ~DataManager() {
                clear();
            }

            // Store the data on heap
            bool store(const DataType * v) {
                std::lock_guard<std::mutex> holder(_dataLock);
                _data.push_back(v);
                if (_data.size() == _maxDataSize) {
                    if (_presort) sort(std::begin(_data), std::end(_data), pointerComp);
                    return fileManager.dumpToFile(_data);
                }
                return true;
            }

            // Store data on stack
            bool store(const DataType & v) {
                DataType * nv = new DataType(v);
                std::lock_guard<std::mutex> holder(_dataLock);
                _data.push_back(nv);
                if (_data.size() == _maxDataSize) {
                    if (_presort) sort(std::begin(_data), std::end(_data), pointerComp);
                    return fileManager.dumpToFile(_data);
                }
                return true;
            }

            // Get sorted stream from file manager
            SortedStream<DataType> * getSortedStream() {
                std::lock_guard<std::mutex> holder(_dataLock);
                if (!_presort) {
                    return nullptr;
                }
                if (_data.size() != 0) {
                    sort(std::begin(_data), std::end(_data), pointerComp);
                    if (!fileManager.dumpToFile(_data)) {
                        L("DataManager: Fail to dump the remaining data to file.\n");
                        return nullptr;
                    }
                }
                return fileManager.getSortedStream();
            }

            // Get unsorted stream from file manager
            UnsortedStream<DataType> * getUnsortedStream () {
                std::lock_guard<std::mutex> holder(_dataLock);
                if (_data.size() != 0) {
                    if (!fileManager.dumpToFile(_data)) {
                        L("DataManager: Fail to dump the remaining data to file.\n");
                        return nullptr;
                    }
                }
                return fileManager.getUnsortedStream();
            }

            void setPresort(bool presort) {
                _presort = presort;
            }
    };
}

#endif
