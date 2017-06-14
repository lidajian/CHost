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
            bool _presort;
            const size_t _maxDataSize;
            std::vector<DataType *> _data;
            std::mutex _dataLock;
            LocalFileManager<DataType> fileManager;
        public:
            static bool comp (const DataType * l, const DataType * r) {
                return (*l) < (*r);
            }
            void clear() {
                _data.clear();
                fileManager.clear();
            }
            DataManager (size_t maxDataSize, std::string & dir): _presort(true), _maxDataSize(maxDataSize), fileManager(dir) {}
            ~DataManager() {
                clear();
            }
            bool store(DataType * v) {
                std::lock_guard<std::mutex> holder(_dataLock);
                _data.push_back(v);
                if (_data.size() == _maxDataSize) {
                    if (_presort) sort(std::begin(_data), std::end(_data), comp);
                    return fileManager.dumpToFile(_data);
                }
                return true;
            }
            bool store(DataType & v) {
                DataType * nv = new DataType(v);
                std::lock_guard<std::mutex> holder(_dataLock);
                _data.push_back(nv);
                if (_data.size() == _maxDataSize) {
                    if (_presort) sort(std::begin(_data), std::end(_data), comp);
                    return fileManager.dumpToFile(_data);
                }
                return true;
            }
            void setPresort(bool presort) {
                _presort = presort;
            }
            SortedStream<DataType> * getSortedStream() {
                if (!_presort) {
                    return NULL;
                }
                if (_data.size() != 0) {
                    sort(std::begin(_data), std::end(_data), comp);
                    fileManager.dumpToFile(_data);
                }
                return fileManager.getSortedStream();
            }
            UnsortedStream<DataType> * getUnsortedStream () {
                if (_data.size() != 0) {
                    fileManager.dumpToFile(_data);
                }
                return fileManager.getUnsortedStream();
            }
    };
}

#endif
