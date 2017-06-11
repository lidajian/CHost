#ifndef DATAMANAGER_H
#define DATAMANAGER_H

#include <vector>
#include <mutex> // mutex
#include <algorithm> // sort
#include "localFileManager.hpp"
#include "sortedStream.hpp"
#include "unsortedStream.hpp"

namespace ch {
    template <class T>
    class DataManager {
        protected:
            bool _presort;
            const size_t _maxDataSize;
            std::vector<T *> _data;
            std::mutex _dataLock;
            LocalFileManager<T> fileManager;
        public:
            static bool comp (const T * l, const T * r) {
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
            bool store(T * v) {
                std::lock_guard<std::mutex> holder(_dataLock);
                _data.push_back(v);
                if (_data.size() == _maxDataSize) {
                    if (_presort) sort(std::begin(_data), std::end(_data), comp);
                    return fileManager.dumpToFile(_data);
                }
                return true;
            }
            bool store(T & v) {
                T * nv = new T(v);
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
            SortedStream<T> * getSortedStream() {
                if (!_presort) {
                    return NULL;
                }
                if (_data.size() != 0) {
                    sort(std::begin(_data), std::end(_data), comp);
                    fileManager.dumpToFile(_data);
                }
                return fileManager.getSortedStream();
            }
            UnsortedStream<T> * getUnsortedStream () {
                if (_data.size() != 0) {
                    fileManager.dumpToFile(_data);
                }
                return fileManager.getUnsortedStream();
            }
    };
}

#endif
