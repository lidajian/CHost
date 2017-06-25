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

    /********************************************
     ************** Declaration *****************
    ********************************************/

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
            void clear();

            // Dereference pointer and compare
            static bool pointerComp (const DataType * l, const DataType * r);
        public:
            // Constructor
            explicit DataManager (const std::string & dir, size_t maxDataSize = DEFAULT_MAX_DATA_SIZE, bool presort = true);

            // Destructor
            ~DataManager();

            // Store the data on heap
            bool store(const DataType * v);

            // Store data on stack
            bool store(const DataType & v);

            // Get sorted stream from file manager
            SortedStream<DataType> * getSortedStream();

            // Get unsorted stream from file manager
            UnsortedStream<DataType> * getUnsortedStream ();

            void setPresort(bool presort);
    };

    /********************************************
     ************ Implementation ****************
    ********************************************/

    // Clear the data manager
    template <class DataType>
    void DataManager<DataType>::clear() {
        _dataLock.lock();
        _data.clear();
        _dataLock.unlock();
        fileManager.clear();
    }

    // Dereference pointer and compare
    template <class DataType>
    bool DataManager<DataType>::pointerComp (const DataType * l, const DataType * r) {
        return (*l) < (*r);
    }

    // Constructor
    template <class DataType>
    DataManager<DataType>::DataManager (const std::string & dir, size_t maxDataSize, bool presort): _presort{presort}, _maxDataSize{maxDataSize}, fileManager{dir} {}

    // Destructor
    template <class DataType>
    DataManager<DataType>::~DataManager() {
        clear();
    }

    // Store the data on heap
    template <class DataType>
    bool DataManager<DataType>::store(const DataType * v) {
        std::lock_guard<std::mutex> holder(_dataLock);
        _data.push_back(v);
        if (_data.size() == _maxDataSize) {
            if (_presort) sort(std::begin(_data), std::end(_data), pointerComp);
            return fileManager.dumpToFile(_data);
        }
        return true;
    }

    // Store data on stack
    template <class DataType>
    bool DataManager<DataType>::store(const DataType & v) {
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
    template <class DataType>
    SortedStream<DataType> * DataManager<DataType>::getSortedStream() {
        std::lock_guard<std::mutex> holder(_dataLock);
        if (!_presort) {
            return nullptr;
        }
        if (_data.size() != 0) {
            sort(std::begin(_data), std::end(_data), pointerComp);
            if (!fileManager.dumpToFile(_data)) {
                E("(DataManager) Fail to dump the remaining data to file.");
                return nullptr;
            }
        }
        return fileManager.getSortedStream();
    }

    // Get unsorted stream from file manager
    template <class DataType>
    UnsortedStream<DataType> * DataManager<DataType>::getUnsortedStream () {
        std::lock_guard<std::mutex> holder(_dataLock);
        if (_data.size() != 0) {
            if (!fileManager.dumpToFile(_data)) {
                E("(DataManager) Fail to dump the remaining data to file.");
                return nullptr;
            }
        }
        return fileManager.getUnsortedStream();
    }

    template <class DataType>
    void DataManager<DataType>::setPresort(bool presort) {
        _presort = presort;
    }
}

#endif
