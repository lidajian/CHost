/*
 * Manager intermediate data
 */

#ifndef DATAMANAGER_H
#define DATAMANAGER_H

#include <vector>               // vector
#include <string>               // string
#include <mutex>                // mutex, lock_guard
#include <algorithm>            // sort

#include "localFileManager.hpp" // LocalFileManager
#include "sortedStream.hpp"     // SortedStream
#include "unsortedStream.hpp"   // UnsortedStream

namespace ch {

    /********************************************
     ************** Declaration *****************
    ********************************************/

    template <typename DataType>
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
            static bool dereferenceComparator (const DataType * & l, const DataType * & r);

        public:

            // Constructor
            explicit DataManager (const std::string & dir, size_t maxDataSize = DEFAULT_MAX_DATA_SIZE, bool presort = true);

            // Copy constructor (deleted)
            DataManager(const DataManager<DataType> & ) = delete;

            // Move constructor (deleted)
            DataManager(DataManager<DataType> && ) = delete;

            // Copy assignment (deleted)
            DataManager<DataType> & operator = (const DataManager<DataType> &) = delete;

            // Move assignment (deleted)
            DataManager<DataType> & operator = (DataManager<DataType> &&) = delete;

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

            LocalFileManager<DataType> * getFileManager();
    };

    /********************************************
     ************ Implementation ****************
    ********************************************/

    // Clear the data manager
    template <typename DataType>
    void DataManager<DataType>::clear() {

        fileManager.clear();

        std::lock_guard<std::mutex> holder{_dataLock};

        for (const DataType * & v: _data) {
            delete v;
        }

        _data.clear();

    }

    // Dereference pointer and compare
    template <typename DataType>
    bool DataManager<DataType>::dereferenceComparator (const DataType * & l, const DataType * & r) {

        return (*l) < (*r);

    }

    // Constructor
    template <typename DataType>
    DataManager<DataType>::DataManager (const std::string & dir, size_t maxDataSize, bool presort)
    : _presort{presort}, _maxDataSize{maxDataSize}, fileManager{dir} {}

    // Destructor
    template <typename DataType>
    DataManager<DataType>::~DataManager() {

        clear();

    }

    // Store the data on heap
    template <typename DataType>
    bool DataManager<DataType>::store(const DataType * v) {

        std::lock_guard<std::mutex> holder{_dataLock};

        _data.push_back(v);

        if (_data.size() == _maxDataSize) {
            if (_presort) sort(std::begin(_data), std::end(_data), dereferenceComparator);
            return fileManager.dumpToFile(_data);
        }

        return true;

    }

    // Store data on stack
    template <typename DataType>
    bool DataManager<DataType>::store(const DataType & v) {

        const DataType * const nv = new DataType{v};

        std::lock_guard<std::mutex> holder{_dataLock};

        _data.push_back(nv);

        if (_data.size() == _maxDataSize) {
            if (_presort) sort(std::begin(_data), std::end(_data), dereferenceComparator);
            return fileManager.dumpToFile(_data);
        }

        return true;

    }

    // Get sorted stream from file manager
    template <typename DataType>
    SortedStream<DataType> * DataManager<DataType>::getSortedStream() {

        std::lock_guard<std::mutex> holder{_dataLock};

        if (_data.size() != 0) {
            sort(std::begin(_data), std::end(_data), dereferenceComparator);
            if (!fileManager.dumpToFile(_data)) {
                E("(DataManager) Fail to dump the remaining data to file.");
                return nullptr;
            }
        }

        return fileManager.getSortedStream();

    }

    // Get unsorted stream from file manager
    template <typename DataType>
    UnsortedStream<DataType> * DataManager<DataType>::getUnsortedStream () {

        std::lock_guard<std::mutex> holder{_dataLock};

        if (_data.size() != 0) {
            if (!fileManager.dumpToFile(_data)) {
                E("(DataManager) Fail to dump the remaining data to file.");
                return nullptr;
            }
        }

        return fileManager.getUnsortedStream();

    }

    template <typename DataType>
    inline void DataManager<DataType>::setPresort(bool presort) {

        _presort = presort;

    }

    template <typename DataType>
    LocalFileManager<DataType> * DataManager<DataType>::getFileManager() {

        std::lock_guard<std::mutex> holder{_dataLock};

        if (_data.size() != 0) {
            sort(std::begin(_data), std::end(_data), dereferenceComparator);
            if (!fileManager.dumpToFile(_data)) {
                E("(DataManager) Fail to dump the remaining data to file.");
                return nullptr;
            }
        }

        return &fileManager;

    }
}

#endif
