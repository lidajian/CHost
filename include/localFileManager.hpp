/*
 * Manager temporary files in local machine
 */

#ifndef LOCALFILEMANAGER_H
#define LOCALFILEMANAGER_H

#include <unistd.h>           // unlink

#include <vector>             // vector
#include <string>             // string
#include <fstream>            // ofstream
#include <memory>             // unique_ptr
#include <mutex>              // lock_guard, mutex
#include <future>             // future

#include "def.hpp"            // RANDOM_FILE_NAME_LENGTH, MERGE_SORT_WAY
#include "sortedStream.hpp"   // SortedStream
#include "unsortedStream.hpp" // UnsortedStream
#include "utils.hpp"          // randomString
#include "threadPool.hpp"     // ThreadPool

namespace ch {

    /********************************************
     ************** Declaration *****************
    ********************************************/

    typedef std::vector<std::string>::iterator FileIter;

    template <typename DataType>
    class LocalFileManager {

        protected:

            // Mutex for getStream
            std::mutex lock;

            // Directory of dump file
            const std::string dumpFileDir;

            // All dump files it holds
            std::vector<std::string> dumpFiles;

            // Sort data if there are no greater than MERGE_SORT_WAY files
            bool unitMergeSort(const FileIter & begin, const FileIter & end);

            // Sort data if there are less than MERGE_SORT_WAY * MERGE_SORT_WAY files
            // and greater than MERGE_SORT_WAY files
            bool gridMergeSort (const FileIter & begin, const FileIter & end);

            // Sort data if there are no less than MERGE_SORT_WAY * MERGE_SORT_WAY files
            bool fullMergeSort (const FileIter & begin, const FileIter & end);

            // Entry that actually do merge sort
            bool doMergeSort ();

        public:

            // Constructor
            LocalFileManager(const std::string & dir);

            // Copy constructor (deleted)
            LocalFileManager(const LocalFileManager<DataType> & fileManager) = delete;

            // Move constructor
            LocalFileManager(LocalFileManager<DataType> && o);

            // Copy assignment (deleted)
            LocalFileManager<DataType> & operator = (const LocalFileManager<DataType> &) = delete;

            // Move assignment
            LocalFileManager<DataType> & operator = (LocalFileManager<DataType> && o);

            // Destructor
            ~LocalFileManager();

            // Remove all temporary files
            void clear();

            // Get output file stream of a new temporary file
            bool getStream(std::ofstream & os);

            // Dump data to file
            bool dumpToFile(std::vector<const DataType *> & data);

            // Get sorted stream with all files
            SortedStream<DataType> * getSortedStream();

            // Get sorted streams grouped by MERGE_SORT_WAY files
            std::vector<std::unique_ptr<SortedStream<DataType> > > getSortedStreams();

            // Get unsorted stream with all files
            UnsortedStream<DataType> * getUnsortedStream();
    };

    /********************************************
     ************ Implementation ****************
    ********************************************/

    // Sort data if there are no greater than MERGE_SORT_WAY files
    template <typename DataType>
    bool LocalFileManager<DataType>::unitMergeSort(const FileIter & begin, const FileIter & end) {

        SortedStream<DataType> stm{begin, end};
        std::ofstream os;

        if (!getStream(os)) {
            return false;
        }

        stm.open();

        DataType temp;

        while (stm.get(temp)) {
            if (!(os << temp) || !os) {
                E("(LocalFileManager) Cannot write to file while merge sort.");
                I("Check if there is no space.");
                return false;
            }
        }

        return true;

    }

    // Sort data if there are less than MERGE_SORT_WAY * MERGE_SORT_WAY files
    // and greater than MERGE_SORT_WAY files
    template <typename DataType>
    bool LocalFileManager<DataType>::gridMergeSort (const FileIter & begin,
                                                    const FileIter & end) {

        const size_t l = end - begin - MERGE_SORT_WAY; // reserve for all slot
        const size_t fullSlots = l / (MERGE_SORT_WAY - 1); // number of full merges
        const size_t remain = l % (MERGE_SORT_WAY - 1) + 1; // number of files to merge
        FileIter it = begin;
        FileIter next_it;

        ThreadPool threadPool{THREAD_POOL_SIZE};
        std::vector<std::future<bool> > isSuccess;

        // Step 1: MERGE_SORT_WAY full merge
        for (size_t i = 0; i < fullSlots; i++) {
            next_it = it + MERGE_SORT_WAY;
            isSuccess.emplace_back(threadPool.addTask([this, it, next_it](){
                return this->unitMergeSort(it, next_it);
            }));
            it = next_it;
        }

        // Step 2: less than MERGE_SORT_WAY merge
        if (remain > 0) {
            next_it = it + remain;
            isSuccess.emplace_back(threadPool.addTask([this, it, next_it](){
                return this->unitMergeSort(it, next_it);
            }));
            it = next_it;
        }

        // Step 3: single files without a need to merge
        while (it < end) {
            std::lock_guard<std::mutex> holder{lock};
            dumpFiles.push_back(*it);
            ++it;
        }

        // Check if success
        for (std::future<bool> & suc: isSuccess) {
            if (!suc.get()) {
                return false;
            }
        }

        return true;

    }

    // Sort data if there are no less than MERGE_SORT_WAY * MERGE_SORT_WAY files
    template <typename DataType>
    bool LocalFileManager<DataType>::fullMergeSort (const FileIter & begin,
                                                    const FileIter & end) {

        FileIter it = begin;
        FileIter next_it = begin + MERGE_SORT_WAY;

        ThreadPool threadPool{THREAD_POOL_SIZE};
        std::vector<std::future<bool> > isSuccess;

        while (next_it < end) {
            isSuccess.emplace_back(threadPool.addTask([this, it, next_it](){
                return this->unitMergeSort(it, next_it);
            }));

            it = next_it;
            next_it = it + MERGE_SORT_WAY;
        }

        isSuccess.emplace_back(threadPool.addTask([this, it, end](){
            return this->unitMergeSort(it, end);
        }));

        // Check if success
        for (std::future<bool> & suc: isSuccess) {
            if (!suc.get()) {
                return false;
            }
        }

        return true;

    }

    // Entry that actually do merge sort
    template <typename DataType>
    bool LocalFileManager<DataType>::doMergeSort () {

        size_t l = dumpFiles.size();

        // full merge sort first
        while (l >= MERGE_SORT_WAY * MERGE_SORT_WAY) {
            // loop invariant: index increase -> size decrease
            std::vector<std::string> files{std::move(dumpFiles)};
            dumpFiles.clear(); // dumpFiles unspecified, clear dumpFiles

            if (!fullMergeSort(files.begin(), files.end())) {
                return false;
            }

            l = dumpFiles.size();
        }

        // grid merge sort (minimize data read!)
        if (l > MERGE_SORT_WAY) { // assert: index increase -> size decrease
            std::vector<std::string> files{std::move(dumpFiles)};
            dumpFiles.clear(); // dumpFiles unspecified, clear dumpFiles

            if (!gridMergeSort(files.begin(), files.end())) {
                return false;
            }

            l = dumpFiles.size();
        }

        return true;

    }

    // Constructor
    template <typename DataType>
    LocalFileManager<DataType>::LocalFileManager(const std::string & dir) : dumpFileDir{dir} {}

    // Move constructor
    template <typename DataType>
    LocalFileManager<DataType>::LocalFileManager(LocalFileManager<DataType> && o)
                : dumpFileDir{o.dumpFileDir}, dumpFiles{std::move(o.dumpFiles)} {

        o.dumpFiles.clear();

    }

    // Move assignment
    template <typename DataType>
    LocalFileManager<DataType> &
        LocalFileManager<DataType>::operator = (LocalFileManager<DataType> && o) {

        dumpFileDir = std::move(o.dumpFileDir);

        dumpFiles = std::move(o.dumpFiles);
        o.dumpFiles.clear();

        return *this;

    }

    // Destructor
    template <typename DataType>
    LocalFileManager<DataType>::~LocalFileManager() {

        clear();

    }

    // Remove all temporary files
    template <typename DataType>
    void LocalFileManager<DataType>::clear() {

        for (const std::string & file: dumpFiles) {
            unlink(file.c_str());
        }

        dumpFiles.clear();

    }

    // Get output file stream of a new temporary file
    template <typename DataType>
    bool LocalFileManager<DataType>::getStream(std::ofstream & os) {

        std::string fullPath{dumpFileDir};
        fullPath.append("/.");
        fullPath += randomString(RANDOM_FILE_NAME_LENGTH);

        os.open(fullPath);
        if (!os) {
            E("(LocalFileManager) Fail to create temporary file.");
            I("Check if there is no space.");
            return false;
        }

        {
            std::lock_guard<std::mutex> holder{lock};
            dumpFiles.push_back(std::move(fullPath));
        }

        return true;

    }

    // Dump data to temporary file
    template <typename DataType>
    bool LocalFileManager<DataType>::dumpToFile(std::vector<const DataType *> & data) {

        std::ofstream os;

        if (!getStream(os)) {
            return false;
        }

        for (size_t i = 0, l = data.size(); i < l; ++i) {
            if (!(os << *(data[i]))) {
                E("(LocalFileManager) Fail to write data to file.");
                I("Check if there is no space.");

                // Clean the space
                for (; i < l; ++i) {
                    delete data[i];
                }
                data.clear();
                os.close();
                return false;
            }
            delete data[i];
        }

        os.close();
        data.clear();
        return true;

    }

    // Get sorted stream with all files
    template <typename DataType>
    SortedStream<DataType> * LocalFileManager<DataType>::getSortedStream() {

        // Sort the data
        if (!doMergeSort()) {
            return nullptr;
        }

        SortedStream<DataType> * ret = new SortedStream<DataType>{std::move(dumpFiles)};
        if (ret->open()) {
            return ret;
        } else {
            delete ret;
            return nullptr;
        }

    }

    // Get sorted streams grouped by MERGE_SORT_WAY files
    template <typename DataType>
    std::vector<std::unique_ptr<SortedStream<DataType> > >
        LocalFileManager<DataType>::getSortedStreams() {

        std::vector<std::unique_ptr<SortedStream<DataType> > > stms;

        if (dumpFiles.size() == 0) {
            return stms;
        }

        std::vector<std::string> files{std::move(dumpFiles)};
        dumpFiles.clear();

        FileIter begin = files.begin();
        FileIter end = files.end();
        FileIter beforeEnd = end - MERGE_SORT_WAY;

        while (begin < beforeEnd) {
            stms.emplace_back(new SortedStream<DataType>{begin, begin + MERGE_SORT_WAY});
            begin += MERGE_SORT_WAY;
        }

        stms.emplace_back(new SortedStream<DataType>{begin, end});

        return stms;

    }

    // Get unsorted stream with all files
    template <typename DataType>
    UnsortedStream<DataType> * LocalFileManager<DataType>::getUnsortedStream() {

        UnsortedStream<DataType> * ret = new UnsortedStream<DataType>{std::move(dumpFiles)};
        if (ret->isValid()) {
            return ret;
        } else {
            delete ret;
            return nullptr;
        }

    }

}

#endif
