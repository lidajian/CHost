/*
 * Manage job resource (configuration file, job library, data file)
 */

#ifndef SOURCE_MANAGER_H
#define SOURCE_MANAGER_H

#include <thread> // thread
#include <vector> // vector
#include <string> // string
#include <atomic> // atomic_uint
#include <unordered_map> // unordered_map
#include <mutex> // mutex

#include "splitter.hpp" // Splitter
#include "utils.hpp" // sconnect, invokeWorker
#include "threadPool.hpp" // ThreadPool

namespace ch {

    class SourceManager {
        public:
            // True if the source manager is good
            virtual bool isValid() const = 0;

            // Poll data from source manager
            virtual bool poll(std::string & ret) = 0;
    };

    /*
     * SourceManagerMaster: source manager for master
     */
    class SourceManagerMaster: public SourceManager {
        protected:
            // Splitter for data file
            Splitter splitter;

            // Path of job file
            const std::string _jobFilePath;

            // Cache of job file
            std::string _jobFileContent;

            // File descriptor of connections
            std::vector<int> connections;

            // Distribution thread(s)
            std::thread * dthread;

            // Results from workers
            std::vector<bool> workerIsSuccess;

            // Rearrange ipconfig to create configure files for other machines
            static void rearrangeIPs(const ipconfig_t & ips, std::string & file, const size_t indexToHead);

        public:

            // Constructor
            SourceManagerMaster(const char * dataFile, const std::string & jobFilePath);

            // Copy constructor (deleted)
            SourceManagerMaster(const SourceManagerMaster &) = delete;

            // Move constructor (deleted)
            SourceManagerMaster(SourceManagerMaster &&) = delete;

            // Copy assignment (deleted)
            SourceManagerMaster & operator = (const SourceManagerMaster &) = delete;

            // Move assignment (deleted)
            SourceManagerMaster & operator = (SourceManagerMaster &&) = delete;

            // Destructor
            ~SourceManagerMaster();

            // Connect to workers and deliver files
            bool connectAndDeliver(const ipconfig_t & ips, unsigned short port = SERVER_PORT);

            // Start distribution thread
            void startDistributionThread();

            // Block the current thread until all distribution threads terminate
            void blockTillDistributionThreadsEnd();

            // True if all worker success
            bool allWorkerSuccess();

            bool isValid() const;

            bool poll(std::string & ret);
    };

    /*
     * SourceManagerWorker: source manager for workers
     */
    class SourceManagerWorker: public SourceManager {
        protected:
            // Socket file descriptor
            int fd;

            // Send poll request
            bool pollRequest() const;

#ifdef MULTIPLE_MAPPER
            // Mutex
            std::mutex lock;
#endif
        public:

            // Constructor for worker
            SourceManagerWorker(int sockfd);

            // Copy Constructor
            SourceManagerWorker(const SourceManagerWorker & o);

            // Move Constructor
            SourceManagerWorker(SourceManagerWorker && o);

            // Copy assignment
            SourceManagerWorker & operator = (const SourceManagerWorker & o);

            // Move assignment
            SourceManagerWorker & operator = (SourceManagerWorker && o);

            // Receive resource files
            // 1. Configuration file
            // 2. Job file
            bool receiveFiles(const std::string & confFilePath, const std::string & jobFilePath);

            bool isValid() const;

            bool poll(std::string & ret);
    };
}

#endif
