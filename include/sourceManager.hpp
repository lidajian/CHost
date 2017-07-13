/*
 * Manage job resource (configuration file, job library, data file)
 */

#ifndef SOURCE_MANAGER_H
#define SOURCE_MANAGER_H

#include <unistd.h>       // close

#include <thread>         // thread
#include <vector>         // vector
#include <string>         // string, to_string
#include <atomic>         // atomic_uint
#include <unordered_map>  // unordered_map
#include <mutex>          // mutex

#include "def.hpp"        // ipconfig_t
#include "splitter.hpp"   // Splitter
#include "utils.hpp"      // sconnect, getWorkingDirectory, receiveFile, invokeWorker,
                          // readFileAsString, sconnect, sendString, Recv, Send,
                          // cancelWorker
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
            SourceManagerMaster(const std::string & dataFile, const std::string & jobFilePath);

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
            bool connectAndDeliver(const ipconfig_t & ips, const std::string & jobName, unsigned short port = SERVER_PORT);

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

#ifdef MULTIPLE_MAPPER
            // Mutex for poll
            std::mutex lock;
#endif

            // Send poll request
            bool pollRequest() const;

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
            bool receiveFiles(std::string & confFilePath, std::string & jobFilePath, std::string & jobName, std::string & workingDir);

            bool isValid() const;

            bool poll(std::string & ret);
    };
}

#endif
