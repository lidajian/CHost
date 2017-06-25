/*
 * Manage job resource (configuration file, job library, data file)
 */

#ifndef SOURCE_MANAGER_H
#define SOURCE_MANAGER_H

#include <thread> // thread
#include <vector> // vector
#include <string> // string
#include "splitter.hpp" // Splitter
#include "utils.hpp" // sconnect, invokeWorker

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

            // Distribution threads
            std::vector<std::thread *> threads;

            // Results from workers
            std::vector<bool> workerIsSuccess;

            // Rearrange ipconfig to create configure files for other machines
            static void rearrangeIPs(const ipconfig_t & ips, std::string & file, const size_t indexToHead);

            // Distribution thread
            static void distributionThread(int i, const ipconfig_t & ips, const unsigned short port, SourceManagerMaster * source);

        public:

            // Constructor
            SourceManagerMaster(const char * dataFile, const std::string & jobFilePath);

            // Copy constructor (deleted)
            SourceManagerMaster(const SourceManagerMaster &) = delete;

            // Move constructor (deleted)
            SourceManagerMaster(SourceManagerMaster &&) = delete;

            // Start distribution threads
            void startFileDistributionThreads(int serverfd, const ipconfig_t & ips, unsigned short port);

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
        public:

            // Constructor for worker
            SourceManagerWorker(int sockfd);

            // Copy Constructor
            SourceManagerWorker(const SourceManagerWorker & o);

            // Move Constructor
            SourceManagerWorker(SourceManagerWorker && o);

            // Receive resource files
            // 1. Configuration file
            // 2. Job file
            bool receiveFiles(const std::string & confFilePath, const std::string & jobFilePath);

            bool isValid() const;

            bool poll(std::string & ret);
    };
}

#endif
