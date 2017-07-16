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

    class ClusterManager {

        public:

            // True if the source manager is good
            virtual bool isValid() const = 0;

            // Poll data from source manager
            virtual bool poll(std::string & ret) = 0;
    };

    /*
     * ClusterManagerMaster: source manager for master
     */
    class ClusterManagerMaster: public ClusterManager {

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
            static void rearrangeIPs(const ipconfig_t & ips, std::string & file, const size_t & indexToHead);

        public:

            // Constructor
            ClusterManagerMaster(const std::string & dataFile, const std::string & jobFilePath);

            // Copy constructor (deleted)
            ClusterManagerMaster(const ClusterManagerMaster &) = delete;

            // Move constructor (deleted)
            ClusterManagerMaster(ClusterManagerMaster &&) = delete;

            // Copy assignment (deleted)
            ClusterManagerMaster & operator = (const ClusterManagerMaster &) = delete;

            // Move assignment (deleted)
            ClusterManagerMaster & operator = (ClusterManagerMaster &&) = delete;

            // Destructor
            ~ClusterManagerMaster();

            // Connect to workers and deliver files
            bool connectAndDeliver(const ipconfig_t & ips, const std::string & jobName, const unsigned short port = SERVER_PORT);

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
     * ClusterManagerWorker: source manager for workers
     */
    class ClusterManagerWorker: public ClusterManager {

        protected:

            // Socket file descriptor
            int fd;

#ifdef MULTITHREAD_SUPPORT
            // Mutex for poll
            std::mutex lock;
#endif

            // Send poll request
            bool pollRequest() const;

        public:

            // Constructor for worker
            ClusterManagerWorker(const int & sockfd);

            // Copy Constructor
            ClusterManagerWorker(const ClusterManagerWorker & o);

            // Move Constructor
            ClusterManagerWorker(ClusterManagerWorker && o);

            // Copy assignment
            ClusterManagerWorker & operator = (const ClusterManagerWorker & o);

            // Move assignment
            ClusterManagerWorker & operator = (ClusterManagerWorker && o);

            // Receive resource files
            // 1. Configuration file
            // 2. Job file
            bool receiveFiles(std::string & confFilePath, std::string & jobFilePath, std::string & jobName, std::string & workingDir);

            bool isValid() const;

            bool poll(std::string & ret);
    };
}

#endif
