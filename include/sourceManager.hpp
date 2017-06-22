/*
 * Manage job resource (configuration file, job library, data file)
 */

#ifndef SOURCE_MANAGER_H
#define SOURCE_MANAGER_H

#include <thread> // thread
#include <vector> // vector
#include <string> // string
#include "splitter.hpp" // Splitter
#include "utils.hpp"

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

            // Distribution thread
            static void distributionThread(int i, const ipconfig_t & ips, const unsigned short port, SourceManagerMaster * source) {
                int csockfd;

                const std::string & ip = ips[i].second;

                // create socket to clients
                if (!sconnect(csockfd, ip.c_str(), port)) {
                    E("(SourceManagerMaster) Cannot connect to client.\n");
                    return;
                }

                // create client on clients
                if(!invokeWorker(csockfd)) {
                    ESS("(SourceManagerMaster) Cannot invoke worker on " << ip << std::endl);
                    close(csockfd);
                    return;
                }

                // Send configuration file
                std::string rearrangedConfiguration;
                rearrangeIPs(ips, rearrangedConfiguration, i);
                sendString(csockfd, rearrangedConfiguration);

                // Send job file
                sendString(csockfd, source->_jobFileContent);

                // provide poll service
                char receivedChar;
                std::string split;
                ssize_t endSize = INVALID;
                while (precv(csockfd, static_cast<void *>(&receivedChar), sizeof(char))) {
                    if (receivedChar == CALL_POLL) {
                        if (!(source->splitter.next(split))) { // end service by server
                            psend(csockfd, static_cast<void *>(&endSize), sizeof(ssize_t));
                            break;
                        }
                        if (!sendString(csockfd, split)) {
                            E("(SourceManagerMaster) Failed to send split.\n");
                            break;
                        }
                        split.clear();
                    }
                }

                if (!precv(csockfd, static_cast<void *>(&receivedChar), sizeof(char))) {
                    E("(SourceManagerMaster) No response from worker.\n");
                    close(csockfd);
                    return;
                }

                source->workerIsSuccess[i] = (receivedChar == RES_SUCCESS);

                close(csockfd);
            }

        public:

            // Constructor
            SourceManagerMaster(const char * dataFile, const std::string & jobFilePath): _jobFilePath{jobFilePath} {
                if (ch::readFileAsString(jobFilePath.c_str(), _jobFileContent)) {
                    if (splitter.open(dataFile)) {
                        D("(SourceManagerMaster) Fail to open data file.\n");
                    }
                } else {
                    D("(SourceManagerMaster) Fail to read job file.\n");
                }
            }

            // Start distribution threads
            bool startFileDistributionThreads(int serverfd, ipconfig_t & ips, unsigned short port) {
                if (!isValid()) {
                    D("(SourceManagerMaster) Cannot start distribution when data file is not opened.\n");
                    return false;
                }
                workerIsSuccess.resize(ips.size(), false);
                for (size_t i = 1, l = ips.size(); i < l; ++i) {
                    std::thread * t = new std::thread(distributionThread, i, std::ref(ips), port, this);
                    threads.push_back(t);
                }
                return true;
            }

            // Block the current thread until all distribution threads terminate
            void blockTillDistributionThreadsEnd() {
                for (std::thread * t: threads) {
                    t->join();
                    delete t;
                }
                threads.clear();
            }

            // True if all worker success
            bool allWorkerSuccess() {
                size_t numIPs = workerIsSuccess.size();
                if (numIPs == 0) {
                    D("(SourceManagerMaster) Cannot get worker results when no threads started.\n");
                    return false;
                }
                for (size_t i = 1; i < numIPs; ++i) {
                    if (!workerIsSuccess[i]) {
                        DSS("(SourceManagerMaster) The " << i << "th worker failed.\n");
                        return false;
                    }
                }
                return true;
            }

            inline bool isValid() const {
                return splitter.isValid();
            }

            bool poll(std::string & ret) {
                ret.clear();
                return splitter.next(ret);
            }
    };

    /*
     * SourceManagerWorker: source manager for workers
     */
    class SourceManagerWorker: public SourceManager {
        protected:
            // Socket file descriptor
            int fd;

            // Send poll request
            bool pollRequest() const {
                const char c = CALL_POLL;
                return psend(fd, static_cast<const void *>(&c), sizeof(char));
            }
        public:

            // Constructor for worker
            SourceManagerWorker(int sockfd): fd{sockfd} {}

            // Receive resource files
            // 1. Configuration file
            // 2. Job file
            bool receiveFiles(const std::string & confFilePath, const std::string & jobFilePath) {
                if (!isValid()) {
                    D("(SourceManagerWorker) The socket failed.\n");
                    return false;
                }
                if (!receiveFile(fd, confFilePath.c_str())) {
                    E("Fail to receive configuration file.\n");
                    return false;
                }
                if (!receiveFile(fd, jobFilePath.c_str())) {
                    E("Fail to receive job file.\n");
                    return false;
                }
                return true;
            }

            inline bool isValid() const {
                return (fd > 0);
            }

            bool poll(std::string & ret) {
                ret.clear();
                if (!isValid()) {
                    D("(SourceManagerWorker) The socket failed.\n");
                    return false;
                } else {
                    // request a block
                    if (!pollRequest()) {
                        D("(SourceManagerWorker) Fail to send poll request. Broken pipe.\n");
                        fd = INVALID_SOCKET;
                        return false;
                    } else if (!receiveString(fd, ret)) {
                        D("(SourceManagerWorker) Fail to receive the string or remote file EOF.\n");
                        fd = INVALID_SOCKET;
                        return false;
                    } else {
                        return true;
                    }
                }
            }
    };
}

#endif
