#ifndef SOURCE_MANAGER_H
#define SOURCE_MANAGER_H

#include <thread>
#include <mutex>
#include <vector>
#include <string>
#include <string.h>
#include <stdio.h> // fopen, fread, ...
#include "utils.hpp"

namespace ch {

    const char C_SERVER = CALL_SERVER;
    const char C_CLIENT = CALL_CLIENT;
    const char C_POLL = CALL_POLL;
    const char C_END = CALL_END;

    class SourceManager;

    struct distribution_thread_in_t {
        int _ind;
        std::vector<std::pair<int, std::string> > & _ips;
        unsigned short _port;
        SourceManager * _source;

        explicit distribution_thread_in_t(int ind, std::vector<std::pair<int, std::string> > & ips, unsigned short port, SourceManager * source): _ind(ind), _ips(ips), _port(port), _source(source) {}
    };

    class BlockedBuffer {
        protected:
            FILE * _fd;
            char buffer[DATA_BLOCK_SIZE + 1];
            std::mutex readLock;
            size_t bufferedLength; // byte cached in buffer

        public:
            BlockedBuffer(): _fd(NULL), bufferedLength(0) {
                buffer[DATA_BLOCK_SIZE] = '\0';
            }

            ~BlockedBuffer() {
                setFd(NULL);
            }

            bool open(const char * file) {
                setFd(NULL);
                _fd = fopen(file, "r");
                return isValid();
            }

            inline bool isValid() {
                return (_fd != NULL);
            }

            void setFd(FILE * fd) {
                if (isValid()) {
                    fclose(_fd);
                }
                _fd = fd;
                bufferedLength = 0;
            }

            bool next(std::string & res) {
                std::lock_guard<std::mutex> holder(readLock);
                res.clear();
                if (!isValid()) {
                    return false;
                }
                size_t rv = fread(buffer + bufferedLength, sizeof(char), DATA_BLOCK_SIZE - bufferedLength, _fd);
                if (rv == 0) { // EOF
                    if (bufferedLength == 0) {
                        setFd(NULL);
                        return false;
                    } else {
                        res.append(buffer, bufferedLength);
                        res.push_back('\n');
                        setFd(NULL);
                        return true;
                    }
                } else {
                    size_t totalLength = rv + bufferedLength;
                    size_t cursor = totalLength - 1;
                    char c;
                    for (cursor = totalLength - 1; cursor >= 0; --cursor) {
                        c = buffer[cursor];
                        if (IS_ESCAPER(c)) {
                            int returnLength = cursor + 1;
                            res.append(buffer, returnLength);
                            bufferedLength = totalLength - returnLength;
                            memmove(static_cast<void *>(buffer), static_cast<void *>(buffer + returnLength), bufferedLength);
                            return true;
                        }
                    }
                    D("Line out of buffer.\n");
                    setFd(NULL);
                    return false;
                }
            }
    };

    class SourceManager {
        protected:
            bool isServer;
            int fd;
            std::vector<std::thread *> threads;
            BlockedBuffer buffer;
            std::string _jobFilePath;
            std::string _jobFile;
        public:
            /*
             * Constructor for server
             */
            SourceManager(const char * dataFile, std::string & jobFilePath): isServer(true), _jobFilePath(jobFilePath) {
                fd = INVALID_SOCKET;
                if (ch::readFileAsString(jobFilePath.c_str(), _jobFile)) {
                    D("Attempt to open data file.\n");
                    fd = (buffer.open(dataFile) ? ~INVALID : INVALID);
                    D("Data file opened.\n");
                }
            }

            /*
             * Constructor for clients
             */
            SourceManager(int sockfd, std::string & jobFilePath): isServer(false), fd(sockfd), _jobFilePath(jobFilePath) {}

            inline bool isValid() {
                return (fd > 0);
            }

            /*
             * get data block
             */
            bool readBlock(std::string & res) {
                if (!isValid()) {
                    return false;
                } else if (isServer) {
                    return buffer.next(res);
                } else {
                    D("Reading data block on client make no sense.\n");
                    return false;
                }
            }

            static void distributionThread(distribution_thread_in_t * args_in) {
                int csockfd;
                std::vector<std::pair<int, std::string> > & ips = args_in->_ips;
                unsigned short port = args_in->_port;
                int i = args_in->_ind;
                SourceManager * source = args_in->_source;
                delete args_in;

                // create socket to clients
                if (sconnect(csockfd, ips[i].second.c_str(), port) < 0) {
                    L("Cannot connect to client.\n");
                    return;
                }

                // create client on clients
                if(psend(csockfd, static_cast<const void *>(&C_CLIENT), sizeof(char)) < 0) {
                    L("Miss one client.\n");
                    // TODO cope with this (fault tolerant)
                    close(csockfd);
                    return;
                }

                // send configuration file
                std::string rearrangedConfiguration;
                rearrangeIPs(ips, rearrangedConfiguration, i);
                sendString(csockfd, rearrangedConfiguration);

                // send job file
                sendString(csockfd, source->_jobFile);

                // provide poll service
                ssize_t rv;
                char receivedChar;
                std::string block;
                ssize_t endSize = INVALID;
                while ((rv = precv(csockfd, static_cast<void *>(&receivedChar), sizeof(char))) > 0) {
                    if (receivedChar == CALL_POLL) {
                        if (!(source->readBlock(block))) { // end service by server
                            psend(csockfd, static_cast<void *>(&endSize), sizeof(ssize_t));
                            D("Poll reach end of line.\n");
                            break;
                        }
                        if (!sendString(csockfd, block)) {
                            D("Failed to send split.\n");
                            break;
                        }
                    } else { // end service by client
                        break;
                    }
                }
                close(csockfd);
            }

            void blockTillDistributionThreadsEnd() {
                for (std::thread * t: threads) {
                    t->join();
                    delete t;
                }
                threads.clear();
            }

            bool startFileDistributionThreads(int serverfd, std::vector<std::pair<int, std::string> > & ips, unsigned short port) {
                if (!isValid()) {
                    return false;
                }
                if (!isServer) {
                    D("Cannot start distribution on client.\n");
                    return false;
                }
                int numIP = ips.size();
                for (int i = 1; i < numIP; ++i) {
                    distribution_thread_in_t * distribution_thread_in = new distribution_thread_in_t(i, ips, port, this);
                    std::thread * t = new std::thread(distributionThread, distribution_thread_in);
                    threads.push_back(t);
                }
                return true;
            }

            bool receiveAsClient(std::string & confFilePath) {
                if (!isValid()) {
                    return false;
                }
                if (isServer) {
                    D("Cannot start receive on server.\n");
                    return false;
                }
                if (!receiveFile(fd, confFilePath.c_str())) {
                    L("Fail to receive configuration file.\n");
                    return false;
                }
                if (!receiveFile(fd, _jobFilePath.c_str())) {
                    L("Fail to receive job file.\n");
                    return false;
                }
                return true;
            }

            bool poll(std::string & res) {
                res.clear();
                if (!isValid()) {
                    return false;
                } else if (isServer) {
                    return readBlock(res);
                } else {
                    // request a block
                    ssize_t rv = psend(fd, static_cast<const void *>(&C_POLL), sizeof(char));
                    if (rv < 0) {
                        L("Broken pipe.\n");
                        return false;
                    } else if (!receiveString(fd, res)) {
                        D("Fail to receive the string or remote file EOF.\n");
                        close(fd);
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
