#ifndef SOURCE_MANAGER_H
#define SOURCE_MANAGER_H

#include <thread>
#include <mutex>
#include <vector>
#include <string>
#include <string.h>
#include "utils.hpp"

namespace ch {

    const char C_SERVER = CALL_SERVER;
    const char C_CLIENT = CALL_CLIENT;
    const char C_POLL = CALL_POLL;
    const char C_END = CALL_END;

    class SourceManager;

    void rearrangeIPs(std::vector<std::pair<int, std::string> > & ips, std::string & file, int indexToHead) {
        file = std::to_string(ips[indexToHead].first) + " " + ips[indexToHead].second + "\n";
        for (int i = 0, l = ips.size(); i < l; ++i) {
            if (i != indexToHead) {
                file += std::to_string(ips[i].first) + " " + ips[i].second + "\n";
            }
        }
    }

    struct distribution_thread_in_t {
        int _ind;
        std::vector<std::pair<int, std::string> > & _ips;
        unsigned short _port;
        SourceManager * _source;

        explicit distribution_thread_in_t(int ind, std::vector<std::pair<int, std::string> > & ips, unsigned short port, SourceManager * source): _ind(ind), _ips(ips), _port(port), _source(source) {}
    };

    class BlockedBuffer {
        protected:
            int _fd;
            bool valid;
            char buffer[DATA_BLOCK_SIZE + 1];
            size_t bufferedLength; // byte cached in buffer

        public:
            BlockedBuffer(): _fd(INVALID), valid(false), bufferedLength(0) {
                buffer[DATA_BLOCK_SIZE] = '\0';
            }

            void setFd(int fd) {
                _fd = fd;
                if (_fd > 0) {
                    valid = true;
                }
            }

            bool next(std::string & res) {
                res.clear();
                if (!valid) {
                    return false;
                }
                ssize_t rv = sread(_fd, static_cast<void *>(buffer + bufferedLength), DATA_BLOCK_SIZE - bufferedLength);
                if (rv < 0) { // EOF
                    bufferedLength = 0;
                    valid = false;
                    return false;
                } else {
                    size_t totalLength = rv + bufferedLength;
                    if (totalLength == 0) {
                        valid = false;
                        return false;
                    }
                    size_t cursor = totalLength - 1;
                    while (cursor > 0 && buffer[cursor] != '\r' && buffer[cursor] != '\n') {
                        --cursor;
                    }
                    if (cursor <= 0 && buffer[0] != '\n' && buffer[0] != '\r') {
                        D("Line out of buffer.\n");
                        bufferedLength = 0;
                        valid = false;
                        return false;
                    }
                    int returnLength = cursor + 1;
                    char charAtSplit = buffer[returnLength];
                    buffer[returnLength] = '\0';
                    res = buffer;
                    bufferedLength = totalLength - returnLength;
                    memmove(static_cast<void *>(buffer), static_cast<void *>(buffer + returnLength), bufferedLength);
                    buffer[0] = charAtSplit;
                    return true;
                }
            }
    };

    class SourceManager {
        protected:
            bool isServer;
            int fd;
            std::mutex dataLock;
            std::vector<std::thread *> threads;
            BlockedBuffer buffer;
            std::string _jobFilePath;
            std::string _jobFile;
        public:
            inline bool isValid() {
                return (fd > 0);
            }
            inline void setInvalid() {
                fd = INVALID_SOCKET;
            }
            bool readBlock(std::string & res) {
                if (!isValid()) {
                    return false;
                } else if (isServer) {
                    bool ret;
                    dataLock.lock();
                    ret = buffer.next(res);
                    dataLock.unlock();
                    return ret;
                } else {
                    D("Reading data block on client make no sense.\n");
                    return false;
                }
            }
            SourceManager(const char * dataFile, std::string & jobFilePath): isServer(true), _jobFilePath(jobFilePath) {
                fd = INVALID_SOCKET;
                if (ch::readFileAsString(jobFilePath.c_str(), _jobFile)) {
                    D("Attempt to open data file.\n");
                    fd = open(dataFile, O_RDONLY);
                    buffer.setFd(fd);
                    D("Data file opened.\n");
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
                ssend(csockfd, static_cast<const void *>(&C_CLIENT), sizeof(char));

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
                while ((rv = srecv(csockfd, static_cast<void *>(&receivedChar), sizeof(char))) > 0) {
                    if (receivedChar == 'P') {
                        if (!(source->readBlock(block))) { // end service by server
                            ssend(csockfd, static_cast<void *>(&endSize), sizeof(ssize_t));
                            D("Poll reach end of line.\n");
                            break;
                        }
                        if (!sendString(csockfd, block)) {
                            L("Failed to send split.\n");
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
            SourceManager(int sockfd, std::string & jobFilePath): isServer(false), fd(sockfd), _jobFilePath(jobFilePath) {}
            ~SourceManager() {
                if (isValid()) {
                    close(fd);
                }
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
                    L("Fail to receive configuration file.\n");
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
                    ssize_t rv = ssend(fd, static_cast<const void *>(&C_POLL), sizeof(char));
                    if (rv < 0) {
                        L("Broken pipe.\n");
                        return false;
                    } else if (!receiveString(fd, res)) {
                        D("Fail to receive the string.\n");
                        ssend(fd, static_cast<const void *>(&C_END), sizeof(char));
                        close(fd);
                        setInvalid();
                        return false;
                    } else {
                        return true;
                    }
                }
            }
    };
}

#endif
