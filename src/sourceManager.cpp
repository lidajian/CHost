#include "sourceManager.hpp"

namespace ch {

    // Rearrange ipconfig to create configure files for other machines
    void SourceManagerMaster::rearrangeIPs(const ipconfig_t & ips, std::string & file, const size_t indexToHead) {
        file = std::to_string(ips[indexToHead].first) + " " + ips[indexToHead].second + "\n";
        for (size_t i = 0, l = ips.size(); i < l; ++i) {
            if (i != indexToHead) {
                file += std::to_string(ips[i].first) + " " + ips[i].second + "\n";
            }
        }
    }

    // Constructor
    SourceManagerMaster::SourceManagerMaster(const char * dataFile, const std::string & jobFilePath): _jobFilePath{jobFilePath} {
        if (ch::readFileAsString(jobFilePath.c_str(), _jobFileContent)) {
            if (splitter.open(dataFile)) {
                D("(SourceManagerMaster) Fail to open data file.");
            }
        } else {
            D("(SourceManagerMaster) Fail to read job file.");
        }
    }

    // Destructor
    SourceManagerMaster::~SourceManagerMaster() {
        for (size_t i = 1, l = connections.size(); i < l; ++i) {
            if (connections[i] >= 0) {
                close(connections[i]);
            }
        }
    }

    // Connect to workers and deliver files
    bool SourceManagerMaster::connectAndDeliver(const ipconfig_t & ips, unsigned short port) {
        size_t l = ips.size();
        if (l == 0) {
            return false;
        }
        connections.resize(l, INVALID_SOCKET);
        size_t nConnections = l - 1;
        if (nConnections > 0) {
            ThreadPool threadPool{MIN_VAL(THREAD_POOL_SIZE, nConnections)};
            for (size_t i = 1; i < l; ++i) {
                int & sockfd = connections[i];
                threadPool.addTask([i, &ips, port, &sockfd, this](){ // Start connection threads

                    const std::string & ip = ips[i].second;

                    // create socket to clients
                    if (!sconnect(sockfd, ip.c_str(), port)) {
                        ESS("(SourceManagerMaster) Cannot connect to client on" << ip);
                        sockfd = INVALID_SOCKET;
                        return;
                    }

                    // create client on clients
                    if(!invokeWorker(sockfd)) {
                        ESS("(SourceManagerMaster) Cannot invoke worker on " << ip);
                        close(sockfd);
                        sockfd = INVALID_SOCKET;
                        return;
                    }

                    // Send configuration file
                    std::string rearrangedConfiguration;
                    rearrangeIPs(ips, rearrangedConfiguration, i);
                    if (!sendString(sockfd, rearrangedConfiguration)) {
                        close(sockfd);
                        sockfd = INVALID_SOCKET;
                        return;
                    }

                    // Send job file
                    if (!sendString(sockfd, this->_jobFileContent)) {
                        close(sockfd);
                        sockfd = INVALID_SOCKET;
                        return;
                    }

                });
            }
            threadPool.stop(); // wait until all connection thread ends
            for (size_t i = 1; i < l; ++i) {
                if (connections[i] < 0) {
                    // Clean up all connections
                    for (i = 1; i < l; ++i) {
                        if (connections[i] >= 0) {
                            close(connections[i]);
                        }
                    }
                    connections.clear();
                    return false;
                }
            }
        }
        return true;
    }

    // Start distribution thread
    void SourceManagerMaster::startDistributionThread() {
        size_t l = connections.size();
        workerIsSuccess.resize(l, false);

        // Start the distribution thread
        dthread = new std::thread{[this, l](){

            if (l < 2) {
                // No worker
                return;
            }

            if (l == 2) {
                // Only one worker, this thread servers as distribution threads
                int & sockfd = this->connections[1];
                char receivedChar;
                std::string split;
                ssize_t endSize = INVALID;
                while (precv(sockfd, static_cast<void *>(&receivedChar), sizeof(char))) {
                    if (receivedChar == CALL_POLL) {
                        if (!(this->splitter.next(split))) { // end service by server
                            psend(sockfd, static_cast<void *>(&endSize), sizeof(ssize_t));
                            break;
                        }
                        if (!sendString(sockfd, split)) {
                            E("(SourceManagerMaster) Failed to send split.");
                            break;
                        }
                        split.clear();
                    }
                }

                if (!precv(sockfd, static_cast<void *>(&receivedChar), sizeof(char))) {
                    E("(SourceManagerMaster) No response from worker.");
                    close(sockfd);
                    return;
                }

                this->workerIsSuccess[1] = (receivedChar == RES_SUCCESS);
                close(sockfd);
                return;
            }

            if (l <= THREAD_POOL_SIZE) {
                // Less than/equals to THREAD_POOL_SIZE threads, start threads directly
                std::vector<std::thread> dthreads;

                for (size_t i = 1; i < l; ++i) {
                    int & sockfd = this->connections[i];
                    dthreads.emplace_back([i, &sockfd, this](){

                        // provide poll service
                        char receivedChar;
                        std::string split;
                        ssize_t endSize = INVALID;
                        while (precv(sockfd, static_cast<void *>(&receivedChar), sizeof(char))) {
                            if (receivedChar == CALL_POLL) {
                                if (!(this->splitter.next(split))) { // end service by server
                                    psend(sockfd, static_cast<void *>(&endSize), sizeof(ssize_t));
                                    break;
                                }
                                if (!sendString(sockfd, split)) {
                                    E("(SourceManagerMaster) Failed to send split.");
                                    break;
                                }
                                split.clear();
                            }
                        }

                        if (!precv(sockfd, static_cast<void *>(&receivedChar), sizeof(char))) {
                            E("(SourceManagerMaster) No response from worker.");
                            close(sockfd);
                            return;
                        }

                        this->workerIsSuccess[i] = (receivedChar == RES_SUCCESS);
                        close(sockfd);
                    });
                }

                for (std::thread & thrd: dthreads) {
                    thrd.join();
                }
                return;
            }

            /*
             * More than THREAD_POOL_SIZE threads: Event loop + thread pool
             */

            size_t nConnections = l - 1;
            int nEvents;
            ThreadPool threadPool{THREAD_POOL_SIZE};

            std::unordered_map<int, bool> repliedEOF;
            std::unordered_map<int, std::string> splitCaches;
            std::unordered_map<int, int> fdToIndex;
            std::atomic_uint endedConnection{0};

#if defined (__CH_KQUEUE__)

            // Register events
            int kq = kqueue();

            if (kq < 0) {
                return;
            }

            struct kevent events[nConnections];
            for (size_t i = 1; i < l; ++i) {
                const int & conn = connections[i];
                EV_SET(events + i - 1, conn, EVFILT_READ, EV_ADD, 0, 0, nullptr);
                repliedEOF[conn] = false;
                fdToIndex[conn] = i;
                splitCaches[conn] = std::string();
            }
            if (Kevent(kq, events, nConnections, nullptr, 0, nullptr) < 0) {
                close(kq);
                return;
            }

            // Handle events
            while (endedConnection < nConnections) {
                nEvents = Kevent(kq, nullptr, 0, events, nConnections, nullptr);
                if (nEvents < 0) {
                    break;
                } else {
                    for (int i = 0; i < nEvents; ++i) {
                        int sockfd = events[i].ident;

#elif defined (__CH_EPOLL__)

            // Register events
            int ep = epoll_create1(0);

            if (ep < 0) {
                return;
            }

            struct epoll_event events[nConnections];
            for (size_t i = 1; i < l; ++i) {
                const int & conn = connections[i];
                events[i - 1].events = EPOLLIN;
                events[i - 1].data.fd = conn;
                if (epoll_ctl(ep, EPOLL_CTL_ADD, conn, events) < 0) {
                    close(ep);
                    return;
                }
                repliedEOF[conn] = false;
                fdToIndex[conn] = i;
                splitCaches[conn] = std::string();
            }

            // Handle events
            while (endedConnection < nConnections) {
                nEvents = Epoll_wait(ep, events, nConnections, -1);
                if (nEvents < 0) {
                    break;
                } else {
                    for (int i = 0; i < nEvents; ++i) {
                        int sockfd = events[i].data.fd;
#else
            // Register events
            fd_set fdset_o, fdset;
            int fdmax = 0;
            FD_ZERO(&fdset_o);
            for (size_t i = 1; i < l; ++i) {
                const int & conn = connections[i];
                fdmax = MAX_VAL(fdmax, conn);
                FD_SET(conn, &fdset_o);
                repliedEOF[conn] = false;
                fdToIndex[conn] = i;
                splitCaches[conn] = std::string();
            }

            // Handle events
            while (endedConnection < nConnections) {
                fdset = fdset_o;
                nEvents = Select(fdmax + 1, &fdset, nullptr, nullptr, nullptr);
                if (nEvents < 0) {
                    break;
                } else {
                    for (size_t i = 1; i < l; ++i) {
                        int sockfd = connections[i];
                        if (!FD_ISSET(sockfd, &fdset)) {
                            continue;
                        }
#endif
                        // It is guaranteed that only one thread for a sockfd runs at the same time
                        if (repliedEOF[sockfd]) {
                            char receivedChar = RES_FAIL;

                            if (!precv(sockfd, static_cast<void *>(&receivedChar), sizeof(char))) {
                                E("(SourceManagerMaster) No response from worker.");
                                ++endedConnection;
                                close(sockfd);
                            } else {
                                if (receivedChar == RES_FAIL) {
                                    ++endedConnection;
                                    close(sockfd);
                                } else if (receivedChar == RES_SUCCESS) {
                                    this->workerIsSuccess[fdToIndex[sockfd]] = (receivedChar == RES_SUCCESS);
                                    ++endedConnection;
                                    close(sockfd);
                                }
                            }
                        } else {
                            // provide poll service
                            char receivedChar;
                            if (!precv(sockfd, static_cast<void *>(&receivedChar), sizeof(char))) {
                                ++endedConnection;
                                close(sockfd);
                            } else {
                                if (receivedChar == CALL_POLL) {
                                    std::string & split = splitCaches[sockfd];
                                    threadPool.addTask([this, sockfd, &repliedEOF, &endedConnection, &split](){
                                        if (!(this->splitter.next(split))) { // end service by server
                                            ssize_t endSize = INVALID;
                                            repliedEOF[sockfd] = true;
                                            psend(sockfd, static_cast<void *>(&endSize), sizeof(ssize_t));
                                        } else if (!sendString(sockfd, split)) {
                                            E("(SourceManagerMaster) Failed to send split.");
                                            ++endedConnection;
                                            close(sockfd);
                                        }
                                    });
                                } else {
                                    ++endedConnection;
                                    close(sockfd);
                                }
                            }
                        }
#if !defined (__CH_KQUEUE__) && !defined (__CH_EPOLL__) // select
                        break;
#endif
                    }
                }
            }
            connections.clear();
#if defined (__CH_KQUEUE__)
            close(kq);
#elif defined (__CH_EPOLL__)
            close(ep);
#endif
        }};
    }

    // Block the current thread until all distribution threads terminate
    void SourceManagerMaster::blockTillDistributionThreadsEnd() {
        if (dthread != nullptr) {
            dthread->join();
            delete dthread;
            dthread = nullptr;
        }
    }

    // True if all worker success
    bool SourceManagerMaster::allWorkerSuccess() {
        size_t numIPs = workerIsSuccess.size();
        if (numIPs == 0) {
            D("(SourceManagerMaster) Cannot get worker results when no threads started.");
            return false;
        }
        for (size_t i = 1; i < numIPs; ++i) {
            if (!workerIsSuccess[i]) {
                DSS("(SourceManagerMaster) The " << i << "th worker failed.");
                return false;
            }
        }
        return true;
    }

    inline bool SourceManagerMaster::isValid() const {
        return splitter.isValid();
    }

    bool SourceManagerMaster::poll(std::string & ret) {
        ret.clear();
        return splitter.next(ret);
    }

    // Send poll request
    bool SourceManagerWorker::pollRequest() const {
        const char c = CALL_POLL;
        return psend(fd, static_cast<const void *>(&c), sizeof(char));
    }

    // Constructor for worker
    SourceManagerWorker::SourceManagerWorker(int sockfd): fd{sockfd} {}

    // Copy Constructor
    SourceManagerWorker::SourceManagerWorker(const SourceManagerWorker & o): fd{o.fd} {}

    // Move Constructor
    SourceManagerWorker::SourceManagerWorker(SourceManagerWorker && o): fd{o.fd} {
        o.fd = INVALID_SOCKET;
    }

    // Copy assignment
    SourceManagerWorker & SourceManagerWorker::operator = (const SourceManagerWorker & o) {
        fd = o.fd;
        return *this;
    }

    // Move assignment
    SourceManagerWorker & SourceManagerWorker::operator = (SourceManagerWorker && o) {
        fd = o.fd;
        o.fd = INVALID_SOCKET;
        return *this;
    }

    // Receive resource files
    // 1. Configuration file
    // 2. Job file
    bool SourceManagerWorker::receiveFiles(const std::string & confFilePath, const std::string & jobFilePath) {
        if (!isValid()) {
            D("(SourceManagerWorker) The socket failed.");
            return false;
        }
        if (!receiveFile(fd, confFilePath.c_str())) {
            E("Fail to receive configuration file.");
            return false;
        }
        if (!receiveFile(fd, jobFilePath.c_str())) {
            E("Fail to receive job file.");
            return false;
        }
        return true;
    }

    inline bool SourceManagerWorker::isValid() const {
        return (fd > 0);
    }

    bool SourceManagerWorker::poll(std::string & ret) {
        ret.clear();
        if (!isValid()) {
            D("(SourceManagerWorker) The socket failed.");
            return false;
        } else {
            // request a block
            if (!pollRequest()) {
                D("(SourceManagerWorker) Fail to send poll request. Broken pipe.");
                fd = INVALID_SOCKET;
                return false;
            } else if (!receiveString(fd, ret)) {
                D("(SourceManagerWorker) Fail to receive the string or remote file EOF.");
                fd = INVALID_SOCKET;
                return false;
            } else {
                return true;
            }
        }
    }
}
