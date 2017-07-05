/*
 * Peer to peer connection between jobs - transport objects
 */

#ifndef STREAMMANAGER_H
#define STREAMMANAGER_H

#include <netinet/in.h> // sockaddr_in
#include <unistd.h> // close
#include <sys/socket.h> // accept

#include <vector> // vector
#include <string> // string
#include <fstream> // ofstream
#include <thread> // thread
#include <chrono> // seconds
#include <memory> // unique_ptr, std::addressof

#include "objectStream.hpp" // ObjectInputStream, ObjectOutputStream
#include "dataManager.hpp" // DataManager
#include "partitioner.hpp" // Partitioner
#include "threadPool.hpp" // ThreadPool

namespace ch {

    /********************************************
     ************** Declaration *****************
    ********************************************/

    template <typename DataType>
    class StreamManager {
        protected:
            // Id of the machine
            size_t selfId;

            // Number of machines in the cluster
            size_t clusterSize;

            // True if connected
            bool connected;

            // Receive threads
            std::vector<std::thread> receiveThreads;

            // Input streams
            std::vector<ObjectInputStream<DataType> *> istreams;

            // Output streams
            std::vector<ObjectOutputStream<DataType> *> ostreams;

            // Data manager
            DataManager<DataType> _data;

            // Partitioner
            const Partitioner * _partitioner;

            // Server thread: accept connections
            static void serverThread(int serverfd, const ipconfig_t & ips, std::vector<ObjectInputStream<DataType> *> & istreams);

            // Connect thread: connect to servers
            // sets stmr to the created object output stream if success
            static void connectThread(const std::string & ip, ObjectOutputStream<DataType> * & stmr);

            // Receive thread: receive objects and store
            static void recvThread(ObjectInputStream<DataType> * & stm, DataManager<DataType> & data);

            // Close and clear all streams
            void clearStreams();

            // Initialize streams
            // 1. Accept and connect to all machines
            // 2. Start receive threads if step 1 success
            void init(const ipconfig_t & ips);
        public:
            // Constructor: given directory of configuration
            StreamManager(const std::string & configureFile, const std::string & dir, size_t maxDataSize = DEFAULT_MAX_DATA_SIZE, bool presort = true, const Partitioner & partitioner = hashPartitioner);

            // Constructor: given vector of IP configuration
            StreamManager(const ipconfig_t & ips, const std::string & dir, size_t maxDataSize = DEFAULT_MAX_DATA_SIZE, bool presort = true, const Partitioner & partitioner = hashPartitioner);

            // Copy constructor (deleted)
            StreamManager(const StreamManager<DataType> &) = delete;

            // Move constructor (deleted)
            StreamManager(StreamManager<DataType> &&) = delete;

            // Copy assignment (deleted)
            StreamManager<DataType> & operator = (const StreamManager<DataType> &) = delete;

            // Move assignment (deleted)
            StreamManager<DataType> & operator = (StreamManager<DataType> &&) = delete;

            // Destructor
            // Send stop signal to other machines and block until receive threads end
            // Close and clear all streams
            ~StreamManager();

            // True if receive threads exists
            bool isReceiving(void) const;

            // True if connected
            bool isConnected(void) const;

            // Start receive on all sockets
            void startRecvThreads(void);

            // Send stop signal to other machines, cause receive thread on other machines to terminate and close connection
            // called when we don't need these connections anymore
            void finalizeSend(void);

            // Send stop signal to other machines, cause receive thread on other machines to terminate
            // called when we need to temporarily stop receiving (e.g. switch from map to reduce)
            void stopSend(void);

            // Cause the current thread to block until all receive thread end and clear resource of receive threads
            void blockTillRecvEnd(void);

            // Push data to the specific machine (partitioned by partitioner)
            bool push(DataType & v);

            // Get sorted stream from data manager
            SortedStream<DataType> * getSortedStream (void);

            // Pour data to text file with temporary files in data manager
            bool pourToTextFile (const char * path);

            // Set presort of data manager
            void setPresort(bool presort);

            // Set partitioner
            void setPartitioner(const Partitioner & partitioner);
    };

    /********************************************
     ************ Implementation ****************
    ********************************************/

    // Server thread: accept connections
    template <typename DataType>
    void StreamManager<DataType>::serverThread(int serverfd, const ipconfig_t & ips, std::vector<ObjectInputStream<DataType> *> & istreams) {

        sockaddr_in remote;
        socklen_t s_size;

        const size_t numIP = ips.size();

        D("(StreamManager) Server accepting client.");

#if defined (__CH_KQUEUE__)
        int kq = kqueue();
        int nEvents;

        if (kq < 0) {
            close(serverfd);
            return;
        }

        struct kevent event;

        // Register
        EV_SET(&event, serverfd, EVFILT_READ, EV_ADD, 0, 0, nullptr);
        if (Kevent(kq, &event, 1, nullptr, 0, nullptr) < 0) {
            close(kq);
            close(serverfd);
            return;
        }

        struct timespec timeout;
        timeout.tv_sec = ACCEPT_TIMEOUT;
        timeout.tv_nsec = 0;

        for (size_t i = 1; i < numIP; ++i) {

            nEvents = Kevent(kq, nullptr, 0, &event, 1, &timeout);

            if (nEvents <= 0) {
                D("(StreamManager) Server failed to accept all clients within timeout.");
                break;
            } else {

#elif defined (__CH_EPOLL__)
        int ep = epoll_create1(0);
        int nEvents;

        if (ep < 0) {
            close(serverfd);
            return;
        }

        struct epoll_event event;

        // Register
        if (epoll_ctl(ep, EPOLL_CTL_ADD, serverfd, &event) < 0) {
            close(ep);
            close(serverfd);
            return;
        }

        for (size_t i = 1; i < numIP; ++i) {

            nEvents = Epoll_wait(ep, &event, 1, ACCEPT_TIMEOUT * 1000);

            if (nEvents <= 0) {
                D("(StreamManager) Server failed to accept all clients within timeout.");
                break;
            } else {

#else
        fd_set accept_fdset;
        struct timeval timeout;
        for (size_t i = 1; i < numIP; ++i) {

            FD_ZERO(&accept_fdset);
            FD_SET(serverfd, &accept_fdset);

            timeout.tv_sec = ACCEPT_TIMEOUT;
            timeout.tv_usec = 0;

            if (Select(serverfd + 1, &accept_fdset, nullptr, nullptr, &timeout) <= 0) {
                D("(StreamManager) Server failed to accept all clients within timeout.");
                break;
            }

            if (!FD_ISSET(serverfd, &accept_fdset)) {
                --i;
            } else {
#endif
                int sockfd = accept(serverfd, reinterpret_cast<struct sockaddr *>(&remote), &s_size);
                if (sockfd > 0 && (getpeername(sockfd, reinterpret_cast<struct sockaddr *>(&remote), &s_size) >= 0)) {
                    PSS("(StreamManager) Server accepted connection from " << inet_ntoa(remote.sin_addr));
                    ObjectInputStream<DataType> * stm = new ObjectInputStream<DataType>(sockfd);
                    istreams.push_back(stm);
                } else {
                    --i;
                    if (sockfd > 0) {
                        close(sockfd);
                    }
                }
            }
        }
#if defined (__CH_KQUEUE__)
        close(kq);
#elif defined (__CH_EPOLL__)
        close(ep);
#endif

        close(serverfd);
    }

    // Connect thread: connect to servers
    // sets stmr to the created object output stream if success
    template <typename DataType>
    void StreamManager<DataType>::connectThread(const std::string & ip, ObjectOutputStream<DataType> * & stmr) {
        ObjectOutputStream<DataType> * stm = new ObjectOutputStream<DataType>{};
        int tries = 0;
        while (tries < MAX_CONNECTION_ATTEMPT && !(stm->open(ip, STREAMMANAGER_PORT))) {
            ++tries;
            // wait at least CONNECTION_RETRY_INTERVAL second before another try
            std::this_thread::yield();
            std::this_thread::sleep_for(std::chrono::seconds(CONNECTION_RETRY_INTERVAL));
        }
        if (tries == MAX_CONNECTION_ATTEMPT) {
            ESS("(StreamManager) Client fail to connect to " << ip);
            delete stm;
        } else {
            PSS("(StreamManager) Client connected to " << ip);
            stmr = stm;
        }
    }

    // Receive thread: receive objects and store
    template <typename DataType>
    void StreamManager<DataType>::recvThread(ObjectInputStream<DataType> * & stm, DataManager<DataType> & data) {
        DataType * got = nullptr;
        while ((got = stm->recv())) {
            if (!data.store(got)) {
                break;
            }
        }
    }

    // Close and clear all streams
    template <typename DataType>
    void StreamManager<DataType>::clearStreams() {
        for (ObjectOutputStream<DataType> * stm: ostreams) {
            if (stm != nullptr) {
                stm->close();
                delete stm;
            }
        }
        ostreams.clear();
        for (ObjectInputStream<DataType> * stm: istreams) {
            if (stm != nullptr) {
                stm->close();
                delete stm;
            }
        }
        istreams.clear();
    }

    // Initialize streams
    // 1. Accept and connect to all machines
    // 2. Start receive threads if step 1 success
    template <typename DataType>
    void StreamManager<DataType>::init(const ipconfig_t & ips){

        if (clusterSize < 2) {
            connected = true;
            return;
        }

        selfId = ips[0].first;

        int serverfd;
        if (!prepareServer(serverfd, STREAMMANAGER_PORT)) {
            E("(StreamManager) Fail to open socket to accept clients.");
            return;
        }

        istreams.clear();
        istreams.reserve(clusterSize - 1);
        ostreams.clear();
        ostreams.resize(clusterSize, nullptr);
        ThreadPool threadPool{MIN_VAL(THREAD_POOL_SIZE, clusterSize)};

        // create server thread to accept connection
        threadPool.addTask(serverThread, serverfd, std::ref(ips), std::ref(istreams));

        // create connect thread to connect to server
        for (size_t i = 1; i < clusterSize; ++i) {
            threadPool.addTask(connectThread, std::ref(ips[i].second), std::ref(ostreams[ips[i].first]));
        }

        threadPool.stop();

        // Check for connection failure
        for (size_t i = 0; i < clusterSize; ++i) {
            if (i != selfId && ostreams[i] == nullptr) {
                E("(StreamManager) One or more of connections are fail.");
                clearStreams();
                return;
            }
        }

        // Check for accept failure
        if (istreams.size() != clusterSize - 1) {
            E("(StreamManager) Failed to accept all connections.");
            clearStreams();
            return;
        }

        connected = true;
        P("(StreamManager) Connection set up successfully.");
    }

    // Constructor: given directory of configuration
    template <typename DataType>
    StreamManager<DataType>::StreamManager(const std::string & configureFile, const std::string & dir, size_t maxDataSize, bool presort, const Partitioner & partitioner):
        connected{false}, _data{dir, maxDataSize, presort}, _partitioner{&partitioner} {
        ipconfig_t ips;
        if (!readIPs(configureFile, ips)) {
            return;
        }
        clusterSize = ips.size();
        if (clusterSize > 0) {
            init(ips);
        } else {
            E("(StreamManager) Empty configuration.");
        }
    }

    // Constructor: given vector of IP configuration
    template <typename DataType>
    StreamManager<DataType>::StreamManager(const ipconfig_t & ips, const std::string & dir, size_t maxDataSize, bool presort, const Partitioner & partitioner):
        clusterSize{ips.size()}, connected{false}, _data{dir, maxDataSize, presort}, _partitioner{&partitioner} {
        if (clusterSize > 0) {
            init(ips);
        } else {
            E("(StreamManager) Empty configuration.");
        }
    }

    // Destructor
    // Send stop signal to other machines and block until receive threads end
    // Close and clear all streams
    template <typename DataType>
    StreamManager<DataType>::~StreamManager(){

        finalizeSend();

        if (isReceiving()) {
            blockTillRecvEnd();
        }

        clearStreams();
    }


    // True if receive threads exists
    template <typename DataType>
    inline bool StreamManager<DataType>::isReceiving(void) const {
        return (receiveThreads.size() + 1 == clusterSize);
    }

    // True if connected
    template <typename DataType>
    inline bool StreamManager<DataType>::isConnected(void) const {
        return connected;
    }

    // Start receive on all sockets
    template <typename DataType>
    void StreamManager<DataType>::startRecvThreads(void) {
        if (isReceiving()) {
            D("(StreamManager) Already receiving.");
        } else if (isConnected()) {

            for (size_t i = 0; i < clusterSize - 1; i++) {
                receiveThreads.emplace_back(recvThread, std::ref(istreams[i]), std::ref(_data));
            }
        }
    }

    // Send stop signal to other machines, cause receive thread on other machines to terminate and close connection
    // called when we don't need these connections anymore
    template <typename DataType>
    void StreamManager<DataType>::finalizeSend(void) {
        for (ObjectOutputStream<DataType> * stm: ostreams) {
            if (stm != nullptr) {
                stm->close();
                delete stm;
            }
        }
        ostreams.clear();
    }

    // Send stop signal to other machines, cause receive thread on other machines to terminate
    // called when we need to temporarily stop receiving (e.g. switch from map to reduce)
    template <typename DataType>
    void StreamManager<DataType>::stopSend(void) {
        for (ObjectOutputStream<DataType> * stm: ostreams) {
            if (stm != nullptr) {
                stm->stop();
            }
        }
    }

    // Cause the current thread to block until all receive thread end and clear resource of receive threads
    template <typename DataType>
    void StreamManager<DataType>::blockTillRecvEnd(void) {
        if (isReceiving()) {
            for (std::thread & thrd: receiveThreads) {
                thrd.join();
            }
            receiveThreads.clear();
            D("(StreamManager) Receive threads ended.");
        }
    }

    // Push data to the specific machine (partitioned by partitioner)
    template <typename DataType>
    bool StreamManager<DataType>::push(DataType & v) {
        size_t id = _partitioner->getPartition(v.hashCode(), clusterSize);
        if (id == selfId) {
            return _data.store(v);
        } else {
            return ostreams[id]->send(v);
        }
    }

    // Get sorted stream from data manager
    template <typename DataType>
    SortedStream<DataType> * StreamManager<DataType>::getSortedStream () {
        return _data.getSortedStream();
    }

    // Pour data to text file with temporary files in data manager
    template <typename DataType>
    bool StreamManager<DataType>::pourToTextFile (const char * path) {
        std::ofstream os(path);
        if (os) {
            UnsortedStream<DataType> * unsorted = _data.getUnsortedStream();
            if (unsorted) {

                std::unique_ptr<UnsortedStream<DataType> > _unsorted{unsorted};

                DataType temp;
                while (unsorted->get(temp)) {
                    os << temp.toString() << std::endl;
                    if (!os) {
                        E("(StreamManager) Fail to write to text file.");
                        I("Check if there is no space.");
                        os.close();
                        return false;
                    }
                }
            }
            os.close();
            return true;
        } else {
            E("(StreamManager) Fail to open file to write.");
            I("Check if there is no space.");
            return false;
        }
    }

    // Set presort of data manager
    template <typename DataType>
    void StreamManager<DataType>::setPresort(bool presort) {
        _data.setPresort(presort);
    }

    // Set partitioner
    template <typename DataType>
    void StreamManager<DataType>::setPartitioner(const Partitioner & partitioner) {
        _partitioner = std::addressof(partitioner);
    }
}

#endif
