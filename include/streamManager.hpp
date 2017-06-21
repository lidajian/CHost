/*
 * Peer to peer connection between jobs - transport objects
 */

#ifndef STREAMMANAGER_H
#define STREAMMANAGER_H

#include <netinet/in.h> // sockaddr_in
#include <unistd.h> // close
#include <sys/socket.h> // accept
#include <sys/select.h> // select

#include <vector> // vector
#include <string> // string
#include <fstream> // ofstream
#include <thread> // thread
#include <chrono> // seconds

#include "objectStream.hpp" // ObjectInputStream, ObjectOutputStream
#include "dataManager.hpp" // DataManager
#include "partitioner.hpp" // Partitioner

namespace ch {

    template <class DataType>
    class StreamManager {
        protected:
            // Id of the machine
            size_t selfId;

            // Number of machines in the cluster
            size_t clusterSize;

            // Pointer to receive threads
            std::thread ** recv_threads;

            // Input streams
            std::vector<ObjectInputStream<DataType> *> istreams;

            // Output streams
            std::vector<ObjectOutputStream<DataType> *> ostreams;

            // Data manager
            DataManager<DataType> _data;

            // Server thread: accept connections
            static void serverThread(int serverfd, const ipconfig_t & ips, std::vector<ObjectInputStream<DataType> *> & istreams) {

                sockaddr_in remote;
                socklen_t s_size;

                fd_set accept_fdset;
                struct timeval timeout;

                const size_t numIP = ips.size();

                L("StreamManager: Server accepting client.\n");

                for (size_t i = 1; i < numIP; ++i) {

                    FD_ZERO(&accept_fdset);
                    FD_SET(serverfd, &accept_fdset);

                    timeout.tv_sec = ACCEPT_TIMEOUT;
                    timeout.tv_usec = 0;

                    int rv;
                    do {
                        rv = select(serverfd + 1, &accept_fdset, nullptr, nullptr, &timeout);
                    } while (rv == -1 && errno == EINTR);

                    if (rv <= 0) {
                        L("StreamManager: Server failed to accept all clients within timeout.\n");
                        close(serverfd);
                        return;
                    }

                    if (FD_ISSET(serverfd, &accept_fdset)) {
                        int sockfd = accept(serverfd, reinterpret_cast<struct sockaddr *>(&remote), &s_size);
                        if (sockfd > 0 && (getpeername(sockfd, reinterpret_cast<struct sockaddr *>(&remote), &s_size) >= 0)) {
                            L("StreamManager: Server accepted connection from " << inet_ntoa(remote.sin_addr) << ".\n");
                            ObjectInputStream<DataType> * stm = new ObjectInputStream<DataType>(sockfd);
                            istreams.push_back(stm);
                        } else {
                            --i;
                        }
                    } else {
                        --i;
                    }
                }

                close(serverfd);
                L("StreamManager: Server accepted all clients.\n");
            }

            // Connect thread: connect to servers
            // sets stmr to the created object output stream if success
            static void connectThread(const std::string & ip, ObjectOutputStream<DataType> * & stmr) {
                ObjectOutputStream<DataType> * stm = new ObjectOutputStream<DataType>();
                int tries = 0;
                while (tries < MAX_CONNECTION_ATTEMPT && !(stm->open(ip, STREAMMANAGER_PORT))) {
                    ++tries;
                    // wait at least CONNECTION_RETRY_INTERVAL second before another try
                    std::this_thread::yield();
                    std::this_thread::sleep_for(std::chrono::seconds(CONNECTION_RETRY_INTERVAL));
                }
                if (tries == MAX_CONNECTION_ATTEMPT) {
                    L("StreamManager: Client fail to connect to " << ip << ".\n");
                    delete stm;
                } else {
                    L("StreamManager: Client connected to " << ip << ".\n");
                    stmr = stm;
                }
            }

            // Receive thread: receive objects and store
            static void recvThread(ObjectInputStream<DataType> * & stm, DataManager<DataType> & data) {
                DataType * got = nullptr;
                while ((got = stm->recv())) {
                    if (!data.store(got)) {
                        break;
                    }
                }
            }

            // Close and clear all streams
            void clearStreams() {
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
            void init(const ipconfig_t & ips){

                selfId = ips[0].first;

                int serverfd;
                if (!prepareServer(serverfd, STREAMMANAGER_PORT)) {
                    L("StreamManager: fail to open socket to accept clients.\n");
                    return;
                }

                istreams.clear();
                istreams.reserve(clusterSize - 1);
                ostreams.clear();
                ostreams.resize(clusterSize, nullptr);

                // create server thread to accept connection
                std::thread server_thread(serverThread, serverfd, std::ref(ips), std::ref(istreams));

                // create connect thread to connect to server
                std::thread ** connect_threads = new std::thread* [clusterSize];
                for (size_t i = 1; i < clusterSize; ++i) {
                    connect_threads[i] = new std::thread(connectThread, std::ref(ips[i].second), std::ref(ostreams[ips[i].first]));
                }

                // Join server thread and connect threads
                server_thread.join();
                for (size_t i = 1; i < clusterSize; ++i) {
                    connect_threads[i]->join();
                    delete connect_threads[i];
                }
                delete [] connect_threads;

                // Check for connection failure
                for (size_t i = 0; i < clusterSize; ++i) {
                    if (i != selfId && ostreams[i] == nullptr) {
                        L("StreamManager: One or more of connections are fail.\n");
                        clearStreams();
                        return;
                    }
                }

                // Check for accept failure
                if (istreams.size() != clusterSize - 1) {
                    L("StreamManager: Failed to accept all connections.\n");
                    clearStreams();
                    return;
                }

                L("StreamManager: Connection set up successfully.\n");

                startRecvThreads();
            }
        public:
            // Constructor: given directory of configuration
            StreamManager(const std::string & configureFile, const std::string & dir, size_t maxDataSize = DEFAULT_MAX_DATA_SIZE): recv_threads{nullptr}, _data{dir, maxDataSize} {
                ipconfig_t ips;
                if (!readIPs(configureFile, ips)) {
                    return;
                }
                clusterSize = ips.size();
                if (clusterSize > 0) {
                    init(ips);
                } else {
                    L("StreamManager: Empty configuration.\n");
                }
            }

            // Constructor: given vector of IP configuration
            StreamManager(const ipconfig_t & ips, const std::string & dir, size_t maxDataSize = DEFAULT_MAX_DATA_SIZE): clusterSize{ips.size()}, recv_threads{nullptr}, _data{dir, maxDataSize} {
                if (clusterSize > 0) {
                    init(ips);
                } else {
                    L("StreamManager: Empty configuration.\n");
                }
            }

            // Destructor
            // Send stop signal to other machines and block until receive threads end
            // Close and clear all streams
            ~StreamManager(){

                finalizeSend();

                if (isReceiving()) {
                    blockTillRecvEnd();
                }

                clearStreams();
            }


            // True if receive threads exists
            inline bool isReceiving(void) const {
                return (recv_threads != nullptr);
            }

            // Start receive on all sockets
            void startRecvThreads(void) {
                if (isReceiving()) {
                    D("StreamManager: Already receiving.\n");
                } else {
                    recv_threads = new std::thread* [clusterSize - 1];

                    for (size_t i = 0; i < clusterSize - 1; i++) {
                        recv_threads[i] = new std::thread(recvThread, std::ref(istreams[i]), std::ref(_data));
                    }
                }
            }

            // Send stop signal to other machines, cause receive thread on other machines to terminate and close connection
            // called when we don't need these connections anymore
            void finalizeSend(void) {
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
            void stopSend(void) {
                for (ObjectOutputStream<DataType> * stm: ostreams) {
                    if (stm != nullptr) {
                        stm->stop();
                    }
                }
            }

            // Cause the current thread to block until all receive thread end and clear resource of receive threads
            void blockTillRecvEnd(void) {
                if (isReceiving() && clusterSize != 0) {
                    for (size_t i = 0; i < clusterSize - 1; ++i) {
                        recv_threads[i]->join();
                        delete recv_threads[i];
                    }
                    delete [] recv_threads;
                    recv_threads = nullptr;
                    L("StreamManager: Receive threads ended.\n");
                }
            }

            // Push data to the specific machine (partitioned by partitioner)
            bool push(DataType & v, const Partitioner & partitioner) {
                size_t id = partitioner.getPartition(v.hashCode(), clusterSize);
                if (id == selfId) {
                    return _data.store(v);
                } else {
                    return ostreams[id]->send(v);
                }
            }

            // Get sorted stream from data manager
            SortedStream<DataType> * getSortedStream () {
                return _data.getSortedStream();
            }

            // Pour data to text file with temporary files in data manager
            bool pourToTextFile (const char * path) {
                std::ofstream os(path);
                if (os) {
                    UnsortedStream<DataType> * unsorted = _data.getUnsortedStream();
                    if (unsorted) {
                        DataType temp;
                        while (unsorted->get(temp)) {
                            os << temp.toString() << std::endl;
                        }
                        delete unsorted;
                    }
                    os.close();
                    return true;
                } else {
                    return false;
                }
            }

            // Set presort of data manager
            void setPresort(bool presort) {
                _data.setPresort(presort);
            }
    };
}

#endif
