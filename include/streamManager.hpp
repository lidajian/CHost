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

#include "objectStream.hpp" // ObjectInputStream, ObjectOutputStream
#include "dataManager.hpp" // DataManager
#include "partitioner.hpp" // Partitioner

namespace ch {

    template <class DataType>
    class StreamManager {
        protected:
            int self_ind;
            int clusterSize;
            std::thread ** recv_threads;
            std::vector<ObjectInputStream<DataType> *> istreams;
            std::vector<ObjectOutputStream<DataType> *> ostreams;
            DataManager<DataType> _data;

            struct server_thread_in_t {
                int _serverfd;
                std::vector<std::pair<int, std::string> > & _ips;
                std::vector<ObjectInputStream<DataType> *> & _istreams;

                explicit server_thread_in_t(int serverfd, std::vector<std::pair<int, std::string> > & ips, std::vector<ObjectInputStream<DataType> *> & istreams): _serverfd(serverfd), _ips(ips), _istreams(istreams) {}
            };

            struct connect_thread_in_t {
                std::string & _ip;
                ObjectOutputStream<DataType> * & _stm;

                explicit connect_thread_in_t(std::string & ip, ObjectOutputStream<DataType> * & stm): _ip(ip), _stm(stm) {}
            };

            struct receive_thread_in_t {
                std::vector<ObjectInputStream<DataType> *> & _istreams;
                int _i;
                DataManager<DataType> & _data;

                explicit receive_thread_in_t(std::vector<ObjectInputStream<DataType> *> & istreams, int i, DataManager<DataType> & data): _istreams(istreams), _i(i), _data(data) {}
            };

            static void serverThread(server_thread_in_t * in_args) {

                sockaddr_in remote;
                socklen_t s_size;

                int numIP = in_args->_ips.size();

                L("Server accepting client.\n");

                for (int i = 1; i < numIP; ++i) {
                    int sockfd = accept(in_args->_serverfd, reinterpret_cast<struct sockaddr *>(&remote), &s_size);
                    if (sockfd >= 0) {
                        L("Accepted one.\n");
                        ObjectInputStream<DataType> * stm = new ObjectInputStream<DataType>(sockfd);
                        in_args->_istreams.push_back(stm);
                    } else {
                        --i;
                    }
                }

                delete in_args;

            }

            static void connectThread(connect_thread_in_t * in_args) {
                ObjectOutputStream<DataType> * stm = new ObjectOutputStream<DataType>();
                int tries = 0;
                L("Client connecting to server.\n");
                while (tries < MAX_CONNECTION_ATTEMPT && !(stm->open(in_args->_ip, STREAMMANAGER_PORT))) {
                    ++tries;
                    std::this_thread::yield();
                }
                if (tries == MAX_CONNECTION_ATTEMPT) {
                    L("Client fail to connect to server.\n");
                    delete stm;
                } else {
                    L("Client connected to server.\n");
                    in_args->_stm = stm;
                }
                delete in_args;
            }

            static void recvThread(receive_thread_in_t * in_args) {
                std::vector<ObjectInputStream<DataType> *> & istreams = in_args->_istreams;
                int i = in_args->_i;
                ObjectInputStream<DataType> & stm = *(istreams[i]);
                DataManager<DataType> & data = in_args->_data;
                delete in_args;
                DataType * got;
                while ((got = stm.recv())) {
                    data.store(got);
                }
            }

            void clearStreams() {
                for (ObjectOutputStream<DataType> * stm: ostreams) {
                    if (stm != NULL) {
                        stm->close();
                        delete stm;
                    }
                }
                ostreams.clear();
                for (ObjectInputStream<DataType> * stm: istreams) {
                    if (stm != NULL) {
                        stm->close();
                        delete stm;
                    }
                }
                istreams.clear();
            }
            void init(std::vector<std::pair<int, std::string> > & ips){

                recv_threads = NULL;
                clusterSize = ips.size();

                if (clusterSize == 0) {
                    return;
                }

                int serverfd;
                if (!prepareServer(serverfd, STREAMMANAGER_PORT)) {
                    return;
                }

                istreams.clear();
                istreams.reserve(clusterSize - 1);
                ostreams.clear();
                ostreams.resize(clusterSize, NULL);

                self_ind = ips[0].first;

                // create server thread to accept connection
                server_thread_in_t * server_thread_in = new server_thread_in_t(serverfd, ips, istreams);
                std::thread server_thread(serverThread, server_thread_in);

                // create connect thread to connect to server
                std::thread ** connect_threads = new std::thread* [clusterSize];
                for (int i = 1; i < clusterSize; ++i) {
                    connect_thread_in_t * connect_thread_in = new connect_thread_in_t(ips[i].second, ostreams[ips[i].first]);
                    connect_threads[i] = new std::thread(connectThread, connect_thread_in);
                }

                // wait for all connection set up
                bool connectIsFail = false;
                server_thread.join();
                close(serverfd);
                for (int i = 1; i < clusterSize; ++i) {
                    connect_threads[i]->join();
                    delete connect_threads[i];
                }
                delete [] connect_threads;
                for (int i = 0; i < clusterSize; ++i) {
                    if (i != self_ind && ostreams[i] == NULL) {
                        connectIsFail = true;
                    }
                }
                if (connectIsFail) {
                    L("One of connection is fail.\n");
                    clearStreams();
                    return;
                }

                L("Connection set up.\n");

                startRecvThreads();
            }
        public:
            StreamManager(const char * configureFile, size_t maxDataSize, std::string & dir): _data(maxDataSize, dir){
                std::vector<std::pair<int, std::string> > ips;
                if (!readIPs(configureFile, ips)) {
                    return;
                }
                init(ips);
            }
            StreamManager(std::vector<std::pair<int, std::string> > & ips, size_t maxDataSize, std::string & dir): _data(maxDataSize, dir) {
                init(ips);
            }
            ~StreamManager(){

                finalizeSend();

                if (isReceiving()) {
                    blockTillRecvEnd();
                }

                clearStreams();
            }
            inline bool isReceiving(void) const {
                return (recv_threads != NULL);
            }
            void startRecvThreads(void) {
                if (isReceiving()) {
                    D("Already receiving.\n");
                } else {
                    recv_threads = new std::thread* [clusterSize];

                    for (int i = 1; i < clusterSize; i++) {
                        receive_thread_in_t * receive_thread_in = new receive_thread_in_t(istreams, i - 1, _data);
                        recv_threads[i] = new std::thread(recvThread, receive_thread_in);
                    }
                }
            }
            void finalizeSend(void) {
                for (ObjectOutputStream<DataType> * stm: ostreams) {
                    if (stm != NULL) {
                        stm->close();
                        delete stm;
                    }
                }
                ostreams.clear();
            }
            void stopSend(void) {
                for (ObjectOutputStream<DataType> * stm: ostreams) {
                    if (stm != NULL) {
                        stm->stop();
                    }
                }
            }
            void blockTillRecvEnd(void) {
                if (isReceiving()) {
                    for (int i = 1; i < clusterSize; ++i) {
                        recv_threads[i]->join();
                        delete recv_threads[i];
                    }
                    delete [] recv_threads;
                    recv_threads = NULL;
                    L("Receive ended.\n");
                }
            }
            bool push(DataType & v, Partitioner & partitioner) {
                int ind = partitioner.getPartition(v.hashCode(), clusterSize);
                if (ind == self_ind) {
                    return _data.store(v);
                } else {
                    return ostreams[ind]->send(v);
                }
            }
            SortedStream<DataType> * getSortedStream () {
                return _data.getSortedStream();
            }
            bool pourToTextFile (const char * path) {
                std::ofstream os(path);
                if (os) {
                    UnsortedStream<DataType> * unsorted = _data.getUnsortedStream();
                    DataType temp;
                    while (unsorted->get(temp)) {
                        os << temp.toString() << std::endl;
                    }
                    os.close();
                    delete unsorted;
                    return true;
                } else {
                    return false;
                }
            }
            void setPresort(bool presort) {
                _data.setPresort(presort);
            }
    };
}

#endif
