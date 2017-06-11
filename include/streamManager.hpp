#ifndef STREAMMANAGER_H
#define STREAMMANAGER_H

#include <vector>
#include <unordered_map>
#include <string>
#include <fstream>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>
#include <thread>
#include <climits>
#include <algorithm>
#include "objectStream.hpp"
#include "dataManager.hpp"

namespace ch {

    template <class T>
    struct server_thread_in_t {
        int _serverfd;
        std::vector<std::pair<int, std::string> > & _ips;
        std::vector<ObjectInputStream<T> *> & _istreams;

        explicit server_thread_in_t(int serverfd, std::vector<std::pair<int, std::string> > & ips, std::vector<ObjectInputStream<T> *> & istreams): _serverfd(serverfd), _ips(ips), _istreams(istreams) {}
    };

    template <class T>
    struct connect_thread_in_t {
        std::string & _ip;
        ObjectOutputStream<T> * & _stm;

        explicit connect_thread_in_t(std::string & ip, ObjectOutputStream<T> * & stm): _ip(ip), _stm(stm) {}
    };

    template <class T>
    struct receive_thread_in_t {
        std::vector<ObjectInputStream<T> *> & _istreams;
        int _i;
        DataManager<T> & _data;

        explicit receive_thread_in_t(std::vector<ObjectInputStream<T> *> & istreams, int i, DataManager<T> & data): _istreams(istreams), _i(i), _data(data) {}
    };

    class Partitioner {
        public:
            virtual int getPartition(int i, int s) = 0;
    };

    class HashPartitioner: public Partitioner {
        public:
            int getPartition(int i, int s) {
                if (i == INT_MIN) {
                    return 0;
                }
                return ((i < 0) ? -i : i) % s;
            }
    } hashPartitioner;

    class ZeroPartitioner: public Partitioner {
        public:
            int getPartition(int i, int s) {
                return 0;
            }
    } zeroPartitioner;

    template <class T>
        void serverThread(server_thread_in_t<T> * in_args) {

            sockaddr_in remote;
            socklen_t s_size;

            int numIP = in_args->_ips.size();

            L("Server accepting client.\n");

            for (int i = 1; i < numIP; ++i) {
                int sockfd = accept(in_args->_serverfd, reinterpret_cast<struct sockaddr *>(&remote), &s_size);
                if (sockfd >= 0) {
                    L("Accepted one.\n");
                    ObjectInputStream<T> * stm = new ObjectInputStream<T>(sockfd);
                    in_args->_istreams.push_back(stm);
                } else {
                    --i;
                }
            }

            delete in_args;

        }

    template <class T>
        void connectThread(connect_thread_in_t<T> * in_args) {
            ObjectOutputStream<T> * stm = new ObjectOutputStream<T>();
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

    template <class T>
        void recvThread(receive_thread_in_t<T> * in_args) {
            std::vector<ObjectInputStream<T> *> & istreams = in_args->_istreams;
            int i = in_args->_i;
            ObjectInputStream<T> & stm = *(istreams[i]);
            DataManager<T> & data = in_args->_data;
            delete in_args;
            T * got;
            while ((got = stm.recv())) {
                data.store(got);
            }
        }

    template <class T>
    class StreamManager {
        protected:
            int self_ind;
            int clusterSize;
            std::thread ** recv_threads;
            std::vector<ObjectInputStream<T> *> istreams;
            std::vector<ObjectOutputStream<T> *> ostreams;
            DataManager<T> _data;
            void clearStreams() {
                for (ObjectOutputStream<T> * stm: ostreams) {
                    if (stm != NULL) {
                        stm->close();
                        delete stm;
                    }
                }
                ostreams.clear();
                for (ObjectInputStream<T> * stm: istreams) {
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
                server_thread_in_t<T> * server_thread_in = new server_thread_in_t<T>(serverfd, ips, istreams);
                std::thread server_thread(serverThread<T>, server_thread_in);

                // create connect thread to connect to server
                std::thread ** connect_threads = new std::thread* [clusterSize];
                for (int i = 1; i < clusterSize; ++i) {
                    connect_thread_in_t<T> * connect_thread_in = new connect_thread_in_t<T>(ips[i].second, ostreams[ips[i].first]);
                    connect_threads[i] = new std::thread(connectThread<T>, connect_thread_in);
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
            inline bool isReceiving(void) {
                return (recv_threads != NULL);
            }
            void startRecvThreads(void) {
                if (isReceiving()) {
                    D("Already receiving.\n");
                } else {
                    recv_threads = new std::thread* [clusterSize];

                    for (int i = 1; i < clusterSize; i++) {
                        receive_thread_in_t<T> * receive_thread_in = new receive_thread_in_t<T>(istreams, i - 1, _data);
                        recv_threads[i] = new std::thread(recvThread<T>, receive_thread_in);
                    }
                }
            }
            void finalizeSend(void) {
                for (ObjectOutputStream<T> * stm: ostreams) {
                    if (stm != NULL) {
                        stm->close();
                        delete stm;
                    }
                }
                ostreams.clear();
            }
            void stopSend(void) {
                for (ObjectOutputStream<T> * stm: ostreams) {
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
            bool push(T & v, Partitioner & partitioner) {
                int ind = partitioner.getPartition(v.hashCode(), clusterSize);
                if (ind == self_ind) {
                    return _data.store(v);
                } else {
                    return ostreams[ind]->send(v);
                }
            }
            SortedStream<T> * getSortedStream () {
                return _data.getSortedStream();
            }
            bool pourToTextFile (const char * path) {
                std::ofstream os(path);
                if (os) {
                    UnsortedStream<T> * unsorted = _data.getUnsortedStream();
                    T temp;
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
