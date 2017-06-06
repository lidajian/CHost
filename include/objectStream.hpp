#ifndef OBJECTSTREAM_H
#define OBJECTSTREAM_H

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <string>
#include <fstream>
#include <vector>
#include "type.hpp"

namespace ch {

    class ObjectStream {
        protected:
            int _sockfd;
        public:
            ObjectStream(): _sockfd(INVALID_SOCKET){}
            virtual ~ObjectStream() {}
            virtual void close(void) = 0;
    };

    template <class T>
    class ObjectInputStream;

    template <class T>
    class ObjectOutputStream: public ObjectStream {
        protected:
            ObjectInputStream<T> * _localInputStream;
        public:
            ObjectOutputStream(){}
            ObjectOutputStream(ObjectInputStream<T> * localInputStream): _localInputStream(localInputStream) {}
            ~ObjectOutputStream() {close();}
            int open(const std::string & ip, unsigned short port) {
                close();
                return sconnect(_sockfd, ip.c_str(), port);
            }
            void stop() {
                if (_sockfd != INVALID_SOCKET) {
                    const id_t id_invalid = ID_INVALID;
                    ssend(_sockfd, static_cast<const void *>(&id_invalid), sizeof(id_t));
                }
            }
            void close(void) {
                _localInputStream = NULL;
                if (_sockfd != INVALID_SOCKET) {
                    const id_t id_invalid = ID_INVALID;
                    ssend(_sockfd, static_cast<const void *>(&id_invalid), sizeof(id_t));
                    ::close(_sockfd);
                    _sockfd = INVALID_SOCKET;
                }
            }
            bool push(T & v) {
                if (_localInputStream != NULL) {
                    T * nv = new T(v);
                    _localInputStream->_received.push_back(nv);
                    D("Store local: " << (v.toString()) << std::endl);
                    return true;
                }
                if (_sockfd == INVALID_SOCKET) {
                    return false;
                }
                D("Sending: " << v.toString() << std::endl);
                id_t id = v.getId();
                if (ssend(_sockfd, static_cast<const void *>(&id), sizeof(id_t)) < 0) {
                    return false;
                }
                if (v.send(_sockfd) < 0) {
                    return false;
                }
                return true;
            }
    };

    template <class T>
    class ObjectInputStream: public ObjectStream {
        friend class ObjectOutputStream<T>;

        private:
            std::vector<T *> _received;
        public:
            ~ObjectInputStream() {close();}
            void setSocket(int sockfd) {
                _sockfd = sockfd;
            }
            void clear() {
                for (T * v: _received) {
                    delete v;
                }
                _received.clear();
            }
            void close(void) {
                if (_sockfd != INVALID_SOCKET) {
                    ::close(_sockfd);
                    _sockfd = INVALID_SOCKET;
                }
                clear();
            }
            void startRecv(void) {
                id_t id = ID_INVALID;
                while (true) {
                    if (srecv(_sockfd, static_cast<void *>(&id), sizeof(id_t)) < 0) {
                        break;
                    }
                    if (id == ID_INVALID) {
                        break;
                    }

                    T * v = new T();
                    if (v->recv(_sockfd) == INVALID) {
                        break;
                    }
                    _received.push_back(v);
                    D("Received: " << (v->toString()) << std::endl);
                }
            }
            void pourToTextFile(std::ofstream & os) {
                for (T * v: _received) {
                    os << v->toString() << std::endl;
                    delete v;
                }
                _received.clear();
            }
            void getReceived (std::vector<T *> & res) {
                res.insert(std::end(res), std::begin(_received), std::end(_received));
                clear();
            }
    };
}

#endif
