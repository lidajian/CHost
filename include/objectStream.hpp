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
            ObjectStream(): _sockfd(INVALID_SOCKET) {}
            ObjectStream(int sockfd): _sockfd(sockfd) {}
            virtual ~ObjectStream() {}
            inline bool isValid(void) {
                return _sockfd != INVALID_SOCKET;
            }
            virtual void close(void) = 0;
    };

    template <class T>
    class ObjectOutputStream: public ObjectStream {
        public:
            ObjectOutputStream() {}
            ~ObjectOutputStream() {close();}
            bool open(const std::string & ip, unsigned short port) {
                close();
                return sconnect(_sockfd, ip.c_str(), port) >= 0;
            }
            void stop() {
                if (isValid()) {
                    const id_t id_invalid = ID_INVALID;
                    ssend(_sockfd, static_cast<const void *>(&id_invalid), sizeof(id_t));
                }
            }
            void close(void) {
                if (isValid()) {
                    const id_t id_invalid = ID_INVALID;
                    ssend(_sockfd, static_cast<const void *>(&id_invalid), sizeof(id_t));
                    ::close(_sockfd);
                    _sockfd = INVALID_SOCKET;
                }
            }
            bool send(T & v) {
                D("Sending: " << v.toString() << std::endl);
                id_t id = T::getId();
                if (ssend(_sockfd, static_cast<const void *>(&id), sizeof(id_t)) == sizeof(id_t)) {
                    return v.send(_sockfd);
                }
                return false;
            }
    };

    template <class T>
    class ObjectInputStream: public ObjectStream {
        public:
            ObjectInputStream(int sockfd): ObjectStream(sockfd) {}
            ~ObjectInputStream() {close();}
            void setSocket(int sockfd) {
                _sockfd = sockfd;
            }
            void close(void) {
                if (isValid()) {
                    ::close(_sockfd);
                    _sockfd = INVALID_SOCKET;
                }
            }
            T * recv(void) {
                id_t id = ID_INVALID;
                if (srecv(_sockfd, static_cast<void *>(&id), sizeof(id_t)) == sizeof(id_t)) {
                    if (id == T::getId()) {
                        T * v = new T();
                        if (!(v->recv(_sockfd))) {
                            delete v;
                            return NULL;
                        }
                        D("Received: " << (v->toString()) << std::endl);
                        return v;
                    }
                }
                return NULL;
            }
    };
}

#endif
