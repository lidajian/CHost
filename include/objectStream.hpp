/*
 * Stream transporting objects (derives of TypeBase)
 */

#ifndef OBJECTSTREAM_H
#define OBJECTSTREAM_H

#include <unistd.h> // close

#include <string> // string
#include <vector> // vector

#include "type.hpp" // id_t

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

    template <class DataType>
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
                    psend(_sockfd, static_cast<const void *>(&id_invalid), sizeof(id_t));
                }
            }
            void close(void) {
                if (isValid()) {
                    const id_t id_invalid = ID_INVALID;
                    psend(_sockfd, static_cast<const void *>(&id_invalid), sizeof(id_t));
                    ::close(_sockfd);
                    _sockfd = INVALID_SOCKET;
                }
            }
            bool send(DataType & v) {
                D("Sending: " << v << std::endl);
                id_t id = DataType::getId();
                if (psend(_sockfd, static_cast<const void *>(&id), sizeof(id_t)) == sizeof(id_t)) {
                    return v.send(_sockfd);
                }
                return false;
            }
    };

    template <class DataType>
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
            DataType * recv(void) {
                id_t id = ID_INVALID;
                if (precv(_sockfd, static_cast<void *>(&id), sizeof(id_t)) == sizeof(id_t)) {
                    if (id == DataType::getId()) {
                        DataType * v = new DataType();
                        if (!(v->recv(_sockfd))) {
                            delete v;
                            return NULL;
                        }
                        D("Received: " << (*v) << std::endl);
                        return v;
                    }
                }
                return NULL;
            }
    };
}

#endif
