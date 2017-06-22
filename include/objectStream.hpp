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

    /*
     * Interface of object streams
     */
    class ObjectStream {
        protected:
            // The socket
            int _sockfd;
        public:
            // Default constructor
            ObjectStream(): _sockfd{INVALID_SOCKET} {}

            // From value
            ObjectStream(int sockfd): _sockfd{sockfd} {}

            // Virtual Destructor
            virtual ~ObjectStream() {}

            // Close the connection
            virtual void close(void) = 0;

            // True if the socket is valid
            inline bool isValid(void) {
                return _sockfd != INVALID_SOCKET;
            }
    };

    /*
     * Implementation of object output stream
     */
    template <class DataType>
    class ObjectOutputStream: public ObjectStream {
        protected:
            inline void sendStopSignal(void) {
                const id_t id_invalid = ID_INVALID;
                psend(_sockfd, static_cast<const void *>(&id_invalid), sizeof(id_t));
            }
        public:
            // Default constructor
            ObjectOutputStream() {}

            // Destructor
            ~ObjectOutputStream() {close();}

            // Connect to an given ip at given port
            bool open(const std::string & ip, unsigned short port) {
                ::close(_sockfd);
                _sockfd = INVALID_SOCKET;
                return sconnect(_sockfd, ip.c_str(), port);
            }

            // Send signal that causes ObjectInputStream::recv return nullptr
            void stop() {
                if (isValid()) {
                    sendStopSignal();
                }
            }

            // Send signal that causes ObjectInputStream::recv return nullptr
            // close the connection as well
            void close(void) {
                if (isValid()) {
                    sendStopSignal();
                    ::close(_sockfd);
                    _sockfd = INVALID_SOCKET;
                }
            }

            // Send data through socket
            bool send(const DataType & v) {
                DSS("ObjectOutputStream: Sending " << v << std::endl);
                id_t id = DataType::getId();
                if (psend(_sockfd, static_cast<const void *>(&id), sizeof(id_t))) {
                    return v.send(_sockfd);
                } else {
                    DSS("ObjectOutputStream: Failed sending " << v << std::endl);
                }
                return false;
            }
    };

    /*
     * Implementation of object input stream
     */
    template <class DataType>
    class ObjectInputStream: public ObjectStream {
        public:
            // From value
            ObjectInputStream(int sockfd): ObjectStream{sockfd} {}

            // Destructor
            ~ObjectInputStream() {
                close();
            }

            // Close the connection
            void close(void) {
                if (isValid()) {
                    ::close(_sockfd);
                    _sockfd = INVALID_SOCKET;
                }
            }

            // Receive data, return pointer to data if success
            // return nullptr if failed
            DataType * recv(void) {
                id_t id = ID_INVALID;
                if (precv(_sockfd, static_cast<void *>(&id), sizeof(id_t))) {
                    if (id == DataType::getId()) {
                        DataType * v = new DataType();
                        if (!(v->recv(_sockfd))) {
                            delete v;
                            return nullptr;
                        }
                        DSS("ObjectInputStream: Received " << (*v) << std::endl);
                        return v;
                    }
                }
                return nullptr;
            }
    };
}

#endif
