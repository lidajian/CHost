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

    /********************************************
     ************** Declaration *****************
    ********************************************/

    /*
     * Interface of object streams
     */
    class ObjectStream {
        protected:
            // The socket
            int _sockfd;
        public:
            // Default constructor
            ObjectStream();

            // From value
            ObjectStream(int sockfd);

            // Copy constructor
            ObjectStream(const ObjectStream & o) = delete;

            // Move constructor
            ObjectStream(ObjectStream && o);

            // Virtual Destructor
            virtual ~ObjectStream();

            // Close the connection
            virtual void close(void) = 0;

            // True if the socket is valid
            bool isValid(void);
    };

    /*
     * Implementation of object output stream
     */
    template <class DataType>
    class ObjectOutputStream: public ObjectStream {
        protected:
            void sendStopSignal(void);
        public:
            // Default constructor
            ObjectOutputStream();

            // Copy constructor
            ObjectOutputStream(const ObjectOutputStream<DataType> & o) = delete;

            // Move constructor
            ObjectOutputStream(ObjectOutputStream<DataType> && o);

            // Destructor
            ~ObjectOutputStream();

            // Connect to an given ip at given port
            bool open(const std::string & ip, unsigned short port);

            // Send signal that causes ObjectInputStream::recv return nullptr
            void stop();

            // Send signal that causes ObjectInputStream::recv return nullptr
            // close the connection as well
            void close(void);

            // Send data through socket
            bool send(const DataType & v);
    };

    /*
     * Implementation of object input stream
     */
    template <class DataType>
    class ObjectInputStream: public ObjectStream {
        public:
            // From value
            ObjectInputStream(int sockfd);

            // Copy constructor
            ObjectInputStream(const ObjectInputStream<DataType> & o) = delete;

            // Move constructor
            ObjectInputStream(ObjectInputStream<DataType> && o);

            // Destructor
            ~ObjectInputStream();

            // Close the connection
            void close(void);

            // Receive data, return pointer to data if success
            // return nullptr if failed
            DataType * recv(void);
    };

    /********************************************
     ************ Implementation ****************
    ********************************************/

    // Default constructor
    ObjectStream::ObjectStream(): _sockfd{INVALID_SOCKET} {}

    // From value
    ObjectStream::ObjectStream(int sockfd): _sockfd{sockfd} {}

    // Move constructor
    ObjectStream::ObjectStream(ObjectStream && o): _sockfd{o._sockfd} {
        o._sockfd = INVALID_SOCKET;
    }

    // Virtual Destructor
    ObjectStream::~ObjectStream() {}

    // True if the socket is valid
    inline bool ObjectStream::isValid(void) {
        return _sockfd != INVALID_SOCKET;
    }

    template <class DataType>
    inline void ObjectOutputStream<DataType>::sendStopSignal(void) {
        const id_t id_invalid = ID_INVALID;
        psend(_sockfd, static_cast<const void *>(&id_invalid), sizeof(id_t));
    }

    // Default constructor
    template <class DataType>
    ObjectOutputStream<DataType>::ObjectOutputStream() {}

    // Move constructor
    template <class DataType>
    ObjectOutputStream<DataType>::ObjectOutputStream(ObjectOutputStream<DataType> && o): ObjectStream{std::move(o)} {}

    // Destructor
    template <class DataType>
    ObjectOutputStream<DataType>::~ObjectOutputStream() {close();}

    // Connect to an given ip at given port
    template <class DataType>
    bool ObjectOutputStream<DataType>::open(const std::string & ip, unsigned short port) {
        ::close(_sockfd);
        _sockfd = INVALID_SOCKET;
        return sconnect(_sockfd, ip.c_str(), port);
    }

    // Send signal that causes ObjectInputStream::recv return nullptr
    template <class DataType>
    void ObjectOutputStream<DataType>::stop() {
        if (isValid()) {
            sendStopSignal();
        }
    }

    // Send signal that causes ObjectInputStream::recv return nullptr
    // close the connection as well
    template <class DataType>
    void ObjectOutputStream<DataType>::close(void) {
        if (isValid()) {
            sendStopSignal();
            ::close(_sockfd);
            _sockfd = INVALID_SOCKET;
        }
    }

    // Send data through socket
    template <class DataType>
    bool ObjectOutputStream<DataType>::send(const DataType & v) {
        DSS("ObjectOutputStream: Sending " << v);
        id_t id = DataType::getId();
        if (psend(_sockfd, static_cast<const void *>(&id), sizeof(id_t))) {
            return v.send(_sockfd);
        } else {
            DSS("ObjectOutputStream: Failed sending " << v);
        }
        return false;
    }

    // From value
    template <class DataType>
    ObjectInputStream<DataType>::ObjectInputStream(int sockfd): ObjectStream{sockfd} {}

    // Move constructor
    template <class DataType>
    ObjectInputStream<DataType>::ObjectInputStream(ObjectInputStream<DataType> && o): ObjectStream{std::move(o)} {}

    // Destructor
    template <class DataType>
    ObjectInputStream<DataType>::~ObjectInputStream() {
        close();
    }

    // Close the connection
    template <class DataType>
    void ObjectInputStream<DataType>::close(void) {
        if (isValid()) {
            ::close(_sockfd);
            _sockfd = INVALID_SOCKET;
        }
    }

    // Receive data, return pointer to data if success
    // return nullptr if failed
    template <class DataType>
    DataType * ObjectInputStream<DataType>::recv(void) {
        id_t id = ID_INVALID;
        if (precv(_sockfd, static_cast<void *>(&id), sizeof(id_t))) {
            if (id == DataType::getId()) {
                DataType * v = new DataType();
                if (!(v->recv(_sockfd))) {
                    delete v;
                    return nullptr;
                }
                DSS("ObjectInputStream: Received " << (*v));
                return v;
            }
        }
        return nullptr;
    }
}

#endif
