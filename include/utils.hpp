/*
 * Utility functions
 */

#ifndef UTILS_H
#define UTILS_H

#include <string.h> // memset
#include <unistd.h> // access
#include <netinet/in.h> // sockaddr_in
#include <arpa/inet.h> // htons, inet_addr
#include <sys/socket.h> // connect, bind, send, recv, socket, listen
#include <sys/stat.h> // mkdir
#include <stdio.h> // fseek, ftell, rewind, fread, ...

#include <fstream> // ifstream
#include <string> // string
#include <vector> // vector
#include <random> // random_device, default_random_engine, uniform_int_distribution, ...
#include <functional> // std::bind

#include "def.hpp"

namespace ch {

    /*
     * Promised I/O wrapper
     */

    // Send with given length
    bool psend(int fd, const void * buffer, size_t len);

    // Receive with given length
    bool precv(int fd, void * buffer, size_t len);

    // fwrite with given length
    bool pfwrite(FILE * fd, const void * buffer, size_t len);

    // fread with given length
    bool pfread(FILE * fd, void * buffer, size_t len);

#if defined (__CH_KQUEUE__)
    // kevent wrapper
    int Kevent(int kq, const struct kevent *changelist, int nchanges, struct kevent *eventlist, int nevents, const struct timespec *timeout);

#elif defined (_CH_EPOLL__)
    // epoll_wait wrapper
    int Epoll_wait(int epfd, struct epoll_event *events, int maxevents, int timeout);

#else
    // select wrapper
    int Select(int nfds, fd_set * readfds, fd_set * writefds, fd_set * errorfds, struct timeval * timeout);

#endif

    /*
     * Network functions
     */

    // Connect to an address: port
    bool sconnect(int & sockfd, const char * ip, const unsigned short port);

    // Prepare to accept connection
    bool prepareServer(int & serverfd, const unsigned short port);

    // Get integer and forward iterator
    // helper function for isValidIP_v4
    int getInt(std::string::iterator & it, const std::string::iterator & end);

    // True if address is valid IPv4 address
    bool isValidIP_v4(std::string & ip);

    // Send file through a socket
    bool sendFile(const int sockfd, const char * file_path);

    // Receive file from socket
    bool receiveFile(const int sockfd, const char * file_path);

    // Send string through socket
    bool sendString(const int sockfd, const std::string & str);

    // Receive string from socket
    bool receiveString(const int sockfd, std::string & str);

    /*
     * File system functions
     */

    // Get file size given file discriptor
    long getFileSize(FILE * fd);

    // True if the file (given by path exists)
    bool fileExist(const char * path);

    // Read ipconfig from configureFile
    bool readIPs(const std::string & configureFile, ipconfig_t & ips);

    // Read file as string (cache the file to avoid multiple read)
    bool readFileAsString(const char * file_path, std::string & str);

    // Get working directory, make directory if does not exist
    bool getWorkingDirectory(std::string & workingDir, const std::string & addictionalDir);

    /*
     * Mimic command
     */

    // cp
    bool Copy(const char * src, const char * dest);

    // mkdir -p
    bool Mkdir(std::string & path);

    /*
     * Invoke functions
     */

    // Invoke master
    bool invokeMaster(const int fd);

    // Invoke worker
    bool invokeWorker(int fd);

    // Return success to chrun program
    void sendSuccess(const int sockfd);

    // Return fail to chrun program
    void sendFail(const int sockfd);

    /*
     * Others
     */

    // Generate random string of length l
    std::string randomString(size_t l);
}

#endif
