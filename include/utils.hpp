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

#include "def.hpp"

namespace ch {

    // Send with given length
    bool psend(int fd, const void * buffer, size_t len) {
        off_t offset = 0;
        const char * cbuf = reinterpret_cast<const char *>(buffer);
        ssize_t sent;
        while (len != 0 && ((sent = send(fd, cbuf + offset, len, 0)) > 0 || (sent == -1 && errno == EINTR))) {
            if (sent > 0) {
                offset += sent;
                len -= sent;
            }
        }
        return (len == 0);
    }

    // Receive with given length
    bool precv(int fd, void * buffer, size_t len) {
        off_t offset = 0;
        char * cbuf = reinterpret_cast<char *>(buffer);
        ssize_t received;
        while (len != 0 && ((received = recv(fd, cbuf + offset, len, 0)) > 0 || (received == -1 && errno == EINTR))) {
            if (received > 0) {
                offset += received;
                len -= received;
            }
        }
        return (len == 0);
    }

    // fwrite with given length
    bool pfwrite(FILE * fd, const void * buffer, size_t len) {
        off_t offset = 0;
        const char * cbuf = reinterpret_cast<const char *>(buffer);
        size_t written;
        while (len != 0 && ((written = fwrite(cbuf + offset, sizeof(char), len, fd)) == len || (errno == EINTR))) {
            offset += written;
            len -= written;
        }

        return (len == 0);
    }

    // fread with given length
    bool pfread(FILE * fd, void * buffer, size_t len) {
        off_t offset = 0;
        char * cbuf = reinterpret_cast<char *>(buffer);
        size_t read_;
        while (len != 0 && ((read_ = fread(cbuf + offset, sizeof(char), len, fd)) == len || (errno == EINTR))) {
            offset += read_;
            len -= read_;
        }

        return (len == 0);
    }

    // Connect to an address: port
    bool sconnect(int & sockfd, const char * ip, const unsigned short port) {

        sockaddr_in addr;

        sockfd = socket(AF_INET, SOCK_STREAM, 0);

        if (sockfd < 0) {
            sockfd = INVALID_SOCKET;
            return INVALID;
        }

        memset(&addr, 0, sizeof(sockaddr_in));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = inet_addr(ip);
        addr.sin_port = htons(port);

        return connect(sockfd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(sockaddr)) >= 0;
    }

    // Get file size given file discriptor
    long getFileSize(FILE * fd) {
        fseek(fd, 0, SEEK_END);
        long ret = ftell(fd);
        rewind(fd);
        return ret;
    }

    // True if the file (given by path exists)
    bool fileExist(const char * path) {
        return access(path, F_OK) != INVALID;
    }

    // Prepare to accept connection
    bool prepareServer(int & serverfd, const unsigned short port){
        sockaddr_in addr;

        serverfd = socket(AF_INET, SOCK_STREAM, 0);

        if (serverfd < 0) {
            D("(prepareServer) Socket failed.\n");
            return false;
        }

        memset(&addr, 0, sizeof(sockaddr_in));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);

        int rv = bind(serverfd, reinterpret_cast<const sockaddr *>(&addr), sizeof(sockaddr));

        if (rv < 0) {
            D("(prepareServer) Bind failed.\n");
            return false;
        }

        rv = listen(serverfd, 5);

        if (rv < 0) {
            D("(prepareServer) Listen failed.\n");
            return false;
        }

        return true;
    }

    // Read ipconfig from configureFile
    bool readIPs(const std::string & configureFile, ipconfig_t & ips) {
        ips.clear();
        std::ifstream in;
        in.open(configureFile);
        if (!in) {
            D("(readIPs) Cannot open configuration file.\n");
            return false;
        }
        std::string ip;
        size_t id;
        while (in >> id >> ip) {
            ips.emplace_back(id, ip);
        }
        in.close();
        return true;
    }

    // Get integer and forward iterator
    // helper function for isValidIP_v4
    int getInt(std::string::iterator & it, const std::string::iterator & end) {
        int n = 0;
        while (it < end && (*it) <= '9' && (*it) >= '0') {
            n = n * 10 + (int)((*it) - '0');
            ++it;
        }
        return n;
    }

    // True if address is valid IPv4 address
    bool isValidIP_v4(std::string & ip) {
        std::string::iterator it = ip.begin();
        const std::string::iterator end = ip.end();
        // skip space
        while (it < end && (*it) == ' ') {
            ++it;
        }
        if (getInt(it, end) >= 256) { // first number
            return false;
        }
        if (it >= end || (*it) != '.') {
            return false;
        }
        ++it;
        if (getInt(it, end) >= 256) { // second number
            return false;
        }
        if (it >= end || (*it) != '.') {
            return false;
        }
        ++it;
        if (getInt(it, end) >= 256) { // third number
            return false;
        }
        if (it >= end || (*it) != '.') {
            return false;
        }
        ++it;
        if (getInt(it, end) >= 256) { // fourth number
            return false;
        }
        // skip space
        while (it < end && (*it) == ' ') {
            ++it;
        }
        return it == end;
    }

    // Rearrange ipconfig to create configure files for other machines
    void rearrangeIPs(const ipconfig_t & ips, std::string & file, const size_t indexToHead) {
        file = std::to_string(ips[indexToHead].first) + " " + ips[indexToHead].second + "\n";
        for (size_t i = 0, l = ips.size(); i < l; ++i) {
            if (i != indexToHead) {
                file += std::to_string(ips[i].first) + " " + ips[i].second + "\n";
            }
        }
    }

    // Send file through a socket
    bool sendFile(const int sockfd, const char * file_path) {
        char buffer[BUFFER_SIZE];
        FILE * fd = fopen(file_path, "r");
        if (fd == NULL) {
            D("(sendFile) Cannot open file to write.\n");
            return false;
        }
        const ssize_t fileSize = getFileSize(fd);
        if (fileSize < 0) {
            D("(sendFile) Cannot get file size.\n");
            fclose(fd);
            return false;
        }

        if (psend(sockfd, static_cast<const void *>(&fileSize), sizeof(ssize_t)) && fileSize > 0 ) {
            ssize_t sentSize = 0;
            ssize_t byteLeft, toSend;
            do {
                byteLeft = fileSize - sentSize;
                toSend = MIN_VAL(byteLeft, BUFFER_SIZE);
                if (!pfread(fd, buffer, toSend)) {
                    D("(sendFile) Unexepected EOF.\n");
                    fclose(fd);
                    return false;
                } else if (psend(sockfd, static_cast<const void *>(buffer), toSend)){
                    sentSize += toSend;
                } else {
                    D("(sendFile) Broken pipe.\n");
                    fclose(fd);
                    return false;
                }
            } while (sentSize < fileSize);
        } else {
            D("(sendFile) Cannot send file size.\n");
            fclose(fd);
            return false;
        }

        fclose(fd);
        return true;
    }

    // copy
    bool Copy(const char * src, const char * dest) {
        char buffer[BUFFER_SIZE];
        FILE * srcfd = fopen(src, "r");
        if (srcfd == NULL) {
            D("(Copy) Cannot open file to read.\n");
            return false;
        }
        const ssize_t fileSize = getFileSize(srcfd);
        if (fileSize < 0) {
            fclose(srcfd);
            return false;
        }
        FILE * destfd = fopen(dest, "w");
        if (destfd == NULL) {
            D("(Copy) Cannot open file to write.\n");
            fclose(srcfd);
            return false;
        }

        ssize_t copiedSize = 0;
        ssize_t byteLeft, toCopy;
        do {
            byteLeft = fileSize - copiedSize;
            toCopy = MIN_VAL(byteLeft, BUFFER_SIZE);
            if (!pfread(srcfd, buffer, toCopy)) {
                D("(Copy) Unexepected EOF.\n");
                fclose(srcfd);
                fclose(destfd);
                return false;
            } else if (pfwrite(destfd, buffer, toCopy)){
                copiedSize += toCopy;
            } else {
                D("(Copy) Broken pipe.\n");
                fclose(srcfd);
                fclose(destfd);
                return false;
            }
        } while (copiedSize < fileSize);

        fclose(srcfd);
        fclose(destfd);
        return true;
    }

    // mkdir -p
    bool Mkdir(std::string & path) {
        size_t l = path.size();
        size_t cursor = 1;
        int rv;
        while (cursor < l) {
            if (path[cursor] == '/') {
                path[cursor] = '\0';
                rv = mkdir(path.c_str(), S_IRWXU | S_IRGRP | S_IXGRP);
                if (rv == -1 && errno != EEXIST) {
                    return false;
                }
                path[cursor] = '/';
            }
            ++cursor;
        }
        rv = mkdir(path.c_str(), S_IRWXU | S_IRGRP | S_IXGRP);
        return !(rv == -1 && errno != EEXIST);
    }

    // Get working directory, make directory if does not exist
    bool getWorkingDirectory(std::string & workingDir) {

        // create the folder
        const char * homedir = getenv("HOME");

        if (homedir == NULL) {
            E("$HOME environment variable not set.\n");
            return false;
        }
        workingDir = homedir;
        workingDir += TEMP_DIR;
        return Mkdir(workingDir);
    }

    // Receive file from socket
    bool receiveFile(const int sockfd, const char * file_path) {
        char buffer[BUFFER_SIZE];
        FILE * fd = fopen(file_path, "w");
        if (fd == NULL) {
            D("(receiveFile) Cannot open file to write.\n");
            return false;
        }
        ssize_t fileSize;
        if (precv(sockfd, static_cast<void *>(&fileSize), sizeof(ssize_t)) && fileSize > 0) {
            ssize_t receivedSize = 0;
            ssize_t byteLeft, toReceive;
            do {
                byteLeft = fileSize - receivedSize;
                toReceive = MIN_VAL(byteLeft, BUFFER_SIZE);
                if (precv(sockfd, static_cast<void *>(buffer), toReceive)) {
                    if (!pfwrite(fd, buffer, toReceive)) {
                        D("(receiveFile) Cannot write file.\n");
                        fclose(fd);
                        return false;
                    }
                    receivedSize += toReceive;
                } else {
                    D("(receiveFile) Broken pipe.\n");
                    fclose(fd);
                    return false;
                }
            } while (receivedSize < fileSize);
        } else {
            D("(receiveFile) Cannot receive file size.\n");
            fclose(fd);
            return false;
        }
        fclose(fd);
        return true;
    }

    // Send string through socket
    bool sendString(const int sockfd, const std::string & str) {
        const ssize_t strSize = str.size();

        if (psend(sockfd, static_cast<const void *>(&strSize), sizeof(ssize_t)) && strSize > 0) {
            const char * strStart = str.data();
            ssize_t sentSize = 0;
            ssize_t byteLeft, toSend;
            do {
                byteLeft = strSize - sentSize;
                toSend = MIN_VAL(byteLeft, BUFFER_SIZE);
                if (!psend(sockfd, static_cast<const void *>(strStart + sentSize), toSend)) {
                    D("(sendString) Broken pipe.\n");
                    return false;
                }
                sentSize += toSend;
            } while (sentSize < strSize);
            return true;
        } else {
            D("(sendString) Cannot send string size.\n");
            return false;
        }
    }

    // Receive string from socket
    bool receiveString(const int sockfd, std::string & str) {
        str.clear();
        char buffer[BUFFER_SIZE];
        ssize_t strSize;
        if (precv(sockfd, static_cast<void *>(&strSize), sizeof(ssize_t)) && strSize > 0) {
            str.reserve(strSize);
            ssize_t receivedSize = 0;
            ssize_t byteLeft, toReceive;
            do {
                byteLeft = strSize - receivedSize;
                toReceive = MIN_VAL(byteLeft, BUFFER_SIZE);
                if (precv(sockfd, static_cast<void *>(buffer), toReceive)) {
                    str.append(buffer, toReceive);
                    receivedSize += toReceive;
                } else {
                    D("(receiveString) Broken pipe.\n");
                    return false;
                }
            } while (receivedSize < strSize);
        } else {
            D("(receiveString) Cannot receive string size.\n");
            return false;
        }
        return true;
    }

    // Read file as string (cache the file to avoid multiple read)
    bool readFileAsString(const char * file_path, std::string & str) {
        str.clear();
        FILE * fd = fopen(file_path, "r");
        if (fd == NULL) {
            D("(readFileAsString) Cannot open file to read.\n");
            return false;
        }
        long tempSize = getFileSize(fd);
        if (tempSize < 0) {
            D("(readFileAsString) Cannot get file size.\n");
            fclose(fd);
            return false;
        }
        size_t size = tempSize;
        str.resize(size);
        if (!pfread(fd, &str[0], size)) {
            D("(readFileAsString) Cannot read the file.\n");
            fclose(fd);
            return false;
        }
        fclose(fd);
        return true;

    }

    // Invoke master
    bool invokeMaster(const int fd) {
        const char c = CALL_MASTER;
        return psend(fd, static_cast<const void *>(&c), sizeof(char));
    }

    // Invoke worker
    bool invokeWorker(int fd) {
        const char c = CALL_WORKER;
        return psend(fd, static_cast<const void *>(&c), sizeof(char));
    }

    // Return success to chrun program
    void sendSuccess(const int sockfd) {
        const char c = RES_SUCCESS;
        ch::psend(sockfd, static_cast<const void *>(&c), sizeof(char));
    }

    // Return fail to chrun program
    void sendFail(const int sockfd) {
        const char c = RES_FAIL;
        ch::psend(sockfd, static_cast<const void *>(&c), sizeof(char));
    }
}

#endif
