#ifndef UTILS_H
#define UTILS_H

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <stdio.h> // fseek, ftell, rewind, fread, ...
#include "def.hpp"

namespace ch {
    ssize_t psend(int fd, const void * buffer, size_t len) {
        off_t offset = 0;
        const char * cbuf = reinterpret_cast<const char *>(buffer);
        ssize_t sent;
        while (len != 0 && ((sent = send(fd, cbuf + offset, len, 0)) > 0 || (sent == -1 && errno == EINTR))) {
            if (sent > 0) {
                offset += sent;
                len -= sent;
            }
        }
        if (len != 0) {
            return INVALID;
        } else {
            return offset;
        }
    }
    ssize_t precv(int fd, void * buffer, size_t len) {
        off_t offset = 0;
        char * cbuf = reinterpret_cast<char *>(buffer);
        ssize_t received;
        while (len != 0 && ((received = recv(fd, cbuf + offset, len, 0)) > 0 || (received == -1 && errno == EINTR))) {
            if (received > 0) {
                offset += received;
                len -= received;
            }
        }
        if (len != 0) {
            return INVALID;
        } else {
            return offset;
        }
    }
    int sconnect(int & sockfd, const char * ip, unsigned short port) {

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

        int rv = connect(sockfd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(sockaddr));

        if (rv < 0) {
            D("Connection failed.\n");
            sockfd = INVALID_SOCKET;
            return INVALID;
        }

        return rv;
    }
    long getFileSize(FILE * fd) {
        fseek(fd, 0, SEEK_END);
        return ftell(fd);
    }
    bool fileExist(const char * path) {
        struct stat buf;
        return (stat(path, &buf) >= 0);
    }
    bool prepareServer(int & serverfd, unsigned short port){
        sockaddr_in addr;

        serverfd = socket(AF_INET, SOCK_STREAM, 0);

        if (serverfd < 0) {
            return false;
        }

        memset(&addr, 0, sizeof(sockaddr_in));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);

        int rv = bind(serverfd, reinterpret_cast<const sockaddr *>(&addr), sizeof(sockaddr));

        if (rv < 0) {
            return false;
        }

        rv = listen(serverfd, 5);

        if (rv < 0) {
            return false;
        }

        return true;
    }
    bool readIPs(const char * configureFile, std::vector<std::pair<int, std::string> > & ips) {
        ips.clear();
        if (configureFile == NULL) {
            return false;
        }
        std::ifstream in;
        in.open(configureFile);
        if (in.fail()) {
            return false;
        }
        std::string ip;
        int id;
        while (in >> id >> ip) {
            ips.emplace_back(id, ip);
        }
        in.close();
        return true;
    }

    void rearrangeIPs(std::vector<std::pair<int, std::string> > & ips, std::string & file, int indexToHead) {
        file = std::to_string(ips[indexToHead].first) + " " + ips[indexToHead].second + "\n";
        for (int i = 0, l = ips.size(); i < l; ++i) {
            if (i != indexToHead) {
                file += std::to_string(ips[i].first) + " " + ips[i].second + "\n";
            }
        }
    }

    bool getWorkingDirectory(std::string & workingDir) {

        // create the folder
        const char * homedir = getenv("HOME");

        if (homedir == NULL) {
            L("$HOME environment variable not set.\n");
            return false;
        }
        workingDir = homedir;
        workingDir += "/.cHadoop";
        std::string cmdString("mkdir -p ");
        cmdString += workingDir;

        system(cmdString.c_str());
        return true;

    }

    bool sendFile(int sockfd, const char * file_path) {
        char buffer[BUFFER_SIZE];
        FILE * fd = fopen(file_path, "r");
        if (fd == NULL) {
            D("Cannot open file to write.\n");
            return false;
        }
        ssize_t fileSize = getFileSize(fd);
        if (fileSize < 0) {
            fclose(fd);
            return false;
        }

        if (psend(sockfd, static_cast<void *>(&fileSize), sizeof(ssize_t)) == sizeof(ssize_t)) {
            ssize_t sentSize = 0;
            ssize_t byteLeft, toSend;
            do {
                byteLeft = fileSize - sentSize;
                toSend = MIN_VAL(byteLeft, BUFFER_SIZE);
                if (toSend > 0) {
                    ssize_t rv = fread(buffer, sizeof(char), toSend, fd);
                    if (rv < toSend) {
                        D("sendFile: Unexepected EOF.\n");
                        fclose(fd);
                        return false;
                    } else {
                        rv = psend(sockfd, static_cast<const void *>(buffer), rv);
                        if (rv < 0) {
                            D("Broken pipe.\n");
                            fclose(fd);
                            return false;
                        }
                        sentSize += rv;
                    }
                }
            } while (sentSize < fileSize);
        } else {
            D("Cannot send file size.\n");
            fclose(fd);
            return false;
        }

        fclose(fd);
        return true;
    }
    bool receiveFile(int sockfd, const char * file_path) {
        char buffer[BUFFER_SIZE];
        FILE * fd = fopen(file_path, "w");
        if (fd == NULL) {
            D("Cannot open file to write.\n");
            return false;
        }
        ssize_t fileSize = INVALID;
        ssize_t rv = precv(sockfd, static_cast<void *>(&fileSize), sizeof(ssize_t));
        if (rv == sizeof(ssize_t) && fileSize >= 0) {
            ssize_t receivedSize = 0;
            ssize_t byteLeft, toReceive;
            do {
                byteLeft = fileSize - receivedSize;
                toReceive = MIN_VAL(byteLeft, BUFFER_SIZE);
                rv = precv(sockfd, static_cast<void *>(buffer), toReceive);
                if (rv < 0) {
                    D("Broken pipe.\n");
                    fclose(fd);
                    return false;
                } else {
                    rv = fwrite(buffer, sizeof(char), rv, fd);
                    if (rv == 0) {
                        D("Cannot write file.\n");
                        fclose(fd);
                        return false;
                    }
                    receivedSize += rv;
                }
            } while (receivedSize < fileSize);
        } else {
            D("Cannot receive file size or file size is 0.\n");
            fclose(fd);
            return false;
        }
        fclose(fd);
        return true;
    }

    bool sendString(int sockfd, std::string & str) {
        ssize_t strSize = str.size();

        if (psend(sockfd, static_cast<const void *>(&strSize), sizeof(ssize_t)) == sizeof(ssize_t)) {
            const char * strStart = str.data();
            ssize_t sentSize = 0;
            ssize_t byteLeft, toSend;
            do {
                byteLeft = strSize - sentSize;
                toSend = MIN_VAL(byteLeft, BUFFER_SIZE);
                if (toSend > 0) {
                    ssize_t rv = psend(sockfd, static_cast<const void *>(strStart + sentSize), toSend);
                    if (rv < 0) {
                        D("Broken pipe.\n");
                        return false;
                    }
                    sentSize += rv;
                }
            } while (sentSize < strSize);
            return true;
        } else {
            D("Cannot send string size.\n");
        }

        return false;
    }

    bool receiveString(int sockfd, std::string & str) {
        str.clear();
        char buffer[BUFFER_SIZE];
        ssize_t strSize;
        ssize_t rv = precv(sockfd, static_cast<void *>(&strSize), sizeof(ssize_t));
        if (rv > 0 && strSize >= 0) {
            str.reserve(strSize);
            ssize_t receivedSize = 0;
            ssize_t byteLeft, toReceive;
            do {
                byteLeft = strSize - receivedSize;
                toReceive = MIN_VAL(byteLeft, BUFFER_SIZE);
                rv = precv(sockfd, static_cast<void *>(buffer), toReceive);
                if (rv < 0) {
                    D("Broken pipe.\n");
                    return false;
                } else {
                    str.append(buffer, rv);
                    receivedSize += rv;
                }
            } while (receivedSize < strSize);
        } else {
            D("Cannot receive string size.\n");
            return false;
        }
        return true;
    }

    bool readFileAsString(const char * file_path, std::string & str) {
        str.clear();
        FILE * fd = fopen(file_path, "r");
        if (fd == NULL) {
            return false;
        }
        long tempSize = getFileSize(fd);
        if (tempSize < 0) {
            fclose(fd);
            return false;
        }
        size_t size = tempSize;
        rewind(fd);
        str.resize(size);
        size_t rv = fread(&str[0], sizeof(char), size, fd);
        if (rv != size) {
            fclose(fd);
            return false;
        }
        fclose(fd);
        return true;

    }

}

#endif
