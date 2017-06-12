#ifndef UTILS_H
#define UTILS_H

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include "def.hpp"

namespace ch {
    ssize_t ssend(int fd, const void * buffer, size_t len) {
        ssize_t rv = send(fd, buffer, len, 0);
        if (rv < 0) {
            D("Send error: " << errno << std::endl);
        }
        return rv;
    }
    ssize_t srecv(int fd, void * buffer, size_t len) {
        ssize_t rv = recv(fd, buffer, len, 0);
        if (rv < 0) {
            D("Recv error: " << errno << std::endl);
        }
        return rv;
    }
    ssize_t sread(int fd, void * buffer, size_t size) {
        int rv = read(fd, buffer, size);
        if (rv < 0) {
            D("Read failed.\n");
        }
        return rv;
    }
    ssize_t swrite(int fd, const void * buffer, size_t size) {
        int rv = write(fd, buffer, size);
        if (rv < 0) {
            D("Write failed.\n");
        }
        return rv;
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
    off_t getFileSize(int fd) {
        struct stat buf;
        int rv = fstat(fd, &buf);
        if (rv < 0) {
            return -1;
        }
        return buf.st_size;
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
        int file = open(file_path, O_RDONLY);
        if (file < 0) {
            L("Cannot open file to write.\n");
            return false;
        }
        ssize_t fileSize = getFileSize(file);

        if (fileSize < 0) {
            L("Empty file.\n");
            close(file);
        }

        if (ssend(sockfd, static_cast<void *>(&fileSize), sizeof(ssize_t)) == sizeof(ssize_t)) {
            ssize_t sentSize = 0;
            ssize_t byteLeft, toSend;
            do {
                byteLeft = fileSize - sentSize;
                toSend = MIN_VAL(byteLeft, BUFFER_SIZE);
                if (toSend > 0) {
                    ssize_t rv = sread(file, static_cast<void *>(buffer), toSend);
                    if (rv < 0) {
                        L("Cannot read file.\n");
                        return false;
                    } else {
                        rv = ssend(sockfd, static_cast<const void *>(buffer), rv);
                        if (rv < 0) {
                            L("Broken pipe.\n");
                            return false;
                        }
                        sentSize += rv;
                    }
                }
            } while (sentSize < fileSize);
        } else {
            L("Cannot send file size.\n");
            close(file);
            return false;
        }

        close(file);
        return true;
    }
    bool receiveFile(int sockfd, const char * file_path) {
        char buffer[BUFFER_SIZE];
        int file = open(file_path, O_WRONLY | O_CREAT, S_CREAT_DEFAULT);
        if (file < 0) {
            L("Cannot open file to write.\n");
            return false;
        }
        ssize_t fileSize;
        ssize_t rv = srecv(sockfd, static_cast<void *>(&fileSize), sizeof(ssize_t));
        if (rv == sizeof(ssize_t) && fileSize > 0) {
            ssize_t receivedSize = 0;
            ssize_t byteLeft, toReceive;
            do {
                byteLeft = fileSize - receivedSize;
                toReceive = MIN_VAL(byteLeft, BUFFER_SIZE);
                rv = srecv(sockfd, static_cast<void *>(buffer), toReceive);
                if (rv < 0) {
                    L("Broken pipe.\n");
                    return false;
                } else {
                    rv = swrite(file, static_cast<const void *>(buffer), rv);
                    if (rv < 0) {
                        L("Cannot write file.\n");
                        return false;
                    }
                    receivedSize += rv;
                }
            } while (receivedSize < fileSize);
        } else {
            L("Cannot receive file size or file size is 0.\n");
            close(file);
            return false;
        }
        close(file);
        return true;
    }

    bool sendString(int sockfd, std::string & str) {
        ssize_t strSize = str.size();

        if (ssend(sockfd, static_cast<const void *>(&strSize), sizeof(ssize_t)) == sizeof(ssize_t)) {
            const char * strStart = str.data();
            ssize_t sentSize = 0;
            ssize_t byteLeft, toSend;
            do {
                byteLeft = strSize - sentSize;
                toSend = MIN_VAL(byteLeft, BUFFER_SIZE);
                if (toSend > 0) {
                    ssize_t rv = ssend(sockfd, static_cast<const void *>(strStart + sentSize), toSend);
                    if (rv < 0) {
                        L("Broken pipe.\n");
                        return false;
                    }
                    sentSize += toSend;
                }
            } while (sentSize < strSize);
            return true;
        } else {
            L("Cannot send string size.\n");
        }

        return false;
    }

    bool receiveString(int sockfd, std::string & str) {
        str.clear();
        char buffer[BUFFER_SIZE];
        ssize_t strSize;
        ssize_t rv = srecv(sockfd, static_cast<void *>(&strSize), sizeof(ssize_t));
        if (rv == sizeof(ssize_t) && strSize >= 0) {
            ssize_t receivedSize = 0;
            ssize_t byteLeft, toReceive;
            do {
                byteLeft = strSize - receivedSize;
                toReceive = MIN_VAL(byteLeft, BUFFER_SIZE);
                rv = srecv(sockfd, static_cast<void *>(buffer), toReceive);
                if (rv < 0) {
                    L("Broken pipe.\n");
                    return false;
                } else {
                    str.append(buffer, rv);
                    receivedSize += rv;
                }
            } while (receivedSize < strSize);
        } else {
            L("Cannot receive string size.\n");
            return false;
        }
        return true;
    }

    bool writeString(int fd, std::string & str) {
        ssize_t strSize = str.size();

        if (swrite(fd, static_cast<const void *>(&strSize), sizeof(ssize_t)) == sizeof(ssize_t)) {
            const char * strStart = str.data();
            ssize_t writtenSize = 0;
            ssize_t byteLeft, toWrite;
            do {
                byteLeft = strSize - writtenSize;
                toWrite = MIN_VAL(byteLeft, BUFFER_SIZE);
                if (toWrite > 0) {
                    ssize_t rv = swrite(fd, static_cast<const void *>(strStart + writtenSize), toWrite);
                    if (rv < 0) {
                        L("Broken pipe.\n");
                        return false;
                    }
                    writtenSize += toWrite;
                }
            } while (writtenSize < strSize);
            return true;
        } else {
            L("Cannot write string size.\n");
        }

        return false;
    }

    bool readString(int fd, std::string & str) {
        str.clear();
        char buffer[BUFFER_SIZE];
        ssize_t strSize;
        ssize_t rv = sread(fd, static_cast<void *>(&strSize), sizeof(ssize_t));
        if (rv == sizeof(ssize_t) && strSize >= 0) {
            ssize_t readSize = 0;
            ssize_t byteLeft, toRead;
            do {
                byteLeft = strSize - readSize;
                toRead = MIN_VAL(byteLeft, BUFFER_SIZE);
                rv = sread(fd, static_cast<void *>(buffer), toRead);
                if (rv < 0) {
                    L("Broken pipe.\n");
                    return false;
                } else {
                    str.append(buffer, rv);
                    readSize += rv;
                }
            } while (readSize < strSize);
        } else {
            L("Cannot read string size.\n");
            return false;
        }
        return true;
    }

    bool readFileAsString(const char * file_path, std::string & str) {
        str.clear();
        char buffer[BUFFER_SIZE];
        int fd = open(file_path, O_RDONLY);
        if (fd > 0) {
            ssize_t fileSize = getFileSize(fd);
            ssize_t readSize = 0;
            ssize_t byteLeft, toRead;
            if (fileSize > 0) {
                ssize_t rv;
                do {
                    byteLeft = fileSize - readSize;
                    toRead = MIN_VAL(byteLeft, BUFFER_SIZE);
                    rv = sread(fd, static_cast<void *>(buffer), toRead);
                    if (rv < 0) {
                        L("Broken pipe.\n");
                        return false;
                    } else {
                        str.append(buffer, rv);
                        readSize += rv;
                    }
                } while (readSize < fileSize);
                close(fd);
                return true;
            } else {
                L("Empty file.\n");
                close(fd);
                return false;
            }
        } else {
            L("File not found.\n");
            return false;
        }
    }

}

#endif
