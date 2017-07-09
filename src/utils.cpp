#include "utils.hpp"

namespace ch {

    /*
     * Promised I/O wrapper
     */

    // Send with given length
    bool psend(int fd, const void * buffer, size_t len) {

        off_t offset = 0;
        const char * cbuf = reinterpret_cast<const char *>(buffer);
        ssize_t sent;

        while (len != 0 &&
                  (
                      (sent = send(fd, cbuf + offset, len, 0)) > 0 ||
                      (sent == -1 && errno == EINTR)
                  )
              ) {
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

        while (len != 0 &&
                  (
                      (received = recv(fd, cbuf + offset, len, 0)) > 0 ||
                      (received == -1 && errno == EINTR)
                  )
              ) {
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

        while (len != 0 &&
                  (
                      (written = fwrite(cbuf + offset, sizeof(char), len, fd)) == len ||
                      (errno == EINTR)
                  )
              ) {
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

        while (len != 0 &&
                  (
                      (read_ = fread(cbuf + offset, sizeof(char), len, fd)) == len ||
                      (errno == EINTR)
                  )
              ) {
            offset += read_;
            len -= read_;
        }

        return (len == 0);

    }

    /*
     * Network functions
     */

#if defined (__CH_KQUEUE__)
    // kevent wrapper
    int Kevent(int kq, const struct kevent *changelist, int nchanges,
            struct kevent *eventlist, int nevents, const struct timespec *timeout) {

        int rv;

        do {
            rv = kevent(kq, changelist, nchanges, eventlist, nevents, timeout);
        } while (rv < 0 && errno == EINTR);

        return rv;

    }

#elif defined (__CH_EPOLL__)
    // epoll_wait wrapper
    int Epoll_wait(int epfd, struct epoll_event *events, int maxevents, int timeout) {

        int rv;

        do {
            rv = epoll_wait(epfd, events, maxevents, timeout);
        } while (rv < 0 && errno == EINTR);

        return rv;

    }

#else
    // select wrapper
    int Select(int nfds, fd_set * readfds, fd_set * writefds, fd_set * errorfds,
            struct timeval * timeout) {

        int rv;

        do {
            rv = select(nfds, readfds, writefds, errorfds, timeout);
        } while (rv < 0 && errno == EINTR);

        return rv;

    }

#endif

    // Connect to an address: port
    bool sconnect(int & sockfd, const char * ip, const unsigned short port) {

        sockaddr_in addr;

        sockfd = socket(AF_INET, SOCK_STREAM, 0);

        if (sockfd < 0) {
            sockfd = INVALID_SOCKET;
            return false;
        }

        memset(&addr, 0, sizeof(sockaddr_in));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = inet_addr(ip);
        addr.sin_port = htons(port);

        return connect(sockfd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(sockaddr)) >= 0;

    }

    // Prepare to accept connection
    bool prepareServer(int & serverfd, const unsigned short port){

        sockaddr_in addr;

        serverfd = socket(AF_INET, SOCK_STREAM, 0);

        if (serverfd < 0) {
            D("(prepareServer) Socket failed.");
            return false;
        }

        memset(&addr, 0, sizeof(sockaddr_in));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);

        int rv = bind(serverfd, reinterpret_cast<const sockaddr *>(&addr), sizeof(sockaddr));

        if (rv < 0) {
            D("(prepareServer) Bind failed.");
            return false;
        }

        rv = listen(serverfd, 5);

        if (rv < 0) {
            D("(prepareServer) Listen failed.");
            return false;
        }

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

    // Send file through a socket
    bool sendFile(const int sockfd, const char * file_path) {

        char buffer[BUFFER_SIZE];

        FILE * fd = fopen(file_path, "r");

        if (fd == NULL) {
            D("(sendFile) Cannot open file to write.");
            return false;
        }

        const ssize_t fileSize = getFileSize(fd);

        if (fileSize < 0) {
            D("(sendFile) Cannot get file size.");
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
                    D("(sendFile) Unexepected EOF.");
                    fclose(fd);
                    return false;
                } else if (psend(sockfd, static_cast<const void *>(buffer), toSend)){
                    sentSize += toSend;
                } else {
                    D("(sendFile) Broken pipe.");
                    fclose(fd);
                    return false;
                }
            } while (sentSize < fileSize);
        } else {
            D("(sendFile) Cannot send file size.");
            fclose(fd);
            return false;
        }

        fclose(fd);

        return true;

    }

    // Receive file from socket
    bool receiveFile(const int sockfd, const char * file_path) {

        char buffer[BUFFER_SIZE];

        if (fileExist(file_path)) {
            D("(receiveFile) File exists.");
        }

        FILE * fd = fopen(file_path, "w");

        if (fd == NULL) {
            D("(receiveFile) Cannot open file to write.");
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
                        D("(receiveFile) Cannot write file.");
                        fclose(fd);
                        return false;
                    }

                    receivedSize += toReceive;
                } else {
                    D("(receiveFile) Broken pipe.");
                    fclose(fd);
                    return false;
                }
            } while (receivedSize < fileSize);
        } else {
            D("(receiveFile) Cannot receive file size.");
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
                    D("(sendString) Broken pipe.");
                    return false;
                }
                sentSize += toSend;
            } while (sentSize < strSize);

            return true;
        } else {
            D("(sendString) Cannot send string size.");
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
                    D("(receiveString) Broken pipe.");
                    return false;
                }
            } while (receivedSize < strSize);

            return true;
        } else {
            D("(receiveString) Cannot receive string size.");
            return false;
        }

    }

    /*
     * File system functions
     */

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

    // Read ipconfig from configureFile
    bool readIPs(const std::string & configureFile, ipconfig_t & ips) {

        ips.clear();

        std::ifstream in{configureFile};

        if (!in) {
            D("(readIPs) Cannot open configuration file.");
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

    // Get working directory, make directory if does not exist
    bool getWorkingDirectory(std::string & workingDir, const std::string & additionalDir) {

        // create the folder
        const char * homedir = getenv("HOME");

        if (homedir == NULL) {
            E("$HOME environment variable not set.");
            return false;
        }
        workingDir = homedir;
        workingDir += TEMP_DIR;
        workingDir += "/" + additionalDir;

        if (!fileExist(workingDir.c_str())) {
            return Mkdir(workingDir);
        }

        return true;

    }

    // Read file as string (cache the file to avoid multiple read)
    bool readFileAsString(const char * file_path, std::string & str) {

        str.clear();

        FILE * fd = fopen(file_path, "r");

        if (fd == NULL) {
            D("(readFileAsString) Cannot open file to read.");
            return false;
        }

        long tempSize = getFileSize(fd);

        if (tempSize < 0) {
            D("(readFileAsString) Cannot get file size.");
            fclose(fd);
            return false;
        }

        size_t size = tempSize;
        str.resize(size);

        if (!pfread(fd, &str[0], size)) {
            D("(readFileAsString) Cannot read the file.");
            fclose(fd);
            return false;
        }

        fclose(fd);

        return true;

    }

    /*
     * Mimic command
     */

    // cp
    bool Copy(const char * src, const char * dest) {

        char buffer[BUFFER_SIZE];

        FILE * srcfd = fopen(src, "r");

        if (srcfd == NULL) {
            D("(Copy) Cannot open file to read.");
            return false;
        }

        const ssize_t fileSize = getFileSize(srcfd);

        if (fileSize < 0) {
            fclose(srcfd);
            return false;
        }

        FILE * destfd = fopen(dest, "w");

        if (destfd == NULL) {
            D("(Copy) Cannot open file to write.");
            fclose(srcfd);
            return false;
        }

        ssize_t copiedSize = 0;
        ssize_t byteLeft, toCopy;

        do {
            byteLeft = fileSize - copiedSize;
            toCopy = MIN_VAL(byteLeft, BUFFER_SIZE);

            if (!pfread(srcfd, buffer, toCopy)) {
                D("(Copy) Unexepected EOF.");
                fclose(srcfd);
                fclose(destfd);
                return false;
            } else if (pfwrite(destfd, buffer, toCopy)){
                copiedSize += toCopy;
            } else {
                D("(Copy) Broken pipe.");
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

    /*
     * Invoke functions
     */

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

    /*
     * Others
     */

    // Generate random string of length l
    std::string randomString(size_t l) {

        std::random_device d;
        std::default_random_engine generator{d()};
        std::uniform_int_distribution<char> distribution{'a', 'z'};
        auto char_dice = std::bind(distribution, generator);

        std::string ret;
        ret.reserve(l);

        for (size_t i = 0; i < l; ++i) {
            ret.push_back(char_dice());
        }

        return ret;

    }
}
