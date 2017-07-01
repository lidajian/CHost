#include "sourceManager.hpp"

namespace ch {

    void SourceManagerMaster::rearrangeIPs(const ipconfig_t & ips, std::string & file, const size_t indexToHead) {
        file = std::to_string(ips[indexToHead].first) + " " + ips[indexToHead].second + "\n";
        for (size_t i = 0, l = ips.size(); i < l; ++i) {
            if (i != indexToHead) {
                file += std::to_string(ips[i].first) + " " + ips[i].second + "\n";
            }
        }
    }
    void SourceManagerMaster::distributionThread(int i, const ipconfig_t & ips, const unsigned short port, SourceManagerMaster * source) {
        int csockfd;

        const std::string & ip = ips[i].second;

        // create socket to clients
        if (!sconnect(csockfd, ip.c_str(), port)) {
            E("(SourceManagerMaster) Cannot connect to client.");
            return;
        }

        // create client on clients
        if(!invokeWorker(csockfd)) {
            ESS("(SourceManagerMaster) Cannot invoke worker on " << ip);
            close(csockfd);
            return;
        }

        // Send configuration file
        std::string rearrangedConfiguration;
        rearrangeIPs(ips, rearrangedConfiguration, i);
        sendString(csockfd, rearrangedConfiguration);

        // Send job file
        sendString(csockfd, source->_jobFileContent);

        // provide poll service
        char receivedChar;
        std::string split;
        ssize_t endSize = INVALID;
        while (precv(csockfd, static_cast<void *>(&receivedChar), sizeof(char))) {
            if (receivedChar == CALL_POLL) {
                if (!(source->splitter.next(split))) { // end service by server
                    psend(csockfd, static_cast<void *>(&endSize), sizeof(ssize_t));
                    break;
                }
                if (!sendString(csockfd, split)) {
                    E("(SourceManagerMaster) Failed to send split.");
                    break;
                }
                split.clear();
            }
        }

        if (!precv(csockfd, static_cast<void *>(&receivedChar), sizeof(char))) {
            E("(SourceManagerMaster) No response from worker.");
            close(csockfd);
            return;
        }

        source->workerIsSuccess[i] = (receivedChar == RES_SUCCESS);

        close(csockfd);
    }

    SourceManagerMaster::SourceManagerMaster(const char * dataFile, const std::string & jobFilePath): _jobFilePath{jobFilePath} {
        if (ch::readFileAsString(jobFilePath.c_str(), _jobFileContent)) {
            if (splitter.open(dataFile)) {
                D("(SourceManagerMaster) Fail to open data file.");
            }
        } else {
            D("(SourceManagerMaster) Fail to read job file.");
        }
    }

    void SourceManagerMaster::startFileDistributionThreads(const ipconfig_t & ips, unsigned short port) {
        if (!isValid()) {
            D("(SourceManagerMaster) Cannot start distribution when data file is not opened.");
            return;
        }
        workerIsSuccess.resize(ips.size(), false);
        for (size_t i = 1, l = ips.size(); i < l; ++i) {
            threads.emplace_back(distributionThread, i, std::ref(ips), port, this);
        }
    }

    void SourceManagerMaster::blockTillDistributionThreadsEnd() {
        for (std::thread & t: threads) {
            t.join();
        }
        threads.clear();
    }

    bool SourceManagerMaster::allWorkerSuccess() {
        size_t numIPs = workerIsSuccess.size();
        if (numIPs == 0) {
            D("(SourceManagerMaster) Cannot get worker results when no threads started.");
            return false;
        }
        for (size_t i = 1; i < numIPs; ++i) {
            if (!workerIsSuccess[i]) {
                DSS("(SourceManagerMaster) The " << i << "th worker failed.");
                return false;
            }
        }
        return true;
    }

    inline bool SourceManagerMaster::isValid() const {
        return splitter.isValid();
    }

    bool SourceManagerMaster::poll(std::string & ret) {
        ret.clear();
        return splitter.next(ret);
    }

    bool SourceManagerWorker::pollRequest() const {
        const char c = CALL_POLL;
        return psend(fd, static_cast<const void *>(&c), sizeof(char));
    }

    // Constructor for worker
    SourceManagerWorker::SourceManagerWorker(int sockfd): fd{sockfd} {}

    // Copy Constructor
    SourceManagerWorker::SourceManagerWorker(const SourceManagerWorker & o): fd{o.fd} {}

    // Move Constructor
    SourceManagerWorker::SourceManagerWorker(SourceManagerWorker && o): fd{o.fd} {
        o.fd = INVALID_SOCKET;
    }

    bool SourceManagerWorker::receiveFiles(const std::string & confFilePath, const std::string & jobFilePath) {
        if (!isValid()) {
            D("(SourceManagerWorker) The socket failed.");
            return false;
        }
        if (!receiveFile(fd, confFilePath.c_str())) {
            E("Fail to receive configuration file.");
            return false;
        }
        if (!receiveFile(fd, jobFilePath.c_str())) {
            E("Fail to receive job file.");
            return false;
        }
        return true;
    }

    inline bool SourceManagerWorker::isValid() const {
        return (fd > 0);
    }

    bool SourceManagerWorker::poll(std::string & ret) {
        ret.clear();
        if (!isValid()) {
            D("(SourceManagerWorker) The socket failed.");
            return false;
        } else {
            // request a block
            if (!pollRequest()) {
                D("(SourceManagerWorker) Fail to send poll request. Broken pipe.");
                fd = INVALID_SOCKET;
                return false;
            } else if (!receiveString(fd, ret)) {
                D("(SourceManagerWorker) Fail to receive the string or remote file EOF.");
                fd = INVALID_SOCKET;
                return false;
            } else {
                return true;
            }
        }
    }
}