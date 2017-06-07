#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <string>
#include <sstream>
#include "sourceManager.hpp"
#include "streamManager.hpp"

// TODO fault tolerence (error process)
std::string dataFilePath;
std::string confFilePath;
std::string outputFilePath;

void mapFun(std::string & block, ch::StreamManager<ch::Tuple<ch::String, ch::Integer> > & sm) {
    std::stringstream ss(block);
    ch::Tuple<ch::String, ch::Integer> res;
    (res.second)->value = 1;
    while (ss >> ((res.first)->value)) {
        sm.push(res, ch::hashPartitioner);
        res.first->reset();
    }
}

void reduceFun(std::vector<ch::Tuple<ch::String, ch::Integer> *> & sorted, ch::StreamManager<ch::Tuple<ch::String, ch::Integer> > & sm) {
    if (sorted.size() == 0) {
        return;
    }
    bool started = false;
    ch::Tuple<ch::String, ch::Integer> res;
    for (ch::Tuple<ch::String, ch::Integer> * e: sorted) {
        if (started == false) {
            res = (*e);
            started = true;
        } else {
            if (res == (*e)) {
                res += (*e);
            } else {
                D("Emit: " << res.toString() << std::endl);
                sm.push(res, ch::zeroPartitioner);
                res = (*e);
            }
        }
        delete e;
    }
    if (started) {
        D("Emit: " << res.toString() << std::endl);
        sm.push(res, ch::zeroPartitioner);
    }
}

void doJob(std::vector<std::pair<int, std::string> > & ips, ch::SourceManager & source, bool isServer) {
    std::string polled;

    ch::StreamManager<ch::Tuple<ch::String, ch::Integer> > stm(ips);

    while (source.poll(polled)) {
        mapFun(polled, stm);
    }

    stm.stopSend();
    stm.blockTillRecvEnd();

    std::vector<ch::Tuple<ch::String, ch::Integer> *> sorted;
    stm.getSorted(sorted);

    stm.startRecvThreads();
    reduceFun(sorted, stm);

    stm.finalizeSend();
    stm.blockTillRecvEnd();

    if (isServer) {
        stm.pourToTextFile(outputFilePath.c_str());
    }
}

bool serveClient(int sockfd) {

    ch::SourceManager source(sockfd);

    if (source.isValid()) {
        if (source.receiveAsClient(confFilePath)) {
            std::vector<std::pair<int, std::string> > ips;
            if (ch::readIPs(confFilePath.c_str(), ips)) {
                // do job
                doJob(ips, source, false);
                return true;
            }
        }
    }
    return false;
}

bool serveServer(int sockfd) {
    D("Server open.\n");
    std::vector<std::pair<int, std::string> > ips;
    if (!ch::readIPs(confFilePath.c_str(), ips)) {
        return false;
    }

    int numIP = ips.size();

    if (numIP == 0) {
        return false;
    }

    ch::SourceManager source(dataFilePath.c_str());

    D("source opened.\n");

    if (source.isValid()) {
        source.startFileDistributionThreads(sockfd, ips, SERVER_PORT);

        // do job
        doJob(ips, source, true);

        source.blockTillDistributionThreadsEnd();

        return true;
    }

    return false;

}

void sendSuccess(int sockfd) {
    const char c = RES_SUCCESS;
    ch::ssend(sockfd, static_cast<const void *>(&c), sizeof(char));
    close(sockfd);
}

void sendFail(int sockfd) {
    const char c = RES_FAIL;
    ch::ssend(sockfd, static_cast<const void *>(&c), sizeof(char));
    close(sockfd);
}

void serve(int * in_args) {
    int sockfd = *in_args;
    delete in_args;
    if (sockfd > 0) { // the socket with the RPC caller
        char c;
        int rv = ch::srecv(sockfd, static_cast<void *>(&c), sizeof(char));
        if (rv > 0) {
            if (c == CALL_SERVER) {
                if (!ch::receiveString(sockfd, dataFilePath)) {
                    sendFail(sockfd);
                }
                if (!ch::receiveString(sockfd, outputFilePath)) {
                    sendFail(sockfd);
                }
                if (!serveServer(sockfd)) {
                    sendFail(sockfd);
                }
            } else if (c == CALL_CLIENT) {
                if (!serveClient(sockfd)) {
                    sendFail(sockfd);
                }
            } else {
                D("Unsupported call.\n");
            }
        }
        sendSuccess(sockfd);
    }
}

int main(int argc, char ** argv) {

    std::string workingDir;
    ch::getWorkingDirectory(workingDir);

    confFilePath = workingDir + IPCONFIG_FILE;

    // run server

    int serverfd = INVALID_SOCKET;

    sockaddr_in remote;
    socklen_t s_size;

    if (ch::prepareServer(serverfd, SERVER_PORT)) {
        while (1) {
            D("Accepting request.\n");
            int * sockfd = new int();
            *sockfd = accept(serverfd, reinterpret_cast<struct sockaddr *>(&remote), &s_size);
            std::thread serve_thread(serve, sockfd);
            serve_thread.join(); // accept one request each time
        }
    } else {
        L("Port occupied.\n");
    }

    return 0;
}
