#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <string>
#include <dlfcn.h>
#include "sourceManager.hpp"
#include "job.hpp"

// TODO fault tolerence (error process)
std::string confFilePath;
std::string jobFilePath;
std::string workingDir;

bool runJob(std::vector<std::pair<int, std::string> > & ips, ch::SourceManager & source, std::string & outputFilePath, bool isServer) {
    void * jobLib = dlopen(jobFilePath.c_str(), RTLD_LAZY);
    if (jobLib == NULL) {
        L("Cannot find library file.\n");
        return false;
    } else {
        ch::job_f * doJob = (ch::job_f *)dlsym(jobLib, "doJob");
        if (doJob == NULL) {
            L("No doJob function found in the library.\n");
            return false;
        } else {
            ch::context_t context(ips, source, outputFilePath, workingDir, isServer);
            doJob(context);
        }
        dlclose(jobLib);
        return true;
    }
}

bool serveClient(int sockfd) {

    ch::SourceManager source(sockfd, jobFilePath);

    std::string outputFilePath;

    if (source.isValid()) {
        if (source.receiveAsClient(confFilePath)) {
            std::vector<std::pair<int, std::string> > ips;
            if (ch::readIPs(confFilePath.c_str(), ips)) {
                // do job
                return runJob(ips, source, outputFilePath, false);
            }
        }
    }
    return false;
}

bool serveServer(int sockfd) {
    D("Server open.\n");

    std::string dataFilePath;
    std::string outputFilePath;

    /*
     * Receive parameter from starter
     */
    if (!ch::receiveString(sockfd, dataFilePath)) {
        return false;
    }
    if (!ch::receiveString(sockfd, outputFilePath)) {
        return false;
    }

    std::vector<std::pair<int, std::string> > ips;
    if (!ch::readIPs(confFilePath.c_str(), ips)) {
        return false;
    }

    int numIP = ips.size();

    if (numIP == 0) {
        return false;
    }

    ch::SourceManager source(dataFilePath.c_str(), jobFilePath);

    if (source.isValid()) {

        D("source opened.\n");
        source.startFileDistributionThreads(sockfd, ips, SERVER_PORT);

        // do job

        bool ret = runJob(ips, source, outputFilePath, true);

        source.blockTillDistributionThreadsEnd();

        return ret;
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
                if (!serveServer(sockfd)) {
                    sendFail(sockfd);
                } else {
                    sendSuccess(sockfd);
                }
            } else if (c == CALL_CLIENT) {
                serveClient(sockfd);
                close(sockfd);
            } else {
                D("Unsupported call.\n");
            }
        }
    }
}

int main(int argc, char ** argv) {

    if (!ch::getWorkingDirectory(workingDir)) {
        L("Cannot get working directory.\n");
        return 0;
    }

    confFilePath = workingDir + IPCONFIG_FILE;
    jobFilePath = workingDir + JOB_FILE;

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
