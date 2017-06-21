#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <string>
#include <dlfcn.h>
#include "sourceManager.hpp"
#include "job.hpp"

// TODO fault tolerence (error process)
// TODO thread safe output
std::string confFilePath;
std::string jobFilePath;
std::string workingDir;

// Get job function from job library and run the job
bool runJob(const ipconfig_t & ips, ch::SourceManager & source, const std::string & outputFilePath, const bool isServer) {
    void * jobLib = dlopen(jobFilePath.c_str(), RTLD_LAZY);
    if (jobLib == NULL) {
        L("CHServer: Cannot find library file.\n");
        return false;
    } else {
        const ch::job_f * doJob = (ch::job_f *)dlsym(jobLib, "doJob");
        if (doJob == NULL) {
            L("CHServer: No doJob function found in the library.\n");
            return false;
        } else {
            ch::context_t context(ips, source, outputFilePath, workingDir, isServer);
            if (!doJob(context)) {
                dlclose(jobLib);
                return false;
            }
        }
        dlclose(jobLib);
        return true;
    }
}

// Run as worker
bool asWorker(const int sockfd) {
    L("CHServer: Running as worker.\n");

    ch::SourceManagerWorker source(sockfd);

    if (source.isValid()) {
        if (source.receiveFiles(confFilePath, jobFilePath)) {
            ipconfig_t ips;
            if (ch::readIPs(confFilePath, ips)) {
                // do job
                const std::string outputFilePath;
                return runJob(ips, source, outputFilePath, false);
            }
        }
    }
    return false;
}

// Run as master
bool asMaster(int sockfd) {
    L("CHServer: Running as master.\n");

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

    ipconfig_t ips;
    if (!ch::readIPs(confFilePath, ips)) {
        return false;
    }

    if (ips.empty()) {
        return false;
    }

    ch::SourceManagerMaster source(dataFilePath.c_str(), jobFilePath);

    if (source.isValid()) {

        source.startFileDistributionThreads(sockfd, ips, SERVER_PORT);

        // do job

        bool ret = runJob(ips, source, outputFilePath, true);

        source.blockTillDistributionThreadsEnd();

        return ret && source.allWorkerSuccess();
    }

    return false;

}

// React based on the call
void serve(const int sockfd) {
    if (sockfd > 0) { // the socket with the RPC caller
        char c;
        if (ch::precv(sockfd, static_cast<void *>(&c), sizeof(char))) {
            if (c == CALL_MASTER) {
                if (!asMaster(sockfd)) {
                    ch::sendFail(sockfd);
                } else {
                    ch::sendSuccess(sockfd);
                }
                close(sockfd);
            } else if (c == CALL_WORKER) {
                // Result to be processed by sourceManager
                if (!asWorker(sockfd)) {
                    ch::sendFail(sockfd);
                } else {
                    ch::sendSuccess(sockfd);
                }
                close(sockfd);
            } else {
                L("CHServer: Unsupported call.\n");
            }
        }
    }
}

int main(int argc, char ** argv) {

    if (!ch::getWorkingDirectory(workingDir)) {
        return 0;
    }

    confFilePath = workingDir + IPCONFIG_FILE;
    jobFilePath = workingDir + JOB_FILE;

    int serverfd = INVALID_SOCKET;

    sockaddr_in remote;
    socklen_t s_size;

    if (ch::prepareServer(serverfd, SERVER_PORT)) {
        while (1) {
            L("CHServer: Accepting request.\n");
            int sockfd = accept(serverfd, reinterpret_cast<struct sockaddr *>(&remote), &s_size);
            std::thread serve_thread(serve, sockfd);
            serve_thread.join(); // accept one request each time
        }
    } else {
        L("CHServer: Port occupied.\n");
    }

    return 0;
}
