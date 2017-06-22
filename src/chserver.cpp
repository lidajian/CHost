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
std::string workingDir;

// Get job function from job library and run the job
bool runJob(const ipconfig_t & ips, ch::SourceManager & source, const std::string & outputFilePath, const std::string & jobFilePath, const bool isServer) {
    void * jobLib = dlopen(jobFilePath.c_str(), RTLD_LAZY);
    if (jobLib == NULL) {
        E("Cannot find library file.");
        return false;
    } else {
        const ch::job_f * doJob = (ch::job_f *)dlsym(jobLib, "doJob");
        if (doJob == NULL) {
            E("No doJob function found in the library.");
            I("Please implement doJob function in the library.");
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

    puts("Running as worker.");

    ch::SourceManagerWorker source(sockfd);

    if (source.isValid()) {
        std::string jobFilePath = workingDir + JOB_FILE;
        if (source.receiveFiles(confFilePath, jobFilePath)) {
            ipconfig_t ips;
            if (ch::readIPs(confFilePath, ips)) {
                // do job
                const std::string outputFilePath;
                return runJob(ips, source, outputFilePath, jobFilePath, false);
            } else {
                E("Cannot read configuration file.");
            }
        } else {
            E("Cannot receive configuration file and job file.");
        }
    } else {
        E("Cannot connect to master.");
    }
    return false;
}

// Run as master
bool asMaster(int sockfd) {

    P("Running as master.");

    std::string dataFilePath;
    std::string outputFilePath;
    std::string jobFilePath;

    /*
     * Receive parameter from starter
     */
    if (!ch::receiveString(sockfd, dataFilePath)) {
        E("Cannot receive data file path.");
        return false;
    }
    if (!ch::receiveString(sockfd, outputFilePath)) {
        E("Cannot receive output file path.");
        return false;
    }
    if (!ch::receiveString(sockfd, jobFilePath)) {
        E("Cannot receive job file path.");
        return false;
    }

    ipconfig_t ips;
    if (!ch::readIPs(confFilePath, ips)) {
        E("Cannot read configuration file.");
        return false;
    }

    if (ips.empty()) {
        E("Configuration file contains no IP information.");
        return false;
    }

    ch::SourceManagerMaster source(dataFilePath.c_str(), jobFilePath);

    if (source.isValid()) {

        source.startFileDistributionThreads(sockfd, ips, SERVER_PORT);

        // do job

        if (!runJob(ips, source, outputFilePath, jobFilePath, true)) {
            E("Job on master failed.");
            return false;
        }

        source.blockTillDistributionThreadsEnd();

        if(!source.allWorkerSuccess()) {
            E("Job on workers failed.");
            return false;
        }

        return true;
    } else {
        E("Fail to open data/job file.");
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
                E("Unsupported call.");
            }
        }
    }
}

int main(int argc, char ** argv) {

    if (!ch::getWorkingDirectory(workingDir)) {
        return 0;
    }

    confFilePath = workingDir + IPCONFIG_FILE;

    int serverfd = INVALID_SOCKET;

    sockaddr_in remote;
    socklen_t s_size;

    if (ch::prepareServer(serverfd, SERVER_PORT)) {
        while (1) {
            P("Accepting request.");
            int sockfd = accept(serverfd, reinterpret_cast<struct sockaddr *>(&remote), &s_size);
            std::thread serve_thread(serve, sockfd);
            serve_thread.detach();
        }
    } else {
        E("Port occupied.");
        I("Close duplicated server.");
    }

    return 0;
}
