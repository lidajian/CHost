#include <sys/socket.h>      // accept, socklen_t
#include <dlfcn.h>           // dlopen, dlsym, dlclose
#include <unistd.h>          // close
#include <signal.h>          // signal
#include <stdlib.h>          // exit
#include <netinet/in.h>      // sockaddr_in

#include <string>            // string
#include <thread>            // thread

#include "def.hpp"           // SERVER_PORT, CALL_xxx
#include "sourceManager.hpp" // SourceManagerWorker, SourceManagerMaster
#include "job.hpp"           // job_f, context_t
#include "utils.hpp"         // readIPs, receiveString, getWorkingDirectory,
                             // sendFail, sendSuccess, prepareServer

int serverfd = INVALID_SOCKET;

// Function call on SIGINT
// Close on Ctrl + C
void sigintHandler(int signo) {

    P("Server exited.");

    if (serverfd > 0) {
        close(serverfd);
    }

    exit(0);

}

// Function called on terminate
// Close on error
void closefd() {

    P("Server terminated unexpectedly.");

    if (serverfd > 0) {
        close(serverfd);
    }

    exit(EXIT_FAILURE);

}

// Get job function from job library and run the job
bool runJob(const std::string & jobFilePath, ch::context_t & context) {

    void * jobLib = dlopen(jobFilePath.c_str(), RTLD_LAZY);

    if (jobLib == nullptr) {
        E("Cannot find library file.");
        return false;
    } else {
        const ch::job_f * doJob = (ch::job_f *)dlsym(jobLib, "doJob");

        if (doJob == nullptr) {
            E("No doJob function found in the library.");
            I("Please implement doJob function in the library.");
            return false;
        } else {
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

    P("Running as worker.");

    ch::SourceManagerWorker source{sockfd};

    if (source.isValid()) {
        std::string jobFilePath;
        std::string jobName;
        std::string confFilePath;
        std::string workingDir;

        if (source.receiveFiles(confFilePath, jobFilePath, jobName, workingDir)) {
            ipconfig_t ips;

            if (ch::readIPs(confFilePath, ips) && (ips.size() != 0)) {
                // do job
                const std::string outputFilePath;
#ifdef MULTITHREAD_SUPPORT
                ch::context_t context(ips, source, outputFilePath, workingDir, jobName, false, true);
#else
                ch::context_t context(ips, source, outputFilePath, workingDir, jobName, false, false);
#endif
                return runJob(jobFilePath, context);
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

    std::string dataFilePath;
    std::string outputFilePath;
    std::string jobFilePath;
    std::string jobName;

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
    if (!ch::receiveString(sockfd, jobName)) {
        E("Cannot receive job name.");
        return false;
    }

    std::string workingDir;

    ch::getWorkingDirectory(workingDir, jobName);

    PSS("Running as master on " << jobName << ".");

    std::string confFilePath = workingDir + IPCONFIG_FILE;

    ipconfig_t ips;
    if (!ch::readIPs(confFilePath, ips)) {
        E("Cannot read configuration file.");
        return false;
    }

    if (ips.empty()) {
        E("Configuration file contains no IP information.");
        return false;
    }

    ch::SourceManagerMaster source{dataFilePath, jobFilePath};

    if (source.isValid()) {

        if (!source.connectAndDeliver(ips, jobName)) {
            E("Fail to connect to workers.");
            return false;
        }

        source.startDistributionThread();

        // do job
#ifdef MULTITHREAD_SUPPORT
        ch::context_t context(ips, source, outputFilePath, workingDir, jobName, true, true);
#else
        ch::context_t context(ips, source, outputFilePath, workingDir, jobName, true, false);
#endif
        if (!runJob(jobFilePath, context)) {
            ESS("Job " << jobName << " on master failed.");
            return false;
        }

        PSS("Finish job " << jobName << ".");

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

        if (ch::Recv(sockfd, static_cast<void *>(&c), sizeof(char))) {
            P("Job accepted.");
            if (c == CALL_MASTER) {
                if (!asMaster(sockfd)) {
                    ch::sendFail(sockfd);
                } else {
                    ch::sendSuccess(sockfd);
                }
            } else if (c == CALL_WORKER) {
                // Result to be processed by sourceManager
                if (!asWorker(sockfd)) {
                    ch::sendFail(sockfd);
                } else {
                    ch::sendSuccess(sockfd);
                }
            } else if (c == CALL_CANCEL) {
                P("Job canceled by master.");
            } else {
                P("Unsupported call.");
            }
        }

        close(sockfd);
    }

}

int main(int argc, char ** argv) {

    std::set_terminate(closefd);
    signal(SIGINT, sigintHandler);

    sockaddr_in remote;
    socklen_t s_size;

    if (ch::prepareServer(serverfd, SERVER_PORT)) {
        P("Accepting request.");

        while (1) {
            const int sockfd = accept(serverfd, reinterpret_cast<struct sockaddr *>(&remote), &s_size);
            std::thread serve_thread{serve, sockfd};
            serve_thread.detach();
        }
    } else {
        E("Port occupied.");
        I("Close duplicated server.");
    }

    return 0;

}
