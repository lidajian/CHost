#include <fcntl.h>
#include <string>
#include <iostream>
#include <fstream>
#include <ctime>
#include "utils.hpp"

const char C_SERVER = CALL_SERVER;

inline void printHelp() {
    std::cout << "Usage: cHadoopStarter -c [configuration file] -i [input data] -o [output path]\n";
}

bool createTargetConfigurationFile (std::string & confFilePath, std::string & targetConfFilePath) {
    std::ifstream cis(confFilePath.c_str());
    std::ofstream tcis(targetConfFilePath.c_str());
    if (cis.fail()) {
        L("Cannot open configuration file.\n");
        return false;
    }
    if (tcis.fail()) {
        L("Cannot open target configuration file.\n");
        return false;
    }

    std::string ip;
    int i = 0;
    while (cis >> ip) {
        tcis << (i++) << " " << ip << std::endl;
    }
    return true;
}

bool getConfFilePath(int argc, char ** argv, std::string & confFilePath, std::string & dataFilePath, std::string & outputFilePath) {
    char c;
    bool hasC = false;
    bool hasI = false;
    bool hasO = false;
    while ((c = getopt(argc, argv, "c:i:o:")) != -1) {
        if (c == 'c') {
            hasC = true;
            confFilePath = optarg;
        } else if (c == 'i') {
            hasI = true;
            dataFilePath = optarg;
        } else if (c == 'o') {
            hasO = true;
            outputFilePath = optarg;
        }
    }
    return (hasC && hasI && hasO);
}

void getResult(int sockfd) {
    char c;
    if (ch::srecv(sockfd, static_cast<void *>(&c), sizeof(char)) <= 0) {
        L("No response from the server.\n");
    } else if (c == RES_SUCCESS) {
        L("Succeed.\n");
    } else {
        L("Fail.\n");
    }
}

int main(int argc, char ** argv) {

    int sockfd;
    time_t startTime, endTime;
    double seconds;

    std::string confFilePath;
    std::string dataFilePath;
    std::string outputFilePath;
    std::string targetConfFilePath;

    if (!getConfFilePath(argc, argv, confFilePath, dataFilePath, outputFilePath)) {
        printHelp();
        return 0;
    }

    if (ch::sconnect(sockfd, "127.0.0.1", SERVER_PORT) < 0) {
        L("Cannot connect to server.\n");
        return 0;
    }

    std::string workingDir;
    ch::getWorkingDirectory(workingDir);

    targetConfFilePath = workingDir + IPCONFIG_FILE;

    if (!createTargetConfigurationFile(confFilePath, targetConfFilePath)) {
        L("Cannot create configuration file\n");
        return 0;
    }

    ch::ssend(sockfd, static_cast<const void *>(&C_SERVER), sizeof(char));

    if (!ch::sendString(sockfd, dataFilePath)) {
        L("Failed sending data file path.\n");
    }

    if (!ch::sendString(sockfd, outputFilePath)) {
        L("Failed sending output file path.\n");
    }

    time(&startTime);

    L("Started.\n");

    getResult(sockfd);

    time(&endTime);

    seconds = difftime(endTime, startTime);

    L("In " << seconds << " seconds.\n");

    close(sockfd);
    return 0;
}
