#include <fcntl.h>
#include <stdlib.h>
#include <string>
#include <iostream>
#include <fstream>
#include <ctime>
#include "utils.hpp"

const char C_SERVER = CALL_SERVER;

inline void printHelp() {
    std::cout << "Usage: chrun\n -c [configuration file]\n -i [input data]\n -o [output file]\n -j [job file]\n";
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
    while (std::getline(cis, ip)) {
        if (ch::isValidIP_v4(ip)) {
            tcis << (i++) << " " << ip << std::endl;
        }
    }
    return true;
}

bool getConfFilePath(int argc, char ** argv, std::string & confFilePath, std::string & dataFilePath, std::string & outputFilePath, std::string & jobFilePath) {
    char c;
    bool hasC = false;
    bool hasI = false;
    bool hasO = false;
    bool hasJ = false;
    while ((c = getopt(argc, argv, "c:i:o:j:")) != -1) {
        if (c == 'c') {
            hasC = true;
            confFilePath = optarg;
        } else if (c == 'i') {
            hasI = true;
            dataFilePath = optarg;
        } else if (c == 'o') {
            hasO = true;
            outputFilePath = optarg;
        } else if (c == 'j') {
            hasJ = true;
            jobFilePath = optarg;
        }
    }
    return (hasC && hasI && hasO && hasJ);
}

void getResult(int sockfd) {
    char c;
    if (ch::precv(sockfd, static_cast<void *>(&c), sizeof(char)) <= 0) {
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
    std::string jobFilePath;
    std::string targetConfFilePath;
    std::string targetJobFilePath;
    std::string cpCmd("cp ");

    if (!getConfFilePath(argc, argv, confFilePath, dataFilePath, outputFilePath, jobFilePath)) {
        printHelp();
        return 0;
    }


    // get full data file path if necessary
    char * current_dir = getcwd(NULL, 0);
    std::string current_dir_str(current_dir);
    current_dir_str.push_back('/');
    free(current_dir);
    if (dataFilePath[0] != '/' && dataFilePath[0] != '~') {
        dataFilePath = current_dir_str + dataFilePath;
    }
    if (outputFilePath[0] != '/' && outputFilePath[0] != '~') {
        outputFilePath = current_dir_str + outputFilePath;
    }

    if (ch::fileExist(outputFilePath.c_str())) {
        L("The output file exists.\n");
        return 0;
    }

    if (ch::sconnect(sockfd, "127.0.0.1", SERVER_PORT) < 0) {
        L("Cannot connect to server.\n");
        return 0;
    }

    std::string workingDir;
    ch::getWorkingDirectory(workingDir);

    targetConfFilePath = workingDir + IPCONFIG_FILE;
    targetJobFilePath = workingDir + JOB_FILE;

    if (!createTargetConfigurationFile(confFilePath, targetConfFilePath)) {
        L("Cannot create configuration file\n");
        return 0;
    }

    cpCmd += jobFilePath;
    cpCmd.push_back(' ');
    cpCmd += targetJobFilePath;

    system(cpCmd.c_str());

    ch::psend(sockfd, static_cast<const void *>(&C_SERVER), sizeof(char));

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
