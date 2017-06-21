#include <unistd.h> // getopt, getcwd
#include <string> // string
#include <iostream> // cout
#include <fstream> // ifstream, ofstream
#include <ctime> // time, difftime
#include "utils.hpp"

inline void printHelp() {
    std::cout << "Usage: chrun\n -c [configuration file]\n -i [input data]\n -o [output file]\n -j [job file]\n";
}

// Index IPs in configuration file
bool createTargetConfigurationFile (const std::string & confFilePath, const std::string & targetConfFilePath) {
    std::ifstream cis(confFilePath);
    std::ofstream tcis(targetConfFilePath);
    if (cis.fail()) {
        L("chrun: Cannot open configuration file.\n");
        return false;
    }
    if (tcis.fail()) {
        L("chrun: Cannot open target configuration file.\n");
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

// Parse arguments
bool parseArgs(const int argc, char * const* argv, std::string & confFilePath, std::string & dataFilePath, std::string & outputFilePath, std::string & jobFilePath) {
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

// Get result from master
void getResult(const int sockfd) {
    char c;
    if (!ch::precv(sockfd, static_cast<void *>(&c), sizeof(char))) {
        L("chrun: No response from the server.\n");
    } else if (c == RES_SUCCESS) {
        L("chrun: Succeed.\n");
    } else {
        L("chrun: Fail.\n");
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

    if (!parseArgs(argc, argv, confFilePath, dataFilePath, outputFilePath, jobFilePath)) {
        printHelp();
        return 0;
    }


    // get full data file path if necessary
    char * current_dir = getcwd(NULL, 0);
    std::string current_dir_str(current_dir);
    current_dir_str.push_back('/');
    free(current_dir);
    if (dataFilePath[0] != '/') {
        dataFilePath = current_dir_str + dataFilePath;
    }
    if (outputFilePath[0] != '/') {
        outputFilePath = current_dir_str + outputFilePath;
    }

    if (ch::fileExist(outputFilePath.c_str())) {
        L("chrun: The output file exists.\n");
        return 0;
    }

    if (!ch::sconnect(sockfd, "127.0.0.1", SERVER_PORT)) {
        L("chrun: Cannot connect to server.\n");
        return 0;
    }

    std::string workingDir;
    ch::getWorkingDirectory(workingDir);

    targetConfFilePath = workingDir + IPCONFIG_FILE;

    if (!createTargetConfigurationFile(confFilePath, targetConfFilePath)) {
        L("chrun: Cannot create configuration file\n");
        return 0;
    }

    if (!ch::invokeMaster(sockfd)) {
        return 0;
    }

    if (!ch::sendString(sockfd, dataFilePath)) {
        L("chrun: Failed sending data file path.\n");
        return 0;
    }

    if (!ch::sendString(sockfd, outputFilePath)) {
        L("chrun: Failed sending output file path.\n");
        return 0;
    }

    if (!ch::sendString(sockfd, jobFilePath)) {
        L("chrun: Failed sending job file path.\n");
        return 0;
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
