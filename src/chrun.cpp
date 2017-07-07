#include <unistd.h> // getopt, getcwd
#include <string> // string
#include <iostream> // cout
#include <fstream> // ifstream, ofstream
#include <chrono> // system_time
#include "utils.hpp"

// Index IPs in configuration file
bool createTargetConfigurationFile (const std::string & confFilePath, const std::string & targetConfFilePath) {
    std::ifstream cis{confFilePath};
    std::ofstream tcis{targetConfFilePath};
    if (cis.fail()) {
        E("Cannot open configuration file.");
        I("Check existence of the given configuration file.");
        return false;
    }
    if (tcis.fail()) {
        E("Cannot open target configuration file.");
        I("Check if the working directory exists and there are enough space on the disk.");
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

    char * current_dir = getcwd(nullptr, 0);
    std::string current_dir_str{current_dir};
    current_dir_str.push_back('/');
    free(current_dir);

    while ((c = getopt(argc, argv, "c:i:o:j:")) != -1) {
        if (c == 'c') {
            hasC = true;
            confFilePath = optarg;
        } else if (c == 'i') {
            hasI = true;
            if (optarg[0] == '/') {
                dataFilePath = optarg;
            } else {
                dataFilePath = current_dir_str + optarg;
            }
        } else if (c == 'o') {
            hasO = true;
            if (optarg[0] == '/') {
                outputFilePath = optarg;
            } else {
                outputFilePath = current_dir_str + optarg;
            }
        } else if (c == 'j') {
            hasJ = true;
            if (optarg[0] == '/') {
                jobFilePath = optarg;
            } else {
                jobFilePath = current_dir_str + optarg;
            }
        }
    }
    return (hasC && hasI && hasO && hasJ);
}

// Get result from master
void getResult(const int sockfd) {
    char c;
    if (!ch::precv(sockfd, static_cast<void *>(&c), sizeof(char))) {
        P("No response from the server.");
    } else if (c == RES_SUCCESS) {
        P("Job Succeed.");
    } else {
        P("Job Fail.");
    }
}

int main(int argc, char ** argv) {

    int sockfd;

    std::string confFilePath;
    std::string dataFilePath;
    std::string outputFilePath;
    std::string jobFilePath;

    std::string targetConfFilePath;

    if (!parseArgs(argc, argv, confFilePath, dataFilePath, outputFilePath, jobFilePath)) {
        P("Usage: chrun\n -c [configuration file]\n -i [input data]\n -o [output file]\n -j [job file]");
        return 0;
    }

    // Check existence of files
    if (!ch::fileExist(confFilePath.c_str())) {
        E("The configuration file does not exist.");
        I("Please specify a valid configuration file path.");
        return 0;
    }
    if (!ch::fileExist(dataFilePath.c_str())) {
        E("The data file does not exist.");
        I("Please specify a valid data file path.");
        return 0;
    }
    if (ch::fileExist(outputFilePath.c_str())) {
        E("The output file exists.");
        I("The output file should not exist before the job runs.");
        return 0;
    }
    if (!ch::fileExist(jobFilePath.c_str())) {
        E("The job file exists.");
        I("Please specify a valid job file path.");
        return 0;
    }

    // Create configuration file
    std::string workingDir;
    ch::getWorkingDirectory(workingDir);
    targetConfFilePath = workingDir + IPCONFIG_FILE;
    if (!createTargetConfigurationFile(confFilePath, targetConfFilePath)) {
        E("Cannot create configuration file for server.");
        I("1. The configuration file may not be well formated.");
        I("2. The configuration file in working directory may exist, try to clean the working directory.");
        return 0;
    }

    // Connect to server and invoke master
    if (!ch::sconnect(sockfd, LOCALHOST, SERVER_PORT)) {
        E("Cannot connect to server.");
        I("Check if the server is running properly on local machine.");
        return 0;
    }
    if (!ch::invokeMaster(sockfd)) {
        E("Server failed to run as master.");
        I("Server may be busy, try again later.");
        close(sockfd);
        return 0;
    }

    // Send parameters
    if (!ch::sendString(sockfd, dataFilePath)) {
        E("Failed sending data file path.");
        I("There may be an error on server and the server may terminate unexpectedly.");
        close(sockfd);
        return 0;
    }
    if (!ch::sendString(sockfd, outputFilePath)) {
        E("Failed sending output file path.");
        I("There may be an error on server and the server may terminate unexpectedly.");
        close(sockfd);
        return 0;
    }
    if (!ch::sendString(sockfd, jobFilePath)) {
        E("Failed sending job file path.");
        I("There may be an error on server and the server may terminate unexpectedly.");
        close(sockfd);
        return 0;
    }

    // Timing job

    using namespace std::chrono;

    system_clock::time_point start = system_clock::now();

    P("Job started.");

    getResult(sockfd);

    std::cout << "In " << duration_cast<milliseconds>(system_clock::now() - start).count() << " ms.\n";

    close(sockfd);
    return 0;
}
