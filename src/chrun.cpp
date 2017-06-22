#include <unistd.h> // getopt, getcwd
#include <string> // string
#include <iostream> // cout
#include <fstream> // ifstream, ofstream
#include <ctime> // time, difftime
#include "utils.hpp"

// Index IPs in configuration file
bool createTargetConfigurationFile (const std::string & confFilePath, const std::string & targetConfFilePath) {
    std::ifstream cis(confFilePath);
    std::ofstream tcis(targetConfFilePath);
    if (cis.fail()) {
        E("Cannot open configuration file.\n");
        I("Check existence of the given configuration file.\n");
        return false;
    }
    if (tcis.fail()) {
        E("Cannot open target configuration file.\n");
        I("Check if the working directory exists and there are enough space on the disk.\n");
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

    char * current_dir = getcwd(NULL, 0);
    std::string current_dir_str(current_dir);
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
        P("No response from the server.\n");
    } else if (c == RES_SUCCESS) {
        P("Job Succeed.\n");
    } else {
        P("Job Fail.\n");
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
        P("Usage: chrun\n -c [configuration file]\n -i [input data]\n -o [output file]\n -j [job file]\n");
        return 0;
    }

    // Check existence of output file
    if (!ch::fileExist(dataFilePath.c_str())) {
        E("The data file does not exist.\n");
        I("Please specify a valid data file.\n");
        return 0;
    }
    if (ch::fileExist(outputFilePath.c_str())) {
        E("The output file exists.\n");
        I("The output file should not exist before the job runs.\n");
        return 0;
    }
    if (!ch::fileExist(jobFilePath.c_str())) {
        E("The job file exists.\n");
        I("Please specify a valid job file.\n");
        return 0;
    }

    // Create configuration file
    std::string workingDir;
    ch::getWorkingDirectory(workingDir);
    targetConfFilePath = workingDir + IPCONFIG_FILE;
    if (!createTargetConfigurationFile(confFilePath, targetConfFilePath)) {
        E("Cannot create configuration file for server.\n");
        I("The working directory may exist, try to clean the working directory.\n");
        return 0;
    }

    // Connect to server and invoke master
    if (!ch::sconnect(sockfd, LOCALHOST, SERVER_PORT)) {
        E("Cannot connect to server.\n");
        I("Check if the server is running properly on local machine.\n");
        return 0;
    }
    if (!ch::invokeMaster(sockfd)) {
        E("Server failed to run as master.\n");
        I("Server may be busy, try again later.\n");
        close(sockfd);
        return 0;
    }

    // Send parameters
    if (!ch::sendString(sockfd, dataFilePath)) {
        E("Failed sending data file path.\n");
        I("There may be an error on server and the server may terminate unexpectedly.\n");
        close(sockfd);
        return 0;
    }
    if (!ch::sendString(sockfd, outputFilePath)) {
        E("Failed sending output file path.\n");
        I("There may be an error on server and the server may terminate unexpectedly.\n");
        close(sockfd);
        return 0;
    }
    if (!ch::sendString(sockfd, jobFilePath)) {
        E("Failed sending job file path.\n");
        I("There may be an error on server and the server may terminate unexpectedly.\n");
        close(sockfd);
        return 0;
    }

    // Timing job
    time_t startTime, endTime;

    time(&startTime);

    P("Job started.\n");

    getResult(sockfd);

    time(&endTime);

    std::cout << "In " << difftime(endTime, startTime) << " seconds.\n";

    close(sockfd);
    return 0;
}
