/*
 * Header for all jobs
 */

#ifndef JOB_H
#define JOB_H

#include <string> // string
#include <vector> // vector

#include "def.hpp" // ipconfig_t
#include "sourceManager.hpp" // SourceManager

namespace ch {
    struct context_t {
        const ipconfig_t & _ips;
        ch::SourceManager & _source;
        const std::string & _outputFilePath;
        const std::string & _workingDir;
        const bool _isServer;

        explicit context_t(const ipconfig_t & ips,
                           ch::SourceManager & source,
                           const std::string & outputFilePath,
                           const std::string & workingDir,
                           const bool isServer = false): _ips(ips), _source(source), _outputFilePath(outputFilePath), _workingDir(workingDir), _isServer(isServer) {}
    };

    typedef bool job_f(context_t & context);
}

#endif
