#ifndef JOB_H
#define JOB_H

#include <string>
#include <vector>
#include "sourceManager.hpp"

namespace ch {
    struct context_t {
        std::vector<std::pair<int, std::string> > & _ips;
        ch::SourceManager & _source;
        std::string & _outputFilePath;
        std::string & _workingDir;
        bool _isServer;

        explicit context_t(std::vector<std::pair<int, std::string> > & ips,
                           ch::SourceManager & source,
                           std::string & outputFilePath,
                           std::string & workingDir,
                           bool isServer = false): _ips(ips), _source(source), _outputFilePath(outputFilePath), _workingDir(workingDir), _isServer(isServer) {}
    };

    typedef void job_f(context_t & context);
}

#endif
