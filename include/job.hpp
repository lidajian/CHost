#ifndef JOB_H
#define JOB_H

#include <string>
#include <vector>
#include "sourceManager.hpp"

typedef void job_f(std::vector<std::pair<int, std::string> > &, ch::SourceManager &, std::string &, bool);

#endif
