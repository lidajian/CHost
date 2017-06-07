#include <string>
#include <sstream>
#include "streamManager.hpp"
#include "sourceManager.hpp"

void mapFun(std::string & block, ch::StreamManager<ch::Tuple<ch::String, ch::Integer> > & sm) {
    std::stringstream ss(block);
    ch::Tuple<ch::String, ch::Integer> res;
    (res.second)->value = 1;
    while (ss >> ((res.first)->value)) {
        sm.push(res, ch::hashPartitioner);
        res.first->reset();
    }
}

void reduceFun(std::vector<ch::Tuple<ch::String, ch::Integer> *> & sorted, ch::StreamManager<ch::Tuple<ch::String, ch::Integer> > & sm) {
    if (sorted.size() == 0) {
        return;
    }
    bool started = false;
    ch::Tuple<ch::String, ch::Integer> res;
    for (ch::Tuple<ch::String, ch::Integer> * e: sorted) {
        if (started == false) {
            res = (*e);
            started = true;
        } else {
            if (res == (*e)) {
                res += (*e);
            } else {
                D("Emit: " << res.toString() << std::endl);
                sm.push(res, ch::zeroPartitioner);
                res = (*e);
            }
        }
        delete e;
    }
    if (started) {
        D("Emit: " << res.toString() << std::endl);
        sm.push(res, ch::zeroPartitioner);
    }
}

extern "C" void doJob(std::vector<std::pair<int, std::string> > & ips, ch::SourceManager & source, std::string & outputFilePath, bool isServer) {
    std::string polled;

    ch::StreamManager<ch::Tuple<ch::String, ch::Integer> > stm(ips);

    while (source.poll(polled)) {
        mapFun(polled, stm);
    }

    stm.stopSend();
    stm.blockTillRecvEnd();

    std::vector<ch::Tuple<ch::String, ch::Integer> *> sorted;
    stm.getSorted(sorted);

    stm.startRecvThreads();
    reduceFun(sorted, stm);

    stm.finalizeSend();
    stm.blockTillRecvEnd();

    if (isServer) {
        stm.pourToTextFile(outputFilePath.c_str());
    }
}
