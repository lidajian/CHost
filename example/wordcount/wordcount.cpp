#include <string>
#include <sstream>
#include "job.hpp"
#include "streamManager.hpp"
#include "sourceManager.hpp"
#include "sortedStream.hpp"

void mapFun(std::string & block, ch::StreamManager<ch::Tuple<ch::String, ch::Integer> > & sm) {
    std::stringstream ss(block);
    ch::Tuple<ch::String, ch::Integer> res;
    (res.second).value = 1;
    while (ss >> ((res.first).value)) {
        sm.push(res, ch::hashPartitioner);
    }
}

void reduceFun(ch::SortedStream<ch::Tuple<ch::String, ch::Integer> > & ss, ch::StreamManager<ch::Tuple<ch::String, ch::Integer> > & sm) {
    bool started = false;
    ch::Tuple<ch::String, ch::Integer> res;
    ch::Tuple<ch::String, ch::Integer> e;
    while (ss.get(e)) {
        if (started == false) {
            res = e;
            started = true;
        } else {
            if (res == e) {
                res += e;
            } else {
                D("Emit: " << res.toString() << std::endl);
                sm.push(res, ch::zeroPartitioner);
                res = e;
            }
        }
    }
    if (started) {
        D("Emit: " << res.toString() << std::endl);
        sm.push(res, ch::zeroPartitioner);
    }
}

extern "C" bool doJob(ch::context_t & context) {
    std::string polled;

    ch::StreamManager<ch::Tuple<ch::String, ch::Integer> > stm(context._ips, context._workingDir, DEFAULT_MAX_DATA_SIZE);

    if (stm.isReceiving()) {

        while (context._source.poll(polled)) {
            mapFun(polled, stm);
        }

        stm.stopSend();
        stm.blockTillRecvEnd();

        ch::SortedStream<ch::Tuple<ch::String, ch::Integer> > * sorted = stm.getSortedStream();

        if (sorted) {

            stm.setPresort(false);

            stm.startRecvThreads();
            reduceFun(*sorted, stm);

            delete sorted;

            stm.finalizeSend();
            stm.blockTillRecvEnd();

            if (context._isServer) {
                return stm.pourToTextFile(context._outputFilePath.c_str());
            }

            return true;

        } else {
            L("Job: Cannot get sorted stream.\n");
        }

    } else {
        L("Job: StreamManager failed. Nothing done.\n");
    }
    return false;
}
