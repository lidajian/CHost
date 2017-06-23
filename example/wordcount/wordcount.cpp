#include <string>
#include <sstream>
#include "job.hpp"

namespace ch {

    void mapper(std::string & block, ch::StreamManager<ch::Tuple<ch::String, ch::Integer> > & sm) {
        std::stringstream ss(block);
        ch::Tuple<ch::String, ch::Integer> res;
        (res.second).value = 1;
        while (ss >> ((res.first).value)) {
            sm.push(res, ch::hashPartitioner);
        }
    }

    void reducer(ch::SortedStream<ch::Tuple<ch::String, ch::Integer> > & ss, ch::StreamManager<ch::Tuple<ch::String, ch::Integer> > & sm) {
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
                    DSS("Emit: " << res.toString());
                    sm.push(res, ch::zeroPartitioner);
                    res = e;
                }
            }
        }
        if (started) {
            DSS("Emit: " << res.toString());
            sm.push(res, ch::zeroPartitioner);
        }
    }

}

extern "C" bool doJob(ch::context_t & context) {
    return ch::simpleJob<ch::Tuple<ch::String, ch::Integer> >(context);
}
