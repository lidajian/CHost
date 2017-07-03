#include <string>
#include <sstream>
#include "job.hpp"

namespace ch {

    void mapper(std::string & block, StreamManager<Tuple<String, Integer> > & sm) {
        std::stringstream ss(block);
        Tuple<String, Integer> res;
        (res.second).value = 1;
        while (ss >> ((res.first).value)) {
            sm.push(res);
        }
    }

    void reducer(SortedStream<Tuple<String, Integer> > & ss, StreamManager<Tuple<String, Integer> > & sm) {
        bool started = false;
        Tuple<String, Integer> res;
        Tuple<String, Integer> e;
        while (ss.get(e)) {
            if (started == false) {
                res = e;
                started = true;
            } else {
                if (res == e) {
                    res += e;
                } else {
                    DSS("Emit: " << res.toString());
                    sm.push(res);
                    res = e;
                }
            }
        }
        if (started) {
            DSS("Emit: " << res.toString());
            sm.push(res);
        }
    }

}

extern "C" bool doJob(ch::context_t & context) {
    return ch::simpleJob<ch::Tuple<ch::String, ch::Integer> >(context);
}
