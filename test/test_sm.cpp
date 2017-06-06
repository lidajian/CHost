#include "../include/streamManager.hpp"

using namespace std;
using namespace ch;

int main(int argc, char ** argv) {
    // (distribute files over cluster)
    // read the input file
    // map
    // reduce
    // write the input file
    // (collect outcomes)
    D("Program start." << endl;)
    vector<pair<int, string> > ips;
    ips.push_back(pair<int, string>(0, "127.0.0.1"));
    StreamManager<Tuple<String, Integer> > sm(ips);
    String str("Hello from server.");
    Integer i(89);
    Tuple<String, Integer> tp(str, i);
    sm.push(tp, zeroPartitioner);
    sm.finalizeSend();
    sm.blockTillRecvEnd();
    return 0;
}
