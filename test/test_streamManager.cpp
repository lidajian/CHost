#include "../include/streamManager.hpp"

using namespace std;
using namespace ch;

int main(int argc, char ** argv) {
    ipconfig_t ips;
    ips.push_back(pair<size_t, string>(0, "127.0.0.1"));
    string s(".");
    StreamManager<Tuple<String, Integer> > sm(ips, s, 100);
    String str("Hello from server.");
    Integer i(89);
    Tuple<String, Integer> tp(str, i);
    sm.push(tp, zeroPartitioner);
    sm.finalizeSend();
    sm.blockTillRecvEnd();
    return 0;
}
