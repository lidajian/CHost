#include "type.hpp"
#include <iostream>

using namespace std;
using namespace ch;

Integer get() {
    return Integer(2);
}

Integer get1(Integer & i) {
    i = 3;
    return i;
}

int main() {
    Integer j(get());
    Integer k(j);
    Integer c(get1(k));
    cout << j.toString();
    cout << k.toString();
    cout << c.toString();
}
