#include "threadPool.hpp"
#include <cstdio>

using namespace ch;

void f(const char * str) {
    for (int i = 0; i < 50; i++) puts(str);
}

int main() {
    ThreadPool pool(5u);
    pool.addTask(f, "1"); // normal function
    pool.addTask([](const char * str){
            for (int i = 0; i < 50; i++) puts(str);
            }, "2");
    pool.addTask(f, "3");
    pool.addTask(f, "4");
    pool.addTask(f, "5");
    pool.addTask(f, "6");
    pool.addTask(f, "7");
}
