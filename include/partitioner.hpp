#ifndef PARTITIONER_H
#define PARTITIONER_H

namespace ch {

    class Partitioner {
        public:
            virtual int getPartition(int i, int s) = 0;
    };

    class HashPartitioner: public Partitioner {
        public:
            int getPartition(int i, int s) {
                if (i == INT_MIN) {
                    return 0;
                }
                return ((i < 0) ? -i : i) % s;
            }
    } hashPartitioner;

    class ZeroPartitioner: public Partitioner {
        public:
            int getPartition(int i, int s) {
                return 0;
            }
    } zeroPartitioner;

}

#endif
