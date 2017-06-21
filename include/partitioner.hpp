/*
 * Partitioner: Guides the StreamManager where the data to send to
 *
 * 1. HashPartitioner
 *     Remainder method
 *
 * 2. ZeroPartitioner
 *     Return 0 anyway
 */

#ifndef PARTITIONER_H
#define PARTITIONER_H

#include <climits> // INT_MIN

namespace ch {

    class Partitioner {
        public:
            // Get the partition
            virtual size_t getPartition(int i, int s) const = 0;
    };

    class HashPartitioner: public Partitioner {
        public:
            size_t getPartition(int i, int s) const {
                if (i == INT_MIN) {
                    return 0;
                }
                return ((i < 0) ? -i : i) % s;
            }
    } hashPartitioner;

    class ZeroPartitioner: public Partitioner {
        public:
            size_t getPartition(int i, int s) const {
                return 0;
            }
    } zeroPartitioner;

}

#endif
