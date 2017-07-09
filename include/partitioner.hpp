/*
 * Partitioner: Guides the StreamManager where the data to send to
 *
 */

#ifndef PARTITIONER_H
#define PARTITIONER_H

#include <climits> // INT_MIN
#include <cstddef> // size_t

namespace ch {

    class Partitioner {

        public:

            // Get the partition
            virtual size_t getPartition(int i, int s) const = 0;
    };

    /*
     * HashPartitioner: Remainder method
     */
    class HashPartitioner: public Partitioner {

        public:

            size_t getPartition(int i, int s) const {
                if (i == INT_MIN) {
                    return 0;
                }
                return ((i < 0) ? -i : i) % s;
            }
    } hashPartitioner;

    /*
     * ZeroPartitioner: Map to master anyway
     */
    class ZeroPartitioner: public Partitioner {

        public:

            size_t getPartition(int i, int s) const {
                return 0;
            }
    } zeroPartitioner;

}

#endif
