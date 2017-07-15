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
            virtual size_t getPartition(const int & i, const size_t & s) const = 0;
    };

    /*
     * HashPartitioner: Remainder method
     */
    class HashPartitioner: public Partitioner {

        public:

            size_t getPartition(const int & i, const size_t & s) const {
                return (size_t)(i) % s;
            }
    } hashPartitioner;

    /*
     * ZeroPartitioner: Map to master anyway
     */
    class ZeroPartitioner: public Partitioner {

        public:

            constexpr size_t getPartition(const int &, const size_t &) const {
                return 0;
            }
    } zeroPartitioner;

}

#endif
