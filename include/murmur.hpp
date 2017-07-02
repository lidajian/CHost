/*
 * Murmur hashing
 */
#ifndef MURMUR_H
#define MURMUR_H

#include <string>

namespace ch {

    template <typename T>
    int murmur2(const T & v) {
        const unsigned int seed = 0x82b19283;
        const unsigned int m = 0x5bd1e995;
        const int r = 24;
        int len = sizeof(T);
        unsigned int h = seed ^ len;
        const unsigned char * data = reinterpret_cast<const unsigned char *>(&v);

        while (len >= 4) {
            unsigned int k = *(unsigned int *)data;

            k *= m;
            k ^= k >> r;
            k *= m;

            h *= m;
            h ^= k;

            data += 4;
            len -= 4;
        }

        switch (len) {
            case 3:
                h ^= data[2] << 16;
                // Fall through
            case 2:
                h ^= data[1] << 8;
                // Fall through
            case 1:
                h ^= data[0];
                h *= m;
        }
        h ^= h >> 13;
        h *= m;
        h ^= h >> 15;

        return h;

    }

    template <>
    int murmur2(const std::string & v) {
        const unsigned int seed = 0x82b19283;
        const unsigned int m = 0x5bd1e995;
        const int r = 24;
        int len = v.size();
        unsigned int h = seed ^ len;
        const char * data = v.data();

        while (len >= 4) {
            unsigned int k = *reinterpret_cast<const unsigned int *>(data);

            k *= m;
            k ^= k >> r;
            k *= m;

            h *= m;
            h ^= k;

            data += 4;
            len -= 4;
        }

        switch (len) {
            case 3:
                h ^= data[2] << 16;
                // Fall through
            case 2:
                h ^= data[1] << 8;
                // Fall through
            case 1:
                h ^= data[0];
                h *= m;
        }
        h ^= h >> 13;
        h *= m;
        h ^= h >> 15;

        return h;

    }
}

#endif
