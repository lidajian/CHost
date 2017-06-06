#ifndef TYPE_HPP
#define TYPE_HPP

#include <string>
#include <iostream>
#include <assert.h>
#include "utils.hpp"

namespace ch {
    const int PRIME = 31;

    typedef unsigned char id_t;

    class Base {
        public:
            virtual ~Base() {}
            inline static id_t getId(void) {
                return ID_INVALID;
            }
            virtual int hashCode(void) = 0;
            virtual std::string toString() = 0;
            virtual ssize_t send(int fd) = 0;
            virtual ssize_t recv(int fd) = 0;
    };

    class Integer: public Base {
        public:
            int value;
            Integer(int v = 0): value(v) {}
            Integer(Integer & i) {value = i.value;}
            ~Integer() {}
            int hashCode(void) {
                return value;
            }
            std::string toString(void) {
                return std::to_string(value);
            }
            inline static id_t getId(void) {
                return ID_INTEGER;
            }
            ssize_t send(int fd) {
                return ssend(fd, static_cast<const void *>(&value), sizeof(int));
            }
            ssize_t recv(int fd) {
                return srecv(fd, static_cast<void *>(&value), sizeof(int));
            }
            bool operator == (const Integer & b) const {
                return (value == b.value);
            }
            bool operator != (const Integer & b) const {
                return (value != b.value);
            }
            bool operator < (const Integer & b) const {
                return (value < b.value);
            }
            bool operator > (const Integer & b) const {
                return (value > b.value);
            }
            bool operator <= (const Integer & b) const {
                return (value <= b.value);
            }
            bool operator >= (const Integer & b) const {
                return (value >= b.value);
            }
            Integer & operator = (const Integer & i) {
                value = i.value;
                return (*this);
            }
            Integer & operator += (const Integer & i) {
                value += i.value;
                return (*this);
            }
    };

    class Long: public Base {
        public:
            long value;
            Long(long v = 0): value(v) {}
            Long(Long & i) {value = i.value;}
            ~Long() {}
            int hashCode(void) {
                return (int)value;
            }
            std::string toString(void) {
                return std::to_string(value);
            }
            inline static id_t getId(void) {
                return ID_LONG;
            }
            ssize_t send(int fd) {
                return ssend(fd, static_cast<const void *>(&value), sizeof(long));
            }
            ssize_t recv(int fd) {
                return srecv(fd, static_cast<void *>(&value), sizeof(long));
            }
            bool operator == (const Long & b) const {
                return (value == b.value);
            }
            bool operator != (const Long & b) const {
                return (value != b.value);
            }
            bool operator < (const Long & b) const {
                return (value < b.value);
            }
            bool operator > (const Long & b) const {
                return (value > b.value);
            }
            bool operator <= (const Long & b) const {
                return (value <= b.value);
            }
            bool operator >= (const Long & b) const {
                return (value >= b.value);
            }
            Long & operator = (const Long & i) {
                value = i.value;
                return (*this);
            }
            Long & operator += (const Long & i) {
                value += i.value;
                return (*this);
            }
    };

    class String: public Base {
        private:
            int hash;
        public:
            std::string value;
            String(): hash(INVALID) {}
            String(String & str) {
                hash = INVALID;
                value = str.value;
            }
            String(std::string & str): hash(INVALID), value(str) {}
            String(std::string str): hash(INVALID), value(str) {}
            ~String() {}
            int hashCode(void) {
                int h = hash;
                if (h == INVALID) {
                    h = 0;
                    for (char c: value) {
                        h = h * PRIME + c;
                    }
                    hash = h;
                }
                return h;
            }
            std::string toString(void) {
                return "\"" + value + "\"";
            }
            inline static id_t getId(void) {
                return ID_STRING;
            }
            ssize_t send(int fd) {
                size_t len = value.size();
                ssize_t rv = ssend(fd, static_cast<const void *>(&len), sizeof(size_t));
                rv += ssend(fd, static_cast<const void *>(value.data()), value.size());
                return rv;
            }
            ssize_t recv(int fd) {
                if (receiveString(fd, value)) {
                    return 1;
                } else {
                    return INVALID;
                }
            }
            void reset(void) {
                hash = INVALID;
                value.clear();
            }
            bool operator == (const String & b) const {
                return (value.compare(b.value) == 0);
            }
            bool operator != (const String & b) const {
                return (value.compare(b.value) != 0);
            }
            bool operator < (const String & b) const {
                return (value.compare(b.value) < 0);
            }
            bool operator > (const String & b) const {
                return (value.compare(b.value) > 0);
            }
            bool operator <= (const String & b) const {
                return (value.compare(b.value) <= 0);
            }
            bool operator >= (const String & b) const {
                return (value.compare(b.value) >= 0);
            }
            String & operator = (const String & str) {
                value = str.value;
                return (*this);
            }
            String & operator += (const String & str) {
                value += str.value;
                return (*this);
            }
    };

    template <class T1, class T2>
    class Tuple: public Base {
        public:
            T1 * first;
            T2 * second;
            Tuple() {
                first = new T1();
                second = new T2();
            }
            Tuple(T1 & v1, T2 & v2) {
                first = new T1(v1);
                second = new T2(v2);
            }
            Tuple(Tuple & t) {
                first = new T1(*(t.first));
                second = new T2(*(t.second));
            }
            ~Tuple() {
                delete first;
                delete second;
            }
            int hashCode(void) {
                return first->hashCode();
            }
            std::string toString() {
                return "(" + first->toString() + ", " + second->toString() + ")";
            }
            inline static id_t getId(void) {
                return (T1::getId() << 4) | T2::getId();
            }
            ssize_t send(int fd) {
                id_t id = getId();
                return ssend(fd, static_cast<const void *>(&id), sizeof(id_t)) + first->send(fd) + second->send(fd);
            }
            ssize_t recv(int fd) {
                id_t id = ID_INVALID;
                srecv(fd, static_cast<void *>(&id), sizeof(id_t));
                if (id == ID_INVALID) {
                    return INVALID;
                }
                ssize_t rv1 = first->recv(fd);
                if (rv1 < 0) {
                    return INVALID;
                }
                ssize_t rv2 = second->recv(fd);
                if (rv2 < 0) {
                    return INVALID;
                }
                return rv1 + rv2;
            }
            bool operator == (const Tuple & b) const {
                return ((*first) == (*(b.first)));
            }
            bool operator != (const Tuple & b) const {
                return ((*first) != (*(b.first)));
            }
            bool operator < (const Tuple & b) const {
                return ((*first) < (*(b.first)));
            }
            bool operator > (const Tuple & b) const {
                return ((*first) > (*(b.first)));
            }
            bool operator <= (const Tuple & b) const {
                return ((*first) <= (*(b.first)));
            }
            bool operator >= (const Tuple & b) const {
                return ((*first) >= (*(b.first)));
            }
            Tuple & operator = (const Tuple & t) {
                (*first) = (*(t.first));
                (*second) = (*(t.second));
                return (*this);
            }
            Tuple & operator += (const Tuple & t) {
                (*second) += (*(t.second));
                return (*this);
            }

    };
}

#endif
