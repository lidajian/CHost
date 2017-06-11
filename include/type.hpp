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
            virtual bool send(int fd) = 0;
            virtual bool recv(int fd) = 0;
            virtual bool read(int fd) = 0;
            virtual bool write(int fd) = 0;
    };

    class Integer: public Base {
        public:
            int value;
            Integer(int v = 0): value(v) {}
            Integer(const Integer & i) {value = i.value;}
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
            bool send(int fd) {
                return ssend(fd, static_cast<const void *>(&value), sizeof(int)) == sizeof(int);
            }
            bool recv(int fd) {
                return srecv(fd, static_cast<void *>(&value), sizeof(int)) == sizeof(int);
            }
            bool read(int fd) {
                return sread(fd, static_cast<void *>(&value), sizeof(int)) == sizeof(int);
            }
            bool write(int fd) {
                return swrite(fd, static_cast<const void *>(&value), sizeof(int)) == sizeof(int);
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
            Long(const Long & i) {value = i.value;}
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
            bool send(int fd) {
                return ssend(fd, static_cast<const void *>(&value), sizeof(long)) == sizeof(long);
            }
            bool recv(int fd) {
                return srecv(fd, static_cast<void *>(&value), sizeof(long)) == sizeof(long);
            }
            bool read(int fd) {
                return sread(fd, static_cast<void *>(&value), sizeof(long)) == sizeof(long);
            }
            bool write(int fd) {
                return swrite(fd, static_cast<const void *>(&value), sizeof(long)) == sizeof(long);
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
            String(const String & str) {
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
            bool send(int fd) {
                return sendString(fd, value);
            }
            bool recv(int fd) {
                return receiveString(fd, value);
            }
            bool read(int fd) {
                return readString(fd, value);
            }
            bool write(int fd) {
                return writeString(fd, value);
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
            T1 first;
            T2 second;
            Tuple() {}
            Tuple(T1 & v1, T2 & v2): first(v1), second(v2) {}
            Tuple(const Tuple<T1, T2> & t): first(t.first), second(t.second) {}
            ~Tuple() {}
            int hashCode(void) {
                return first.hashCode();
            }
            std::string toString() {
                return "(" + first.toString() + ", " + second.toString() + ")";
            }
            inline static id_t getId(void) {
                return (T1::getId() << 4) | T2::getId();
            }
            bool send(int fd) {
                id_t id = getId();
                if (ssend(fd, static_cast<const void *>(&id), sizeof(id_t)) == sizeof(id_t)) {
                    return first.send(fd) && second.send(fd);
                }
                return false;
            }
            bool recv(int fd) {
                id_t id = ID_INVALID;
                if (srecv(fd, static_cast<void *>(&id), sizeof(id_t)) == sizeof(id_t)) {
                    if (id == getId()) {
                        return first.recv(fd) && second.recv(fd);
                    }
                }
                return false;
            }
            bool read(int fd) {
                return first.read(fd) && second.read(fd);
            }
            bool write(int fd) {
                return first.write(fd) && second.write(fd);
            }
            bool operator == (const Tuple & b) const {
                return (first == b.first);
            }
            bool operator != (const Tuple & b) const {
                return (first != b.first);
            }
            bool operator < (const Tuple & b) const {
                return (first < b.first);
            }
            bool operator > (const Tuple & b) const {
                return (first > b.first);
            }
            bool operator <= (const Tuple & b) const {
                return (first <= b.first);
            }
            bool operator >= (const Tuple & b) const {
                return (first >= b.first);
            }
            Tuple & operator = (const Tuple & t) {
                first = t.first;
                second = t.second;
                return (*this);
            }
            Tuple & operator += (const Tuple & t) {
                second += t.second;
                return (*this);
            }

    };
}

#endif
