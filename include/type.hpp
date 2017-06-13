#ifndef TYPE_HPP
#define TYPE_HPP

#include <string>
#include <iostream>
#include <fstream>
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
            virtual std::ifstream & read(std::ifstream & is) = 0;
            virtual std::ofstream & write(std::ofstream & os) const = 0;

            friend std::ofstream & operator << (std::ofstream & os, const Base & v);
            friend std::ifstream & operator >> (std::ifstream & is, const Base & v);
    };

    std::ofstream & operator << (std::ofstream & os, const Base & v) {
        return v.write(os);
    }
    std::ifstream & operator >> (std::ifstream & is, Base & v) {
        return v.read(is);
    }

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
            std::ifstream & read(std::ifstream & is) {
                is.read(reinterpret_cast<char *>(&value), sizeof(int));
                return is;
            }
            std::ofstream & write(std::ofstream & os) const {
                os.write(reinterpret_cast<const char *>(&value), sizeof(int));
                return os;
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
            std::ifstream & read(std::ifstream & is) {
                size_t l = 0;
                if (is.read(reinterpret_cast<char *>(&l), sizeof(size_t))) {
                    value.resize(l);
                    is.read(&value[0], l);
                }
                return is;
            }
            std::ofstream & write(std::ofstream & os) const {
                size_t l = value.size();
                if (os.write(reinterpret_cast<const char *>(&l), sizeof(size_t))) {
                    os.write(value.data(), l);
                }
                return os;
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
                return first.send(fd) && second.send(fd);
            }
            bool recv(int fd) {
                return first.recv(fd) && second.recv(fd);
            }
            std::ifstream & read(std::ifstream & is) {
                return is >> first >> second;
            }
            std::ofstream & write(std::ofstream & os) const {
                return os << first << second;
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
