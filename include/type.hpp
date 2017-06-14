#ifndef TYPE_HPP
#define TYPE_HPP

#include <assert.h>

#include <string> // string
#include <iostream> // ostream
#include <fstream> // ifstream, ofstream

#include "utils.hpp"

namespace ch {
    const int PRIME = 31;

    typedef unsigned char id_t;

    class TypeBase {
        public:
            virtual ~TypeBase() {}
            inline static id_t getId(void) {
                return ID_INVALID;
            }
            virtual int hashCode(void) = 0;
            virtual std::string toString() const = 0;
            virtual bool send(int fd) const = 0;
            virtual bool recv(int fd) = 0;
            virtual std::ifstream & read(std::ifstream & is) = 0;
            virtual std::ofstream & write(std::ofstream & os) const = 0;

            friend std::ofstream & operator << (std::ofstream & os, const TypeBase & v);
            friend std::ifstream & operator >> (std::ifstream & is, const TypeBase & v);
            friend std::ostream & operator << (std::ostream & os, const TypeBase & v);
    };

    std::ofstream & operator << (std::ofstream & os, const TypeBase & v) {
        return v.write(os);
    }
    std::ifstream & operator >> (std::ifstream & is, TypeBase & v) {
        return v.read(is);
    }
    std::ostream & operator << (std::ostream & os, const TypeBase & v) {
        return os << v.toString();
    }

    class Integer: public TypeBase {
        public:
            int value;
            Integer(int v = 0): value(v) {}
            Integer(const Integer & i) {value = i.value;}
            ~Integer() {}
            int hashCode(void) {
                return value;
            }
            std::string toString(void) const {
                return std::to_string(value);
            }
            inline static id_t getId(void) {
                return ID_INTEGER;
            }
            bool send(int fd) const {
                return psend(fd, static_cast<const void *>(&value), sizeof(int)) == sizeof(int);
            }
            bool recv(int fd) {
                return precv(fd, static_cast<void *>(&value), sizeof(int)) == sizeof(int);
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

    class String: public TypeBase {
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
            std::string toString(void) const {
                return "\"" + value + "\"";
            }
            inline static id_t getId(void) {
                return ID_STRING;
            }
            bool send(int fd) const {
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

    template <class DataType_1, class DataType_2>
    class Tuple: public TypeBase {
        public:
            DataType_1 first;
            DataType_2 second;
            Tuple() {}
            Tuple(DataType_1 & v1, DataType_2 & v2): first(v1), second(v2) {}
            Tuple(const Tuple<DataType_1, DataType_2> & t): first(t.first), second(t.second) {}
            ~Tuple() {}
            int hashCode(void) {
                return first.hashCode();
            }
            std::string toString() const {
                return "(" + first.toString() + ", " + second.toString() + ")";
            }
            inline static id_t getId(void) {
                return (DataType_1::getId() << 4) | DataType_2::getId();
            }
            bool send(int fd) const {
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
            bool operator == (const Tuple<DataType_1, DataType_2> & b) const {
                return (first == b.first);
            }
            bool operator != (const Tuple<DataType_1, DataType_2> & b) const {
                return (first != b.first);
            }
            bool operator < (const Tuple<DataType_1, DataType_2> & b) const {
                return (first < b.first);
            }
            bool operator > (const Tuple<DataType_1, DataType_2> & b) const {
                return (first > b.first);
            }
            bool operator <= (const Tuple<DataType_1, DataType_2> & b) const {
                return (first <= b.first);
            }
            bool operator >= (const Tuple<DataType_1, DataType_2> & b) const {
                return (first >= b.first);
            }
            Tuple & operator = (const Tuple<DataType_1, DataType_2> & t) {
                first = t.first;
                second = t.second;
                return (*this);
            }
            Tuple & operator += (const Tuple<DataType_1, DataType_2> & t) {
                second += t.second;
                return (*this);
            }

    };
}

#endif
