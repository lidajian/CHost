/*
 * Definition of types
 */

#ifndef TYPE_HPP
#define TYPE_HPP

#include <string>     // string, to_string
#include <iostream>   // ostream
#include <fstream>    // ifstream, ofstream

#include "def.hpp"    // ID_xxx
#include "utils.hpp"  // Recv, Send, sendString, receiveString
#include "murmur.hpp" // murmur2

namespace ch {
    const int PRIME = 31;

    typedef unsigned char id_t;

    class TypeBase {

        public:

            virtual ~TypeBase() {}

            // Get id of the object
            inline static id_t getId() {
                return ID_INVALID;
            }

            // Internal hash code for tuple
            virtual int _hashCode() = 0;

            // Get hash code of the object
            virtual int hashCode() = 0;

            // Get string representation of the object
            virtual std::string toString() const = 0;

            // Send the object through a socket
            virtual bool send(int fd) const = 0;

            // Receive the object through a socket
            virtual bool recv(int fd) = 0;

            // Read from file stream
            virtual std::ifstream & read(std::ifstream & is) = 0;

            // write to file stream
            virtual std::ofstream & write(std::ofstream & os) const = 0;

            // Output to file
            friend std::ofstream & operator << (std::ofstream & os, const TypeBase & v);

            // Input from file
            friend std::ifstream & operator >> (std::ifstream & is, const TypeBase & v);

            // Output to ostream
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

            // From value
            Integer(int v = 0): value{v} {}

            // Copy constructor
            Integer(const Integer & i): value{i.value} {}

            // Destructor
            ~Integer() {}

            // Virtual functions implementation
            inline int _hashCode() {
                return murmur2(value);
            }
            int hashCode() {
                return _hashCode();
            }
            std::string toString() const {
                return std::to_string(value);
            }
            inline static id_t getId() {
                return ID_INTEGER;
            }
            bool send(int fd) const {
                return Send(fd, static_cast<const void *>(&value), sizeof(int));
            }
            bool recv(int fd) {
                return Recv(fd, static_cast<void *>(&value), sizeof(int));
            }
            std::ifstream & read(std::ifstream & is) {
                is.read(reinterpret_cast<char *>(&value), sizeof(int));
                return is;
            }
            std::ofstream & write(std::ofstream & os) const {
                os.write(reinterpret_cast<const char *>(&value), sizeof(int));
                return os;
            }

            // Operator overriding
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

        protected:

            bool hashGot;

            int hash;

        public:

            std::string value;

            // Default constructor
            String(): hashGot{false} {}

            // From value
            String(const std::string & str): hashGot{false}, value{str} {}

            // From rvalue
            String(std::string && str): hashGot{false}, value{std::move(str)} {}

            // Copy constructor
            String(const String & str): hashGot{str.hashGot}, hash{str.hash}, value{str.value} {}

            // Move constructor
            String(String && str)
            : hashGot{str.hashGot}, hash{str.hash}, value{std::move(str.value)} {
                str.hashGot = false;
            }

            // Destructor
            ~String() {}

            // Virtual functions implementation
            inline int _hashCode() {
                return murmur2(value);
            }
            int hashCode() {
                if (!hashGot) {
                    hash = _hashCode();
                    hashGot = true;
                }
                return hash;
            }
            std::string toString() const {
                return "\"" + value + "\"";
            }
            inline static id_t getId() {
                return ID_STRING;
            }
            bool send(int fd) const {
                return sendString(fd, value);
            }
            bool recv(int fd) {
                hashGot = false;
                return receiveString(fd, value);
            }
            std::ifstream & read(std::ifstream & is) {
                size_t l = 0;
                hashGot = false;
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

            // Operator overriding
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
                hashGot = false;
                value = str.value;
                return (*this);
            }
            String & operator += (const String & str) {
                hashGot = false;
                value += str.value;
                return (*this);
            }

            // Move assignment
            String & operator = (String && str) {
                hashGot = false;
                value = std::move(str.value);
                return (*this);
            }
    };

    template <typename DataType_1, typename DataType_2>
    class Tuple: public TypeBase {

        public:

            DataType_1 first;
            DataType_2 second;

            // Default constructor
            Tuple() {}

            // From values
            Tuple(const DataType_1 & v1, const DataType_2 & v2): first{v1}, second{v2} {}

            // From rvalues
            Tuple(DataType_1 && v1, DataType_2 && v2)
            : first{std::move(v1)}, second{std::move(v2)} {}

            // Copy constructor
            Tuple(const Tuple<DataType_1, DataType_2> & t): first{t.first}, second{t.second} {}

            // Move constructor
            Tuple(Tuple<DataType_1, DataType_2> && t)
            : first{std::move(t.first)}, second{std::move(t.second)} {}

            // Destructor
            ~Tuple() {}

            // Virtual functions implementation

            // Called if it is in first field of a root tuple
            // Hash that take all fields into account
            inline int _hashCode() {
                return first._hashCode() ^ second._hashCode();
            }
            int hashCode() {
                return first._hashCode();
            }
            std::string toString() const {
                return "(" + first.toString() + ", " + second.toString() + ")";
            }
            inline static id_t getId() {
                return (DataType_1::getId() << 3) ^ DataType_2::getId();
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

            // Operator overriding
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
            // Move assignment
            Tuple & operator = (Tuple<DataType_1, DataType_2> && t) {
                first = std::move(t.first);
                second = std::move(t.second);
                return (*this);
            }
    };
}

#endif
