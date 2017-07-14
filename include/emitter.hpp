/*
 * Emitter for mapper and reducer
 */

#ifndef EMITTER_H
#define EMITTER_H

#include <fstream> // ofstream

namespace ch {

    /********************************************
     ************** Declaration *****************
    ********************************************/

    template <typename DataType>
    class Emitter {

        public:

            // Emit the data
            virtual bool emit(DataType & v) const = 0;

    };

    /*
     * Emit to local file
     */
    template <typename DataType>
    class LocalEmitter: public Emitter<DataType> {

        private:

            // ofstream of local file
            std::ofstream * _os;

        public:

            // Constructor
            LocalEmitter(std::ofstream * os);

            bool emit(DataType & v) const;

    };

    /********************************************
     ************ Implementation ****************
    ********************************************/

    // Constructor
    template <typename DataType>
    LocalEmitter<DataType>::LocalEmitter(std::ofstream * os): _os{os} {}

    template <typename DataType>
    bool LocalEmitter<DataType>::emit(DataType & v) const {
        return (bool)((*_os) << v);
    }

}

#endif
