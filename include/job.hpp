/*
 * Header for all jobs
 */

#ifndef JOB_H
#define JOB_H

#include <string> // string
#include <vector> // vector
#include <memory> // unique_ptr

#include "def.hpp" // ipconfig_t
#include "sourceManager.hpp" // SourceManager
#include "streamManager.hpp" // StreamManager

namespace ch {

    /*
     * Parameter of doJob function
     */
    struct context_t {
        const ipconfig_t & _ips;
        ch::SourceManager & _source;
        const std::string & _outputFilePath;
        const std::string & _workingDir;
        const bool _isServer;

        explicit context_t(const ipconfig_t & ips,
                           ch::SourceManager & source,
                           const std::string & outputFilePath,
                           const std::string & workingDir,
                           const bool isServer = false): _ips(ips), _source(source), _outputFilePath(outputFilePath), _workingDir(workingDir), _isServer(isServer) {}
    };

    // Job function type
    typedef bool job_f(context_t & context);

    /*
     * Default mapper definition
     */
    template <typename MapperOutputType>
    void mapper(std::string & block, StreamManager<MapperOutputType> & sm) {
        E("Cannot find mapper function.");
        return;
    }

    /*
     * Default reducer definition
     */
    template <typename ReducerInputType, typename ReducerOutputType>
    void reducer(SortedStream<ReducerInputType> & ss, StreamManager<ReducerOutputType> & sm) {
        E("Cannot find reducer function.");
        return;
    }

    /*
     * Simple job:
     * output type of mapper and reducer are the same so that we can reuse stream manager
     */
    template <typename MapperReducerOutputType>
    bool simpleJob(context_t & context) {
        std::string polled;

        StreamManager<MapperReducerOutputType> stm{context._ips, context._workingDir, DEFAULT_MAX_DATA_SIZE};

        if (!stm.isConnected()) { // Not connected
            E("(Job) StreamManager connect failed. Nothing done.");
            return false;
        }

        stm.startRecvThreads();

        if (!stm.isReceiving()) { // Fail to start receive threads
            E("(Job) StreamManager start receive threads failed. Nothing done.");
            return false;
        }

        // Map
        while (context._source.poll(polled)) {
            mapper(polled, stm);
        }
        stm.stopSend();
        stm.blockTillRecvEnd();
        // End of map

        SortedStream<MapperReducerOutputType> * sorted = stm.getSortedStream();

        if (sorted) {

            std::unique_ptr<SortedStream<MapperReducerOutputType> > _sorted{sorted};

            stm.setPresort(false);
            stm.startRecvThreads();

            if (!stm.isReceiving()) { // Fail to start receive threads
                E("(Job) StreamManager start receive threads failed. Fail to perform reduce on this machine.");
                return false;
            }

            // Reduce
            reducer(*sorted, stm);
            stm.finalizeSend();
            stm.blockTillRecvEnd();
            // End of reduce

            if (context._isServer) {
                return stm.pourToTextFile(context._outputFilePath.c_str());
            }
        }
        return true;
    }

    /*
     * Complete job:
     * output type of mapper and reducer are different
     */
    template <typename MapperOutputType, typename ReducerOutputType>
    bool completeJob(context_t & context) {
        std::string polled;

        StreamManager<MapperOutputType> stm_mapper{context._ips, context._workingDir, DEFAULT_MAX_DATA_SIZE};

        if (!stm_mapper.isConnected()) { // Not connected
            E("(Job) StreamManager connect failed. Nothing done.");
            return false;
        }

        stm_mapper.startRecvThreads();

        if (!stm_mapper.isReceiving()) { // Fail to start receive threads
            E("(Job) StreamManager start receive threads failed. Nothing done.");
            return false;
        }

        // Map
        while (context._source.poll(polled)) {
            mapper(polled, stm_mapper);
        }
        stm_mapper.finalizeSend();
        stm_mapper.blockTillRecvEnd();
        // End of map

        SortedStream<MapperOutputType> * sorted = stm_mapper.getSortedStream();

        if (sorted) {

            std::unique_ptr<SortedStream<MapperOutputType> > _sorted{sorted};

            StreamManager<ReducerOutputType> stm_reducer{context._ips, context._workingDir, DEFAULT_MAX_DATA_SIZE, false};

            if (!stm_reducer.isConnected()) { // Not connected
                E("(Job) StreamManager connect failed. Fail to perform reduce on this machine.");
                return false;
            }

            stm_reducer.startRecvThreads();

            if (!stm_reducer.isReceiving()) { // Fail to start receive threads
                E("(Job) StreamManager start receive threads failed. Fail to perform reduce on this machine.");
                return false;
            }

            // Reduce
            reducer(*sorted, stm_reducer);
            stm_reducer.finalizeSend();
            stm_reducer.blockTillRecvEnd();
            // End of reduce

            if (context._isServer) {
                return stm_reducer.pourToTextFile(context._outputFilePath.c_str());
            }
        }
        return true;
    }
}

#endif
