/*
 * Header for all jobs
 */

#ifndef JOB_H
#define JOB_H

#include <string> // string
#include <vector> // vector

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

    typedef bool job_f(context_t & context);

    /*
     * Default mapper definition
     */
    template <class MapperOutputType>
    void mapper(std::string & block, StreamManager<MapperOutputType> & sm) {
        E("Cannot find mapper function.");
        return;
    }

    /*
     * Default reducer definition
     */
    template <class ReducerInputType, class ReducerOutputType>
    void reducer(SortedStream<ReducerInputType> & ss, StreamManager<ReducerOutputType> & sm) {
        E("Cannot find reducer function.");
        return;
    }

    /*
     * Simple job:
     * output type of mapper and reducer are the same so that we can reuse stream manager
     */
    template <class MapperReducerOutputType>
    bool simpleJob(context_t & context) {
        std::string polled;
        StreamManager<MapperReducerOutputType> stm(context._ips, context._workingDir, DEFAULT_MAX_DATA_SIZE);
        if (stm.isReceiving()) {
            while (context._source.poll(polled)) {
                mapper(polled, stm);
            }
            stm.stopSend();
            stm.blockTillRecvEnd();

            SortedStream<MapperReducerOutputType> * sorted = stm.getSortedStream();
            if (sorted) {
                stm.setPresort(false);
                stm.startRecvThreads();
                reducer(*sorted, stm);

                delete sorted;

                stm.finalizeSend();
                stm.blockTillRecvEnd();

                if (context._isServer) {
                    return stm.pourToTextFile(context._outputFilePath.c_str());
                }
            }
            return true;
        } else {
            D("Job: StreamManager failed. Nothing done.\n");
        }
        return false;
    }

    /*
     * Complete job:
     * output type of mapper and reducer are different
     */
    template <class MapperOutputType, class ReducerOutputType>
    bool completeJob(context_t & context) {
        std::string polled;
        StreamManager<MapperOutputType> stm_mapper(context._ips, context._workingDir, DEFAULT_MAX_DATA_SIZE);
        if (stm_mapper.isReceiving()) {
            while (context._source.poll(polled)) {
                mapper(polled, stm_mapper);
            }
            stm_mapper.finalizeSend();
            stm_mapper.blockTillRecvEnd();

            SortedStream<MapperOutputType> * sorted = stm_mapper.getSortedStream();
            if (sorted) {
                StreamManager<ReducerOutputType> stm_reducer(context._ips, context._workingDir, DEFAULT_MAX_DATA_SIZE, false);

                if (stm_reducer.isReceiving()) {
                    reducer(*sorted, stm_reducer);

                    delete sorted;

                    stm_reducer.finalizeSend();
                    stm_reducer.blockTillRecvEnd();

                    if (context._isServer) {
                        return stm_reducer.pourToTextFile(context._outputFilePath.c_str());
                    }
                } else {
                    D("Job: StreamManager failed. Fail to perform reduce on this machine.\n");
                    delete sorted;
                    return false;
                }
            }
            return true;
        } else {
            D("Job: StreamManager failed. Nothing done.\n");
        }
        return false;
    }
}

#endif
