/*
 * Header for all jobs
 */

#ifndef JOB_H
#define JOB_H

#include <string> // string
#include <vector> // vector
#include <memory> // unique_ptr
#include <thread> // thread

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
        const std::string & _jobName;
        const bool _isServer;
        const bool _supportMultipleMapper;

        explicit context_t(const ipconfig_t & ips,
                           ch::SourceManager & source,
                           const std::string & outputFilePath,
                           const std::string & workingDir,
                           const std::string & jobName,
                           const bool isServer = false,
                           const bool supportMultipleMapper = false): _ips(ips), _source(source), _outputFilePath(outputFilePath), _workingDir(workingDir), _jobName(jobName), _isServer(isServer), _supportMultipleMapper(supportMultipleMapper) {}
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

        StreamManager<MapperReducerOutputType> stm{context._ips, context._workingDir, context._jobName, DEFAULT_MAX_DATA_SIZE};

        if (!stm.isConnected()) { // Not connected
            E("(Job) StreamManager connect failed. Nothing done.");
            return false;
        }

        stm.startReceive();

        if (!stm.isReceiving()) { // Fail to start receive threads
            E("(Job) StreamManager start receive threads failed. Nothing done.");
            return false;
        }

        // Map
#ifndef MULTIPLE_MAPPER
        std::string polled;
        while (context._source.poll(polled)) {
            mapper(polled, stm);
        }
#else
        if (context._supportMultipleMapper) {
            std::vector<std::thread> mappers;
            std::vector<std::string> polleds(NUM_MAPPER);
            SourceManager & source = context._source;
            for (size_t i = 0; i < NUM_MAPPER; ++i) {
                std::string & polled = polleds[i];
                mappers.emplace_back([&source, &polled, &stm](){
                    while (source.poll(polled)) {
                        mapper(polled, stm);
                    }
                });
            }
            for (std::thread & thrd: mappers) {
                thrd.join();
            }
        } else {
            std::string polled;
            while (context._source.poll(polled)) {
                mapper(polled, stm);
            }
        }
#endif
        stm.stopSend();
        stm.blockTillRecvEnd();
        // End of map

        SortedStream<MapperReducerOutputType> * sorted = stm.getSortedStream();
        std::unique_ptr<SortedStream<MapperReducerOutputType> > _sorted{sorted};

        stm.setPresort(false);
        stm.startReceive();

        if (!stm.isReceiving()) { // Fail to start receive threads
            E("(Job) StreamManager start receive threads failed. Fail to perform reduce on this machine.");
            return false;
        }

        // Reduce
        if (sorted) {
            stm.setPartitioner(zeroPartitioner);
            reducer(*sorted, stm);
        }
        stm.finalizeSend();
        stm.blockTillRecvEnd();
        // End of reduce

        if (context._isServer) {
            return stm.pourToTextFile(context._outputFilePath.c_str());
        }
        return true;
    }

    /*
     * Complete job:
     * output type of mapper and reducer are different
     */
    template <typename MapperOutputType, typename ReducerOutputType>
    bool completeJob(context_t & context) {

        StreamManager<MapperOutputType> stm_mapper{context._ips, context._workingDir, context._jobName, DEFAULT_MAX_DATA_SIZE};

        if (!stm_mapper.isConnected()) { // Not connected
            E("(Job) StreamManager connect failed. Nothing done.");
            return false;
        }

        stm_mapper.startReceive();

        if (!stm_mapper.isReceiving()) { // Fail to start receive threads
            E("(Job) StreamManager start receive threads failed. Nothing done.");
            return false;
        }

        // Map
#ifndef MULTIPLE_MAPPER
        std::string polled;
        while (context._source.poll(polled)) {
            mapper(polled, stm_mapper);
        }
#else
        if (context._supportMultipleMapper) {
            std::vector<std::thread> mappers;
            std::vector<std::string> polleds(NUM_MAPPER);
            SourceManager & source = context._source;
            for (size_t i = 0; i < NUM_MAPPER; ++i) {
                std::string & polled = polleds[i];
                mappers.emplace_back([&source, &polled, &stm_mapper](){
                    while (source.poll(polled)) {
                        mapper(polled, stm_mapper);
                    }
                });
            }
            for (std::thread & thrd: mappers) {
                thrd.join();
            }
        } else {
            std::string polled;
            while (context._source.poll(polled)) {
                mapper(polled, stm_mapper);
            }
        }
#endif
        stm_mapper.finalizeSend();
        stm_mapper.blockTillRecvEnd();
        // End of map

        SortedStream<MapperOutputType> * sorted = stm_mapper.getSortedStream();
        std::unique_ptr<SortedStream<MapperOutputType> > _sorted{sorted};

        StreamManager<ReducerOutputType> stm_reducer{context._ips, context._workingDir, context._jobName, DEFAULT_MAX_DATA_SIZE, false};

        if (!stm_reducer.isConnected()) { // Not connected
            E("(Job) StreamManager connect failed. Fail to perform reduce on this machine.");
            return false;
        }

        stm_reducer.startReceive();

        if (!stm_reducer.isReceiving()) { // Fail to start receive threads
            E("(Job) StreamManager start receive threads failed. Fail to perform reduce on this machine.");
            return false;
        }

        // Reduce
        if (sorted) {
            stm_reducer.setPartitioner(zeroPartitioner);
            reducer(*sorted, stm_reducer);
        }
        stm_reducer.finalizeSend();
        stm_reducer.blockTillRecvEnd();
        // End of reduce

        if (context._isServer) {
            return stm_reducer.pourToTextFile(context._outputFilePath.c_str());
        }
        return true;
    }
}

#endif
