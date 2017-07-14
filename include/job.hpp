/*
 * Header for all jobs
 */

#ifndef JOB_H
#define JOB_H

#include <string>               // string
#include <vector>               // vector
#include <memory>               // unique_ptr
#include <thread>               // thread
#include <fstream>              // ofstream

#include "def.hpp"              // ipconfig_t
#include "sourceManager.hpp"    // SourceManager
#include "sortedStream.hpp"     // SortedStream
#include "streamManager.hpp"    // StreamManager, ClusterEmitter
#include "localFileManager.hpp" // LocalFileManager
#include "emitter.hpp"          // LocalEmitter
#include "threadPool.hpp"       // ThreadPool

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
        const bool _supportMultithread;

        explicit context_t(const ipconfig_t & ips,
                           ch::SourceManager & source,
                           const std::string & outputFilePath,
                           const std::string & workingDir,
                           const std::string & jobName,
                           const bool isServer = false,
                           const bool supportMultithread = false)
        : _ips(ips), _source(source), _outputFilePath(outputFilePath),
          _workingDir(workingDir), _jobName(jobName), _isServer(isServer),
          _supportMultithread(supportMultithread) {}
    };

    // Job function type
    typedef bool job_f(context_t & context);

    /*
     * Default mapper definition
     */
    template <typename MapperOutputType>
    void mapper(std::string & block, const Emitter<MapperOutputType> & emitter) {

        E("Cannot find mapper function.");
        return;

    }

    /*
     * Default reducer definition
     */
    template <typename ReducerInputType, typename ReducerOutputType>
    void reducer(SortedStream<ReducerInputType> & ss, const Emitter<ReducerOutputType> & emitter) {

        E("Cannot find reducer function.");
        return;

    }

    /*
     * Simple job:
     * output type of mapper and reducer are the same so that we can reuse stream manager
     */
    template <typename MapperReducerOutputType>
    bool simpleJob(context_t & context) {

        StreamManager<MapperReducerOutputType> stm{context._ips, context._workingDir,
                                                   context._jobName, DEFAULT_MAX_DATA_SIZE};

        if (!stm.isConnected()) { // Not connected
            E("(Job) StreamManager connect failed. Nothing done.");
            return false;
        }

        stm.startReceive();

        if (!stm.isReceiving()) { // Fail to start receive threads
            E("(Job) StreamManager start receive threads failed. Nothing done.");
            return false;
        }

        const ClusterEmitter<MapperReducerOutputType> & emitter = stm.getEmitter();

        // Map
#ifndef MULTITHREAD_SUPPORT
        std::string polled;
        while (context._source.poll(polled)) {
            mapper(polled, emitter);
        }
#else
        if (context._supportMultithread) {
            std::vector<std::thread> mappers;
            std::vector<std::string> polleds(NUM_MAPPER);
            SourceManager & source = context._source;

            for (size_t i = 0; i < NUM_MAPPER; ++i) {
                std::string & polled = polleds[i];

                mappers.emplace_back([&source, &polled, &emitter](){
                    while (source.poll(polled)) {
                        mapper(polled, emitter);
                    }
                });
            }
            for (std::thread & thrd: mappers) {
                thrd.join();
            }
        } else {
            std::string polled;

            while (context._source.poll(polled)) {
                mapper(polled, emitter);
            }
        }
#endif
        stm.stopSend();
        stm.blockTillRecvEnd();
        // End of map

        stm.setPresort(false);

        // Reduce
#ifndef MULTITHREAD_SUPPORT
        SortedStream<MapperReducerOutputType> * sorted = stm.getSortedStream();

        stm.startReceive();

        if (!stm.isReceiving()) { // Fail to start receive threads
            E("(Job) StreamManager start receive threads failed. Fail to perform"
              " reduce on this machine.");
            return false;
        }

        if (sorted) {
            std::unique_ptr<SortedStream<MapperReducerOutputType> > _sorted{sorted};
            stm.setPartitioner(zeroPartitioner);
            reducer(*sorted, emitter);
        }
#else
        if (context._supportMultithread) {
            LocalFileManager<MapperReducerOutputType> fileManager{std::move(*stm.getFileManager())};

            stm.startReceive();

            if (!stm.isReceiving()) { // Fail to start receive threads
                E("(Job) StreamManager start receive threads failed. Fail to perform"
                  " reduce on this machine.");
                return false;
            }

            // Iteratively reduce files
            while (true) {
                std::vector<std::unique_ptr<SortedStream<MapperReducerOutputType> > > &&
                                                    sortedStreams = fileManager.getSortedStreams();

                size_t nStreams = sortedStreams.size();

                if (nStreams == 0) {
                    break;
                } else if (nStreams == 1) { // Send to master
                    std::unique_ptr<SortedStream<MapperReducerOutputType> > &
                                                                    sorted = sortedStreams[0];

                    if (sorted->open()) {
                        reducer(*sorted, emitter);
                    }

                    break;
                } else { // Store locally for next iteration
                    ThreadPool threadPool{MIN_VAL(THREAD_POOL_SIZE, nStreams)};
                    std::vector<std::ofstream> ostms(nStreams);

                    for (size_t i = 0; i < nStreams; ++i) {
                        std::ofstream & ostm = ostms[i];
                        fileManager.getStream(ostm);

                        std::unique_ptr<SortedStream<MapperReducerOutputType> > &
                                                                    sorted = sortedStreams[i];

                        threadPool.addTask([&sorted, &ostm](){
                            if (sorted->open()) {
                                LocalEmitter<MapperReducerOutputType> emitter{&ostm};
                                reducer(*sorted, emitter);
                            }
                        });
                    }

                    threadPool.stop();
                    // ofstream close
                }
                // sortedStreams destruction
            }
        } else {
            SortedStream<MapperReducerOutputType> * sorted = stm.getSortedStream();

            stm.startReceive();

            if (!stm.isReceiving()) { // Fail to start receive threads
                E("(Job) StreamManager start receive threads failed. Fail to perform"
                  " reduce on this machine.");
                return false;
            }

            if (sorted) {
                std::unique_ptr<SortedStream<MapperReducerOutputType> > _sorted{sorted};
                stm.setPartitioner(zeroPartitioner);
                reducer(*sorted, emitter);
            }
        }
#endif
        stm.finalizeSend();
        stm.blockTillRecvEnd();
        // End of reduce

        if (context._isServer) {
            return stm.pourToTextFile(context._outputFilePath.c_str());
        }

        return true;

    }

    /*
     * Complex job:
     * output type of mapper and reducer are different
     */
    template <typename MapperOutputType, typename ReducerOutputType>
    bool complexJob(context_t & context) {

        StreamManager<MapperOutputType> stm_mapper{context._ips, context._workingDir,
                                                   context._jobName, DEFAULT_MAX_DATA_SIZE};

        if (!stm_mapper.isConnected()) { // Not connected
            E("(Job) StreamManager connect failed. Nothing done.");
            return false;
        }

        stm_mapper.startReceive();

        if (!stm_mapper.isReceiving()) { // Fail to start receive threads
            E("(Job) StreamManager start receive threads failed. Nothing done.");
            return false;
        }

        const ClusterEmitter<MapperOutputType> & emitter_mapper = stm_mapper.getEmitter();

        // Map
#ifndef MULTITHREAD_SUPPORT
        std::string polled;
        while (context._source.poll(polled)) {
            mapper(polled, emitter_mapper);
        }
#else
        if (context._supportMultithread) {
            std::vector<std::thread> mappers;
            std::vector<std::string> polleds(NUM_MAPPER);
            SourceManager & source = context._source;

            for (size_t i = 0; i < NUM_MAPPER; ++i) {
                std::string & polled = polleds[i];

                mappers.emplace_back([&source, &polled, &emitter_mapper](){
                    while (source.poll(polled)) {
                        mapper(polled, emitter_mapper);
                    }
                });
            }
            for (std::thread & thrd: mappers) {
                thrd.join();
            }
        } else {
            std::string polled;

            while (context._source.poll(polled)) {
                mapper(polled, emitter_mapper);
            }
        }
#endif
        stm_mapper.finalizeSend();
        stm_mapper.blockTillRecvEnd();
        // End of map

        // Reduce
        StreamManager<ReducerOutputType> stm_reducer{context._ips, context._workingDir,
                                                context._jobName, DEFAULT_MAX_DATA_SIZE, false};

        if (!stm_reducer.isConnected()) { // Not connected
            E("(Job) StreamManager connect failed. Fail to perform reduce on this machine.");
            return false;
        }

        stm_reducer.startReceive();

        if (!stm_reducer.isReceiving()) { // Fail to start receive threads
            E("(Job) StreamManager start receive threads failed. Fail to perform reduce"
              " on this machine.");
            return false;
        }

        SortedStream<MapperOutputType> * sorted = stm_mapper.getSortedStream();

        if (sorted) {
            const ClusterEmitter<ReducerOutputType> & emitter_reducer = stm_reducer.getEmitter();

            std::unique_ptr<SortedStream<MapperOutputType> > _sorted{sorted};

            stm_reducer.setPartitioner(zeroPartitioner);

            reducer(*sorted, emitter_reducer);
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
