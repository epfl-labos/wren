/*
 * WREN 
 *
 * Copyright 2018 Operating Systems Laboratory EPFL
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SCC_KVSERVICE_EXPERIMENT_H
#define SCC_KVSERVICE_EXPERIMENT_H

#include "kvservice/public_kv_client.h"
#include "groupservice/group_client.h"
#include "common/types.h"
#include "common/utils.h"
#include "common/generators.h"
#include "common/wait_handle.h"
#include "common/sys_config.h"
#include <sys/types.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>
#include <unistd.h>
#include <string>
#include <numeric>
#include <thread>
#include <chrono>
#include <algorithm>
#include <iostream>
#include <stdexcept>

namespace scc {

    typedef struct {
        int ThreadId;
        std::string workload;
        std::vector<std::string> ServerNames;
        std::vector<int> ServerPorts;
        std::vector<PublicTxClient *> Clients;
        PublicTxClient *Client;
        int LocalPartitionId;
        int NumPartitions;
        int NumReplicas;
        int TotalNumItems;
        int NumValueBytes;
        int locality;

        bool ReservoirSampling;

        int ReservoirSamplingLimit;
        atomic<bool> *stopOperation;

        int servingReplica;
        int servingPartition;
        std::string RequestsDistribution;

        int ReadRatio;
        int WriteRatio;
        int TxRatio;

        Generator<uint64_t> *generator;
        WaitHandle OpReadyEvent;
        WaitHandle OpStartEvent;
        WaitHandle OpEndEvent;

        int NumTxsPerThread;

        int NumStartOpsPerTx;
        int NumReadOpsPerTx;
        int NumWriteOpsPerTx;
        int NumCommitOpsPerTx;

        int TotalNumStartOpsPerThread;
        int TotalNumReadOpsPerThread;
        int TotalNumWriteOpsPerThread;
        int TotalNumCommitOpsPerThread;
        int TotalNumTxOpsPerThread;

        std::vector<double> Latencies;
        std::vector<double> OpStartLatencies;
        std::vector<double> OpReadLatencies;
        std::vector<double> OpWriteLatencies;
        std::vector<double> OpCommitLatencies;
        std::vector<double> OpTxLatencies;

        double StartLatenciesSum;
        double ReadLatenciesSum;
        double WriteLatenciesSum;
        double CommitLatenciesSum;
        double TxLatenciesSum;

        double MinStartLatency;
        double MaxStartLatency;

        double MinReadLatency;
        double MaxReadLatency;

        double MinWriteLatency;
        double MaxWriteLatency;

        double MinTxLatency;
        double MaxTxLatency;

        double MinCommitLatency;
        double MaxCommitLatency;

        int NumTxReadItems;
        int NumTxWriteItems;

        int numHotBlockKeys;
        int clientResetNumber;
        bool enableClientReset;

        int TotalNumReadItems;

        double warmUpDuration;

        int64_t numItemsReadFromClientWriteSet;
        int64_t numItemsReadFromClientReadSet;
        int64_t numItemsReadFromClientWriteCache;
        int64_t numItemsReadFromStore;
        int64_t totalNumClientReadItems;

    } ThreadArg;

    typedef struct {
        int NumPartitions;
        int NumReplicas;
        int NumThreadsPerReplica;
        int NumOpsPerThread;
        int NumHotBlockedKeys;
        int NumStartOpsPerTx;
        int NumReadOpsPerTx;
        int NumWriteOpsPerTx;
        int NumCommitOpsPerTx;
        double OpThroughput;
        double TxThroughput;
        int64_t TotalOps;
        int64_t TotalTxOps;
        int64_t TotalStartOps;
        int64_t TotalReadOps;
        int64_t TotalWriteOps;
        int64_t TotalCommitOps;
        int64_t TotalExpDuration;
        int64_t TotalNumReadItems;
        Statistic OpStartLatencyStats;
        Statistic OpReadLatencyStats;
        Statistic OpWriteLatencyStats;
        Statistic OpCommitLatencyStats;
        Statistic OpTxLatencyStats;
        std::vector<double> startLatencies;
        std::vector<double> readLatencies;
        std::vector<double> writeLatencies;
        std::vector<double> commitLatencies;
        std::vector<double> txLatencies;
        int64_t numItemsReadFromClientWriteSet;
        int64_t numItemsReadFromClientReadSet;
        int64_t numItemsReadFromClientWriteCache;
        int64_t numItemsReadFromStore;
        int64_t totalNumClientReadItems;

    } Results;

    class Experiment {
    public:
        std::atomic<bool> stopOperation;

        Experiment(char *argv[]);

        void runExperiment();

        Configuration config;
    private:
        std::vector<std::vector<DBPartition>> allPartitions;
        std::vector<ThreadArg *> threadArgs;
        std::vector<std::thread *> threads;
        std::list<WaitHandle *> opReadyEvents;
        std::list<WaitHandle *> opEndEvents;

        void initializeThreadRequestDistribution(ThreadArg *t);

        void buildThreadArguments();

        void launchBenchmarkingClientThreads();

        void calculatePerformanceMeasures(double duration);

        void writeResultsCSV(FILE *&stream, Results res);

        void freeResources();

        void printExperimentParameters();

        void writeLatencyDistribution(std::string fileName, vector<double> vector);

        void calculateVariation_StandardDeviation(Statistic &stat, vector<double> vect);

        void calculateMedian_Percentiles(vector<double> vect, Statistic &stat);



    };

} // namespace scc

#endif
