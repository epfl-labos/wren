#include "kvservice/experiment.h"
#include "common/sys_logger.h"
#include "experiment.h"
#include <boost/format.hpp>

#define DO_EXPERIMENT
#define DO_READS
#define DO_WRITES

namespace scc {

    static int thinkTime = 0;

    static int _partitionsToReadFrom = -1;

    double IntToDouble(int integer) {
        if (integer == 100 || integer == 75 || integer == 50 || integer == 25)
            return ((double) integer) / 100.0;
        if (integer == 12) // 1/8
            return 12.5 / 100.0;
        if (integer == 6) // 1/16
            return 6.25 / 100.0;
        if (integer == 3) // 1/32
            return 3.125 / 100.0;
        if (integer == 1)
            return 1.5625 / 100.0; // 1/64
        fprintf(stderr, "Integer not valid %d\n", integer);
        assert(false);
    }

    std::string GetGlobalRandomKey(int totalNumItems) {
        return std::to_string(abs(random()) % totalNumItems);
    }

    std::string GetKey(Generator<uint64_t> *generator) {
        int key = generator->Next();
        return std::to_string(key);
    }

    std::string GetRandomKeyAtPartition(int partitionId, int numPartitions, int totalNumItems) {
        int key;

        do {
            key = abs(random()) % (totalNumItems * numPartitions);
        } while (key % numPartitions != partitionId);

        return std::to_string(key);
    }

    std::string GetKeyAtPartition(Generator<uint64_t> *generator, int partitionId, int numPartitions) {
        int rand, key;

        rand = generator->Next();
        key = rand * numPartitions + partitionId;

        return std::to_string(key);
    }

    std::string GetKeyAtPartitionAtReplica(Generator<uint64_t> *generator, int partitionId, int numPartitions,
                                           int replicaId, int numLocalKeysPerReplica) {
        int rand, key;
        rand = generator->Next();//rand-th element of the replicaId-th slice of a partition

        rand += (replicaId *
                 numLocalKeysPerReplica); //Sum an offset to obtain the index of the desired key in the desired partition

        key = rand * numPartitions +
              partitionId; //Now compute the absolute value of the key, that takes into account the hashing function

        return std::to_string(key);
    }

    /* >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Experiment Start - Read - Write - Commit <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< */

    void warmUp(ThreadArg *arg, PublicTxClient &client);

    void addNumReadItemsStats(ThreadArg *arg, PublicTxClient &client);

    void experiment1(ThreadArg *arg) {
        fflush(stdout);
        try {
            srand(Utils::GetThreadId() * time(0));
            PublicTxClient client(arg->ServerNames[arg->servingPartition], arg->ServerPorts[arg->servingPartition]);
            client.setTotalNumKeyInKVStore(arg->TotalNumItems);

            std::string dummyValue(arg->NumValueBytes, 'o');
            vector<int> partition_ids;
            int pId;
            bool retR, retC, retW;
            PhysicalTimeSpec startTime, endTime, txStartTime, txEndTime;
            double txDuration, duration;

#ifdef RESERVOIR_SAMPLING
            int offsetR = 0;
            int offsetW = 0;
            int offsetTx = 0;
#endif
            // notify readiness
            arg->OpReadyEvent.Set();

            // wait to start
            arg->OpStartEvent.WaitAndReset();

            for (int i = 0; i < arg->NumPartitions; ++i) {
                partition_ids.push_back(i);
            }

            srand(time(0));
            random_shuffle(partition_ids.begin(), partition_ids.end());

            std::vector<std::string> readKeys;
            std::vector<std::string> writeKeys;
            std::vector<std::string> readValues;
            std::vector<std::string> writeValues;

            const int partitionsToReadFrom = (int) (IntToDouble(arg->ReadRatio) *
                                                    ((double) partition_ids.size())) > 0 ?
                                             (int) ((IntToDouble(arg->ReadRatio) * ((double) partition_ids.size())))
                                                                                         : 1;

            const int partitionsToWriteTo = (int) (IntToDouble(arg->WriteRatio) *
                                                   ((double) partition_ids.size())) > 0 ?
                                            (int) ((IntToDouble(arg->WriteRatio) * ((double) partition_ids.size())))
                                                                                        : 1;

            arg->NumReadOpsPerTx = partitionsToReadFrom;
            arg->NumWriteOpsPerTx = partitionsToWriteTo;
            arg->NumCommitOpsPerTx = 1;

            fprintf(stdout, "[INFO]: Reading from %d partitions.\n", partitionsToReadFrom);
            fflush(stdout);
            fprintf(stdout, "[INFO]: Writing to %d partitions.\n", partitionsToWriteTo);
            fflush(stdout);

            if (arg->ThreadId == 0)
                _partitionsToReadFrom = partitionsToReadFrom;

            warmUp(arg, client);

#ifdef DO_EXPERIMENT
            while (!(*arg->stopOperation)) {

                /* ------------------- START TRANSACTION ------------------- */
                txStartTime = Utils::GetCurrentClockTime();

                PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
                if (client.TxStart()) {

                    PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();
                    arg->StartLatenciesSum += duration;
                    arg->TotalNumStartOpsPerThread++;

                    if (arg->MaxStartLatency < duration) {
                        arg->MaxStartLatency = duration;
                    }

                    if (arg->MinStartLatency > duration) {
                        arg->MinStartLatency = duration;
                    }

                    arg->OpStartLatencies.push_back(duration);
//                     Read items
#ifdef DO_READS
                    readKeys.clear();
                    readValues.clear();

                    /* -------------------  READ PHASE -------------------*/
                    // Read from N partitions, starting from a random one
                    pId = rand() % arg->NumPartitions;
                    for (int rCount = 0; rCount < partitionsToReadFrom; rCount++) {
                        std::string opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                        readKeys.push_back(opKey);
                        pId = (pId + 1) % arg->NumPartitions;
                    }

                    arg->TotalNumReadItems += readKeys.size();

                    //record read operation start time
                    startTime = Utils::GetCurrentClockTime();
                    retR = client.TxRead(readKeys, readValues);
                    if (!retR) {
                        fprintf(stdout, "[ERROR]: Reading failed.\n");
                    }
                    endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();


                    arg->ReadLatenciesSum += duration;
                    arg->TotalNumReadOpsPerThread++;

                    if (arg->MaxReadLatency < duration) {
                        arg->MaxReadLatency = duration;
                    }

                    if (arg->MinReadLatency > duration) {
                        arg->MinReadLatency = duration;
                    }

#ifdef RESERVOIR_SAMPLING

                    if (arg->OpReadLatencies.size() < arg->ReservoirSamplingLimit) {

                        arg->OpReadLatencies.push_back(duration);
                    }
                    else {
                        offsetRead++;
                        int position = abs(random()) % (arg->ReservoirSamplingLimit + offsetRead);
                        if (position < arg->ReservoirSamplingLimit) {
                            arg->OpReadLatencies[position] = duration;
                        }
                    }
#else
                    arg->OpReadLatencies.push_back(duration);
#endif
#endif //DO_READS

#ifdef DO_WRITES

                    /* ------------------- WRITE PHASE ------------------- */
                    // Write items
                    writeKeys.clear();
                    writeValues.clear();

                    // Writing keys to M partitions, starting from a random one
                    pId = rand() % arg->NumPartitions;
                    for (int wCount = 0; wCount < partitionsToWriteTo; wCount++) {
                        std::string opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                        writeKeys.push_back(opKey);
                        writeValues.push_back(dummyValue);
                        pId = (pId + 1) % arg->NumPartitions;
                    }

                    //record write operation start time
                    startTime = Utils::GetCurrentClockTime();
                    retW = client.TxWrite(writeKeys, writeValues);
                    if (!retW) {
                        fprintf(stdout, "[ERROR]: Writing failed.\n");
                    }
                    endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();

                    arg->WriteLatenciesSum += duration;
                    arg->TotalNumWriteOpsPerThread++;

                    if (arg->MaxWriteLatency < duration) {
                        arg->MaxWriteLatency = duration;
                    }

                    if (arg->MinWriteLatency > duration) {
                        arg->MinWriteLatency = duration;
                    }

#ifdef RESERVOIR_SAMPLING

                    if (arg->OpWriteLatencies.size() < arg->ReservoirSamplingLimit) {

                        arg->OpWriteLatencies.push_back(duration);
                    }
                    else {
                        offsetWrite++;
                        int position = abs(random()) % (arg->ReservoirSamplingLimit + offsetWrite);
                        if (position < arg->ReservoirSamplingLimit) {
                            arg->OpWriteLatencies[position] = duration;
                        }
                    }
#else
                    arg->OpWriteLatencies.push_back(duration);
#endif
#endif //DO_WRITES

                    /* ------------------- COMMIT TRANSACTION ------------------- */
                    // Commit transaction

                    //record commit operation start time
                    startTime = Utils::GetCurrentClockTime();
                    retC = client.TxCommit();
                    if (!retC) {
                        fprintf(stdout, "[Error]:Transaction could not be committed.\n");
                    }
                    //record commit operation end time
                    endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();
                    arg->CommitLatenciesSum += duration;
                    arg->TotalNumCommitOpsPerThread++;

                    if (arg->MaxCommitLatency < duration) {
                        arg->MaxCommitLatency = duration;
                    }

                    if (arg->MinCommitLatency > duration) {
                        arg->MinCommitLatency = duration;
                    }

                    //Record transaction duration
                    txEndTime = Utils::GetCurrentClockTime();
                    txDuration = (txEndTime - txStartTime).toMilliSeconds();
                    arg->OpTxLatencies.push_back(txDuration);
                    arg->TxLatenciesSum += txDuration;
                    arg->TotalNumTxOpsPerThread++;

                    if (arg->MaxTxLatency < duration) {
                        arg->MaxTxLatency = duration;
                    }

                    if (arg->MinTxLatency > duration) {
                        arg->MinTxLatency = duration;
                    }

                } else {
                    fprintf(stdout, "[Error]:Transaction could not be started.\n");
                }
            } // end while

            arg->numHotBlockKeys = 0;
            addNumReadItemsStats(arg, client);

#endif //DO_EXPERIMENT

            // notify stopping
            arg->OpEndEvent.Set();

        } catch (SocketException &e) {
            fprintf(stdout, "[ERROR]:Experiment 1: SocketException: %s\n", e.what());
            exit(1);
        }

    }

    void addNumReadItemsStats(ThreadArg *arg, PublicTxClient &client) {
        arg->numItemsReadFromStore = client.getNumItemsReadFromStore();
        arg->numItemsReadFromClientWriteSet = client.getNumItemsReadFromClientWriteSet();
        arg->numItemsReadFromClientWriteCache = client.getNumItemsReadFromClientWriteCache();
        arg->numItemsReadFromClientReadSet = client.getNumItemsReadFromClientReadSet();
        arg->totalNumClientReadItems = client.getTotalNumReadItems();
        SLOG((boost::format("arg->numItemsReadFromStore= %d") % arg->numItemsReadFromStore).str());
        SLOG((boost::format("arg->numItemsReadFromClientWriteSet= %d") % arg->numItemsReadFromClientWriteSet).str());
        SLOG((boost::format("arg->numItemsReadFromClientWriteCache= %d") %
              arg->numItemsReadFromClientWriteCache).str());
        SLOG((boost::format("arg->numItemsReadFromClientReadSet= %d") % arg->numItemsReadFromClientReadSet).str());
        SLOG((boost::format("arg->totalNumClientReadItems= %d") % arg->totalNumClientReadItems).str());
        SLOG((boost::format("arg->TotalNumReadItems= %d") % arg->TotalNumReadItems).str());
    }

    void warmUp(ThreadArg *arg, PublicTxClient &client) {
#ifdef WARM_UP
        vector<string> rKeys, wKeys, rValues, wValues;
        std::string dummyValue(arg->NumValueBytes, 'o');

        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime(), now;
        while (((now = Utils::GetCurrentClockTime()) - startTime).toMilliSeconds() < arg->warmUpDuration) {

            if (client.TxStart()) {

                rKeys.clear();
                rValues.clear();
                wKeys.clear();
                wValues.clear();

                /* read from every partition */
                for (int pId = 0; pId < arg->NumPartitions; pId++) {
                    string readKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                    string writeKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                    rKeys.push_back(readKey);
                    wKeys.push_back(writeKey);
                    wValues.push_back(dummyValue);
                }

                bool retR = client.TxRead(rKeys, rValues);
                if (!retR) {
                    fprintf(stdout, "[ERROR]: Reading failed.\n");
                }

                bool retW = client.TxWrite(wKeys, wValues);
                if (!retW) {
                    fprintf(stdout, "[ERROR]: Writing failed.\n");
                }

                bool retC = client.TxCommit();
                if (!retC) {
                    fprintf(stdout, "[Error]:Transaction could not be committed.\n");
                }
            } else {
                fprintf(stdout, "[Error]:Transaction could not be started.\n");
            }
        }
#endif

    }


    void experiment2(ThreadArg *arg) {
        fflush(stdout);
        try {
            srand(Utils::GetThreadId() * time(0));

            PublicTxClient client(arg->ServerNames[arg->servingPartition], arg->ServerPorts[arg->servingPartition]);

            client.setTotalNumKeyInKVStore(arg->TotalNumItems);

            std::string dummyValue(arg->NumValueBytes, 'o');
            vector<int> partition_ids;
            int pId;
            bool retR, retC, retW;
            PhysicalTimeSpec startTime, endTime, txStartTime, txEndTime;
            double txDuration, duration;

#ifdef RESERVOIR_SAMPLING
            int offsetR = 0;
            int offsetW = 0;
            int offsetTx = 0;
#endif
            // notify readiness
            arg->OpReadyEvent.Set();

            // wait to start
            arg->OpStartEvent.WaitAndReset();

            for (int i = 0; i < arg->NumPartitions; ++i) {
                partition_ids.push_back(i);
            }

            srand(time(0));
            random_shuffle(partition_ids.begin(), partition_ids.end());

            std::vector<std::string> readKeys;
            std::vector<std::string> writeKeys;
            std::vector<std::string> readValues;
            std::vector<std::string> writeValues;

            const int partitionsToReadFrom = arg->NumTxReadItems;
            const int partitionsToWriteTo = arg->NumTxWriteItems;

            arg->NumReadOpsPerTx = arg->NumTxReadItems;
            arg->NumWriteOpsPerTx = arg->NumTxWriteItems;

            fprintf(stdout, "[INFO]: Reading from %d partitions.\n", arg->NumReadOpsPerTx);
            fflush(stdout);
            fprintf(stdout, "[INFO]: Writing to %d partitions.\n", partitionsToWriteTo);
            fflush(stdout);

            if (arg->ThreadId == 0)
                _partitionsToReadFrom = partitionsToReadFrom;

            warmUp(arg, client);

#ifdef DO_EXPERIMENT
            while (!(*arg->stopOperation)) {

                /* ------------------- START TRANSACTION ------------------- */
                txStartTime = Utils::GetCurrentClockTime();

                PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
                if (client.TxStart()) {

                    PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();
                    arg->StartLatenciesSum += duration;
                    arg->TotalNumStartOpsPerThread++;

                    if (arg->MaxStartLatency < duration) {
                        arg->MaxStartLatency = duration;
                    }

                    if (arg->MinStartLatency > duration) {
                        arg->MinStartLatency = duration;
                    }

                    arg->OpStartLatencies.push_back(duration);

//                     Read items
#ifdef DO_READS
                    readKeys.clear();
                    readValues.clear();

                    /* -------------------  READ PHASE -------------------*/
                    // Read from X partitions, starting from a random one

                    pId = rand() % arg->NumPartitions;
                    for (int rCount = 0; rCount < partitionsToReadFrom; rCount++) {
                        std::string opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                        readKeys.push_back(opKey);
                        pId = (pId + 1) % arg->NumPartitions;
                    }

                    //record operation start time
                    arg->TotalNumReadItems += readKeys.size();

                    startTime = Utils::GetCurrentClockTime();
                    retR = client.TxRead(readKeys, readValues);
                    if (!retR) {
                        fprintf(stdout, "[ERROR]: Reading failed.\n");
                    }
                    endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();


                    arg->ReadLatenciesSum += duration;
                    arg->TotalNumReadOpsPerThread++;

                    if (arg->MaxReadLatency < duration) {
                        arg->MaxReadLatency = duration;
                    }

                    if (arg->MinReadLatency > duration) {
                        arg->MinReadLatency = duration;
                    }

#ifdef RESERVOIR_SAMPLING

                    if (arg->OpReadLatencies.size() < arg->ReservoirSamplingLimit) {

                        arg->OpReadLatencies.push_back(duration);
                    }
                    else {
                        offsetRead++;
                        int position = abs(random()) % (arg->ReservoirSamplingLimit + offsetRead);
                        if (position < arg->ReservoirSamplingLimit) {
                            arg->OpReadLatencies[position] = duration;
                        }
                    }
#else
                    arg->OpReadLatencies.push_back(duration);
#endif
#endif //DO_READS

#ifdef DO_WRITES

                    /* ------------------- WRITE PHASE ------------------- */
                    // Write items
                    writeKeys.clear();
                    writeValues.clear();

                    // Writing keys to M partitions, starting from a random one
                    pId = rand() % arg->NumPartitions;
                    for (int wCount = 0; wCount < partitionsToWriteTo; wCount++) {
                        std::string opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                        writeKeys.push_back(opKey);
                        writeValues.push_back(dummyValue);
                        pId = (pId + 1) % arg->NumPartitions;
                    }

                    //record operation start time
                    PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
                    retW = client.TxWrite(writeKeys, writeValues);
                    if (!retW) {
                        fprintf(stdout, "[ERROR]: Writing failed.\n");
                    }

                    endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();

                    arg->WriteLatenciesSum += duration;
                    arg->TotalNumWriteOpsPerThread++;

                    if (arg->MaxWriteLatency < duration) {
                        arg->MaxWriteLatency = duration;
                    }

                    if (arg->MinWriteLatency > duration) {
                        arg->MinWriteLatency = duration;
                    }

#ifdef RESERVOIR_SAMPLING

                    if (arg->OpWriteLatencies.size() < arg->ReservoirSamplingLimit) {

                        arg->OpWriteLatencies.push_back(duration);
                    }
                    else {
                        offsetWrite++;
                        int position = abs(random()) % (arg->ReservoirSamplingLimit + offsetWrite);
                        if (position < arg->ReservoirSamplingLimit) {
                            arg->OpWriteLatencies[position] = duration;
                        }
                    }
#else
                    arg->OpWriteLatencies.push_back(duration);
#endif
#endif //DO_WRITES

                    /* ------------------- COMMIT TRANSACTION ------------------- */
                    //record commit operation start time
                    startTime = Utils::GetCurrentClockTime();
                    retC = client.TxCommit();
                    if (!retC) {
                        fprintf(stdout, "[Error]:Transaction could not be committed.\n");
                    }
                    //record commit operation end time
                    endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();
                    arg->CommitLatenciesSum += duration;
                    arg->TotalNumCommitOpsPerThread++;

                    if (arg->MaxCommitLatency < duration) {
                        arg->MaxCommitLatency = duration;
                    }

                    if (arg->MinCommitLatency > duration) {
                        arg->MinCommitLatency = duration;
                    }

                    txEndTime = Utils::GetCurrentClockTime();
                    txDuration = (txEndTime - txStartTime).toMilliSeconds();
                    arg->OpTxLatencies.push_back(txDuration);
                    arg->TxLatenciesSum += txDuration;
                    arg->TotalNumTxOpsPerThread++;

                    if (arg->MaxTxLatency < duration) {
                        arg->MaxTxLatency = duration;
                    }

                    if (arg->MinTxLatency > duration) {
                        arg->MinTxLatency = duration;
                    }

                } else {
                    fprintf(stdout, "[Error]:Transaction could not be started.\n");
                }
            } // end while

            arg->numHotBlockKeys = 0;
            addNumReadItemsStats(arg, client);
#endif //DO_EXPERIMENT

            // notify stopping
            arg->OpEndEvent.Set();

        } catch (SocketException &e) {
            fprintf(stdout, "[ERROR]:Experiment 1: SocketException: %s\n", e.what());
            exit(1);
        }

    }

    void experiment3(ThreadArg *arg) {
        fflush(stdout);
        try {
            srand(Utils::GetThreadId() * time(0));
            PublicTxClient client(arg->ServerNames[arg->servingPartition], arg->ServerPorts[arg->servingPartition]);
            client.setTotalNumKeyInKVStore(arg->TotalNumItems);

            std::string dummyValue(arg->NumValueBytes, 'o');
            vector<int> partition_ids;
            int pId;
            bool retR, retC, retW;
            PhysicalTimeSpec startTime, endTime, txStartTime, txEndTime;
            double txDuration, duration;

#ifdef RESERVOIR_SAMPLING
            int offsetR = 0;
            int offsetW = 0;
            int offsetTx = 0;
#endif
            // notify readiness
            arg->OpReadyEvent.Set();

            // wait to start
            arg->OpStartEvent.WaitAndReset();

            for (int i = 0; i < arg->NumPartitions; ++i) {
                partition_ids.push_back(i);
            }

            srand(time(0));
            random_shuffle(partition_ids.begin(), partition_ids.end());

            std::vector<std::string> readKeys;
            std::vector<std::string> writeKeys;
            std::vector<std::string> readValues;
            std::vector<std::string> writeValues;

            const int partitionsToReadFrom = (int) (arg->ReadRatio * arg->NumPartitions / 100);
            int numKeysToRead = (int) arg->NumTxReadItems / partitionsToReadFrom;
            int remainingPartitionsToRead = arg->NumTxReadItems - numKeysToRead * partitionsToReadFrom;

            const int partitionsToWriteFrom = (int) (arg->WriteRatio * arg->NumPartitions / 100);
            int numKeysToWrite = (int) arg->NumTxWriteItems / partitionsToWriteFrom;
            int remainingPartitionsToWrite = arg->NumTxWriteItems - numKeysToWrite * partitionsToWriteFrom;


            arg->NumReadOpsPerTx =  arg->NumTxReadItems;
            arg->NumWriteOpsPerTx = arg->NumTxWriteItems;
            arg->NumCommitOpsPerTx = 1;

            fprintf(stdout, "[INFO]: Reading %d keys from %d partitions(plus %d keys more).\n", numKeysToRead,
                    partitionsToReadFrom, remainingPartitionsToRead);
            fflush(stdout);
            fprintf(stdout, "[INFO]: Writing %d keys from %d partitions(plus %d keys more).\n", numKeysToWrite,
                    partitionsToWriteFrom, remainingPartitionsToWrite);
            fflush(stdout);

            if (arg->ThreadId == 0)
                _partitionsToReadFrom = partitionsToReadFrom;

            warmUp(arg, client);

#ifdef DO_EXPERIMENT
            while (!(*arg->stopOperation)) {

                /* ------------------- START TRANSACTION ------------------- */
                txStartTime = Utils::GetCurrentClockTime();

                PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
                if (client.TxStart()) {

                    PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();
                    arg->StartLatenciesSum += duration;
                    arg->TotalNumStartOpsPerThread++;

                    if (arg->MaxStartLatency < duration) {
                        arg->MaxStartLatency = duration;
                    }

                    if (arg->MinStartLatency > duration) {
                        arg->MinStartLatency = duration;
                    }

                    arg->OpStartLatencies.push_back(duration);
//                     Read items
#ifdef DO_READS
                    readKeys.clear();
                    readValues.clear();

                    /* -------------------  READ PHASE -------------------*/
                    // Read from N partitions, starting from a random one
                    pId = rand() % arg->NumPartitions;
                    for (int rCount = 0; rCount < partitionsToReadFrom; rCount++) {
                        for (int kCount = 0; kCount < numKeysToRead; kCount++) {
                            std::string opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                            readKeys.push_back(opKey);
                        }
                        if (remainingPartitionsToRead > 0) {
                            std::string opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                            readKeys.push_back(opKey);
                            remainingPartitionsToRead--;
                        }
                        pId = (pId + 1) % arg->NumPartitions;
                    }

                    arg->TotalNumReadItems += readKeys.size();

                    //record read operation start time
                    startTime = Utils::GetCurrentClockTime();
                    retR = client.TxRead(readKeys, readValues);
                    if (!retR) {
                        fprintf(stdout, "[ERROR]: Reading failed.\n");
                    }
                    endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();


                    arg->ReadLatenciesSum += duration;
                    arg->TotalNumReadOpsPerThread++;

                    if (arg->MaxReadLatency < duration) {
                        arg->MaxReadLatency = duration;
                    }

                    if (arg->MinReadLatency > duration) {
                        arg->MinReadLatency = duration;
                    }

#ifdef RESERVOIR_SAMPLING

                    if (arg->OpReadLatencies.size() < arg->ReservoirSamplingLimit) {

                        arg->OpReadLatencies.push_back(duration);
                    }
                    else {
                        offsetRead++;
                        int position = abs(random()) % (arg->ReservoirSamplingLimit + offsetRead);
                        if (position < arg->ReservoirSamplingLimit) {
                            arg->OpReadLatencies[position] = duration;
                        }
                    }
#else
                    arg->OpReadLatencies.push_back(duration);
#endif
#endif //DO_READS

#ifdef DO_WRITES

                    /* ------------------- WRITE PHASE ------------------- */
                    // Write items
                    writeKeys.clear();
                    writeValues.clear();

                    // Writing keys to M partitions, starting from a random one
                    pId = rand() % arg->NumPartitions;
                    for (int wCount = 0; wCount < partitionsToWriteFrom; wCount++) {
                        for (int kCount = 0; kCount < numKeysToWrite; kCount++) {
                            std::string opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                            writeKeys.push_back(opKey);
                            writeValues.push_back(dummyValue);
                        }
                        if (remainingPartitionsToWrite > 0) {
                            std::string opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                            writeKeys.push_back(opKey);
                            writeValues.push_back(dummyValue);
                            remainingPartitionsToWrite--;
                        }

                        pId = (pId + 1) % arg->NumPartitions;
                    }

                    //record write operation start time
                    startTime = Utils::GetCurrentClockTime();
                    retW = client.TxWrite(writeKeys, writeValues);
                    if (!retW) {
                        fprintf(stdout, "[ERROR]: Writing failed.\n");
                    }
                    endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();

                    arg->WriteLatenciesSum += duration;
                    arg->TotalNumWriteOpsPerThread++;

                    if (arg->MaxWriteLatency < duration) {
                        arg->MaxWriteLatency = duration;
                    }

                    if (arg->MinWriteLatency > duration) {
                        arg->MinWriteLatency = duration;
                    }

#ifdef RESERVOIR_SAMPLING

                    if (arg->OpWriteLatencies.size() < arg->ReservoirSamplingLimit) {

                        arg->OpWriteLatencies.push_back(duration);
                    }
                    else {
                        offsetWrite++;
                        int position = abs(random()) % (arg->ReservoirSamplingLimit + offsetWrite);
                        if (position < arg->ReservoirSamplingLimit) {
                            arg->OpWriteLatencies[position] = duration;
                        }
                    }
#else
                    arg->OpWriteLatencies.push_back(duration);
#endif
#endif //DO_WRITES

                    /* ------------------- COMMIT TRANSACTION ------------------- */
                    // Commit transaction

                    //record commit operation start time
                    startTime = Utils::GetCurrentClockTime();
                    retC = client.TxCommit();
                    if (!retC) {
                        fprintf(stdout, "[Error]:Transaction could not be committed.\n");
                    }
                    //record commit operation end time
                    endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();
                    arg->CommitLatenciesSum += duration;
                    arg->TotalNumCommitOpsPerThread++;

                    if (arg->MaxCommitLatency < duration) {
                        arg->MaxCommitLatency = duration;
                    }

                    if (arg->MinCommitLatency > duration) {
                        arg->MinCommitLatency = duration;
                    }

                    //Record transaction duration
                    txEndTime = Utils::GetCurrentClockTime();
                    txDuration = (txEndTime - txStartTime).toMilliSeconds();
                    arg->OpTxLatencies.push_back(txDuration);
                    arg->TxLatenciesSum += txDuration;
                    arg->TotalNumTxOpsPerThread++;

                    if (arg->MaxTxLatency < duration) {
                        arg->MaxTxLatency = duration;
                    }

                    if (arg->MinTxLatency > duration) {
                        arg->MinTxLatency = duration;
                    }

                } else {
                    fprintf(stdout, "[Error]:Transaction could not be started.\n");
                }
            } // end while

            arg->numHotBlockKeys = 0;
            addNumReadItemsStats(arg, client);

#endif //DO_EXPERIMENT

            // notify stopping
            arg->OpEndEvent.Set();

        } catch (SocketException &e) {
            fprintf(stdout, "[ERROR]:Experiment 1: SocketException: %s\n", e.what());
            exit(1);
        }

    }

    void experiment4(ThreadArg *arg) {
        fflush(stdout);
        try {
            srand(Utils::GetThreadId() * time(0));
            PublicTxClient client(arg->ServerNames[arg->servingPartition], arg->ServerPorts[arg->servingPartition]);
            client.setTotalNumKeyInKVStore(arg->TotalNumItems);

            std::string dummyValue(arg->NumValueBytes, 'o');
            vector<int> partition_ids;
            int pId;
            bool retR, retC, retW;
            PhysicalTimeSpec startTime, endTime, txStartTime, txEndTime;
            double txDuration, duration;

#ifdef RESERVOIR_SAMPLING
            int offsetR = 0;
            int offsetW = 0;
            int offsetTx = 0;
#endif
            // notify readiness
            arg->OpReadyEvent.Set();

            // wait to start
            arg->OpStartEvent.WaitAndReset();

            for (int i = 0; i < arg->NumPartitions; ++i) {
                partition_ids.push_back(i);
            }

            srand(time(0));
            random_shuffle(partition_ids.begin(), partition_ids.end());

            std::vector<std::string> readKeys;
            std::vector<std::string> writeKeys;
            std::vector<std::string> readValues;
            std::vector<std::string> writeValues;

            /*In Experiment 4 the ReadRatio and WriteRatio serve as number of partitions
             * to read from and write to respectively*/
            const int partitionsToReadFrom = arg->ReadRatio;
            int numKeysToRead = (int) arg->NumTxReadItems / partitionsToReadFrom;
            int remainingPartitionsToRead = arg->NumTxReadItems - numKeysToRead * partitionsToReadFrom;

            const int partitionsToWriteFrom = arg->WriteRatio;
            int numKeysToWrite = (int) arg->NumTxWriteItems / partitionsToWriteFrom;
            int remainingPartitionsToWrite = arg->NumTxWriteItems - numKeysToWrite * partitionsToWriteFrom;


            arg->NumReadOpsPerTx = arg->ReadRatio;
            arg->NumWriteOpsPerTx = arg->WriteRatio;
            arg->NumCommitOpsPerTx = 1;

            fprintf(stdout, "[INFO]: Reading %d keys from %d partitions(plus %d keys more).\n", numKeysToRead,
                    partitionsToReadFrom, remainingPartitionsToRead);
            fflush(stdout);
            fprintf(stdout, "[INFO]: Writing %d keys from %d partitions(plus %d keys more).\n", numKeysToWrite,
                    partitionsToWriteFrom, remainingPartitionsToWrite);
            fflush(stdout);

            if (arg->ThreadId == 0)
                _partitionsToReadFrom = partitionsToReadFrom;

            warmUp(arg, client);

#ifdef DO_EXPERIMENT
            while (!(*arg->stopOperation)) {

                /* ------------------- START TRANSACTION ------------------- */
                txStartTime = Utils::GetCurrentClockTime();

                PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
                if (client.TxStart()) {

                    PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();
                    arg->StartLatenciesSum += duration;
                    arg->TotalNumStartOpsPerThread++;

                    if (arg->MaxStartLatency < duration) {
                        arg->MaxStartLatency = duration;
                    }

                    if (arg->MinStartLatency > duration) {
                        arg->MinStartLatency = duration;
                    }

                    arg->OpStartLatencies.push_back(duration);
//                     Read items
#ifdef DO_READS
                    readKeys.clear();
                    readValues.clear();

                    /* -------------------  READ PHASE -------------------*/
                    // Read from N partitions, starting from a random one
                    pId = rand() % arg->NumPartitions;
                    for (int rCount = 0; rCount < partitionsToReadFrom; rCount++) {
                        for (int kCount = 0; kCount < numKeysToRead; kCount++) {
                            std::string opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                            readKeys.push_back(opKey);
                        }
                        if (remainingPartitionsToRead > 0) {
                            std::string opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                            readKeys.push_back(opKey);
                            remainingPartitionsToRead--;
                        }
                        pId = (pId + 1) % arg->NumPartitions;
                    }

                    arg->TotalNumReadItems += readKeys.size();

                    //record read operation start time
                    startTime = Utils::GetCurrentClockTime();
                    retR = client.TxRead(readKeys, readValues);
                    if (!retR) {
                        fprintf(stdout, "[ERROR]: Reading failed.\n");
                    }
                    endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();


                    arg->ReadLatenciesSum += duration;
                    arg->TotalNumReadOpsPerThread++;

                    if (arg->MaxReadLatency < duration) {
                        arg->MaxReadLatency = duration;
                    }

                    if (arg->MinReadLatency > duration) {
                        arg->MinReadLatency = duration;
                    }

#ifdef RESERVOIR_SAMPLING

                    if (arg->OpReadLatencies.size() < arg->ReservoirSamplingLimit) {

                        arg->OpReadLatencies.push_back(duration);
                    }
                    else {
                        offsetRead++;
                        int position = abs(random()) % (arg->ReservoirSamplingLimit + offsetRead);
                        if (position < arg->ReservoirSamplingLimit) {
                            arg->OpReadLatencies[position] = duration;
                        }
                    }
#else
                    arg->OpReadLatencies.push_back(duration);
#endif
#endif //DO_READS

#ifdef DO_WRITES

                    /* ------------------- WRITE PHASE ------------------- */
                    // Write items
                    writeKeys.clear();
                    writeValues.clear();

                    // Writing keys to M partitions, starting from a random one
                    pId = rand() % arg->NumPartitions;
                    for (int wCount = 0; wCount < partitionsToWriteFrom; wCount++) {
                        for (int kCount = 0; kCount < numKeysToWrite; kCount++) {
                            std::string opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                            writeKeys.push_back(opKey);
                            writeValues.push_back(dummyValue);
                        }
                        if (remainingPartitionsToWrite > 0) {
                            std::string opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                            writeKeys.push_back(opKey);
                            writeValues.push_back(dummyValue);
                            remainingPartitionsToWrite--;
                        }

                        pId = (pId + 1) % arg->NumPartitions;
                    }

                    //record write operation start time
                    startTime = Utils::GetCurrentClockTime();
                    retW = client.TxWrite(writeKeys, writeValues);
                    if (!retW) {
                        fprintf(stdout, "[ERROR]: Writing failed.\n");
                    }
                    endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();

                    arg->WriteLatenciesSum += duration;
                    arg->TotalNumWriteOpsPerThread++;

                    if (arg->MaxWriteLatency < duration) {
                        arg->MaxWriteLatency = duration;
                    }

                    if (arg->MinWriteLatency > duration) {
                        arg->MinWriteLatency = duration;
                    }

#ifdef RESERVOIR_SAMPLING

                    if (arg->OpWriteLatencies.size() < arg->ReservoirSamplingLimit) {

                        arg->OpWriteLatencies.push_back(duration);
                    }
                    else {
                        offsetWrite++;
                        int position = abs(random()) % (arg->ReservoirSamplingLimit + offsetWrite);
                        if (position < arg->ReservoirSamplingLimit) {
                            arg->OpWriteLatencies[position] = duration;
                        }
                    }
#else
                    arg->OpWriteLatencies.push_back(duration);
#endif
#endif //DO_WRITES

                    /* ------------------- COMMIT TRANSACTION ------------------- */
                    // Commit transaction

                    //record commit operation start time
                    startTime = Utils::GetCurrentClockTime();
                    retC = client.TxCommit();
                    if (!retC) {
                        fprintf(stdout, "[Error]:Transaction could not be committed.\n");
                    }
                    //record commit operation end time
                    endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();
                    arg->CommitLatenciesSum += duration;
                    arg->TotalNumCommitOpsPerThread++;

                    if (arg->MaxCommitLatency < duration) {
                        arg->MaxCommitLatency = duration;
                    }

                    if (arg->MinCommitLatency > duration) {
                        arg->MinCommitLatency = duration;
                    }

                    //Record transaction duration
                    txEndTime = Utils::GetCurrentClockTime();
                    txDuration = (txEndTime - txStartTime).toMilliSeconds();
                    arg->OpTxLatencies.push_back(txDuration);
                    arg->TxLatenciesSum += txDuration;
                    arg->TotalNumTxOpsPerThread++;

                    if (arg->MaxTxLatency < duration) {
                        arg->MaxTxLatency = duration;
                    }

                    if (arg->MinTxLatency > duration) {
                        arg->MinTxLatency = duration;
                    }

                } else {
                    fprintf(stdout, "[Error]:Transaction could not be started.\n");
                }
            } // end while

            arg->numHotBlockKeys = 0;
            addNumReadItemsStats(arg, client);

#endif //DO_EXPERIMENT

            // notify stopping
            arg->OpEndEvent.Set();

        } catch (SocketException &e) {
            fprintf(stdout, "[ERROR]:Experiment 1: SocketException: %s\n", e.what());
            exit(1);
        }

    }

    void experiment5(ThreadArg *arg) {
        fflush(stdout);
        try {
            srand(Utils::GetThreadId() * time(0));
            PublicTxClient client(arg->ServerNames[arg->servingPartition], arg->ServerPorts[arg->servingPartition]);
            client.setTotalNumKeyInKVStore(arg->TotalNumItems);

            std::string dummyValue(arg->NumValueBytes, 'o');
            vector<int> partition_ids;
            int pId;
            bool retR, retC, retW;
            PhysicalTimeSpec startTime, endTime, txStartTime, txEndTime;
            double txDuration, duration;

#ifdef RESERVOIR_SAMPLING
            int offsetR = 0;
            int offsetW = 0;
            int offsetTx = 0;
#endif
            // notify readiness
            arg->OpReadyEvent.Set();

            // wait to start
            arg->OpStartEvent.WaitAndReset();

            for (int i = 0; i < arg->NumPartitions; ++i) {
                partition_ids.push_back(i);
            }

            srand(time(0));
            random_shuffle(partition_ids.begin(), partition_ids.end());

            std::vector<std::string> readKeys;
            std::vector<std::string> writeKeys;
            std::vector<std::string> readValues;
            std::vector<std::string> writeValues;

            const int partitionsToReadFrom = (int) (IntToDouble(arg->ReadRatio) *
                                                    ((double) partition_ids.size())) > 0 ?
                                             (int) ((IntToDouble(arg->ReadRatio) * ((double) partition_ids.size())))
                                                                                         : 1;

            const int partitionsToWriteTo = (int) (IntToDouble(arg->WriteRatio) *
                                                   ((double) partition_ids.size())) > 0 ?
                                            (int) ((IntToDouble(arg->WriteRatio) * ((double) partition_ids.size())))
                                                                                        : 1;

            arg->NumReadOpsPerTx = partitionsToReadFrom;
            arg->NumWriteOpsPerTx = partitionsToWriteTo;
            arg->NumCommitOpsPerTx = 1;

            fprintf(stdout, "[INFO]: Reading from %d partitions.\n", partitionsToReadFrom);
            fflush(stdout);
            fprintf(stdout, "[INFO]: Writing to %d partitions.\n", partitionsToWriteTo);
            fflush(stdout);

            if (arg->ThreadId == 0)
                _partitionsToReadFrom = partitionsToReadFrom;

            warmUp(arg, client);

#ifdef DO_EXPERIMENT
            while (!(*arg->stopOperation)) {

                /* ------------------- START TRANSACTION ------------------- */
                txStartTime = Utils::GetCurrentClockTime();

                PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
                if (client.TxStart()) {

                    PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();
                    arg->StartLatenciesSum += duration;
                    arg->TotalNumStartOpsPerThread++;

                    if (arg->MaxStartLatency < duration) {
                        arg->MaxStartLatency = duration;
                    }

                    if (arg->MinStartLatency > duration) {
                        arg->MinStartLatency = duration;
                    }

                    arg->OpStartLatencies.push_back(duration);

//                     Read items
#ifdef DO_READS
                    readKeys.clear();
                    readValues.clear();

                    /* -------------------  READ PHASE -------------------*/
                    // Read from N partitions, starting from a random one
                    pId = rand() % arg->NumPartitions;
                    for (int rCount = 0; rCount < partitionsToReadFrom; rCount++) {
                        std::string opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                        readKeys.push_back(opKey);
                        //record read operation start time
                        startTime = Utils::GetCurrentClockTime();

                        retR = client.TxRead(readKeys, readValues);
                        arg->TotalNumReadItems += readKeys.size();

                        if (!retR) {
                            fprintf(stdout, "[ERROR]: Reading failed.\n");
                        }

                        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                        duration = (endTime - startTime).toMilliSeconds();

                        readKeys.clear();
                        readValues.clear();

                        arg->ReadLatenciesSum += duration;
                        arg->TotalNumReadOpsPerThread++;

                        if (arg->MaxReadLatency < duration) {
                            arg->MaxReadLatency = duration;
                        }

                        if (arg->MinReadLatency > duration) {
                            arg->MinReadLatency = duration;
                        }

#ifdef RESERVOIR_SAMPLING

                        if (arg->OpReadLatencies.size() < arg->ReservoirSamplingLimit) {

                        arg->OpReadLatencies.push_back(duration);
                    }
                    else {
                        offsetRead++;
                        int position = abs(random()) % (arg->ReservoirSamplingLimit + offsetRead);
                        if (position < arg->ReservoirSamplingLimit) {
                            arg->OpReadLatencies[position] = duration;
                        }
                    }
#else
                        arg->OpReadLatencies.push_back(duration);
#endif

                        pId = (pId + 1) % arg->NumPartitions;
                    }


#endif //DO_READS

#ifdef DO_WRITES

                    /* ------------------- WRITE PHASE ------------------- */
                    // Write items
                    writeKeys.clear();
                    writeValues.clear();

                    // Writing keys to M partitions, starting from a random one
                    pId = rand() % arg->NumPartitions;
                    for (int wCount = 0; wCount < partitionsToWriteTo; wCount++) {
                        std::string opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                        writeKeys.push_back(opKey);
                        writeValues.push_back(dummyValue);
                        pId = (pId + 1) % arg->NumPartitions;
                        //record write operation start time
                        startTime = Utils::GetCurrentClockTime();

                        retW = client.TxWrite(writeKeys, writeValues);
                        if (!retW) {
                            fprintf(stdout, "[ERROR]: Writing failed.\n");
                        }

                        endTime = Utils::GetCurrentClockTime();
                        duration = (endTime - startTime).toMilliSeconds();

                        writeKeys.clear();
                        writeValues.clear();

                        arg->WriteLatenciesSum += duration;
                        arg->TotalNumWriteOpsPerThread++;

                        if (arg->MaxWriteLatency < duration) {
                            arg->MaxWriteLatency = duration;
                        }

                        if (arg->MinWriteLatency > duration) {
                            arg->MinWriteLatency = duration;
                        }


                        arg->OpWriteLatencies.push_back(duration);
                    }


#endif //DO_WRITES

                    /* ------------------- COMMIT TRANSACTION ------------------- */
                    // Commit transaction

                    //record commit operation start time
                    startTime = Utils::GetCurrentClockTime();
                    retC = client.TxCommit();
                    if (!retC) {
                        fprintf(stdout, "[Error]:Transaction could not be committed.\n");
                    }
                    //record commit operation end time
                    endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();
                    arg->CommitLatenciesSum += duration;
                    arg->TotalNumCommitOpsPerThread++;

                    if (arg->MaxCommitLatency < duration) {
                        arg->MaxCommitLatency = duration;
                    }

                    if (arg->MinCommitLatency > duration) {
                        arg->MinCommitLatency = duration;
                    }

                    //Record transaction duration
                    txEndTime = Utils::GetCurrentClockTime();
                    txDuration = (txEndTime - txStartTime).toMilliSeconds();
                    arg->OpTxLatencies.push_back(txDuration);
                    arg->TxLatenciesSum += txDuration;
                    arg->TotalNumTxOpsPerThread++;

                    if (arg->MaxTxLatency < duration) {
                        arg->MaxTxLatency = duration;
                    }

                    if (arg->MinTxLatency > duration) {
                        arg->MinTxLatency = duration;
                    }

                } else {
                    fprintf(stdout, "[Error]:Transaction could not be started.\n");
                }
            } // end while

            arg->numHotBlockKeys = 0;
            addNumReadItemsStats(arg, client);

#endif //DO_EXPERIMENT

            // notify stopping
            arg->OpEndEvent.Set();

        } catch (SocketException &e) {
            fprintf(stdout, "[ERROR]:Experiment 1: SocketException: %s\n", e.what());
            exit(1);
        }

    }

    void experiment6(ThreadArg *arg) {
        fflush(stdout);
        try {
            srand(Utils::GetThreadId() * time(0));

            PublicTxClient client(arg->ServerNames[arg->servingPartition], arg->ServerPorts[arg->servingPartition]);

            client.setTotalNumKeyInKVStore(arg->TotalNumItems);

            std::string dummyValue(arg->NumValueBytes, 'o');
            vector<int> partition_ids;
            int pId;
            bool retR, retC, retW;
            PhysicalTimeSpec startTime, endTime, txStartTime, txEndTime;
            double txDuration, duration;

#ifdef RESERVOIR_SAMPLING
            int offsetR = 0;
            int offsetW = 0;
            int offsetTx = 0;
#endif
            // notify readiness
            arg->OpReadyEvent.Set();

            // wait to start
            arg->OpStartEvent.WaitAndReset();

            for (int i = 0; i < arg->NumPartitions; ++i) {
                partition_ids.push_back(i);
            }

            srand(time(0));
            random_shuffle(partition_ids.begin(), partition_ids.end());

            std::vector<std::string> readKeys;
            std::vector<std::string> writeKeys;
            std::vector<std::string> readValues;
            std::vector<std::string> writeValues;

            const int partitionsToReadFrom = arg->NumTxReadItems;
            const int partitionsToWriteTo = arg->NumTxWriteItems;

            arg->NumReadOpsPerTx = arg->NumTxReadItems;
            arg->NumWriteOpsPerTx = arg->NumTxWriteItems;

            fprintf(stdout, "[INFO]: Reading from %d partitions.\n", arg->NumReadOpsPerTx);
            fflush(stdout);
            fprintf(stdout, "[INFO]: Writing to %d partitions.\n", partitionsToWriteTo);
            fflush(stdout);

            if (arg->ThreadId == 0)
                _partitionsToReadFrom = partitionsToReadFrom;

            warmUp(arg, client);

#ifdef DO_EXPERIMENT
            while (!(*arg->stopOperation)) {

                /* ------------------- START TRANSACTION ------------------- */
                txStartTime = Utils::GetCurrentClockTime();

                PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
                if (client.TxStart()) {

                    PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();
                    arg->StartLatenciesSum += duration;
                    arg->TotalNumStartOpsPerThread++;

                    if (arg->MaxStartLatency < duration) {
                        arg->MaxStartLatency = duration;
                    }

                    if (arg->MinStartLatency > duration) {
                        arg->MinStartLatency = duration;
                    }

                    arg->OpStartLatencies.push_back(duration);

//                     Read items
#ifdef DO_READS
                    readKeys.clear();
                    readValues.clear();

                    /* -------------------  READ PHASE -------------------*/
                    // Read from X partitions, starting from a random one

                    pId = rand() % arg->NumPartitions;
                    for (int rCount = 0; rCount < partitionsToReadFrom; rCount++) {
                        std::string opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                        readKeys.push_back(opKey);
                        pId = (pId + 1) % arg->NumPartitions;
                        //record operation start time
                        startTime = Utils::GetCurrentClockTime();
                        retR = client.TxRead(readKeys, readValues);
                        arg->TotalNumReadItems += readKeys.size();

                        if (!retR) {
                            fprintf(stdout, "[ERROR]: Reading failed.\n");
                        }

                        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                        duration = (endTime - startTime).toMilliSeconds();
                        readKeys.clear();
                        readValues.clear();

                        arg->ReadLatenciesSum += duration;
                        arg->TotalNumReadOpsPerThread++;

                        if (arg->MaxReadLatency < duration) {
                            arg->MaxReadLatency = duration;
                        }

                        if (arg->MinReadLatency > duration) {
                            arg->MinReadLatency = duration;
                        }


                        arg->OpReadLatencies.push_back(duration);
                    }


#endif //DO_READS

#ifdef DO_WRITES

                    /* ------------------- WRITE PHASE ------------------- */
                    // Write items
                    writeKeys.clear();
                    writeValues.clear();

                    // Writing keys to M partitions, starting from a random one
                    pId = rand() % arg->NumPartitions;
                    for (int wCount = 0; wCount < partitionsToWriteTo; wCount++) {
                        std::string opKey = GetKeyAtPartition(arg->generator, pId, arg->NumPartitions);
                        writeKeys.push_back(opKey);
                        writeValues.push_back(dummyValue);
                        pId = (pId + 1) % arg->NumPartitions;
                        //record operation start time
                        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
                        retW = client.TxWrite(writeKeys, writeValues);
                        if (!retW) {
                            fprintf(stdout, "[ERROR]: Writing failed.\n");
                        }

                        endTime = Utils::GetCurrentClockTime();
                        duration = (endTime - startTime).toMilliSeconds();
                        writeKeys.clear();
                        writeValues.clear();

                        arg->WriteLatenciesSum += duration;
                        arg->TotalNumWriteOpsPerThread++;

                        if (arg->MaxWriteLatency < duration) {
                            arg->MaxWriteLatency = duration;
                        }

                        if (arg->MinWriteLatency > duration) {
                            arg->MinWriteLatency = duration;
                        }


                        arg->OpWriteLatencies.push_back(duration);
                    }


#endif //DO_WRITES

                    /* ------------------- COMMIT TRANSACTION ------------------- */
                    //record commit operation start time
                    startTime = Utils::GetCurrentClockTime();
                    retC = client.TxCommit();
                    if (!retC) {
                        fprintf(stdout, "[Error]:Transaction could not be committed.\n");
                    }
                    //record commit operation end time
                    endTime = Utils::GetCurrentClockTime();
                    duration = (endTime - startTime).toMilliSeconds();
                    arg->CommitLatenciesSum += duration;
                    arg->TotalNumCommitOpsPerThread++;

                    if (arg->MaxCommitLatency < duration) {
                        arg->MaxCommitLatency = duration;
                    }

                    if (arg->MinCommitLatency > duration) {
                        arg->MinCommitLatency = duration;
                    }

                    txEndTime = Utils::GetCurrentClockTime();
                    txDuration = (txEndTime - txStartTime).toMilliSeconds();
                    arg->OpTxLatencies.push_back(txDuration);
                    arg->TxLatenciesSum += txDuration;
                    arg->TotalNumTxOpsPerThread++;

                    if (arg->MaxTxLatency < duration) {
                        arg->MaxTxLatency = duration;
                    }

                    if (arg->MinTxLatency > duration) {
                        arg->MinTxLatency = duration;
                    }

                } else {
                    fprintf(stdout, "[Error]:Transaction could not be started.\n");
                }
            } // end while

            arg->numHotBlockKeys = 0;
            addNumReadItemsStats(arg, client);

#endif //DO_EXPERIMENT

            // notify stopping
            arg->OpEndEvent.Set();

        } catch (SocketException &e) {
            fprintf(stdout, "[ERROR]:Experiment 1: SocketException: %s\n", e.what());
            exit(1);
        }

    }


/* >>>>>>>>>>>>>>>>>>>>>>>>        Experiment functions    <<<<<<<<<<<<<<<<<<<<<<<<<<<<<< */


    Experiment::Experiment(char *argv[]) {

        this->config.RequestsDistribution = argv[2];
        this->config.RequestsDistributionParameter = atof(argv[3]);
        this->config.ReadRatio = atoi(argv[4]);
        this->config.WriteRatio = atoi(argv[5]);

        this->config.GroupServerName = argv[6];
        this->config.GroupServerPort = atoi(argv[7]);

        GroupClient groupClient(config.GroupServerName, config.GroupServerPort);
        allPartitions = groupClient.GetRegisteredPartitions();
        this->config.NumPartitions = allPartitions.size();
        this->config.NumReplicasPerPartition = allPartitions[0].size();

        this->config.TotalNumItems = atoi(argv[8]);
        this->config.NumValueBytes = atoi(argv[9]);
        this->config.NumThreads = atoi(argv[11]);
        this->config.ServingPartitionId = atoi(argv[12]);
        this->config.ServingReplicaId = atoi(argv[13]);
        this->config.clientServerName = argv[14];
        this->config.expResultsOutputFileName = argv[15];
        this->config.reservoirSampling = (strcmp(argv[16], "true") == 0);
        this->config.reservoirSamplingLimit = atoi(argv[17]);
        this->config.warmUpDuration = 5000; //milliseconds
        this->config.experimentDuration = atof(argv[18]);
        this->config.experimentType = atoi(argv[19]);
        this->config.clientResetNumber = atoi(argv[20]);
        this->config.enableClientReset = (strcmp(argv[21], "true") == 0);
        this->config.numTxReadItems = atoi(argv[22]);
        this->config.numTxWriteItems = atoi(argv[23]);
        this->config.ReadTxRatio = 0;
        this->config.NumOpsPerThread = 0;
        this->config.TotalNumOps = 0;
        this->config.TotalNumReadItems = 0;
        this->stopOperation = false;
        this->config.locality = 0;
        this->config.numItemsReadFromStore = 0;
        this->config.numItemsReadFromClientWriteCache = 0;
        this->config.numItemsReadFromClientReadSet = 0;
        this->config.numItemsReadFromClientWriteSet = 0;
        this->config.totalNumClientReadItems = 0;

//        printExperimentParameters();
    }

    void Experiment::printExperimentParameters() {
        fprintf(stdout, "##EXPERIMENT TCC\n");
        fprintf(stdout, "##____________________________________\n");
        fprintf(stdout, "##EXPERIMENT_TYPE %d \n", config.experimentType);
        fprintf(stdout, "##REQUEST_DISTRIBUTION %s \n", config.RequestsDistribution.c_str());
        fprintf(stdout, "##REQUEST_DISTRIBUTION_PARAM %lf \n", config.RequestsDistributionParameter);
        fprintf(stdout, "##GROUPSERVER_NAME %s \n", config.GroupServerName.c_str());
        fprintf(stdout, "##GROUPSERVER_PORT %d \n", config.GroupServerPort);
        fprintf(stdout, "##NUM_PARTITIONS %d \n", config.NumPartitions);
        fprintf(stdout, "##NUM_REPLICAS_PER_PARTITION %d \n", config.NumReplicasPerPartition);
        fprintf(stdout, "##TOTAL_NUM_ITEMS %d \n", config.TotalNumItems);
        fprintf(stdout, "##NUM_VALUE_BYTES %d \n", config.NumValueBytes);
        fprintf(stdout, "##NUM_OPS_PER_THERAD %d \n", config.NumOpsPerThread);
        fprintf(stdout, "##NUM_THREADS %d \n", config.NumThreads);
        fprintf(stdout, "##SERVING_PARTITION_ID %d \n", config.ServingPartitionId);
        fprintf(stdout, "##SERVING_REPLICA %d \n", config.ServingReplicaId);
        fprintf(stdout, "##CLIENT_SERVER_NAME %s \n", config.clientServerName.c_str());
        fprintf(stdout, "##LATENCY OUTPUT FILE NAME %s \n", config.expResultsOutputFileName.c_str());
        fprintf(stdout, "##RESERVOIR SAMPLING %d \n", config.reservoirSampling);
        fprintf(stdout, "##RESERVOIR SAMPLING LIMIT %d \n", config.reservoirSamplingLimit);
        fprintf(stdout, "##EXPERIMENT DURATION %d \n", config.experimentDuration);
        fprintf(stdout, "##WARM_UP DURATION %d \n", config.warmUpDuration);
        fprintf(stdout, "##NUM_TxReadItems %d \n", config.numTxReadItems);
        fprintf(stdout, "##NUM_TxWriteItems %d \n", config.numTxWriteItems);
        fprintf(stdout, "##ReadRatio %d\n", config.ReadRatio);
        fprintf(stdout, "##WriteRatio %d\n", config.WriteRatio);

        fflush(stdout);
    }

    void Experiment::buildThreadArguments() {
        for (int i = 0; i < config.NumThreads; i++) {
            ThreadArg *arg = new ThreadArg;
            arg->ThreadId = i;

            int partitionIndex = config.ServingPartitionId;
            arg->LocalPartitionId = partitionIndex;
            int replicaIndex = config.ServingReplicaId;

            for (int k = 0; k < config.NumPartitions; ++k) {
                DBPartition p = allPartitions[k][replicaIndex];
                arg->ServerNames.push_back(p.Name);
                arg->ServerPorts.push_back(p.PublicPort);
            }

            arg->NumPartitions = config.NumPartitions;
            arg->NumReplicas = config.NumReplicasPerPartition;
            arg->TotalNumItems = config.TotalNumItems;

            arg->warmUpDuration = config.warmUpDuration;

            arg->NumTxsPerThread = 0;
            arg->NumValueBytes = config.NumValueBytes;

            arg->ReadRatio = config.ReadRatio;
            arg->WriteRatio = config.WriteRatio;
            arg->TxRatio = config.ReadTxRatio;

            arg->ReservoirSampling = config.reservoirSampling;
            arg->ReservoirSamplingLimit = config.reservoirSamplingLimit;
            arg->stopOperation = &(this->stopOperation);

            arg->servingPartition = config.ServingPartitionId;
            arg->servingReplica = config.ServingReplicaId;

            arg->StartLatenciesSum = 0;
            arg->ReadLatenciesSum = 0;
            arg->WriteLatenciesSum = 0;
            arg->CommitLatenciesSum = 0;
            arg->TxLatenciesSum = 0;

            arg->TotalNumStartOpsPerThread = 0;
            arg->TotalNumReadOpsPerThread = 0;
            arg->TotalNumWriteOpsPerThread = 0;
            arg->TotalNumCommitOpsPerThread = 0;
            arg->TotalNumTxOpsPerThread = 0;

            arg->MinStartLatency = 100000000;
            arg->MaxStartLatency = 0;
            arg->MinReadLatency = 100000000;
            arg->MaxReadLatency = 0;
            arg->MinWriteLatency = 100000000;
            arg->MaxWriteLatency = 0;
            arg->MinCommitLatency = 100000000;
            arg->MaxCommitLatency = 0;

            arg->MinTxLatency = 100000000;
            arg->MaxTxLatency = 0;


            arg->numHotBlockKeys = 0;
            arg->clientResetNumber = config.clientResetNumber;
            arg->enableClientReset = config.enableClientReset;

            arg->NumTxReadItems = config.numTxReadItems;
            arg->NumTxWriteItems = config.numTxWriteItems;

            arg->RequestsDistribution = config.RequestsDistribution;
            arg->locality = config.locality;

            arg->TotalNumReadItems = 0;

            arg->numItemsReadFromClientWriteSet = 0;
            arg->numItemsReadFromClientReadSet = 0;
            arg->numItemsReadFromClientWriteCache = 0;
            arg->numItemsReadFromStore = 0;

            initializeThreadRequestDistribution(arg);

            threadArgs.push_back(arg);
            opReadyEvents.push_back(&arg->OpReadyEvent);
            opEndEvents.push_back(&arg->OpEndEvent);

        }
    }

    void Experiment::initializeThreadRequestDistribution(ThreadArg *t) {

        int numItems;
        if (config.experimentType == 5)
            numItems = (config.TotalNumItems / config.NumPartitions) / config.NumReplicasPerPartition - 1;
        else
            numItems = config.TotalNumItems / config.NumPartitions - 1;

        if (t->RequestsDistribution == "uniform") {
            srand(Utils::GetThreadId() * time(0));
            std::this_thread::sleep_for(std::chrono::milliseconds(10 * rand() % 10));
            t->generator = new UniformGenerator(numItems);

        } else if (t->RequestsDistribution == "zipfian") {
            t->generator = new ZipfianGenerator(t->ThreadId, 0, numItems,
                                                config.RequestsDistributionParameter);

        } else if (t->RequestsDistribution == "scrambledZipfian") {
            t->generator = new ScrambledZipfianGenerator(numItems);
        } else if (config.RequestsDistribution == "skewLatest") {
            CounterGenerator insert_key_sequence_(0);
            insert_key_sequence_.Set(numItems + 1);
            t->generator = new SkewedLatestGenerator(t->ThreadId, insert_key_sequence_);
        } else if (t->RequestsDistribution == "hotSpot") {
            t->generator = new HotSpotGenerator(numItems,
                                                config.RequestsDistributionParameter, 0.01);
        } else {
            printf("Non existing distribution.");
        }
    }


    void Experiment::launchBenchmarkingClientThreads() {
        void (*func)(ThreadArg *arg);
        switch (config.experimentType) {
            case 1:
                func = &experiment1;
                break;
            case 2:
                func = &experiment2;
                break;
            case 3:
                func = &experiment3;
                break;
            case 4:
                func = &experiment4;
                break;
            case 5:
                func = &experiment5;
                break;
            case 6:
                break;
            case 7:
                break;
            default:
                fprintf(stdout,
                        "[ERROR]: Unknown experiment type. The default experiment, experiment 1 will be executed.\n");
                func = &experiment1;
                break;
        }

        for (int i = 0; i < config.NumThreads; i++) {

            std::thread *t = new std::thread(func, this->threadArgs[i]);
            t->detach();
            threads.push_back(t);
        }

        fprintf(stdout, "[INFO]: Launched all threads.\n");

    }

    void Experiment::writeResultsCSV(FILE *&stream, Results results) {
        std::string p = std::to_string(results.NumPartitions);
        std::string r = std::to_string(results.NumReplicas);
        char separator = ';';
        char defaultSeparator = ',';
        std::string resultStr = "";

        /* First add server statistics header */
        std::vector<std::string> header{"PartitionId", "ReplicaId", "NumPartitions", "NumReplicas",
                                        "TotalTxOps",
                                        "TotalOps", "TotalStartOps", "TotalReadOps", "TotalWriteOps", "TotalCommitOps",
                                        "ExpRealDuration[ms]", "NumReadOpsPerTx", "NumWriteOpsPerTx",
                                        "Throughput[Tx/ms]", "Throughput[Ops/ms]",
                                        "TxLatencySum", "TxLatencyAverage", "TxLatencyMedian", "TxLatency75",
                                        "TxLatency90", "TxLatency95",
                                        "TxLatency99", "TxLatencyMin", "TxLatencyMax", "TxLatencyVar",
                                        "TxLatencySTDev",
                                        "StartLatencySum", "StartLatencyAverage", "StartLatencyMedian",
                                        "StartLatency75", "StartLatency90", "StartLatency95", "StartLatency99",
                                        "StartLatencyMin", "StartLatencyMax", "StartLatencyVar", "StartLatencySTDev",
                                        "ReadLatencySum", "ReadLatencyAverage", "ReadLatencyMedian",
                                        "ReadLatency75", "ReadLatency90", "ReadLatency95", "ReadLatency99",
                                        "ReadLatencyMin", "ReadLatencyMax", "ReadLatencyVar", "ReadLatencySTDev",
                                        "WriteLatencySum", "WriteLatencyAverage", "WriteLatencyMedian",
                                        "WriteLatency75", "WriteLatency90", "WriteLatency95", "WriteLatency99",
                                        "WriteLatencyMin", "WriteLatencyMax", "WriteLatencyVar",
                                        "WriteLatencySTDev",
                                        "CommitLatencySum", "CommitLatencyAverage", "CommitLatencyMedian",
                                        "CommitLatency75", "CommitLatency90", "CommitLatency95",
                                        "CommitLatency99", "CommitLatencyMin", "CommitLatencyMax", "CommitLatencyVar",
                                        "CommitLatencySTDev",
                                        "TotalNumReadItems",
                                        "NumItemsReadFromClientReadSet",
                                        "NumItemsReadFromClientWriteSet",
                                        "NumItemsReadFromClientWriteCache",
                                        "NumItemsReadFromStore",
                                        "TotalNumClientReadItems"};


        std::string sname = " ;ServerName";
        Utils::appendToCSVString(resultStr, sname);
        for (int i = 0; i < header.size(); i++) {
            Utils::appendToCSVString(resultStr, header[i]);
        }

        resultStr += "\n";
        /* Then add server statistics  */
        std::string serverName = config.clientServerName + std::to_string(config.ServingPartitionId) + "_" +
                                 std::to_string(config.ServingReplicaId);
        Utils::appendToCSVString(resultStr, ">>>;" + serverName);

        Utils::appendToCSVString(resultStr, config.ServingPartitionId);
        Utils::appendToCSVString(resultStr, config.ServingReplicaId);
        Utils::appendToCSVString(resultStr, p);
        Utils::appendToCSVString(resultStr, r);
        Utils::appendToCSVString(resultStr, results.TotalTxOps);
        Utils::appendToCSVString(resultStr, results.TotalOps);
        Utils::appendToCSVString(resultStr, results.TotalStartOps);
        Utils::appendToCSVString(resultStr, results.TotalReadOps);
        Utils::appendToCSVString(resultStr, results.TotalWriteOps);
        Utils::appendToCSVString(resultStr, results.TotalCommitOps);
        Utils::appendToCSVString(resultStr, results.TotalExpDuration);
        Utils::appendToCSVString(resultStr, results.NumReadOpsPerTx);
        Utils::appendToCSVString(resultStr, results.NumWriteOpsPerTx);
        Utils::appendToCSVString(resultStr, results.TxThroughput);
        Utils::appendToCSVString(resultStr, results.OpThroughput);

        Utils::appendToCSVString(resultStr, results.OpTxLatencyStats.sum);
        Utils::appendToCSVString(resultStr, results.OpTxLatencyStats.average);
        Utils::appendToCSVString(resultStr, results.OpTxLatencyStats.median);
        Utils::appendToCSVString(resultStr, results.OpTxLatencyStats._75Percentile);
        Utils::appendToCSVString(resultStr, results.OpTxLatencyStats._90Percentile);
        Utils::appendToCSVString(resultStr, results.OpTxLatencyStats._95Percentile);
        Utils::appendToCSVString(resultStr, results.OpTxLatencyStats._99Percentile);
        Utils::appendToCSVString(resultStr, results.OpTxLatencyStats.min);
        Utils::appendToCSVString(resultStr, results.OpTxLatencyStats.max);
        Utils::appendToCSVString(resultStr, results.OpTxLatencyStats.variance);
        Utils::appendToCSVString(resultStr, results.OpTxLatencyStats.standardDeviation);

        Utils::appendToCSVString(resultStr, results.OpStartLatencyStats.sum);
        Utils::appendToCSVString(resultStr, results.OpStartLatencyStats.average);
        Utils::appendToCSVString(resultStr, results.OpStartLatencyStats.median);
        Utils::appendToCSVString(resultStr, results.OpStartLatencyStats._75Percentile);
        Utils::appendToCSVString(resultStr, results.OpStartLatencyStats._90Percentile);
        Utils::appendToCSVString(resultStr, results.OpStartLatencyStats._95Percentile);
        Utils::appendToCSVString(resultStr, results.OpStartLatencyStats._99Percentile);
        Utils::appendToCSVString(resultStr, results.OpStartLatencyStats.min);
        Utils::appendToCSVString(resultStr, results.OpStartLatencyStats.max);
        Utils::appendToCSVString(resultStr, results.OpStartLatencyStats.variance);
        Utils::appendToCSVString(resultStr, results.OpStartLatencyStats.standardDeviation);

        Utils::appendToCSVString(resultStr, results.OpReadLatencyStats.sum);
        Utils::appendToCSVString(resultStr, results.OpReadLatencyStats.average);
        Utils::appendToCSVString(resultStr, results.OpReadLatencyStats.median);
        Utils::appendToCSVString(resultStr, results.OpReadLatencyStats._75Percentile);
        Utils::appendToCSVString(resultStr, results.OpReadLatencyStats._90Percentile);
        Utils::appendToCSVString(resultStr, results.OpReadLatencyStats._95Percentile);
        Utils::appendToCSVString(resultStr, results.OpReadLatencyStats._99Percentile);
        Utils::appendToCSVString(resultStr, results.OpReadLatencyStats.min);
        Utils::appendToCSVString(resultStr, results.OpReadLatencyStats.max);
        Utils::appendToCSVString(resultStr, results.OpReadLatencyStats.variance);
        Utils::appendToCSVString(resultStr, results.OpReadLatencyStats.standardDeviation);

        Utils::appendToCSVString(resultStr, results.OpWriteLatencyStats.sum);
        Utils::appendToCSVString(resultStr, results.OpWriteLatencyStats.average);
        Utils::appendToCSVString(resultStr, results.OpWriteLatencyStats.median);
        Utils::appendToCSVString(resultStr, results.OpWriteLatencyStats._75Percentile);
        Utils::appendToCSVString(resultStr, results.OpWriteLatencyStats._90Percentile);
        Utils::appendToCSVString(resultStr, results.OpWriteLatencyStats._95Percentile);
        Utils::appendToCSVString(resultStr, results.OpWriteLatencyStats._99Percentile);
        Utils::appendToCSVString(resultStr, results.OpWriteLatencyStats.min);
        Utils::appendToCSVString(resultStr, results.OpWriteLatencyStats.max);
        Utils::appendToCSVString(resultStr, results.OpWriteLatencyStats.variance);
        Utils::appendToCSVString(resultStr, results.OpWriteLatencyStats.standardDeviation);

        Utils::appendToCSVString(resultStr, results.OpCommitLatencyStats.sum);
        Utils::appendToCSVString(resultStr, results.OpCommitLatencyStats.average);
        Utils::appendToCSVString(resultStr, results.OpCommitLatencyStats.median);
        Utils::appendToCSVString(resultStr, results.OpCommitLatencyStats._75Percentile);
        Utils::appendToCSVString(resultStr, results.OpCommitLatencyStats._90Percentile);
        Utils::appendToCSVString(resultStr, results.OpCommitLatencyStats._95Percentile);
        Utils::appendToCSVString(resultStr, results.OpCommitLatencyStats._99Percentile);
        Utils::appendToCSVString(resultStr, results.OpCommitLatencyStats.min);
        Utils::appendToCSVString(resultStr, results.OpCommitLatencyStats.max);
        Utils::appendToCSVString(resultStr, results.OpCommitLatencyStats.variance);
        Utils::appendToCSVString(resultStr, results.OpCommitLatencyStats.standardDeviation);

        Utils::appendToCSVString(resultStr, results.TotalNumReadItems);
        Utils::appendToCSVString(resultStr, results.numItemsReadFromClientReadSet);
        Utils::appendToCSVString(resultStr, results.numItemsReadFromClientWriteSet);
        Utils::appendToCSVString(resultStr, results.numItemsReadFromClientWriteCache);
        Utils::appendToCSVString(resultStr, results.numItemsReadFromStore);
        Utils::appendToCSVString(resultStr, results.totalNumClientReadItems);

        if (strcmp(&separator, &defaultSeparator) != 0) {
            replace(resultStr.begin(), resultStr.end(), ',', separator);
        }

        cout << resultStr << endl;

        fprintf(stream, "%s\n", resultStr.c_str());

    }

    void Experiment::writeLatencyDistribution(std::string fileName, vector<double> vector) {

        std::sort(vector.begin(), vector.end());

        FILE *stream;
        stream = fopen(fileName.c_str(), "w");

        for (int i = 0; i < vector.size(); i++) {
            fprintf(stream, "%.5lf;", vector[i]);
        }
        fprintf(stream, "\n\n");

        fclose(stream);
    }


    void calculateLatencyMeasurements(std::vector<double> opLatencies, std::vector<double> &latencies,
                                      double &totalLatency,
                                      double &minLatency, double &maxLatency) {


        latencies.insert(latencies.end(), opLatencies.begin(), opLatencies.end());

        totalLatency += std::accumulate(opLatencies.begin(),
                                        opLatencies.end(), 0.0);

        if (opLatencies.size() > 0) {
            minLatency = *(std::min_element(std::begin(opLatencies),
                                            std::end(opLatencies)));

            maxLatency = *(std::max_element(std::begin(opLatencies),
                                            std::end(opLatencies)));
        }

        //SLOG("[INFO]: Calculated latency measurements!");
    }

    void Experiment::calculateMedian_Percentiles(std::vector<double> vect, Statistic &stat) {

        if (vect.size() != 0) {
            std::sort(vect.begin(), vect.end());

            stat.median = vect.at((int) std::round(vect.size() * 0.50));

            stat._75Percentile = vect.at((int) std::round(vect.size() * 0.75) - 1);

            stat._90Percentile = vect.at((int) std::round(vect.size() * 0.90) - 1);

            stat._95Percentile = vect.at((int) std::round(vect.size() * 0.95) - 1);

            stat._99Percentile = vect.at((int) std::round(vect.size() * 0.99) - 1);
        } else {
            stat.median = 0;

            stat._75Percentile = 0;
            stat._90Percentile = 0;
            stat._95Percentile = 0;
            stat._99Percentile = 0;
        }

    }

    void Experiment::calculateVariation_StandardDeviation(Statistic &stat, std::vector<double> vect) {

        stat.variance = 0;

        for (int i = 0; i < vect.size(); i++) {
            stat.variance += std::pow(stat.average - vect[i], 2);
        }

        if (vect.size() > 0) {
            stat.variance = stat.variance / vect.size();
            stat.standardDeviation = std::sqrt(stat.variance);
        } else {
            stat.variance = 0;
            stat.standardDeviation = 0;
        }

    }


    void Experiment::calculatePerformanceMeasures(double duration) {
        //TODO: refactor this function

//        SLOG("[INFO]: Calculating performance measurements!");

        double totalOps = config.TotalNumOps;
        double totalTxs = config.TotalNumTxs;
        double totalExpDuration = duration;

        double opRate = totalOps / totalExpDuration;
        double txRate = totalTxs / totalExpDuration;
        double totalStartLatency = 0.0;
        double totalReadLatency = 0.0;
        double totalWriteLatency = 0.0;
        double totalCommitLatency = 0.0;
        double totalTxLatency = 0.0;

        int64_t totalStart = 0.0;
        int64_t totalRead = 0.0;
        int64_t totalWrite = 0.0;
        int64_t totalCommit = 0.0;
        int64_t totalTx = 0.0;

        double minStartLatency = 100000000;
        double minReadLatency = 100000000;
        double minWriteLatency = 100000000;
        double minCommitLatency = 100000000;
        double minTxLatency = 100000000;

        double maxStartLatency = 0;
        double maxReadLatency = 0;
        double maxWriteLatency = 0;
        double maxCommitLatency = 0;
        double maxTxLatency = 0;

        Results results;

        results.NumStartOpsPerTx = threadArgs[0]->NumStartOpsPerTx;
        results.NumReadOpsPerTx = threadArgs[0]->NumReadOpsPerTx;
        results.NumWriteOpsPerTx = threadArgs[0]->NumWriteOpsPerTx;
        results.NumCommitOpsPerTx = threadArgs[0]->NumWriteOpsPerTx;

        results.NumHotBlockedKeys = 0;
        results.TotalNumReadItems = 0;
        results.totalNumClientReadItems = 0;

        for (int i = 0; i < config.NumThreads; i++) {

            calculateLatencyMeasurements(threadArgs[i]->OpStartLatencies, results.startLatencies,
                                         totalStartLatency,
                                         minStartLatency, maxStartLatency);

            calculateLatencyMeasurements(threadArgs[i]->OpReadLatencies, results.readLatencies,
                                         totalReadLatency,
                                         minReadLatency, maxReadLatency);

            calculateLatencyMeasurements(threadArgs[i]->OpWriteLatencies, results.writeLatencies,
                                         totalWriteLatency,
                                         minWriteLatency, maxWriteLatency);

            calculateLatencyMeasurements(threadArgs[i]->OpCommitLatencies, results.commitLatencies,
                                         totalCommitLatency,
                                         minCommitLatency, maxCommitLatency);

            calculateLatencyMeasurements(threadArgs[i]->OpTxLatencies, results.txLatencies,
                                         totalTxLatency,
                                         minTxLatency, maxTxLatency);

            results.OpStartLatencyStats.sum += threadArgs[i]->StartLatenciesSum;
            results.OpStartLatencyStats.average += threadArgs[i]->StartLatenciesSum;
            totalStart += threadArgs[i]->TotalNumStartOpsPerThread;
            results.OpStartLatencyStats.max = max(results.OpStartLatencyStats.max, maxStartLatency);
            results.OpStartLatencyStats.min = min(results.OpStartLatencyStats.min, minStartLatency);

            results.OpReadLatencyStats.sum += threadArgs[i]->ReadLatenciesSum;
            results.OpReadLatencyStats.average += (threadArgs[i]->ReadLatenciesSum);
            totalRead += threadArgs[i]->TotalNumReadOpsPerThread;
            results.OpReadLatencyStats.max = max(results.OpReadLatencyStats.max, maxReadLatency);
            results.OpReadLatencyStats.min = min(results.OpReadLatencyStats.min, minReadLatency);

            results.OpWriteLatencyStats.sum += threadArgs[i]->WriteLatenciesSum;
            results.OpWriteLatencyStats.average += (threadArgs[i]->WriteLatenciesSum);
            totalWrite += threadArgs[i]->TotalNumWriteOpsPerThread;
            results.OpWriteLatencyStats.max = max(results.OpWriteLatencyStats.max, maxWriteLatency);
            results.OpWriteLatencyStats.min = min(results.OpWriteLatencyStats.min, minWriteLatency);

            results.OpCommitLatencyStats.sum += threadArgs[i]->CommitLatenciesSum;
            results.OpCommitLatencyStats.average += (threadArgs[i]->CommitLatenciesSum);
            totalCommit += threadArgs[i]->TotalNumCommitOpsPerThread;
            results.OpCommitLatencyStats.max = max(results.OpCommitLatencyStats.max, maxCommitLatency);
            results.OpCommitLatencyStats.min = min(results.OpCommitLatencyStats.min, minCommitLatency);

            results.OpTxLatencyStats.sum += threadArgs[i]->TxLatenciesSum;
            results.OpTxLatencyStats.average += (threadArgs[i]->TxLatenciesSum);
            totalTx += threadArgs[i]->TotalNumTxOpsPerThread;
            results.OpTxLatencyStats.max = max(results.OpTxLatencyStats.max, maxTxLatency);
            results.OpTxLatencyStats.min = min(results.OpTxLatencyStats.min, minTxLatency);

            results.NumHotBlockedKeys += threadArgs[i]->numHotBlockKeys;
        }

        results.OpStartLatencyStats.average /= totalStart;
        results.OpReadLatencyStats.average /= totalRead;
        results.OpWriteLatencyStats.average /= totalWrite;
        results.OpCommitLatencyStats.average /= totalCommit;

        if (totalTx > 0) {
            results.OpTxLatencyStats.average /= totalTx;
        } else {
            results.OpTxLatencyStats.average = 0;
        }

        results.TotalStartOps = totalStart;
        results.TotalReadOps = totalRead;
        results.TotalWriteOps = totalWrite;
        results.TotalCommitOps = totalCommit;
        results.TotalTxOps = totalTx;
        results.TotalOps = totalOps;
        results.NumOpsPerThread = config.NumOpsPerThread;
        results.NumPartitions = config.NumPartitions;
        results.NumReplicas = config.NumReplicasPerPartition;
        results.OpThroughput = opRate;
        results.TxThroughput = txRate;
        results.NumThreadsPerReplica = config.NumThreads;
        results.TotalExpDuration = totalExpDuration;

        calculateMedian_Percentiles(results.startLatencies, results.OpStartLatencyStats);
        calculateMedian_Percentiles(results.readLatencies, results.OpReadLatencyStats);
        calculateMedian_Percentiles(results.writeLatencies, results.OpWriteLatencyStats);
        calculateMedian_Percentiles(results.commitLatencies, results.OpCommitLatencyStats);
        calculateMedian_Percentiles(results.txLatencies, results.OpTxLatencyStats);

        calculateVariation_StandardDeviation(results.OpStartLatencyStats, results.startLatencies);
        calculateVariation_StandardDeviation(results.OpReadLatencyStats, results.readLatencies);
        calculateVariation_StandardDeviation(results.OpWriteLatencyStats, results.writeLatencies);
        calculateVariation_StandardDeviation(results.OpCommitLatencyStats, results.commitLatencies);
        calculateVariation_StandardDeviation(results.OpTxLatencyStats, results.txLatencies);

        results.numItemsReadFromClientWriteSet = config.numItemsReadFromClientWriteSet;
        results.numItemsReadFromClientReadSet = config.numItemsReadFromClientReadSet;
        results.numItemsReadFromClientWriteCache = config.numItemsReadFromClientWriteCache;
        results.numItemsReadFromStore = config.numItemsReadFromStore;
        results.totalNumClientReadItems = config.totalNumClientReadItems;
        results.TotalNumReadItems = config.TotalNumReadItems;

        FILE *pFile;

        std::string name =
                config.expResultsOutputFileName + "_" + std::to_string(config.ServingPartitionId) + "_" +
                std::to_string(config.ServingReplicaId) + ".txt";
        pFile = fopen(name.c_str(), "w");
        if (pFile != NULL) {
            writeResultsCSV(pFile, results);
            fclose(pFile);
            SLOG("Results are written!");
        } else {
            SLOG("File didn't open correctly.");
        }
    }

    void Experiment::freeResources() {

        for (int i = 0; i < config.NumThreads; i++) {

            delete threadArgs[i]->generator;
            delete threadArgs[i];
            delete threads[i];
        }
    }

    void Experiment::runExperiment() {

        buildThreadArguments();
        // launch benchmarking client threads
        launchBenchmarkingClientThreads();

        // wait for all threads ready to run
        WaitHandle::WaitAll(opReadyEvents);

        // record start time
        PhysicalTimeSpec startTimeExp = Utils::GetCurrentClockTime();

        // signal threads to start
        for (int i = 0; i < config.NumThreads; i++) {
            threadArgs[i]->OpStartEvent.Set();
        }

        std::this_thread::sleep_for(
                std::chrono::milliseconds(this->config.experimentDuration + this->config.warmUpDuration));
        this->stopOperation = true;
//        fprintf(stdout, "Stopping the experiment. Waiting for the clients to finish.\n");
//        fflush(stdout);

        // wait for all threads finishing
        WaitHandle::WaitAll(opEndEvents);

        // record end time
        PhysicalTimeSpec endTimeExp = Utils::GetCurrentClockTime();
        double duration = (endTimeExp - startTimeExp).toMilliSeconds();
        fprintf(stdout, "[INFO] ALL CLIENT OPERATIONS ARE DONE\n");
        fflush(stdout);

        config.TotalNumTxs = 0;
        config.NumTxsPerThread = 0;
        config.TotalNumOps = 0;
        config.NumOpsPerThread = 0;

        for (int i = 0; i < config.NumThreads; i++) {
            config.TotalNumTxs += threadArgs[i]->TotalNumTxOpsPerThread;
            config.TotalNumOps +=
                    threadArgs[i]->TotalNumReadOpsPerThread + threadArgs[i]->TotalNumWriteOpsPerThread;
            config.numItemsReadFromClientWriteSet += threadArgs[i]->numItemsReadFromClientWriteSet;
            config.numItemsReadFromClientReadSet += threadArgs[i]->numItemsReadFromClientReadSet;
            config.numItemsReadFromClientWriteCache += threadArgs[i]->numItemsReadFromClientWriteCache;
            config.numItemsReadFromStore += threadArgs[i]->numItemsReadFromStore;
            config.totalNumClientReadItems += threadArgs[i]->totalNumClientReadItems;
            config.TotalNumReadItems += threadArgs[i]->TotalNumReadItems;

        }

        calculatePerformanceMeasures(duration);

        // free resources
        freeResources();
        SLOG("Resources are FREE!");

    }

}
