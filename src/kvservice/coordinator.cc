#include "kvservice/coordinator.h"
#include "kvstore/mv_kvstore.h"
#include "kvstore/log_manager.h"
#include "common/sys_logger.h"
#include "common/utils.h"
#include "common/types.h"
#include "common/sys_stats.h"
#include "common/sys_config.h"
#include <thread>
#include <unordered_map>
#include <queue>
#include <pthread.h>
#include <sched.h>
#include <iostream>
#include "messages/tx_messages.pb.h"

#ifndef LOCAL_LINEARIZABLE
#error LOCAL_LINEARIZABLE MUST BE TURNED ON
#endif


namespace scc {

    // both times must be in 64 but format
#if defined(H_CURE) || defined(WREN)

    double getHybridToPhysicalClockDifferenceInMilliSecs(PhysicalTimeSpec hybridTime,
                                                         PhysicalTimeSpec physicalTime, bool &positive) {

        PhysicalTimeSpec physToHybridTime;

        SET_HYB_PHYS(physToHybridTime.Seconds, physicalTime.Seconds);
        SET_HYB_LOG(physToHybridTime.Seconds, 0);

        PhysicalTimeSpec diff;

        if (hybridTime > physToHybridTime) {
            diff = hybridTime - physToHybridTime;
            positive = true;
        } else {
            diff = physToHybridTime - hybridTime;
            positive = false;
        }
        FROM_64(diff, diff);

        return diff.toMilliSeconds();

    }

#endif

    CoordinatorTx::CoordinatorTx(std::string name, int publicPort, int totalNumKeys)
            : _currentPartition(name, publicPort),
              _partitionId(0),
              _replicaId(0),
              _totalNumPreloadedKeys(totalNumKeys),
              _isDistributed(false),
              _groupClient(NULL),
              _readyToServeRequests(false),
              _delta(0, 0),
              _tx_id(0),
              _tx_lock(0),
              _tx_counter(0) {
        TO_64(_delta, _delta);
#if defined(H_CURE) || defined(WREN)
        //Convert physical 64 bit to hybrid
        SET_HYB_PHYS(_delta.Seconds, _delta.Seconds);
        SET_HYB_LOG(_delta.Seconds, 0);
#endif

    }

    CoordinatorTx::CoordinatorTx(std::string serverName,
                                 int publicPort,
                                 int partitionPort,
                                 int replicationPort,
                                 int partitionId,
                                 int replicaId,
                                 int totalNumKeys,
                                 std::string groupServerName,
                                 int groupServerPort)
            : _currentPartition(serverName, publicPort, partitionPort,
                                replicationPort, partitionId, replicaId),
              _partitionId(partitionId),
              _replicaId(replicaId),
              _totalNumPreloadedKeys(totalNumKeys),
              _isDistributed(true),
              _groupClient(NULL),
              _readyToServeRequests(false),
              _delta(0, 0),
              _tx_id(0),
              _tx_lock(0),
              _tx_counter(0) {
        _groupClient = new GroupClient(groupServerName, groupServerPort);
        TO_64(_delta, _delta);
#if defined(H_CURE) || defined(WREN)
        //Convert physical 64 bit to hybrid
        SET_HYB_PHYS(_delta.Seconds, _delta.Seconds);
        SET_HYB_LOG(_delta.Seconds, 0);
#endif

    }

    CoordinatorTx::~CoordinatorTx() {
        if (_isDistributed) {
            delete _groupClient;

            for (unsigned int i = 0; i < _myReplicas.size(); i++) {
                delete _replicationClients[_myReplicas[i].ReplicaId];
            }

            for (unsigned int i = 0; i < _myPartitions.size(); i++) {
                delete _partitionClients[_myPartitions[i].PartitionId];
                delete _partitionWritesClients[_myPartitions[i].PartitionId];
            }
        }
    }

    /* initialization functions */

    void CoordinatorTx::Initialize() {
        ItemAnchor::_replicaId = _currentPartition.ReplicaId;

        if (!_isDistributed) {
            // load keys
            loadKeys();

            _readyToServeRequests = true;
        } else {
            // register at the group manager
            _groupClient->RegisterPartition(_currentPartition);

            // get all partitions in the system
            _allPartitions = _groupClient->GetRegisteredPartitions();
            _numPartitions = _allPartitions.size();
            _tx_counter = _currentPartition.PartitionId;
            _numReplicasPerPartition = _allPartitions[0].size();

            // initialize _myReplicas and _myPartitions from _allPartitions
            initializeReplicasAndPartitions();

            // initialize Log Manager
            LogManager::Instance()->Initialize(_numReplicasPerPartition);

            // initialize MVKVTXStore
            MVKVTXStore::Instance()->SetPartitionInfo(_currentPartition, _numPartitions, _numReplicasPerPartition);

            MVKVTXStore::Instance()->Initialize();
            SLOG("[INFO]: MVKVTXStore initialized.\n");

            // load keys 
            loadKeys();

            // connect to remote replicas
            if (_numReplicasPerPartition > 1) {
                connectToRemoteReplicas();
            }

            // connect to local partitions
            if (_numPartitions > 1) {
                connectToLocalPartitions();
            }

#ifdef CURE
            // initialize dependency vector
            PhysicalTimeSpec pt(0, 0);
            _sdata.DV.resize(_numReplicasPerPartition, pt);

            if (_numPartitions > 1) {
                launchGSVStabilizationProtocolThread();
            }
#endif

#ifdef WREN
            PhysicalTimeSpec now = Utils::getCurrentClockTimeIn64bitFormat();

            SET_HYB_PHYS(HybridClock.Seconds, now.Seconds);
            SET_HYB_LOG(HybridClock.Seconds, 0);

            _sdata.GRST.setToZero();
            _sdata.GLST.setToZero();
            if (_numPartitions > 1) {
                launchStabilizationProtocolThread();
            }
#endif

#ifdef H_CURE

                // initialize the hybrid clock
                PhysicalTimeSpec now = Utils::getCurrentClockTimeIn64bitFormat();

                SET_HYB_PHYS(HybridClock.Seconds, now.Seconds);
                SET_HYB_LOG(HybridClock.Seconds, 0);
                _sdata.RST.setToZero();
    if (_numPartitions > 1) {
                launchRSTStabilizationProtocolThread();
                }

#endif

            SLOG("[INFO]: Launch update persistence and replication thread");
            if (_numReplicasPerPartition > 1) {
                std::thread t(&CoordinatorTx::PersistAndPropagateUpdates, this);
                t.detach();
            }

            // I'm ready! Notify the group manager
            _groupClient->NotifyReadiness(_currentPartition);

            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            _readyToServeRequests = true;

            prevUBCase = 0;

            SLOG("[INFO]: Ready to serve client requests.");
        }
    }

    /* action functions */
    void CoordinatorTx::Echo(const std::string &input,
                             std::string &output) {
        output = input;
    }

    bool CoordinatorTx::Add(const std::string &key, const std::string &value) {
        return MVKVTXStore::Instance()->Add(key, value);
    }

    bool CoordinatorTx::ShowItem(const std::string &key, std::string &itemVersions) {
        int partitionId = std::stoi(key) % _numPartitions;
        if (partitionId == _currentPartition.PartitionId) {
            return MVKVTXStore::Instance()->ShowItem(key, itemVersions);
        } else {
            return _partitionClients[partitionId]->ShowItem(key, itemVersions);
        }
    }

    bool CoordinatorTx::ShowDB(std::string &allItemVersions) {
        return MVKVTXStore::Instance()->ShowDB(allItemVersions);
    }

    bool CoordinatorTx::ShowState(std::string &stateStr) {
        return MVKVTXStore::Instance()->ShowStateTx(stateStr);
    }

    bool CoordinatorTx::ShowStateCSV(std::string &stateStr) {
        return MVKVTXStore::Instance()->ShowStateTxCSV(stateStr);
    }

    bool CoordinatorTx::DumpLatencyMeasurement(std::string &resultStr) {
        return MVKVTXStore::Instance()->DumpLatencyMeasurement(resultStr);
    }

    Transaction *CoordinatorTx::GetTransaction(unsigned long id) {
        Transaction *ret;
        LOCK_XACT();

            assert(_active_txs.find(id) != _active_txs.end());
            ret = _active_txs[id];
        UNLOCK_XACT();
        assert(ret != NULL);
        return ret;
    }

    void CoordinatorTx::initializeReplicasAndPartitions() {
        for (int i = 0; i < _numReplicasPerPartition; i++) {
            if (i != _currentPartition.ReplicaId) {
                _myReplicas.push_back(_allPartitions[_currentPartition.PartitionId][i]);
            }
        }
        for (int i = 0; i < _numPartitions; i++) {
            if (i != _currentPartition.PartitionId) {
                _myPartitions.push_back(_allPartitions[i][_currentPartition.ReplicaId]);
            }
        }
    }


    void CoordinatorTx::loadKeys() const {

        for (int i = 0; i < _totalNumPreloadedKeys; i++) {

            string key = to_string(i);
            string value(8, 'x'); // default value size is hard coded

            if (!_isDistributed) {
                MVKVTXStore::Instance()->Add(key, value);
            } else {
                if (i % _numPartitions == _currentPartition.PartitionId) {
                    MVKVTXStore::Instance()->Add(key, value);
                }
            }
        }

        int keyCount = MVKVTXStore::Instance()->Size();
        SLOG((boost::format("[INFO]: Loaded all %d keys.") % keyCount).str());
    }

    void CoordinatorTx::connectToRemoteReplicas() {
        for (unsigned int i = 0; i < _myReplicas.size(); i++) {
            DBPartition &p = _myReplicas[i];
            ReplicationKVClient *client = new ReplicationKVClient(p.Name, p.ReplicationPort, _replicaId);
            _replicationClients[p.ReplicaId] = client;
        }
        SLOG("[INFO]: Connected to all remote replicas.");
    }

    void CoordinatorTx::connectToLocalPartitions() {
        for (unsigned int i = 0; i < _myPartitions.size(); i++) {
            DBPartition &p = _myPartitions[i];

            PartitionKVClient *client = new PartitionKVClient(p.Name, p.PartitionPort);
            _partitionClients[p.PartitionId] = client;

            PartitionKVClient *clientWrites = new PartitionKVClient(p.Name, p.PartitionPort + 100);
            _partitionWritesClients[p.PartitionId] = clientWrites;
        }

        this_thread::sleep_for(milliseconds(500));
        SLOG("[INFO]: Connected to all local partitions.");
    }


/* ************************   Start of transaction related functions    ************************ */

#ifdef CURE

    bool CoordinatorTx::TxStart(TxConsistencyMetadata &cdata) {
        updateServerMetadataIfSmaller(cdata.DV);
        std::vector<PhysicalTimeSpec> snapshotVector = _sdata.DV;

        PhysicalTimeSpec max, remote, local, clock;

        remote = getMinFromRemoteEntries(snapshotVector);
        local = snapshotVector[_replicaId];
        clock = Utils::getCurrentClockTimeIn64bitFormat();

        max = MAX(remote, local);
        max = MAX(max, clock);

        snapshotVector[_replicaId] = max;
        cdata.DV = snapshotVector;

        unsigned long tid;

        LOCK_XACT();
            tid = getAndIncrementTxCount();
            _active_txs[tid] = new Transaction(tid, new TxContex(snapshotVector));
        UNLOCK_XACT();
        cdata.txId = tid;

        return true;
    }

    void CoordinatorTx::waitOnTxStart(std::vector<PhysicalTimeSpec> &cdataDV) {

        for (int i = 0; i < cdataDV.size(); i++) {
            while (cdataDV[i] > _sdata.DV[i]) {
                std::this_thread::sleep_for(std::chrono::microseconds(10));
            }
        }
    }

#endif

#ifdef H_CURE

    bool CoordinatorTx::TxStart(TxConsistencyMetadata &cdata) {

        PhysicalTimeSpec snapshotLDT, snapshotRST, clock;

        updateServerMetadataIfSmaller(cdata.RST);
        snapshotRST = _sdata.RST;

#ifdef HYB_PROFILER
        calculateHybToPhysicalTxStartClockDiffStats();
#endif

        clock = HybridClock;

        if (SysConfig::FreshnessParameter != 0) {

            PhysicalTimeSpec ms, hyb_ms;
            ms.addMilliSeconds(SysConfig::FreshnessParameter);
            TO_64(ms, ms);
            SET_HYB_PHYS(hyb_ms.Seconds, ms.Seconds);
            SET_HYB_LOG(hyb_ms.Seconds, 0);

            clock = clock - hyb_ms;
        }

        snapshotLDT = MAX(snapshotRST, cdata.LDT);
        snapshotLDT = MAX(snapshotLDT, clock);

        cdata.LDT = snapshotLDT;
        cdata.RST = snapshotRST;


        unsigned long tid;

        LOCK_XACT();
            tid = getAndIncrementTxCount();
            assert(_active_txs.find(tid) == _active_txs.end());
            _active_txs[tid] = new Transaction(tid, new TxContex(snapshotLDT, snapshotRST));
            assert(_active_txs.find(tid) != _active_txs.end());
        UNLOCK_XACT();

        cdata.txId = tid;

        return true;
    }

#endif

#ifdef WREN

    bool CoordinatorTx::TxStart(TxConsistencyMetadata &cdata) {

        updateServerMetadataIfSmaller(cdata.GLST, cdata.GRST);
        getGSTs(cdata.GLST, cdata.GRST);

#ifdef WREN_NEWER_SNAPSHOT
        PhysicalTimeSpec snapshotGLST, clock;
        clock = HybridClock;
        snapshotGLST = MAX(cdata.GRST, cdata.GLST);
        snapshotGLST = MAX(snapshotGLST, clock);
        cdata.GLST = snapshotGLST;
#else
        PhysicalTimeSpec lst(cdata.GLST.Seconds - 1, cdata.GLST.NanoSeconds);
        cdata.GRST = MIN(lst, cdata.GRST);
#endif


        unsigned long tid;

        LOCK_XACT();
            tid = getAndIncrementTxCount();
            assert(_active_txs.find(tid) == _active_txs.end());
            _active_txs[tid] = new Transaction(tid, new TxContex(cdata.GLST, cdata.GRST));
            assert(_active_txs.find(tid) != _active_txs.end());
        UNLOCK_XACT();

        cdata.txId = tid;
        return true;
    }

#endif

/* ************************    Transactional read related functions     ************************ */
#if defined(CURE) || defined(H_CURE) || defined(WREN)

    bool CoordinatorTx::TxRead(int txId, const std::vector<std::string> &keySet,
                               std::vector<std::string> &valueSet) {

#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
        SysStats::NumReadItemsFromKVStore = SysStats::NumReadItemsFromKVStore + keySet.size();
#endif

        int numReadSlices;

        Transaction *tx = GetTransaction(txId);

#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec startTime1 = Utils::GetCurrentClockTime();
#endif

        mapKeysToPartitionInReadPhase(keySet, valueSet, tx->partToReadSliceMap);

#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec endTime1 = Utils::GetCurrentClockTime();
        double duration1 = (endTime1 - startTime1).toMilliSeconds();
        SysStats::CoordinatorMapingKeysToShardsLatencySum =
                SysStats::CoordinatorMapingKeysToShardsLatencySum + duration1;
        SysStats::NumCoordinatorMapingKeysToShardsLatencyMeasurements++;

        PhysicalTimeSpec startTime2 = Utils::GetCurrentClockTime();
#endif

        // Notify readSlicesWaitHandle of how many threads it has to wait
        numReadSlices = tx->partToReadSliceMap.size();
        tx->readSlicesWaitHandle = new WaitHandle(numReadSlices);

        sendTxReadSliceRequestsToOtherPartitions(tx);

#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec endTime2 = Utils::GetCurrentClockTime();
        double duration2 = (endTime2 - startTime2).toMilliSeconds();
        SysStats::CoordinatorSendingReadReqToShardsLatencySum =
                SysStats::CoordinatorSendingReadReqToShardsLatencySum + duration2;
        SysStats::NumCoordinatorSendingReadReqToShardsLatencyMeasurements++;

        PhysicalTimeSpec startTime3 = Utils::GetCurrentClockTime();
#endif

        readLocalKeys(tx);

#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec endTime3 = Utils::GetCurrentClockTime();
        double duration3 = (endTime3 - startTime3).toMilliSeconds();
        SysStats::CoordinatorReadingLocalKeysLatencySum = SysStats::CoordinatorReadingLocalKeysLatencySum + duration3;
        SysStats::NumCoordinatorReadingLocalKeysLatencyMeasurements++;

        PhysicalTimeSpec startTime4 = Utils::GetCurrentClockTime();
#endif

        processReadRequestsReplies(tx->partToReadSliceMap, valueSet);

#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec endTime4 = Utils::GetCurrentClockTime();
        double duration4 = (endTime4 - startTime4).toMilliSeconds();
        SysStats::CoordinatorProcessingOtherShardsReadRepliesLatencySum =
                SysStats::CoordinatorProcessingOtherShardsReadRepliesLatencySum + duration4;
        SysStats::NumCoordinatorProcessingOtherShardsReadRepliesLatencyMeasurements++;
#endif

        for (auto it : tx->partToReadSliceMap) {
            delete it.second;
        }

        tx->partToReadSliceMap.clear();

#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
        double duration = (endTime - startTime).toMilliSeconds();
        SysStats::ServerReadLatencySum = SysStats::ServerReadLatencySum + duration;
        SysStats::NumServerReadLatencyMeasurements++;
#endif
        return true;
    }

    void CoordinatorTx::mapKeysToPartitionInReadPhase(const vector<string, allocator<string>> &keySet,
                                                      vector<string, allocator<string>> &valueSet,
                                                      std::unordered_map<int, TxReadSlice *> &txPartitionToReadSliceMap) {
        string key;
        int partitionId;

        txPartitionToReadSliceMap.clear();

        for (unsigned int i = 0; i < keySet.size(); ++i) {

            key = keySet[i];
            valueSet.push_back("");
            partitionId = stoi(key) % _numPartitions;
            if (!txPartitionToReadSliceMap.count(partitionId)) {
                //Slice for partitionId does not exists
                txPartitionToReadSliceMap[partitionId] = new TxReadSlice();
            }

            TxReadSlice *slice;
            slice = txPartitionToReadSliceMap[partitionId];

            slice->keys.push_back(key);
            slice->positionIds.push_back(i);
        }
    }

    void CoordinatorTx::readLocalKeys(Transaction *tx) {
#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec startTime1 = Utils::GetCurrentClockTime();
        PhysicalTimeSpec startTime2, endTime2;
#endif

        int localPartitionId = _currentPartition.PartitionId;

        if (tx->partToReadSliceMap.count(localPartitionId) > 0) {
            TxReadSlice *localSlice = tx->partToReadSliceMap[localPartitionId];
            vector<string> keys = localSlice->keys;


            int numKeys = keys.size();
            localSlice->values.resize(numKeys, "");

#if defined(WREN)
            InternalTxSliceReadKeys(tx->txId, tx->txContex->GLST, tx->txContex->GRST, localSlice->keys,
                                    localSlice->values);
#else
            InternalTxSliceReadKeys(tx->txId, *(tx->txContex), localSlice->keys, localSlice->values);
#endif
            localSlice->txWaitOnReadTime = tx->txContex->waited_xact;

#ifdef MEASURE_STATISTICS
            PhysicalTimeSpec endTime1 = Utils::GetCurrentClockTime();
            double duration1 = (endTime1 - startTime1).toMilliSeconds();
            SysStats::CoordinatorLocalKeyReadsLatencySum =
                    SysStats::CoordinatorLocalKeyReadsLatencySum + duration1;
            SysStats::NumCoordinatorLocalKeyReadsLatencyMeasurements++;

            startTime2 = Utils::GetCurrentClockTime();
#endif
            tx->readSlicesWaitHandle->DecrementAndWaitIfNonZero();
#ifdef MEASURE_STATISTICS
            endTime2 = Utils::GetCurrentClockTime();
            double duration2 = (endTime2 - startTime2).toMilliSeconds();
            SysStats::CoordinatorWaitingForShardsRepliesLatencySum =
                    SysStats::CoordinatorWaitingForShardsRepliesLatencySum + duration2;
            SysStats::NumCoordinatorWaitingForShardsRepliesMeasurements++;
#endif
        } else {
#ifdef MEASURE_STATISTICS
            startTime2 = Utils::GetCurrentClockTime();
#endif
            tx->readSlicesWaitHandle->WaitIfNonZero();

#ifdef MEASURE_STATISTICS
            endTime2 = Utils::GetCurrentClockTime();
            double duration2 = (endTime2 - startTime2).toMilliSeconds();
            SysStats::CoordinatorWaitingForShardsRepliesLatencySum =
                    SysStats::CoordinatorWaitingForShardsRepliesLatencySum + duration2;
            SysStats::NumCoordinatorWaitingForShardsRepliesMeasurements++;
#endif
        }
    }

#ifdef CURE

    bool
    CoordinatorTx::InternalTxSliceReadKeys(unsigned int txId, TxContex &cdata, const std::vector<std::string> &keys,
                                           std::vector<std::string> &values) {
#ifdef MEASURE_STATISTICS
        SysStats::NumInternalTxSliceReadRequests++;
#endif
        bool ret = true;

        LOCK_DV();
            for (int i = 0; i < _sdata.DV.size(); i++) {
                _sdata.DV[i] = MAX(_sdata.DV[i], cdata.DV[i]);
            }
        UNLOCK_DV();

        WaitOnTxSlice(txId, cdata);

        for (int i = 0; i < keys.size(); i++) {
            ret = ret && MVKVTXStore::Instance()->LocalTxSliceGet(cdata, keys[i], values[i]);
        }

        return ret;
    }

#endif

#ifdef H_CURE

    bool
    CoordinatorTx::InternalTxSliceReadKeys(unsigned int txId, TxContex &cdata, const std::vector<std::string> &keys,
                                           std::vector<std::string> &values) {
#ifdef MEASURE_STATISTICS
        SysStats::NumInternalTxSliceReadRequests++;
#endif
        bool ret = true;

        updateServerMetadataIfSmaller(cdata.RST);

#ifdef HYB_PROFILER
        if (!HybridClock.Zero()) {
            PhysicalTimeSpec currentTime = Utils::getCurrentClockTimeIn64bitFormat();

            bool positive = true;
            double hybPhysDiff = getHybridToPhysicalClockDifferenceInMilliSecs(HybridClock, currentTime,
                                                                               positive); //milliseconds

            if (positive) {
                SysStats::PosDiff_HybToPhys_TxRead = SysStats::PosDiff_HybToPhys_TxRead + hybPhysDiff;
                SysStats::CountNegDiff_HybToPhys_TxRead++;
            } else {
                SysStats::NegDiff_HybToPhys_TxRead = SysStats::NegDiff_HybToPhys_TxRead + hybPhysDiff;
                SysStats::CountNegDiff_HybToPhys_TxRead++;
            }
        }


        PhysicalTimeSpec diffHybVV, vv, HybridClockValue ;
        double diff;
        bool positive;
        HybridClockValue = HybridClock;
        vv = MVKVTXStore::Instance()->GetVV(_replicaId);

        if (!vv.Zero() && !HybridClock.Zero()) {

            if (HybridClockValue > vv) {
                diffHybVV = HybridClockValue - vv;
                positive = true;
            } else {
                diffHybVV = vv - HybridClockValue;
                positive = false;
            }
            FROM_64(diffHybVV,diffHybVV);

            diff = diffHybVV.toMilliSeconds();

            if (positive) {
                SysStats::PosDiff_HybToVV_TxRead = SysStats::PosDiff_HybToVV_TxRead + diff;
                SysStats::CountPosDiff_HybToVV_TxRead++;
            } else {
                SysStats::NegDiff_HybToVV_TxRead = SysStats::NegDiff_HybToVV_TxRead + diff;
                SysStats::CountNegDiff_HybToVV_TxRead++;
            }
        }
#endif

        WaitOnTxSlice(txId, cdata);

#ifdef HYB_PROFILER
        if (cdata.waited_xact > 0) {
            if (positive) {
                SysStats::PosDiff_HybToVV_TxRead_Block = SysStats::PosDiff_HybToVV_TxRead_Block + diff;
                SysStats::CountPosDiff_HybToVV_TxRead_Block++;
            } else {
                SysStats::NegDiff_HybToVV_TxRead_Block = SysStats::NegDiff_HybToVV_TxRead_Block + diff;
                SysStats::CountNegDiff_HybToVV_TxRead_Block++;
            }
        }
#endif

        for (int i = 0; i < keys.size(); i++) {
            ret = ret && MVKVTXStore::Instance()->LocalTxSliceGet(cdata, keys[i], values[i]);
        }

        return ret;
    }

#endif

    void CoordinatorTx::processReadRequestsReplies(std::unordered_map<int, TxReadSlice *> &txPartitionToReadSliceMap,
                                                   vector<string, allocator<string>> &valueSet) {
        int partitionId;
        double maxWaitingTime = 0;

        for (auto it:txPartitionToReadSliceMap) {
            partitionId = it.first;
            TxReadSlice *slice = it.second;
            for (int i = 0; i < slice->values.size(); i++) {
                int position = slice->positionIds[i];
                valueSet[position] = slice->values[i];
            }
        }

#ifdef MEASURE_STATISTICS
        SysStats::NumTxReadPhases++;
        if (maxWaitingTime > 0) {
            SysStats::TxReadBlockDurationAtCoordinator =
                    SysStats::TxReadBlockDurationAtCoordinator + (double) maxWaitingTime;
            SysStats::NumTxReadBlocksAtCoordinator++;
        }
#endif

    }


#endif

#if defined(WREN)

    bool
    CoordinatorTx::InternalTxSliceReadKeys(unsigned int txId, const PhysicalTimeSpec &cGLST,
                                           const PhysicalTimeSpec &cGRST, const std::vector<std::string> &keys,
                                           std::vector<std::string> &values) {

#ifdef MEASURE_STATISTICS
        SysStats::NumInternalTxSliceReadRequests++;
#endif
        bool ret = true;

        assert(cGLST <= MVKVTXStore::Instance()->GetVV(_replicaId));
        setGlobalStabilizationTimesIfSmaller(cGLST, cGRST);

        for (int i = 0; i < keys.size(); i++) {
            ret = ret && MVKVTXStore::Instance()->LocalTxSliceGet(cGLST, cGRST, keys[i], values[i]);
        }

        return ret;

    }


#endif

    void CoordinatorTx::sendTxReadSliceRequestsToOtherPartitions(Transaction *tx) {
        int partitionId;
        int localPartitionId = _currentPartition.PartitionId;

        for (auto it : tx->partToReadSliceMap) {
            partitionId = it.first;
            if (partitionId != localPartitionId) {
                vector<string> keys = it.second->keys;
                _partitionClients[partitionId]->TxSliceReadKeys(tx->txId, *(tx->txContex), keys, localPartitionId);
            }
        }
    }

#ifdef CURE

    void CoordinatorTx::launchGSVStabilizationProtocolThread() {
        if (SysConfig::GSTDerivationMode == GSTDerivationType::TREE) {
            // build GST tree
            BuildGSTTree();
            if (_currentTreeNode->NodeType == TreeNodeType::RootNode) {
                {
                    // launch GST sending thread if current node is root
                    thread t(&CoordinatorTx::SendGSTAtRoot, this);

                    sched_param sch;
                    int policy;
                    pthread_getschedparam(t.native_handle(), &policy, &sch);
                    sch.sched_priority = sched_get_priority_max(SCHED_FIFO);
                    if (pthread_setschedparam(t.native_handle(), SCHED_FIFO, &sch)) {
                        cout << "Failed to setschedparam: " << strerror(errno) << '\n';
                    }

                    t.detach();
                }
            }

        } else {
            cout << "Unexisting/Unsupported GSTDerivationType: \n";
        }
        SLOG("[INFO]: GSV stabilization protocol thread scheduled\n");
    }

#elif defined(WREN)

    void CoordinatorTx::launchStabilizationProtocolThread() {
        if (SysConfig::GSTDerivationMode == GSTDerivationType::TREE) {
            // build GST tree
            BuildGSTTree();
            if (_currentTreeNode->NodeType == TreeNodeType::RootNode) {
                {
                    // launch global stabilization thread if current node is root
                    thread t(&CoordinatorTx::SendGSTAtRoot, this);

                    sched_param sch;
                    int policy;
                    pthread_getschedparam(t.native_handle(), &policy, &sch);
                    sch.sched_priority = sched_get_priority_max(SCHED_FIFO);
                    if (pthread_setschedparam(t.native_handle(), SCHED_FIFO, &sch)) {
                        cout << "Failed to setschedparam: " << strerror(errno) << '\n';
                    }

                    t.detach();
                }
            }

        } else {
            cout << "Unexisting/Unsupported GSTDerivationType: \n";
        }
        SLOG("[INFO]: GST stabilization protocol thread scheduled\n");
    }



#elif defined(H_CURE)

    void CoordinatorTx::launchRSTStabilizationProtocolThread() {

        if (SysConfig::GSTDerivationMode == GSTDerivationType::TREE) {
            // build GST tree
            BuildGSTTree();
            if (_currentTreeNode->NodeType == TreeNodeType::RootNode) {
                {
                    // launch global stabilization thread if current node is root
                    thread t(&CoordinatorTx::SendGSTAtRoot, this);

                    sched_param sch;
                    int policy;
                    pthread_getschedparam(t.native_handle(), &policy, &sch);
                    sch.sched_priority = sched_get_priority_max(SCHED_FIFO);
                    if (pthread_setschedparam(t.native_handle(), SCHED_FIFO, &sch)) {
                        cout << "Failed to setschedparam: " << strerror(errno) << '\n';
                    }

                    t.detach();
                }
            }

        } else {
            cout << "Unexisting/Unsupported GSTDerivationType: \n";
        }
        SLOG("[INFO]: GST stabilization protocol thread scheduled\n");

    }

#endif


#ifdef H_CURE

    void CoordinatorTx::calculateHybToPhysicalTxStartClockDiffStats() {
        if (!HybridClock.Zero()) {
            PhysicalTimeSpec currentTime = Utils::getCurrentClockTimeIn64bitFormat();

            double hybPhysDiff;
            bool positive = true;

            hybPhysDiff = getHybridToPhysicalClockDifferenceInMilliSecs(HybridClock, currentTime, positive); //ms

            if (positive) {
                SysStats::PosDiff_HybToPhys_TxStart = SysStats::PosDiff_HybToPhys_TxStart + hybPhysDiff;
                SysStats::CountPosDiff_HybToPhys_TxStart++;
            } else {
                SysStats::NegDiff_HybToPhys_TxStart = SysStats::NegDiff_HybToPhys_TxStart + hybPhysDiff;
                SysStats::CountNegDiff_HybToPhys_TxStart++;
            }
        }
    }

#endif

#ifdef CURE

    void CoordinatorTx::updateServerMetadataIfSmaller(std::vector<PhysicalTimeSpec> &dv) {
        setGSVIfSmaller(dv);
    }

    void CoordinatorTx::updateServerRemoteDVEntriesByPairwiseMaxValues(std::vector<PhysicalTimeSpec> &dv) {
        int i;

        LOCK_DV();
            for (i = 0; i < _sdata.DV.size(); i++) {
                if (i != _replicaId) {
                    _sdata.DV[i] = MAX(_sdata.DV[i], dv[i]);
                }
            }
        UNLOCK_DV();
    }

    PhysicalTimeSpec CoordinatorTx::getMaxFromRemoteServerDVEntries() {
        int i;
        PhysicalTimeSpec max;

        LOCK_DV();
            for (i = 0; i < _sdata.DV.size(); i++) {
                if (i != _replicaId) {
                    if (max < _sdata.DV[i]) {
                        max = _sdata.DV[i];
                    }
                }
            }
        UNLOCK_DV();

        return max;
    }

    PhysicalTimeSpec CoordinatorTx::getMinFromRemoteServerDVEntries() {
        int i;
        PhysicalTimeSpec min;

        assert(_sdata.DV.size() > 1);

        LOCK_DV();

            if (_replicaId == 0) {
                min = _sdata.DV[1];
            } else {
                min = _sdata.DV[0];
            }

            for (i = 0; i < _sdata.DV.size(); i++) {
                if (i != _replicaId) {
                    if (min > _sdata.DV[i]) {
                        min = _sdata.DV[i];
                    }
                }
            }
        UNLOCK_DV();

        return min;
    }

    PhysicalTimeSpec CoordinatorTx::getMinFromRemoteEntries(std::vector<PhysicalTimeSpec> &vect) {
        int i;
        PhysicalTimeSpec min;

        if (_replicaId == 0) {
            min = vect[1];
        } else {
            min = vect[0];
        }

        for (i = 0; i < vect.size(); i++) {
            if (i != _replicaId) {
                if (min > vect[i]) {
                    min = vect[i];
                }
            }
        }

        return min;
    }

#endif


    bool CoordinatorTx::TxCommit(TxConsistencyMetadata &cdata,
                                 const std::vector<std::string> &writeKeySet,
                                 const std::vector<std::string> &writeValueSet) {

#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
#endif

        Transaction *tx = GetTransaction(cdata.txId);

        if (!writeKeySet.empty()) {

            PhysicalTimeSpec startTime1 = Utils::GetCurrentClockTime();

#ifdef WREN
            PhysicalTimeSpec ht = MAX(tx->txContex->GRST, tx->txContex->GLST);
            ht = MAX(ht, cdata.CT);
            tx->txContex->HT = ht;
#endif

            int localPartitionId = _currentPartition.PartitionId;
            int numPrepareMsgs;
            std::string key, val;
            PhysicalTimeSpec commitTime;

            mapKeysToPartitionInCommitPhase(writeKeySet, writeValueSet, tx);

#ifdef MEASURE_STATISTICS
            PhysicalTimeSpec endTime1 = Utils::GetCurrentClockTime();
            double duration1 = (endTime1 - startTime1).toMilliSeconds();
            SysStats::CoordinatorCommitMappingKeysToShardsLatencySum =
                    SysStats::CoordinatorCommitMappingKeysToShardsLatencySum + duration1;
            SysStats::NumCoordinatorCommitMappingKeysToShardsLatencyMeasurements++;

            startTime1 = Utils::GetCurrentClockTime();
#endif

            int partitionId;

            numPrepareMsgs = tx->partToPrepReqMap.size();

            // Notify prepareRequestWaitHandle of how many threads it has to wait
            tx->prepareReqWaitHandle = new WaitHandle(numPrepareMsgs);

            sendPrepareRequestsToOtherPartitions(tx->partToPrepReqMap);

#ifdef MEASURE_STATISTICS
            endTime1 = Utils::GetCurrentClockTime();
            duration1 = (endTime1 - startTime1).toMilliSeconds();
            SysStats::CoordinatorCommitSendingPrepareReqToShardsLatencySum =
                    SysStats::CoordinatorCommitSendingPrepareReqToShardsLatencySum + duration1;
            SysStats::NumCoordinatorCommitSendingPrepareReqToShardsLatencyMeasurements++;

            startTime1 = Utils::GetCurrentClockTime();
#endif
            int localCount = tx->partToPrepReqMap.count(localPartitionId);

            //In case you have keys to write to the local partition
            if (localCount > 0) {
                prepareLocalKeys(tx);
            }

#ifdef MEASURE_STATISTICS
            endTime1 = Utils::GetCurrentClockTime();
            duration1 = (endTime1 - startTime1).toMilliSeconds();
            SysStats::CoordinatorCommitLocalPrepareLatencySum =
                    SysStats::CoordinatorCommitLocalPrepareLatencySum + duration1;
            SysStats::NumCoordinatorCommitLocalPrepareLatencyMeasurements++;

            startTime1 = Utils::GetCurrentClockTime();
#endif

            if (localCount > 0) {
                tx->prepareReqWaitHandle->DecrementAndWaitIfNonZero();
            } else {
                tx->prepareReqWaitHandle->WaitIfNonZero();
            }

#ifdef MEASURE_STATISTICS
            endTime1 = Utils::GetCurrentClockTime();
            duration1 = (endTime1 - startTime1).toMilliSeconds();
            SysStats::CoordinatorCommitWaitingOtherShardsPrepareRepliesLatencySum =
                    SysStats::CoordinatorCommitWaitingOtherShardsPrepareRepliesLatencySum + duration1;
            SysStats::NumCoordinatorCommitWaitingOtherShardsPrepareRepliesLatencyMeasurements++;
            startTime1 = Utils::GetCurrentClockTime();
#endif
            commitTime = processPrepareRequestsReplies(tx);
            assert(commitTime.Seconds > 0);

#if defined(H_CURE) || defined(WREN)
            cdata.CT = commitTime;
#elif defined(CURE)
            PhysicalTimeSpec init;

            cdata.CT.resize(_numReplicasPerPartition, init);
            assert(_sdata.DV.size() == _numReplicasPerPartition);

            for (int i = 0; i < _sdata.DV.size(); i++) {
                cdata.CT[i] = _sdata.DV[i];
            }
            cdata.CT[_replicaId] = commitTime;
#endif

#ifdef MEASURE_STATISTICS
            endTime1 = Utils::GetCurrentClockTime();
            duration1 = (endTime1 - startTime1).toMilliSeconds();
            SysStats::CoordinatorCommitProcessingPrepareRepliesLatencySum =
                    SysStats::CoordinatorCommitProcessingPrepareRepliesLatencySum + duration1;
            SysStats::NumCoordinatorCommitProcessingPrepareRepliesLatencyMeasurements++;

            startTime1 = Utils::GetCurrentClockTime();
#endif

            sendCommitRequests(cdata.CT, tx->partToPrepReqMap);

#ifdef MEASURE_STATISTICS
            endTime1 = Utils::GetCurrentClockTime();
            duration1 = (endTime1 - startTime1).toMilliSeconds();
            SysStats::CoordinatorCommitSendingCommitReqLatencySum =
                    SysStats::CoordinatorCommitSendingCommitReqLatencySum + duration1;
            SysStats::NumCoordinatorCommitSendingCommitReqLatencyMeasurements++;
#endif
        }

        LOCK_XACT();
            _active_txs.erase(tx->txId);
            assert(_active_txs.find(tx->txId) == _active_txs.end());
        UNLOCK_XACT();
        delete tx;

#ifdef MEASURE_STATISTICS
        SysStats::NumCommitedTxs++;

        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
        double duration = (endTime - startTime).toMilliSeconds();
        SysStats::CoordinatorCommitLatencySum = SysStats::CoordinatorCommitLatencySum + duration;
        SysStats::NumCoordinatorCommitMeasurements++;
#endif

        return true;
    }

#ifdef CURE

    void CoordinatorTx::sendCommitRequests(std::vector<PhysicalTimeSpec> commitTime,
                                           std::unordered_map<int, PrepareRequest *> &txPartToPrepReqMap)
#elif defined(H_CURE) || defined(WREN)

    void CoordinatorTx::sendCommitRequests(PhysicalTimeSpec commitTime,
                                           std::unordered_map<int, PrepareRequest *> &txPartToPrepReqMap)
#endif
    {
        int partitionId;
        int localPartitionId = _currentPartition.PartitionId;

        for (
            auto it :
                txPartToPrepReqMap) {
            partitionId = it.first;
            PrepareRequest *pReqTemp = it.second;

            if (partitionId != localPartitionId) {
                _partitionWritesClients[partitionId]->
                        CommitRequest(pReqTemp->id, commitTime);
            } else {
                LocalCommitRequest(pReqTemp->id, commitTime);
            }
        }
    }

    PhysicalTimeSpec &CoordinatorTx::processPrepareRequestsReplies(const Transaction *tx) {
        double maxWaitingTime = 0.0;
        PhysicalTimeSpec commitTime;

        for (auto it: tx->partToPrepReqMap) {
            PhysicalTimeSpec prepareTime = it.second->prepareTime;
            commitTime = MAX(commitTime, prepareTime);
#ifdef MEASURE_STATISTICS
            maxWaitingTime = MAX(maxWaitingTime, it.second->prepareBlockDuration);
#endif
        }

#ifdef MEASURE_STATISTICS
        if (maxWaitingTime > 0) {
            SysStats::TxPreparePhaseBlockDurationAtCoordinator =
                    SysStats::TxPreparePhaseBlockDurationAtCoordinator + (double) maxWaitingTime;
            SysStats::NumTxPreparePhaseBlocksAtCoordinator++;
        }
#endif
        return commitTime;
    }

    void CoordinatorTx::prepareLocalKeys(Transaction *tx) {
        int localPartitionId = _currentPartition.PartitionId;

        PrepareRequest *localPrepReq = tx->partToPrepReqMap[localPartitionId];
        localPrepReq->prepareTime = LocalPrepareRequest(tx->txId, *(tx->txContex),
                                                        localPrepReq->keys,
                                                        localPrepReq->values);
        localPrepReq->prepareBlockDuration = tx->txContex->timeWaitedOnPrepareXact;
    }

    void CoordinatorTx::mapKeysToPartitionInCommitPhase(const vector<string, allocator<string>> &writeKeySet,
                                                        const vector<string, allocator<string>> &writeValueSet,
                                                        Transaction *tx) {

        int partitionId;
        std::string key, val;

        for (auto it : tx->partToPrepReqMap) {
            delete it.second;
        }

        tx->partToPrepReqMap.clear();


        // Create helper maps that map keys and values to partitions
        for (unsigned int i = 0; i < writeKeySet.size(); ++i) {
            key = writeKeySet[i];
            val = writeValueSet[i];
            partitionId = std::stoi(key) % _numPartitions;
            if (!tx->partToPrepReqMap.count(partitionId)) {
#if defined(H_CURE) || defined(WREN)
                tx->partToPrepReqMap[partitionId] = new PrepareRequest(tx->txId, *(tx->txContex));
#elif defined(CURE)
                tx->partToPrepReqMap[partitionId] = new PrepareRequest(tx->txId, *(tx->txContex),
                                                                       _numReplicasPerPartition);
#endif
            }

            PrepareRequest *prepareRequest;
            prepareRequest = tx->partToPrepReqMap[partitionId];

            assert(prepareRequest->id == tx->txId);

            prepareRequest->keys.push_back(key);
            prepareRequest->values.push_back(val);

#ifdef  CURE
            assert(prepareRequest->metadata.DV.size() == _numReplicasPerPartition);
#endif
        }

    }

    void
    CoordinatorTx::sendPrepareRequestsToOtherPartitions(std::unordered_map<int, PrepareRequest *> &txPartToPrepReqMap) {
        int partitionId;
        int localPartitionId = _currentPartition.PartitionId;

        for (auto it : txPartToPrepReqMap) {
            partitionId = it.first;
            PrepareRequest *tempReq = it.second;
            if (partitionId != localPartitionId) {
                std::vector<std::string> keys = tempReq->keys;
                std::vector<std::string> vals = tempReq->values;
#ifdef CURE
                if (_numReplicasPerPartition != tempReq->metadata.DV.size()) {
                    SLOG((boost::format(
                            "numReplicas = %s  tempReq->metadata.DV.size() = %d\n")
                          % _numReplicasPerPartition
                          % tempReq->metadata.DV.size()).str());
                }
                assert(tempReq->metadata.DV.size() == _numReplicasPerPartition);
#endif
                _partitionWritesClients[partitionId]->PrepareRequest(tempReq->id, tempReq->metadata, keys, vals,
                                                                     localPartitionId);
            }
        }
    }


#if defined(H_CURE) || defined(WREN)

    void CoordinatorTx::LocalCommitRequest(unsigned int txId, PhysicalTimeSpec ct) {
        LOCK_PREPARED_REQUESTS();

            updateClockOnCommit(ct);

            assert(txIdToPreparedRequestsMap.find(txId) != txIdToPreparedRequestsMap.end());

            PrepareRequest *req = txIdToPreparedRequestsMap[txId];
            assert(req != NULL);
            assert(req->id == txId);

            req->commitTime = ct;

            PrepareRequestElement *pRequestEl = new PrepareRequestElement(req->id, req->prepareTime);

#ifdef PROFILE_STATS
            req->metadata.endTimePreparedSet = Utils::GetCurrentClockTime();
            double duration = (req->metadata.endTimePreparedSet - req->metadata.startTimePreparedSet).toMilliSeconds();

            {
                std::lock_guard<std::mutex> lk(SysStats::timeInPreparedSet.valuesMutex);
                SysStats::timeInPreparedSet.values.push_back(duration);
            }
#endif

            assert(txIdToPreparedRequestsMap.find(txId) != txIdToPreparedRequestsMap.end());
            txIdToPreparedRequestsMap.erase(txId);
            assert(txIdToPreparedRequestsMap.find(txId) == txIdToPreparedRequestsMap.end());

            int pReqElSize = preparedReqElSet.size();
            preparedReqElSet.erase(pRequestEl);

            assert(preparedReqElSet.size() == (pReqElSize - 1));
            assert(preparedReqElSet.find(pRequestEl) == preparedReqElSet.end());

            LOCK_COMMIT_REQUESTS();

                assert(commitRequests.find(req) == commitRequests.end());
                int setSize = commitRequests.size();
                commitRequests.insert(req);
                assert(commitRequests.size() == (setSize + 1));

            UNLOCK_COMMIT_REQUESTS();

#ifdef PROFILE_STATS
            req->metadata.startTimeCommitedSet = Utils::GetCurrentClockTime();
#endif

        UNLOCK_PREPARED_REQUESTS();
    }

#elif defined(CURE)

    void CoordinatorTx::LocalCommitRequest(unsigned int txId, std::vector<PhysicalTimeSpec> ct) {
        LOCK_PREPARED_REQUESTS();

            PrepareRequest *req(txIdToPreparedRequestsMap[txId]);
            req->commitTime = ct; // vector or scalar
            req->replicaId = _replicaId; // this is filled only here because it's needed for the comparison function for the commit requests set
            assert(req->metadata.DV.size() == _numReplicasPerPartition);

            PrepareRequestElement *pRequestEl = new PrepareRequestElement(txIdToPreparedRequestsMap[txId]->id,
                                                                          txIdToPreparedRequestsMap[txId]->prepareTime);
#ifdef PROFILE_STATS
            req->metadata.endTimePreparedSet = Utils::GetCurrentClockTime();
            double duration = (req->metadata.endTimePreparedSet - req->metadata.startTimePreparedSet).toMilliSeconds();

            {
                std::lock_guard<std::mutex> lk(SysStats::timeInPreparedSet.valuesMutex);
                SysStats::timeInPreparedSet.values.push_back(duration);
            }
#endif

            txIdToPreparedRequestsMap.erase(txId);
            preparedReqElSet.erase(pRequestEl);

            LOCK_COMMIT_REQUESTS();
                commitRequests.insert(req);
            UNLOCK_COMMIT_REQUESTS();

#ifdef PROFILE_STATS
            req->metadata.startTimeCommitedSet = Utils::GetCurrentClockTime();
#endif

        UNLOCK_PREPARED_REQUESTS();
    }

#endif

#if defined(H_CURE) || defined(WREN)

    void CoordinatorTx::updateClock(PhysicalTimeSpec time) {

        updateClockOnPrepare(time);


    }

#endif

#if defined(H_CURE) || defined(WREN)

    void CoordinatorTx::updateClockOnCommit(PhysicalTimeSpec time) {


        int64_t c_p, max;
        PhysicalTimeSpec maxTime;
        TO_64(maxTime, maxTime);

        LOCK_HLC();
            PhysicalTimeSpec currentTime = Utils::getCurrentClockTimeIn64bitFormat();

            SET_HYB_PHYS(c_p, currentTime.Seconds);
            SET_HYB_LOG(c_p, 0);

            max = MAX(c_p, HybridClock.Seconds);
            max = MAX(max, time.Seconds);

            maxTime.Seconds = max;

            HybridClock = maxTime;

        UNLOCK_HLC();

    }

#endif


#if defined(H_CURE) || defined(WREN)

    PhysicalTimeSpec CoordinatorTx::updateClockOnPrepare(PhysicalTimeSpec time) {
        PhysicalTimeSpec updatedTime;


        int64_t c_p, max_p, max_l, hc_p, hc_l, t_p, t_l;
        PhysicalTimeSpec oldHybridClock;
        int maxCase = 0;

        LOCK_HLC();
            //This is real time and it needs to be converted to the hybrid clock format
            PhysicalTimeSpec currentTime = Utils::getCurrentClockTimeIn64bitFormat();
            SET_HYB_PHYS(c_p, currentTime.Seconds);
            SET_HYB_LOG(c_p, 0);

            /* Take the physical part from time  */
            HYB_TO_PHYS(t_p, time.Seconds);
            HYB_TO_LOG(t_l, time.Seconds);

            /* Take the physical part from the node's Hybrid clock */
            oldHybridClock = HybridClock;
            HYB_TO_PHYS(hc_p, HybridClock.Seconds);
            HYB_TO_LOG(hc_l, HybridClock.Seconds);

            //We can compare the whole time, not only the physical part

            if (c_p > hc_p) {
                if (c_p > t_p) {
                    // the current physical clock is max
                    max_p = c_p;
                    max_l = 0;
                } else {
                    //the update time is max
                    max_p = t_p;
                    max_l = t_l + 1;
                }

            } else {
                if (hc_p > t_p) {
                    //the physical part of the hybrid clock is the max
                    max_p = hc_p;
                    max_l = hc_l + 1;
                } else if (hc_p == t_p) {
                    max_p = hc_p;
                    max_l = MAX(hc_l, t_l) + 1;
                } else {
                    max_p = t_p;
                    max_l = t_l + 1;
                }
            }

            assert(max_l < MAX_LOG);
            PhysicalTimeSpec maxTime;
            TO_64(maxTime, maxTime);

            SET_HYB_PHYS(maxTime.Seconds, max_p);
            SET_HYB_LOG(maxTime.Seconds, max_l);
            HybridClock = maxTime;

            if (prevHybridClockValue >= HybridClock) {
                SLOG((boost::format(
                        "UpdateClockOnPrepare :: maxCase =%d \n prevHybridClockValue = %s  HybridClock = %s .\n time = %s HCL = %s now = %s")
                      % maxCase
                      % Utils::physicaltime2str(prevHybridClockValue)
                      % Utils::physicaltime2str(HybridClock)
                      % Utils::physicaltime2str(time)
                      % Utils::physicaltime2str(oldHybridClock)
                      % Utils::physicaltime2str(currentTime)).str());
            }

            assert(prevHybridClockValue < HybridClock);
            prevHybridClockValue = HybridClock;
            updatedTime = HybridClock;
        UNLOCK_HLC();


        return updatedTime;
    }

#endif

////////////////////////////////////////////////// partitions  //////////////////////////////////////////////////


#if defined(H_CURE) || defined(WREN)

    void CoordinatorTx::HandleInternalCommitRequest(unsigned int txId, PhysicalTimeSpec ct) {
#ifdef MEASURE_STATISTICS
        SysStats::NumInternalCommitRequests += 1;
#endif
        LocalCommitRequest(txId, ct);
    }

#endif

#ifdef H_CURE

    void CoordinatorTx::updateServerMetadataIfSmaller(PhysicalTimeSpec rst) {

        if (_sdata.RST < rst) {
            LOCK_S_RST();
                _sdata.RST = MAX(_sdata.RST, rst);
            UNLOCK_S_RST();
        }

    }

#endif
#if defined(WREN)

    void CoordinatorTx::updateServerMetadataIfSmaller(PhysicalTimeSpec glst, PhysicalTimeSpec grst) {

        if (_sdata.GLST < glst) {
            LOCK_S_GST();
                _sdata.GLST = MAX(_sdata.GLST, glst);
                _sdata.GRST = MAX(_sdata.GRST, grst);
            UNLOCK_S_GST();
#ifdef MEASURE_VISIBILITY_LATENCY
            MVKVTXStore::Instance()->CheckAndRecordVisibilityLatency(_sdata.GLST, _sdata.GRST);
#endif
        } else {
            if (_sdata.GRST < grst) {
                LOCK_S_GST();
                    _sdata.GRST = MAX(_sdata.GRST, grst);
                UNLOCK_S_GST();
#ifdef MEASURE_VISIBILITY_LATENCY
                MVKVTXStore::Instance()->CheckAndRecordVisibilityLatency(_sdata.GLST, _sdata.GRST);
#endif
            }

        }


    }

#endif


#if defined(CURE)

    void CoordinatorTx::HandleInternalCommitRequest(unsigned int txId, std::vector<PhysicalTimeSpec> ct) {
#ifdef MEASURE_STATISTICS
        SysStats::NumInternalCommitRequests += 1;
#endif
        LocalCommitRequest(txId, ct);
    }

#endif

    PhysicalTimeSpec
    CoordinatorTx::LocalPrepareRequest(int txId, TxContex &cdata, const std::vector<std::string> &keys,
                                       std::vector<std::string> &values) {

        PhysicalTimeSpec prepareTime;
        PhysicalTimeSpec before_action, after_action;
        double action_duration = 0;
        LOCK_PREPARED_REQUESTS();
#ifdef H_CURE
            before_action = Utils::GetCurrentClockTime();

            prepareTime = updateClockOnPrepare(cdata.LDT);

            updateServerMetadataIfSmaller(cdata.RST);

            after_action = Utils::GetCurrentClockTime();

#endif

#ifdef WREN
            before_action = Utils::GetCurrentClockTime();

            prepareTime = updateClockOnPrepare(cdata.HT);

            updateServerMetadataIfSmaller(cdata.GLST, cdata.GRST);

            after_action = Utils::GetCurrentClockTime();

#endif
#ifdef CURE
            before_action = Utils::GetCurrentClockTime();

            WaitOnPrepare(cdata);

            bool update = false;
            int i;
            for (i = 0; i < cdata.DV.size(); i++) {
                if (i != _replicaId) {
                    if (_sdata.DV[i] < cdata.DV[i]) {
                        update = true;
                        break;
                    }
                }
            }
            if (update) {
                LOCK_DV();
                    for (; i < cdata.DV.size(); i++) {
                        _sdata.DV[i] = MAX(_sdata.DV[i], cdata.DV[i]);
                    }
                UNLOCK_DV();
            }
            after_action = Utils::GetCurrentClockTime();

            prepareTime = Utils::getCurrentClockTimeIn64bitFormat();

#endif

            action_duration = (after_action - before_action).toMilliSeconds();
            SysStats::PrepareActionDuration = SysStats::PrepareActionDuration + action_duration;
            SysStats::NumTimesPrepareActionIsExecuted++;

            PrepareRequest *pRequest = new PrepareRequest(txId, cdata, keys, values);
            PrepareRequestElement *pRequestEl = new PrepareRequestElement(txId, prepareTime);

            pRequest->prepareTime = prepareTime;

            pRequest->metadata.startTimePreparedSet = Utils::GetCurrentClockTime();

            txIdToPreparedRequestsMap[txId] = pRequest;
            assert(txIdToPreparedRequestsMap.find(txId) != txIdToPreparedRequestsMap.end());

            assert(preparedReqElSet.find(pRequestEl) == preparedReqElSet.end());
            int pReqElSetSize = preparedReqElSet.size();
            preparedReqElSet.insert(pRequestEl);
            assert(preparedReqElSet.size() == (pReqElSetSize + 1));

        UNLOCK_PREPARED_REQUESTS();

        return prepareTime;
    }

    void CoordinatorTx::InternalPrepareRequest(int txId, TxContex &cdata, const std::vector<std::string> &keys,
                                               std::vector<std::string> &values, int src) {
#ifdef MEASURE_STATISTICS
        SysStats::NumInternalPrepareRequests += 1;
#endif
        PhysicalTimeSpec prepareTime = LocalPrepareRequest(txId, cdata, keys, values);

        PbRpcPartitionClientPrepareRequestResult opResult;

        opResult.set_id(txId);
        opResult.set_src(_partitionId);
        opResult.mutable_pt()->set_seconds(prepareTime.Seconds);
        opResult.mutable_pt()->set_nanoseconds(prepareTime.NanoSeconds);

#ifdef MEASURE_STATISTICS
        opResult.set_blockduration(cdata.timeWaitedOnPrepareXact);
#endif

        _partitionWritesClients[src]->SendPrepareReply(opResult);
    }

    void CoordinatorTx::HandleInternalPrepareReply(unsigned int id, int srcPartition, PhysicalTimeSpec pt,
                                                   double blockDuration) {

        Transaction *tx = GetTransaction(id);
        {
            tx->partToPrepReqMap[srcPartition]->prepareTime = pt;
            tx->partToPrepReqMap[srcPartition]->prepareBlockDuration = blockDuration;
        }
        tx->prepareReqWaitHandle->SetIfCountZero();
    }

#if !defined(WREN)

    void CoordinatorTx::WaitOnTxSlice(unsigned int txId, TxContex &cdata) {

#ifdef FAKE_TX_WAIT
        cdata.waited_xact = 0;
        return;
#endif //FAKE_TX_WAIT

#ifdef MEASURE_STATISTICS
        bool slept = false;
        PhysicalTimeSpec initWait;
#endif //MEASURE_STATISTICS

        PhysicalTimeSpec initVV;

#ifdef H_CURE
        while (cdata.LDT >= MVKVTXStore::Instance()->GetVV(_replicaId)) {
#elif defined(CURE)
        while (cdata.DV[_replicaId] >= (initVV = MVKVTXStore::Instance()->GetVV(_replicaId))) {
#endif //PROTOCOLS

#ifdef MEASURE_STATISTICS
            if (!slept) {
                slept = true;
                initWait = Utils::GetCurrentClockTime();
            }
#endif //MEASURE_STATISTICS
            int useconds = 500;

            std::this_thread::sleep_for(std::chrono::microseconds(useconds));
        } // end of while

#ifdef MEASURE_STATISTICS
        if (slept) {
            PhysicalTimeSpec currentTime = Utils::GetCurrentClockTime();
            PhysicalTimeSpec endWait = currentTime - initWait;
            cdata.waited_xact = endWait.toMilliSeconds();

            SysStats::NumTxReadBlocks++;
            SysStats::TxReadBlockDuration = SysStats::TxReadBlockDuration + endWait.toMilliSeconds();
        }
#endif //MEASURE_STATISTICS
    }

#endif


#ifdef CURE

    void CoordinatorTx::WaitOnPrepare(TxContex &cdata) {

#ifdef MEASURE_STATISTICS
        bool slept = false;
        PhysicalTimeSpec initWait;
#endif //MEASURE_STATISTICS

        PhysicalTimeSpec currentTime, sleepTime, depTime;

        FROM_64(depTime, cdata.DV[_replicaId]);

        while (depTime >= (currentTime = Utils::GetCurrentClockTime())) {

#ifdef MEASURE_STATISTICS
            if (!slept) {
                slept = true;
                initWait = currentTime;
            }
#endif //MEASURE_STATISTICS
            sleepTime = depTime - currentTime;
            int sleep_us = MIN(sleepTime.toMicroSeconds() + 1, 10);
            std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));
        }

        assert(depTime < Utils::GetCurrentClockTime());

#ifdef MEASURE_STATISTICS
        if (slept) {
            PhysicalTimeSpec timeNow = Utils::GetCurrentClockTime();
            PhysicalTimeSpec endWait = timeNow - initWait;
            cdata.timeWaitedOnPrepareXact = endWait.toMilliSeconds();
            assert(cdata.timeWaitedOnPrepareXact > 0);

            SysStats::NumTxPreparePhaseBlocks++;
            SysStats::TxPreparePhaseBlockDuration =
                    SysStats::TxPreparePhaseBlockDuration + cdata.timeWaitedOnPrepareXact;

        } else {
            cdata.timeWaitedOnPrepareXact = 0;
        }
#endif //MEASURE_STATISTICS
    }


    void CoordinatorTx::WaitOnCommit(std::vector<PhysicalTimeSpec> cv) {


#ifdef FAKE_TX_WAIT
        std::lock_guard <std::mutex> lk(SysStats::CommitPhaseBlockingTimesMutex);
        SysStats::CommitPhaseBlockingTimes.push_back(0.0);
        return;
#endif //FAKE_TX_WAIT

#ifdef MEASURE_STATISTICS
        bool slept = false;
        PhysicalTimeSpec initWait;
#endif //MEASURE_STATISTICS

        PhysicalTimeSpec depTime, maxTime;
        std::vector<PhysicalTimeSpec>::iterator maxIt;
        maxIt = std::max_element(cv.begin(), cv.end());
        //convert max value to real time value
        FROM_64(depTime, (*maxIt));

        PhysicalTimeSpec currentTime, sleepTime;

        while (depTime >= (currentTime = Utils::GetCurrentClockTime())) {

#ifdef MEASURE_STATISTICS
            if (!slept) {
                slept = true;
                initWait = currentTime;
            }
#endif //MEASURE_STATISTICS
            sleepTime = depTime - currentTime;
            int sleep_us = sleepTime.toMicroSeconds() + 1;
            std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));

        }

        assert(depTime < Utils::GetCurrentClockTime());

#ifdef MEASURE_STATISTICS
        if (slept) {
            PhysicalTimeSpec currentTime = Utils::GetCurrentClockTime();
            PhysicalTimeSpec endWait = currentTime - initWait;
            {
                std::lock_guard<std::mutex> lk(SysStats::CommitPhaseBlockingTimesMutex);
                SysStats::CommitPhaseBlockingTimes.push_back(endWait.toMilliSeconds());

            }
        } else {
            std::lock_guard<std::mutex> lk(SysStats::CommitPhaseBlockingTimesMutex);
            SysStats::CommitPhaseBlockingTimes.push_back(0);
        }
#endif //MEASURE_STATISTICS
    }

#endif //CURE

////////////////////////////////////////////////// update replication //////////////////////////////////////////////////

    bool CoordinatorTx::HandlePropagatedUpdate(std::vector<PropagatedUpdate *> &updates) {
#ifdef MEASURE_STATISTICS
        if (!updates.empty()) {
            SysStats::NumRecvUpdateReplicationMsgs += 1;
            SysStats::NumRecvUpdateReplications += updates.size();
        }
#endif

        return MVKVTXStore::Instance()->HandlePropagatedUpdate(updates);
    }

    bool CoordinatorTx::HandleHeartbeat(Heartbeat &hb, int srcReplica) {
#ifdef MEASURE_STATISTICS
        SysStats::NumRecvHeartbeats += 1;
#endif
        return MVKVTXStore::Instance()->HandleHeartbeat(hb, srcReplica);
    }

    PhysicalTimeSpec CoordinatorTx::calculateUpdateBatchTime() {

        PhysicalTimeSpec ub;
        LOCK_PREPARED_REQUESTS();
            TO_64(ub, ub);
            /* Case where there are PrepareRequests waiting */
            if (preparedReqElSet.size() > 0) {
                std::set<PrepareRequestElement *, preparedRequestsElementComparator>::iterator it;
                // Elements of preparedReqElSet are ordered by their prepare time,
                // so the first element of the set would be the element with lowest propose time

                ub = (*(preparedReqElSet.begin()))->prepareTime;
#ifndef WREN
                ub.Seconds = ub.Seconds - 1;
#endif

                if (ub < ubPrevValue) {
                    SLOG((boost::format(
                            "calculateUpdateBatchTime CASE 1 :: UB = %s UBPrevValue = %s from case %d\n")
                          % Utils::physicaltime2str(ub) % Utils::physicaltime2str(ubPrevValue) % prevUBCase).str());
                }
                assert(ub >= ubPrevValue);
                ubPrevValue = ub;
                prevUBCase = 1;

            } else {

                assert(preparedReqElSet.size() == 0);
                /* Case where there are no PrepareRequests waiting */
#if defined(H_CURE) || defined(WREN)
                PhysicalTimeSpec now = Utils::getCurrentClockTimeIn64bitFormat();
                SET_HYB_PHYS(now.Seconds, now.Seconds);
                SET_HYB_LOG(now.Seconds, 0);

                ub = MAX(HybridClock, now);
#ifndef WREN
                ub.Seconds = ub.Seconds - 1;
#endif

#elif defined(CURE)

                ub = Utils::getCurrentClockTimeIn64bitFormat();
                ub.Seconds = ub.Seconds - 1;
#endif
                if (ub < ubPrevValue) {
                    SLOG((boost::format(
                            "calculateUpdateBatchTime CASE 2 :: UB = %s UBPrevValue = %s from case %d \n")
                          % Utils::physicaltime2str(ub) % Utils::physicaltime2str(ubPrevValue) % prevUBCase).str());
                }
                assert(ub >= ubPrevValue);
                ubPrevValue = ub;
                prevUBCase = 2;
            }

        UNLOCK_PREPARED_REQUESTS();
        return ub;
    }

    void CoordinatorTx::PersistAndPropagateUpdates() {

        /*
         * Initialize replication
         */

        for (unsigned int i = 0; i < _myReplicas.size(); i++) {
            int rId = _myReplicas[i].ReplicaId;
            _replicationClients[rId]->InitializeReplication(_currentPartition);
        }

        SLOG("[INFO]: Update propagation thread is up.");

        int maxWaitTime = SysConfig::ReplicationHeartbeatInterval; // microseconds

        std::vector<LocalUpdate *> updatesToBeReplicatedQueue;
        std::set<PrepareRequest *, commitRequestsComparator>::iterator itLow, itUpp, it, it_temp;
        std::vector<PrepareRequest *> _localTxRequests;
        std::vector<PrepareRequest *>::iterator it_lc;
        bool sendHearbeat = true;
        int numUpdates = 0;
        PhysicalTimeSpec temp;

        // wait until all replicas are ready
        while (!_readyToServeRequests) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        while (true) {
            std::this_thread::sleep_for(std::chrono::microseconds(maxWaitTime));

            _localTxRequests.clear();
            PhysicalTimeSpec prevCommitReqTS;

            PhysicalTimeSpec before_action, after_action;
            double action_duration = 0;

            before_action = Utils::GetCurrentClockTime();
            PhysicalTimeSpec updateBatchTime = calculateUpdateBatchTime();
            after_action = Utils::GetCurrentClockTime();

            action_duration = (after_action - before_action).toMilliSeconds();
            SysStats::CalcUBTActionDuration = SysStats::PrepareActionDuration + action_duration;
            SysStats::NumTimesCalcUBTIsExecuted++;

            sendHearbeat = true;

            LOCK_COMMIT_REQUESTS();

                int sizeBefore = 0, sizeAfter = 0;
                sizeBefore = commitRequests.size();
                PhysicalTimeSpec prevCommT;
                if (commitRequests.size() > 0) {
                    assert(commitRequests.size() > 0);
                    /* Don't send heartbeat, there are commit requests waiting */
                    sendHearbeat = false;
                    /*
                     * Find the requests that have commit time lower than the update batch time and
                     * set them apart as local updates that need to be persisted and propagated
                     * */

                    itLow = commitRequests.begin();

#if defined(H_CURE) || defined(WREN)
                    PrepareRequest *ubPrepReq = new PrepareRequest(updateBatchTime);

#elif defined(CURE)
                    PrepareRequest *ubPrepReq = new PrepareRequest(updateBatchTime, _replicaId,
                                                                   _numReplicasPerPartition);

#endif
                    itUpp = commitRequests.lower_bound(ubPrepReq);
                    if (itLow != itUpp) {
                        std::copy(itLow, itUpp, std::back_inserter(_localTxRequests));
                        commitRequests.erase(itLow, itUpp);
                    }

                    sizeAfter = commitRequests.size();
                    assert(sizeAfter <= sizeBefore);

                }
            UNLOCK_COMMIT_REQUESTS();

            if (sendHearbeat) {

                /* No commit requests present. Sending Heartbeats messages. */
                MVKVTXStore::Instance()->updateAndGetVV(_replicaId, updateBatchTime);

                Heartbeat hb;
                hb.PhysicalTime = updateBatchTime;

                assert(hb.PhysicalTime >= hbPhysTimePrevValue);
                hbPhysTimePrevValue = hb.PhysicalTime;
                hb.LogicalTime = MVKVTXStore::Instance()->getLocalClock();

                // send heartbeat
                for (unsigned int j = 0; j < _myReplicas.size(); j++) {
                    int rId = _myReplicas[j].ReplicaId;

                    _replicationClients[rId]->SendHeartbeat(hb);
                }
            } else {
                /* There are transactions that need to be persisted and replicated  */

                if (_localTxRequests.size() > 0) {
                    //Persist and send the updates

                    for (int i = 0; i < _localTxRequests.size(); i++) {

#if defined(H_CURE) || defined(WREN)
                        assert(temp <= _localTxRequests[i]->commitTime);
                        temp = _localTxRequests[i]->commitTime;

#elif defined(CURE)
                        assert(temp <= _localTxRequests[i]->commitTime[_replicaId]);
                        temp = _localTxRequests[i]->commitTime[_replicaId];
#endif

                        updatesToBeReplicatedQueue.clear();
                        for (int j = 0; j < _localTxRequests[i]->keys.size(); j++) {

                            MVKVTXStore::Instance()->Write(_localTxRequests[i]->commitTime,
                                                           _localTxRequests[i]->metadata,
                                                           _localTxRequests[i]->keys[j],
                                                           _localTxRequests[i]->values[j],
                                                           updatesToBeReplicatedQueue);

#ifdef PROFILE_STATS
                            _localTxRequests[i]->metadata.endTimeCommitedSet = Utils::GetCurrentClockTime();
                            double duration = (_localTxRequests[i]->metadata.endTimeCommitedSet -
                                               _localTxRequests[i]->metadata.startTimeCommitedSet).toMilliSeconds();

                            {
                                std::lock_guard<std::mutex> lk(SysStats::timeInCommitedSet.valuesMutex);
                                SysStats::timeInCommitedSet.values.push_back(duration);
                            }
#endif

                        }

                        MVKVTXStore::Instance()->updateVV(_replicaId, updateBatchTime);

                        for (unsigned int j = 0; j < _myReplicas.size(); j++) {
                            int rId = _myReplicas[j].ReplicaId;
                            _replicationClients[rId]->SendUpdate(updatesToBeReplicatedQueue);

#ifdef MEASURE_STATISTICS
                            SysStats::NumSentUpdates += updatesToBeReplicatedQueue.size();
                            SysStats::NumSentBatches += 1;
#endif
                        }

                        for (unsigned int i = 0; i < updatesToBeReplicatedQueue.size(); i++) {
                            delete updatesToBeReplicatedQueue[i];
                        }

                        updatesToBeReplicatedQueue.clear();

                    }
                }
            }


        }
    }

///////////////// GST Tree //////////////////////////////////////////

    void CoordinatorTx::BuildGSTTree() {
        std::queue<TreeNode *> treeNodes;

        // root node
        int i = 0;
        _treeRoot = new TreeNode();
        _treeRoot->Name = _allPartitions[i][_replicaId].Name;
        _treeRoot->PartitionId = i;
        _treeRoot->Parent = nullptr;
        _treeRoot->PartitionClient = (i != _partitionId ? _partitionClients[i] : nullptr);

        treeNodes.push(_treeRoot);

        if (i == _partitionId) {
            _currentTreeNode = _treeRoot;
        }
        i += 1;

        // internal & leaf nodes
        while (!treeNodes.empty()) {
            TreeNode *parent = treeNodes.front();
            treeNodes.pop();

            for (int j = 0; j < SysConfig::NumChildrenOfTreeNode && i < _numPartitions; ++j) {
                TreeNode *node = new TreeNode();
                node->Name = _allPartitions[i][_replicaId].Name;
                node->PartitionId = i;
                node->Parent = parent;
                node->PartitionClient = (i != _partitionId ? _partitionClients[i] : nullptr);

                treeNodes.push(node);

                if (i == _partitionId) {
                    _currentTreeNode = node;
                }
                ++i;

                parent->Children.push_back(node);
            }
        }

        // set current node type
        std::string nodeTypeStr;
        if (_currentTreeNode->Parent == nullptr) {
            _currentTreeNode->NodeType = TreeNodeType::RootNode;
            nodeTypeStr = "root";
        } else if (_currentTreeNode->Children.empty()) {
            _currentTreeNode->NodeType = TreeNodeType::LeafNode;
            nodeTypeStr = "leaf";
        } else {
            _currentTreeNode->NodeType = TreeNodeType::InternalNode;
            nodeTypeStr = "internal";
        }

        _gstRoundNum = 0;

        std::string parentStr = _currentTreeNode->Parent ?
                                std::to_string(_currentTreeNode->Parent->PartitionId) : "null";
        std::string childrenStr;
        for (unsigned int i = 0; i < _currentTreeNode->Children.size(); ++i) {
            childrenStr += std::to_string(_currentTreeNode->Children[i]->PartitionId) + " ";
        }
        SLOG("I'm a/an " + nodeTypeStr + " node. Parent is " + parentStr + ". Children are " +
             childrenStr);
    }


    void CoordinatorTx::SendGSTAtRoot() {
        // wait until all replicas are ready

        while (!_readyToServeRequests) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        while (true) {
            std::this_thread::sleep_for(std::chrono::microseconds(SysConfig::GSTComputationInterval));

#ifdef MEASURE_STATISTICS
            SysStats::NumRecvGSTs += 1;
#endif

            if (_numPartitions == 1) {
#ifdef CURE
                setGSV(MVKVTXStore::Instance()->GetPVV());

#elif defined(H_CURE)
                std::vector<PhysicalTimeSpec> vv = MVKVTXStore::Instance()->GetVV();

                setRST(MVKVTXStore::Instance()->getRemoteVVmin());

#elif defined(WREN)
                PhysicalTimeSpec remote, local;
                MVKVTXStore::Instance()->getCurrentRemoteAndLocalValues(local, remote);

                setGSTs(local, remote);

#endif
            } else {


#ifdef CURE
                std::vector<PhysicalTimeSpec> gsv = getGSV();

                for (unsigned int i = 0; i < _currentTreeNode->Children.size(); ++i) {
                    _currentTreeNode->Children[i]->PartitionClient->SendGSV(gsv);
                }

#endif


#if defined(H_CURE)
                PhysicalTimeSpec rst = getRST();

                for (unsigned int i = 0; i < _currentTreeNode->Children.size(); ++i) {
                    _currentTreeNode->Children[i]->PartitionClient->SendGST(rst);
                }
#endif


#if defined(WREN)

                PhysicalTimeSpec lst, rst;
                getGSTs(lst, rst);

                for (unsigned int i = 0; i < _currentTreeNode->Children.size(); ++i) {
                    _currentTreeNode->Children[i]->PartitionClient->SendGST(lst, rst);
                }

#endif
            }
        }
    }

#ifdef H_CURE

    void CoordinatorTx::setRSTIfSmaller(PhysicalTimeSpec rst) {
        if (_sdata.RST < rst) {
            LOCK_S_RST();
                _sdata.RST = MAX(_sdata.RST, rst);
            UNLOCK_S_RST();

#ifdef MEASURE_VISIBILITY_LATENCY
            MVKVTXStore::Instance()->CheckAndRecordVisibilityLatency( _sdata.RST);
#endif
        }
    }

    void CoordinatorTx::setRST(PhysicalTimeSpec rst) {
        LOCK_S_RST();
            _sdata.RST = rst;
        UNLOCK_S_RST();
    }

#endif

#ifdef WREN

    void CoordinatorTx::setGRST(PhysicalTimeSpec t) {
        LOCK_S_GST();
            _sdata.GRST = t;
        UNLOCK_S_GST();
    }

    void CoordinatorTx::setGLST(PhysicalTimeSpec t) {
        LOCK_S_GST();
            _sdata.GLST = t;
        UNLOCK_S_GST();
    }

    void CoordinatorTx::setGSTs(PhysicalTimeSpec &lst, PhysicalTimeSpec &rst) {
        LOCK_S_GST();
            _sdata.GLST = lst;
            _sdata.GRST = rst;
        UNLOCK_S_GST();
    }


#endif

#if defined(H_CURE)

    void CoordinatorTx::HandleGSTFromParent(PhysicalTimeSpec gst) {

        setRSTIfSmaller(gst);

        if (_currentTreeNode->NodeType == TreeNodeType::InternalNode) {
            // send GST to children
            for (unsigned int i = 0; i < _currentTreeNode->Children.size(); ++i) {
                _currentTreeNode->Children[i]->PartitionClient->SendGST(gst);
            }
        } else if (_currentTreeNode->NodeType == TreeNodeType::LeafNode) {
            // start another round of GST computation

            _gstRoundNum += 1;

#ifdef MEASURE_STATISTICS
            SysStats::NumGSTRounds += 1;
#endif

            PhysicalTimeSpec lst;

            lst = MVKVTXStore::Instance()->getRemoteVVmin();
            int round = _gstRoundNum;

            _currentTreeNode->Parent->PartitionClient->SendLST(lst, round);
        }
    }

#endif

#ifdef WREN

    void CoordinatorTx::HandleGSTFromParent(PhysicalTimeSpec lst, PhysicalTimeSpec rst) {

        updateServerMetadataIfSmaller(lst, rst);

        if (_currentTreeNode->NodeType == TreeNodeType::InternalNode) {
            // send GST(i.e. GLST,GRST) to children
            for (unsigned int i = 0; i < _currentTreeNode->Children.size(); ++i) {
                _currentTreeNode->Children[i]->PartitionClient->SendGST(lst, rst);
            }
        } else if (_currentTreeNode->NodeType == TreeNodeType::LeafNode) {
            // start another round of GST computation
            _gstRoundNum += 1;

#ifdef MEASURE_STATISTICS
            SysStats::NumGSTRounds += 1;
#endif

            PhysicalTimeSpec glst, grst;

            MVKVTXStore::Instance()->getCurrentRemoteAndLocalValues(glst, grst);

            _currentTreeNode->Parent->PartitionClient->SendLST(glst, grst, _gstRoundNum);
        }
    }

    void CoordinatorTx::setGlobalStabilizationTimesIfSmaller(PhysicalTimeSpec lst, PhysicalTimeSpec rst) {

        if (_sdata.GLST < lst) {
            LOCK_S_GST();

                _sdata.GLST = MAX(_sdata.GLST, lst);
                _sdata.GRST = MAX(_sdata.GRST, rst);
            UNLOCK_S_GST();

#ifdef MEASURE_VISIBILITY_LATENCY
            MVKVTXStore::Instance()->CheckAndRecordVisibilityLatency(_sdata.GLST, _sdata.GRST);
#endif
        } else {
            if (_sdata.GRST < rst) {
                LOCK_S_GST();
                    _sdata.GRST = MAX(_sdata.GRST, rst);
                UNLOCK_S_GST();
#ifdef MEASURE_VISIBILITY_LATENCY
                MVKVTXStore::Instance()->CheckAndRecordVisibilityLatency(_sdata.GLST, _sdata.GRST);
#endif
            }

        }

    }


#endif


#ifdef WREN

    void CoordinatorTx::HandleLSTFromChildren(PhysicalTimeSpec lst, PhysicalTimeSpec rst, int round) {

        std::lock_guard<std::mutex> lk(_minLSTMutex);

        if (_receivedLSTCounts.find(round) == _receivedLSTCounts.end()) {
            _receivedLSTCounts[round] = 1;
            _minLSTs[round] = lst;
            _minRSTs[round] = rst;
        } else {
            _receivedLSTCounts[round] += 1;
            _minLSTs[round] = MIN(_minLSTs[round], lst);
            _minRSTs[round] = MIN(_minRSTs[round], rst);
        }

        while (true) {
            int currentRound = _gstRoundNum + 1;

            if (_receivedLSTCounts.find(currentRound) != _receivedLSTCounts.end()) {
                unsigned int lstCount = _receivedLSTCounts[currentRound];
                PhysicalTimeSpec minLST = _minLSTs[currentRound];
                PhysicalTimeSpec minRST = _minRSTs[currentRound];

                if (lstCount == _currentTreeNode->Children.size()) {

                    PhysicalTimeSpec myLST, myRST;
                    MVKVTXStore::Instance()->getCurrentRemoteAndLocalValues(myLST, myRST);

                    minLST = MIN(minLST, myLST);
                    minRST = MIN(minRST, myRST);


                    // received all LSTs, RSTs from children
                    if (_currentTreeNode->NodeType == TreeNodeType::InternalNode) {
                        // send LST and RST to parent
                        _currentTreeNode->Parent->PartitionClient->SendLST(minLST, minRST, currentRound);
                    } else if (_currentTreeNode->NodeType == TreeNodeType::RootNode) {
                        // update GLST and GRST at root
                        setGlobalStabilizationTimesIfSmaller(minLST, minRST);
                    }

                    _receivedLSTCounts.erase(currentRound);
                    _minLSTs.erase(currentRound);
                    _minRSTs.erase(currentRound);
                    _gstRoundNum = currentRound;

                } else {
                    break;
                }
            } else {

                break;
            }
        }

    }

#endif

#if defined(H_CURE)

    void CoordinatorTx::HandleLSTFromChildren(PhysicalTimeSpec lst, int round) {
        {
            std::lock_guard<std::mutex> lk(_minLSTMutex);

            if (_receivedLSTCounts.find(round) == _receivedLSTCounts.end()) {
                _receivedLSTCounts[round] = 1;
                _minLSTs[round] = lst;
            } else {
                _receivedLSTCounts[round] += 1;
                _minLSTs[round] = MIN(_minLSTs[round], lst);
            }

            while (true) {
                int currentRound = _gstRoundNum + 1;

                if (_receivedLSTCounts.find(currentRound) != _receivedLSTCounts.end()) {
                    unsigned int lstCount = _receivedLSTCounts[currentRound];
                    PhysicalTimeSpec minLST = _minLSTs[currentRound];

                    if (lstCount == _currentTreeNode->Children.size()) {
                        PhysicalTimeSpec myLST;

                        myLST = MVKVTXStore::Instance()->getRemoteVVmin();
                        minLST = MIN(minLST, myLST);

                        // received all LSTs from children
                        if (_currentTreeNode->NodeType == TreeNodeType::InternalNode) {
                            // send LST to parent
                            _currentTreeNode->Parent->PartitionClient->SendLST(minLST, currentRound);

                        } else if (_currentTreeNode->NodeType == TreeNodeType::RootNode) {
                            PhysicalTimeSpec gst = minLST;
                            // update GST at root

                            setRSTIfSmaller(gst);
                        }

                        _receivedLSTCounts.erase(currentRound);
                        _minLSTs.erase(currentRound);
                        _gstRoundNum = currentRound;
                    } else {
                        break;
                    }
                } else {

                    break;
                }
            }
        }
    }


#endif

#ifdef CURE

    void CoordinatorTx::HandleGSVFromParent(std::vector<PhysicalTimeSpec> gsv) {

        setGSVIfSmaller(gsv);

        if (_currentTreeNode->NodeType == TreeNodeType::InternalNode) {
            // send GST to children
            for (unsigned int i = 0; i < _currentTreeNode->Children.size(); ++i) {
                _currentTreeNode->Children[i]->PartitionClient->SendGSV(gsv);
            }
        } else if (_currentTreeNode->NodeType == TreeNodeType::LeafNode) {
            // start another round of GST computation

            _gstRoundNum += 1;

#ifdef MEASURE_STATISTICS
            SysStats::NumGSTRounds += 1;
#endif
            std::vector<PhysicalTimeSpec> pvv = MVKVTXStore::Instance()->GetPVV();
            int round = _gstRoundNum;

            _currentTreeNode->Parent->PartitionClient->SendPVV(pvv, round);
        }
    }

    void CoordinatorTx::HandlePVVFromChildren(std::vector<PhysicalTimeSpec> pvv, int round) {
        {
            std::lock_guard<std::mutex> lk(_minLSTMutex);

            if (_receivedLSTCounts.find(round) == _receivedLSTCounts.end()) {
                _receivedLSTCounts[round] = 1;
                _minPVVs[round] = pvv;
            } else {
                _receivedLSTCounts[round] += 1;
                for (int i = 0; i < pvv.size(); i++) {
                    if (_minPVVs[round][i] > pvv[i])
                        _minPVVs[round][i] = pvv[i];
                }
            }

            while (true) {
                int currentRound = _gstRoundNum + 1;

                if (_receivedLSTCounts.find(currentRound) != _receivedLSTCounts.end()) {
                    unsigned int lstCount = _receivedLSTCounts[currentRound];
                    std::vector<PhysicalTimeSpec> minPVV = _minPVVs[currentRound];

                    if (lstCount == _currentTreeNode->Children.size()) {
                        std::vector<PhysicalTimeSpec> myPVV = MVKVTXStore::Instance()->GetPVV();
                        for (int i = 0; i < myPVV.size(); i++) {
                            if (minPVV[i] > myPVV[i])
                                minPVV[i] = myPVV[i];
                        }

                        // received all LSTs from children
                        if (_currentTreeNode->NodeType == TreeNodeType::InternalNode) {
                            // send LST to parent
                            _currentTreeNode->Parent->PartitionClient->SendPVV(minPVV, currentRound);

                        } else if (_currentTreeNode->NodeType == TreeNodeType::RootNode) {
                            std::vector<PhysicalTimeSpec> gsv = minPVV;
                            // update GST at root

                            setGSVIfSmaller(gsv);
                        }

                        _receivedLSTCounts.erase(currentRound);
                        _minPVVs.erase(currentRound);
                        _gstRoundNum = currentRound;
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
    }

    void CoordinatorTx::setGSVIfSmaller(std::vector<PhysicalTimeSpec> &gsv) {
        //Assumes 64 bit
#ifndef SIXTY_FOUR_BIT_CLOCK
        assert(false);
#endif
        bool smaller = false;
        int i, j;

        for (i = 0; i < gsv.size(); i++) {
            if (_sdata.DV[i] < gsv[i]) {
                smaller = true;
                break;
            }
        }

        if (smaller) {
            LOCK_DV();
                for (j = i; j < gsv.size(); j++) {
                    if (_sdata.DV[j] < gsv[j]) {
                        _sdata.DV[j] = gsv[j];
                    }
                }
            UNLOCK_DV();

#ifdef MEASURE_VISIBILITY_LATENCY
            MVKVTXStore::Instance()->CheckAndRecordVisibilityLatency(_sdata.DV);
#endif
        }
    }

    void CoordinatorTx::setGSV(std::vector<PhysicalTimeSpec> gsv) {
        LOCK_DV();
            for (int i = 0; i < gsv.size(); i++) {
                if (_sdata.DV[i] > gsv[i]) {
                    ASSERT(false);
                }
                _sdata.DV[i] = gsv[i];
            }
        UNLOCK_DV();

#ifdef MEASURE_VISIBILITY_LATENCY
        MVKVTXStore::Instance()->CheckAndRecordVisibilityLatency(_sdata.DV);
#endif
    }

    std::vector<PhysicalTimeSpec> CoordinatorTx::getGSV() {
        return _sdata.DV;
    }


#endif

#ifdef H_CURE

    void CoordinatorTx::UpdateRST() {

        // Wait until all partitions are ready.
        while (!_readyToServeRequests) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        while (true) {

            std::this_thread::sleep_for(std::chrono::microseconds(SysConfig::RSTComputationInterval));

            PhysicalTimeSpec rst = MVKVTXStore::Instance()->updateAndGetRST();

            for (auto it = _partitionClients.begin(); it != _partitionClients.end(); ++it) {
                PartitionKVClient *partitionClient = it->second;

                partitionClient->SendRST(rst, _partitionId);
            }
        }
    }

    PhysicalTimeSpec CoordinatorTx::getRST() {

        return _sdata.RST;
    };

    void CoordinatorTx::HandleRSTFromPeer(PhysicalTimeSpec rst, int peerId) {

        MVKVTXStore::Instance()->HandleRSTFromPeer(rst, peerId);
        LOCK_S_RST();
            _sdata.RST = MVKVTXStore::Instance()->getRSTMin();
        UNLOCK_S_RST();
    }


#endif

#ifdef WREN

    void CoordinatorTx::UpdateStabilizationTimes() {

        // Wait until all partitions are ready.
        while (!_readyToServeRequests) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        while (true) {

            std::this_thread::sleep_for(std::chrono::microseconds(SysConfig::RSTComputationInterval));

            PhysicalTimeSpec rst, lst;
            MVKVTXStore::Instance()->updateAndGetStabilizationTimes(lst, rst);

            for (auto it = _partitionClients.begin(); it != _partitionClients.end(); ++it) {
                PartitionKVClient *partitionClient = it->second;
                partitionClient->SendStabilizationTimesToPeers(lst, rst, _partitionId);
            }
        }
    }

    void CoordinatorTx::getGSTs(PhysicalTimeSpec &glst, PhysicalTimeSpec &grst) {

        glst = _sdata.GLST;
        grst = _sdata.GRST;

    };


    PhysicalTimeSpec CoordinatorTx::GetRST() {

        PhysicalTimeSpec rst;
        rst = _sdata.GRST;

        return rst;
    };

    PhysicalTimeSpec CoordinatorTx::GetLST() {

        PhysicalTimeSpec lst;
        lst = _sdata.GLST;

        return lst;
    };

    void CoordinatorTx::HandleStabilizationTimesFromPeer(PhysicalTimeSpec lst, PhysicalTimeSpec rst, int peerId) {
        MVKVTXStore::Instance()->HandleStabilizationTimesFromPeer(lst, rst, peerId);

        PhysicalTimeSpec rstMin = MVKVTXStore::Instance()->getRSTMin();
        PhysicalTimeSpec lstMin = MVKVTXStore::Instance()->getLSTMin();
        LOCK_S_GST();
            _sdata.GRST = rstMin;
            _sdata.GLST = lstMin;
        UNLOCK_S_GST();
    }


#endif


} // namespace scc

