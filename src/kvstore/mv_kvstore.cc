#include "kvstore/mv_kvstore.h"

namespace scc {

    MVKVTXStore *MVKVTXStore::Instance() {
        static MVKVTXStore store;
        return &store;
    }

    MVKVTXStore::MVKVTXStore() {
        for (int i = 0; i < SysConfig::NumItemIndexTables; i++) {
            _indexTables.push_back(new ItemIndexTable);
            _indexTableMutexes.push_back(new std::mutex);
        }
    }

    MVKVTXStore::~MVKVTXStore() {
        for (int i = 0; i < SysConfig::NumItemIndexTables; i++) {
            delete _indexTables[i];
            delete _indexTableMutexes[i];
        }
    }

    void MVKVTXStore::SetPartitionInfo(DBPartition p, int numPartitions, int numReplicasPerPartition) {
        _partitionId = p.PartitionId;
        _replicaId = p.ReplicaId;
        _numPartitions = numPartitions;
        _numReplicasPerPartition = numReplicasPerPartition;
    }

    void MVKVTXStore::Initialize() {

        _localClock = 0;
        _replicaClock = 0;

        _pendingInvisibleVersions.resize(_numReplicasPerPartition);
        _numCheckedVersions = 0;

        PhysicalTimeSpec pt(0, 0);
        for (int i = 0; i < _numReplicasPerPartition; ++i) {
            _PVV.push_back(pt);
            _PVV_latest.push_back(pt);
            _LVV.push_back(0);

#if defined(H_CURE) || defined(WREN)
            _HVV.push_back(pt);
            _HVV_latest.push_back(pt);

#endif

//#ifdef CURE
//            DV.push_back(PhysicalTimeSpec(0, 0));
//#endif
        }

        _initItemVersionUT = pt;

#ifdef SPIN_LOCK
        _gst_lock = 0;
        _pvv_lock = 0;
#endif

#if defined(H_CURE) || defined(WREN)
        for (int i = 0; i < 32; i++) {
            _hvv_l[i] = 0;
        }
#endif
#if defined(H_CURE) || defined(WREN)

        for (int i = 0; i < _numPartitions; i++) {
            _RST[i] = pt;
            _RSTMutex[i] = new std::mutex;
#ifdef WREN
            _LST[i] = pt;
            _LSTMutex[i] = new std::mutex;
#endif
        }
#endif


        SLOG("[INFO]: MVKVTXStore initialized!");
    }

    void MVKVTXStore::GetSysClockTime(PhysicalTimeSpec &physicalTime, int64_t &logicalTime) {


#if defined(H_CURE) || defined(WREN)
        int64_t l_p, c_p;
        LOCK_VV(_replicaId);
            logicalTime = _localClock;
            physicalTime = Utils::GetCurrentClockTime();
            TO_64(physicalTime, physicalTime);
            SET_HYB_PHYS(l_p, physicalTime.Seconds);
            SET_HYB_LOG(l_p, 0);
            //Update only if local real phyisical catches up with hybrid
            //TODO: should we still move it forward by Delta?
            if (l_p > _HVV[_replicaId].Seconds) {
                _HVV[_replicaId].Seconds = l_p;
            }
            physicalTime = _HVV[_replicaId];
        UNLOCK_VV(_replicaId);

#else //CURE
        LOCK_VV(_replicaId);

            physicalTime = Utils::GetCurrentClockTime();
            TO_64(physicalTime, physicalTime);
            logicalTime = _localClock;

            // also update local element of VV
            _PVV[_replicaId] = physicalTime;
            _LVV[_replicaId] = logicalTime;

        UNLOCK_VV(_replicaId);

#endif
    }

    // only used for initialization (key loading)

    bool MVKVTXStore::Add(const std::string &key, const std::string &value) {
        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        // check if key exists
        bool keyExisting = false;
        {
            std::lock_guard<std::mutex> lk(*(_indexTableMutexes[ti]));
            if (_indexTables[ti]->find(key) != _indexTables[ti]->end()) {
                keyExisting = true;
            }
        }

        if (!keyExisting) {
            // create new item anchor and version
            ItemAnchor *newAnchor = new ItemAnchor(key, _numReplicasPerPartition, _replicaId);
            ItemVersion *newVersion = new ItemVersion(value);

            newVersion->LUT = 0;
            newVersion->RIT = Utils::GetCurrentClockTime();
#ifdef MEASURE_VISIBILITY_LATENCY
            newVersion->CreationTime = _initItemVersionUT;
#endif
            newVersion->UT = _initItemVersionUT;
            newVersion->SrcReplica = 0;
#if defined(H_CURE) || defined(WREN)
            newVersion->RST = _initItemVersionUT;
#endif

#ifdef LOCALITY
            int keyInPartition = std::stoi(key) / _numPartitions;
            int srcReplica = _replicaId;

            assert(srcReplica <=_numReplicasPerPartition);
            //fprintf(stdout, "Adding key %d, the %d for partition %d. Assigned to replica %d with PUT %f\n", std::stoi(key), keyInPartition,
            //        _partitionId, srcReplica, _initItemVersionPUT.toMilliSeconds());

            newVersion->SrcReplica = srcReplica;
#endif


            newVersion->SrcPartition = _partitionId;
            newVersion->Persisted = true;

#ifdef CURE
            newVersion->DV.resize(_numReplicasPerPartition);
            for (int j = 0; j < _numReplicasPerPartition; j++) {
                newVersion->DV[j] = _initItemVersionUT; //PhysicalTimeSpec(0, 0);
            }
#endif //CURE


           newAnchor->InsertVersion(newVersion);

            // insert into the index if key still does not exist
            {
                std::lock_guard<std::mutex> lk(*(_indexTableMutexes[ti]));
                if (_indexTables[ti]->find(key) != _indexTables[ti]->end()) {
                    keyExisting = true;

                    // key already exists, release pre-created objects
                    delete newAnchor;
                    delete newVersion;

                } else {
                    _indexTables[ti]->insert(ItemIndexTable::value_type(key, newAnchor));
                }
            }
        }

        return !keyExisting;
    }

    bool MVKVTXStore::AddAgain(const std::string &key, const std::string &value) {

        assert(false); //Not sure it works with other  flags now
        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        //Treat key as non-existing



        //Retrieve existing anchor
        ItemAnchor *newAnchor = _indexTables[ti]->at(key);
        ItemVersion *newVersion = new ItemVersion(value);

        newVersion->LUT = 0;
        newVersion->RIT = Utils::GetCurrentClockTime();
#ifdef MEASURE_VISIBILITY_LATENCY
        newVersion->CreationTime = _initItemVersionUT;
#endif
        PhysicalTimeSpec putTime = _initItemVersionUT;
        putTime.Seconds += 1;//Newer version w.r.t. previous one
        TO_64(newVersion->UT, putTime);
        newVersion->SrcReplica = 0;

#ifdef LOCALITY
        int keyInPartition = std::stoi(key) / _numPartitions;
        int srcReplica = ((double) keyInPartition) / 333333.0;
        fprintf(stdout, "(Be sure #keys/dc is correct) Adding again key %d, the %d for partition %d. Assigned to replica %d with PUT %f\n", std::stoi(key), keyInPartition,
                _partitionId, srcReplica,newVersion->UT.toMilliSeconds());
        newVersion->SrcReplica = srcReplica;
#endif

        newVersion->SrcPartition = _partitionId;
        newVersion->Persisted = true;

#ifdef CURE

        newVersion->DV.resize(_numReplicasPerPartition);
        for (int j = 0; j < _numReplicasPerPartition; j++) {
            PhysicalTimeSpec p(1, 0); //With DV fixed to zero, this would be invisible remotely
            newVersion->DV[j] = p;
        }
#endif


        newAnchor->InsertVersion(newVersion);


        // no logging for this operation

        return true;
    }


    bool
    MVKVTXStore::Write(
#if defined(H_CURE) || defined(WREN)
            PhysicalTimeSpec commitTime,
#elif defined(CURE)
            std::vector<PhysicalTimeSpec> commitTime,
#endif
            TxContex &cdata, const std::string &key, const std::string &value,
            std::vector<LocalUpdate *> &updatesToBeReplicatedQueue) {
#ifndef SIXTY_FOUR_BIT_CLOCK
        assert(false);
#endif

        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        ItemAnchor *itemAnchor = NULL;

        /**TODO: return the lock */
        {
            std::lock_guard<std::mutex> lk(*(_indexTableMutexes[ti]));
            try {
                itemAnchor = _indexTables[ti]->at(key);
            } catch (std::out_of_range &e) {
                if (SysConfig::Debugging) {
                    SLOG((boost::format("MVKVTXStore: Set key %s does not exist.") % key).str());
                }
                assert(false);
                return false;
            }
        }

        ItemVersion *newVersion = new ItemVersion(value);

        // deleted at Coordinator::SendUpdate()
        LocalUpdate *update = new LocalUpdate();
        update->UpdatedItemAnchor = itemAnchor;
        update->UpdatedItemVersion = newVersion;

        newVersion->SrcPartition = _partitionId;
        newVersion->SrcReplica = _replicaId;
        newVersion->Persisted = false;
        newVersion->Key = key;

#ifdef H_CURE
        newVersion->RST = cdata.RST;
#elif  defined(WREN)
        newVersion->RST = cdata.GRST;
#elif defined(CURE)
        PhysicalTimeSpec init;
        assert(cdata.DV.size() == _numReplicasPerPartition);
        newVersion->DV.resize(cdata.DV.size(), init);
        for (int i = 0; i < cdata.DV.size(); i++) {
            newVersion->DV[i] = cdata.DV[i];
        }

#endif

        LOCK_VV(_replicaId);

            _localClock += 1;
            _replicaClock += 1;
            _LVV[_replicaId] = _localClock;

            newVersion->LUT = _localClock;
            PhysicalTimeSpec now = Utils::GetCurrentClockTime(), currentTime; //real time, used for stats
            newVersion->RIT = now; //real time
            TO_64(currentTime, now);
#ifdef MEASURE_VISIBILITY_LATENCY
            newVersion->CreationTime = currentTime; //real time in64_bit format
#endif
#if defined(H_CURE) || defined(WREN)
            newVersion->UT = commitTime;
#elif defined(CURE)
            newVersion->UT = commitTime[_replicaId];
#endif

            itemAnchor->InsertVersion(newVersion);

#if defined(H_CURE) || defined(WREN)
            PbLogTccWrenSetRecord record;

            record.set_key(newVersion->Key);
            record.set_value(newVersion->Value);

            record.mutable_ut()->set_seconds(newVersion->UT.Seconds);
            record.mutable_ut()->set_nanoseconds(newVersion->UT.NanoSeconds);

            record.set_srcreplica(newVersion->SrcReplica);
            record.set_lut(newVersion->LUT);

            record.mutable_rst()->set_seconds(newVersion->RST.Seconds);
            record.mutable_rst()->set_nanoseconds(newVersion->RST.NanoSeconds);

#ifdef MEASURE_VISIBILITY_LATENCY
            record.mutable_creationtime()->set_seconds(newVersion->CreationTime.Seconds);
            record.mutable_creationtime()->set_nanoseconds(newVersion->CreationTime.NanoSeconds);
#endif

            update->SerializedRecord = record.SerializeAsString();

#elif defined(CURE)
            PbLogTccCureSetRecord record;

            record.set_key(newVersion->Key);
            record.set_value(newVersion->Value);

            record.mutable_ut()->set_seconds(newVersion->UT.Seconds);
            record.mutable_ut()->set_nanoseconds(newVersion->UT.NanoSeconds);

            record.set_srcreplica(newVersion->SrcReplica);
            record.set_lut(newVersion->LUT);

            for (int i = 0; i < newVersion->DV.size(); i++) {
                PbPhysicalTimeSpec *vv = record.add_dv();
                vv->set_seconds(newVersion->DV[i].Seconds);
                vv->set_nanoseconds(newVersion->DV[i].NanoSeconds);
            }

#ifdef MEASURE_VISIBILITY_LATENCY
             record.mutable_creationtime()->set_seconds(newVersion->CreationTime.Seconds);
            record.mutable_creationtime()->set_nanoseconds(newVersion->CreationTime.NanoSeconds);
#endif

            update->SerializedRecord = record.SerializeAsString();
#endif

            updatesToBeReplicatedQueue.push_back(update);

        UNLOCK_VV(_replicaId);

#ifdef WREN
#ifdef MEASURE_VISIBILITY_LATENCY
        {
            std::lock_guard<std::mutex> lk(_pendingInvisibleVersionsMutex);
            //fprintf(stdout,"Adding from src replica %d\n",newVersion->SrcReplica);
            _pendingInvisibleVersions[_replicaId].push(newVersion);
        }
#endif
#endif
        return true;
    }

#ifdef WREN

    bool MVKVTXStore::LocalTxSliceGet(PhysicalTimeSpec cdataGLST, PhysicalTimeSpec cdataGRST, const std::string &key,
                                      std::string &value) {
        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        ItemAnchor *itemAnchor = NULL;
        {
            std::lock_guard<std::mutex> lk(*(_indexTableMutexes[ti]));
            if (_indexTables[ti]->find(key) == _indexTables[ti]->end()) {
                assert(false);
            } else {
                itemAnchor = _indexTables[ti]->at(key);
            }
        }

        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
        ItemVersion *getVersion = itemAnchor->LatestSnapshotVersion(cdataGLST, cdataGRST);
        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
        double duration = (endTime - startTime).toMilliSeconds();
        SysStats::ChainLatencySum = SysStats::ChainLatencySum + duration;
        SysStats::NumChainLatencyMeasurements++;


        assert(getVersion != NULL);

        value = getVersion->Value;

        return true;
    }


#else

    bool MVKVTXStore::LocalTxSliceGet(TxContex &cdata, const std::string &key, std::string &value) {

        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        ItemAnchor *itemAnchor = NULL;
        {
            std::lock_guard<std::mutex> lk(*(_indexTableMutexes[ti]));
            if (_indexTables[ti]->find(key) == _indexTables[ti]->end()) {
                assert(false);
            } else {
                itemAnchor = _indexTables[ti]->at(key);
            }
        }

        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
#ifdef H_CURE
        ItemVersion *getVersion = itemAnchor->LatestSnapshotVersion(cdata.LDT, cdata.RST);
        value = getVersion->Value;
        assert(getVersion != NULL);

#elif defined(CURE)
        ItemVersion *getVersion = itemAnchor->LatestSnapshotVersion(cdata.DV);
        value = getVersion->Value;
        assert(getVersion != NULL);

#endif

        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
        double duration = (endTime - startTime).toMilliSeconds();
        SysStats::ChainLatencySum = SysStats::ChainLatencySum + duration;
        SysStats::NumChainLatencyMeasurements++;

        return true;
    }

#endif

    bool MVKVTXStore::ShowItem(const std::string &key, std::string &itemVersions) {
        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        ItemAnchor *itemAnchor = NULL;

        /** TODO: return the lock */
        {
            std::lock_guard<std::mutex> lk(*(_indexTableMutexes[ti]));
            if (_indexTables[ti]->find(key) == _indexTables[ti]->end()) {
                return false;
            } else {
                itemAnchor = _indexTables[ti]->at(key);
            }
        }

        itemVersions = itemAnchor->ShowItemVersions();

        return true;
    }

    bool MVKVTXStore::ShowDB(std::string &allItemVersions) {
        for (int i = 0; i < SysConfig::NumItemIndexTables; i++) {
            std::lock_guard<std::mutex> lk(*(_indexTableMutexes[i]));
            for (ItemIndexTable::iterator it = _indexTables[i]->begin();
                 it != _indexTables[i]->end(); it++) {
                std::string itemVersions = it->second->ShowItemVersions();
                allItemVersions += (itemVersions + "\n");
            }
        }
        return true;
    }

    bool MVKVTXStore::ShowStateTx(std::string &stateStr) {

        std::vector<PhysicalTimeSpec> vv; //hvv or pvv depending on the protocol
        std::vector<int64_t> lvv;
        PhysicalTimeSpec gst, lst;

        StalenessStatistics stat;
        VisibilityStatistics vs;

#if defined(H_CURE) || defined(WREN)
        vv = GetVV();
#else
        {
            PVV_ALL_LOCK();
                vv = _PVV;
                lvv = _LVV;
            PVV_ALL_UNLOCK();
        }
#endif

#ifdef MEASURE_STATISTICS
        //calculateStalenessStatistics(stat);
#endif
        // ShowVisibilityLatency(vs);


        stateStr += addToStateStr("replica clock", _replicaClock);
        stateStr += addToStateStr("GST", Utils::physicaltime2str(gst));
        stateStr += addToStateStr("LST", Utils::physicaltime2str(lst));
        stateStr += addToStateStr("VV", Utils::physicaltv2str(vv));
        stateStr += addToStateStr("LVV", Utils::logicaltv2str(lvv));

        stateStr += addToStateStr("NumPublicTxStartRequests", (long) SysStats::NumPublicTxStartRequests);
        stateStr += addToStateStr("NumPublicTxReadRequests", (long) SysStats::NumPublicTxReadRequests);
        stateStr += addToStateStr("NumPublicTxWriteRequests", (long) SysStats::NumPublicTxWriteRequests);
        stateStr += addToStateStr("NumPublicTxCommitRequests", (long) SysStats::NumPublicTxCommitRequests);

        stateStr += addToStateStr("NumInternalTxSliceReadRequests", (long) SysStats::NumInternalTxSliceReadRequests);
        stateStr += addToStateStr("NumInternalCommitRequests", (long) SysStats::NumInternalCommitRequests);
        stateStr += addToStateStr("NumInternalPrepareRequests", (long) SysStats::NumInternalPrepareRequests);

        stateStr += addToStateStr("NumTransactions", (int) SysStats::NumTransactions);
        stateStr += addToStateStr("NumTxReadBlocks", (long) SysStats::NumTxReadBlocks);
        stateStr += addToStateStr("TxReadBlockDuration", (((double) SysStats::TxReadBlockDuration)));
        stateStr += addToStateStr("NumTxReadPhases", (long) SysStats::NumTxReadPhases);
        stateStr += addToStateStr("NumCommitedTxs", (long) SysStats::NumCommitedTxs);

        stateStr += addToStateStr("NumDelayedLocalUpdates", (long) SysStats::NumDelayedLocalUpdates);

        stateStr += addToStateStr("NumPendingPropagatedUpdates", (long) SysStats::NumPendingPropagatedUpdates);
        stateStr += addToStateStr("NumReplicatedBytes", (long) LogManager::Instance()->NumReplicatedBytes);
        stateStr += addToStateStr("NumSentReplicationBytes", (long) SysStats::NumSentReplicationBytes);
        stateStr += addToStateStr("NumRecvReplicationBytes", (long) SysStats::NumRecvReplicationBytes);
        stateStr += addToStateStr("NumRecvUpdateReplicationMsgs", (long) SysStats::NumRecvUpdateReplicationMsgs);
        stateStr += addToStateStr("NumRecvUpdateReplications", (long) SysStats::NumRecvUpdateReplications);
        stateStr += addToStateStr("NumReceivedPropagatedUpdates", (int) SysStats::NumReceivedPropagatedUpdates);
        stateStr += addToStateStr("NumSentUpdates", (int) SysStats::NumSentUpdates);
        stateStr += addToStateStr("NumSentBatches", (int) SysStats::NumSentBatches);
        stateStr += addToStateStr("NumReceivedBatches", (int) SysStats::NumReceivedBatches);
        stateStr += addToStateStr("NumRecvHeartbeats", (int) SysStats::NumRecvHeartbeats);
        stateStr += addToStateStr("NumUpdatesStoredInLocalUpdateQueue",
                                  (int) SysStats::NumUpdatesStoredInLocalUpdateQueue);
        stateStr += addToStateStr("NumUpdatesStoredInToPropagateLocalUpdateQueue",
                                  (int) SysStats::NumUpdatesStoredInToPropagateLocalUpdateQueue);

        stateStr += addToStateStr("NumSentRSTs", (long) SysStats::NumSentRSTs);
        stateStr += addToStateStr("NumRecvRSTs", (long) SysStats::NumRecvRSTs);
        stateStr += addToStateStr("NumSentRSTBytes", (long) SysStats::NumSentRSTBytes);
        stateStr += addToStateStr("NumRecvRSTBytes", (long) SysStats::NumRecvRSTBytes);

        stateStr += addToStateStr("NumGSTRounds", (long) SysStats::NumGSTRounds);
        stateStr += addToStateStr("NumRecvGSTs", (long) SysStats::NumRecvGSTs);
        stateStr += addToStateStr("NumRecvGSTBytes", (long) SysStats::NumRecvGSTBytes);
        stateStr += addToStateStr("NumSentGSTBytes", (long) SysStats::NumSentGSTBytes);
        stateStr += addToStateStr("NumSendInternalLSTBytes", (long) SysStats::NumSendInternalLSTBytes);
        stateStr += addToStateStr("NumRecvInternalLSTBytes", (long) SysStats::NumRecvInternalLSTBytes);

        stateStr += addToStateStr("NumLatencyCheckedVersions", (long) _numCheckedVersions);

        stateStr += addToStateStr("NumReturnedItemVersions", (long) SysStats::NumReturnedItemVersions);
        stateStr += addToStateStr("NumReturnedStaleItemVersions", (long) SysStats::NumReturnedStaleItemVersions);
        stateStr += addToStateStr("AvgNumFresherVersionsInItemChain", stat.averageNumFresherVersionsInItemChain);
        stateStr += addToStateStr("AverageUserPerceivedStalenessTime", stat.averageUserPerceivedStalenessTime);
        stateStr += addToStateStr("MinUserPerceivedStalenessTime", stat.minStalenessTime);
        stateStr += addToStateStr("MaxUserPerceivedStalenessTime", stat.maxStalenessTime);
        stateStr += addToStateStr("MedianUserPerceivedStalenessTime", stat.medianStalenessTime);
        stateStr += addToStateStr("90PercentileStalenessTime", stat._90PercentileStalenessTime);
        stateStr += addToStateStr("95PercentileStalenessTime", stat._95PercentileStalenessTime);
        stateStr += addToStateStr("99PercentileStalenessTime", stat._99PercentileStalenessTime);


        stateStr += addToStateStr("sum_fresher_versions",
                                  (int) std::accumulate(SysStats::NumFresherVersionsInItemChain.begin(),
                                                        SysStats::NumFresherVersionsInItemChain.end(), 0));
        stateStr += addToStateStr("NumSentGSTBytes", (int) SysStats::NumSentGSTBytes);
        stateStr += addToStateStr("NumRecvInternalLSTBytes", (int) SysStats::NumRecvInternalLSTBytes);
        stateStr += addToStateStr("NumSentReplicationBytes", (int) SysStats::NumSentReplicationBytes);
        stateStr += addToStateStr("NumRecvReplicationBytes", (int) SysStats::NumRecvReplicationBytes);
        stateStr += addToStateStr("NumSentBatches", (int) SysStats::NumSentBatches);
        stateStr += addToStateStr("NumReceivedBatches", (int) SysStats::NumReceivedBatches);
        stateStr += addToStateStr("SumDelayedLocalUpdates", (int) SysStats::SumDelayedLocalUpdates);

        stateStr += addToStateStr("SumDelayedLocalUpdates", (int) SysStats::SumDelayedLocalUpdates);

        return true;

    }

    bool MVKVTXStore::ShowStateTxCSV(std::string &stateStr) {

        char separator = ';';

        std::vector<PhysicalTimeSpec> vv; //hvv or pvv depending on the protocol
        std::vector<int64_t> lvv;
        PhysicalTimeSpec gst, lst;
        PhysicalTimeVector gsv, rst;

        StalenessStatistics stat;
        VisibilityStatistics vs, vsLocal;
        std::string protocol = "";

#ifdef H_CURE
        protocol = "H_CURE";
#elif defined(WREN)
        protocol = "WREN_";
#elif defined(CURE)
        protocol = "CURE";
#endif

        vv = GetVV();
        lvv = GetLVV();

#ifdef PROFILE_STATS
        SysStats::timeInPreparedSet.calculate();
        SysStats::timeInCommitedSet.calculate();
#endif

#ifdef MEASURE_STATISTICS
        calculateStalenessStatistics(stat);
#endif
        ShowVisibilityLatency(vs, true); //remote
#ifdef WREN
        ShowVisibilityLatency(vsLocal, false); //local
#endif

        std::string serverName = stateStr + std::to_string(_partitionId) + "_" + std::to_string(_replicaId);
        stateStr = "";
        /* First add server statistics header */
        std::vector<std::string> header{"Protocol", "FreshnessParameter", "LocalClock", "ReplicaClock", "VV", "LVV",
                                        "NumTransactions",
                                        "NumPublicTxStartRequests",
                                        "NumPublicTxReadRequests",
                                        "NumPublicTxCommitRequests",
                                        "NumInternalTxSliceReadRequests",
                                        "NumInternalCommitRequests",
                                        "NumInternalPrepareRequests",
                                        "NumTxReadBlocks",
                                        "TxReadBlockDuration",
                                        "NumTxReadBlocksAtCoordinator",
                                        "TxReadBlockDurationAtCoordinator",
                                        "NumTxReadPhases",
                                        "NumTxPreparePhaseBlocks",
                                        "TxPreparePhaseBlockDuration",
                                        "NumTxPreparePhaseBlocksAtCoordinator",
                                        "TxPreparePhaseBlockDurationAtCoordinator",
                                        "NumSentReplicationBytes", "NumRecvReplicationBytes",
                                        "NumRecvUpdateReplicationMsgs", "NumRecvUpdateReplications",
                                        "NumReceivedPropagatedUpdates", "NumSentUpdates", "NumSentBatches",
                                        "NumReceivedBatches", "NumRecvHeartbeats", "#vals_in_timeInPreparedSet",
                                        "timeInPreparedSet_AVG",
                                        "timeInPreparedSet_MIN", "timeInPreparedSet_MAX",
                                        "#vals_in_timeInCommitedSet", "timeInCommitedSet_AVG",
                                        "timeInCommitedSet_MIN", "timeInCommitedSet_MAX", "NUM_BLOCKS_ON_PREPARE",
                                        "BLOCKS_DIFF", "NUM_NO_BLOCKS_ON_PREPARE", "NO_BLOCKS_DIFF",
                                        "NUM_TIMES_WAIT_ON_PREPARE_IS_ENTERED", "NUM_TIMES_GSS_IS_ZERO",
                                        "NUM_TIMES_DEP_TIME_IS_ZERO", "PREPARE_ACTION_DURATION",
                                        "NUM_TIMES_PREPARE_EXECUTED", "CALC_UBT_ACTION_DURATION",
                                        "NUM_TIMES_CALC_UBT_EXECUTED",
                                        "PosDiff_HybToPhys_TxStart",
                                        "CountPosDiff_HybToPhys_TxStart",
                                        "NegDiff_HybToPhys_TxStart",
                                        "CountNegDiff_HybToPhys_TxStart",
                                        "PosDiff_HybToPhys_TxRead",
                                        "CountPosDiff_HybToPhys_TxRead",
                                        "NegDiff_HybToPhys_TxRead",
                                        "CountNegDiff_HybToPhys_TxRead",
                                        "PosDiff_HybToVV_TxRead",
                                        "CountPosDiff_HybToVV_TxRead",
                                        "NegDiff_HybToVV_TxRead",
                                        "CountNegDiff_HybToVV_TxRead",
                                        "PosDiff_HybToVV_TxRead_Block",
                                        "CountPosDiff_HybToVV_TxRead_Block",
                                        "NegDiff_HybToVV_TxRead_Block",
                                        "CountNegDiff_HybToVV_TxRead_Block",
                                        "NumReturnedItemVersions",
                                        "NumReturnedStaleItemVersions",
                                        "AvgNumFresherVersionsInItemChain",
                                        "AverageUserPerceivedStalenessTime",
                                        "MinUserPerceivedStalenessTime",
                                        "MaxUserPerceivedStalenessTime",
                                        "MedianUserPerceivedStalenessTime",
                                        "90PercentileStalenessTime",
                                        "95PercentileStalenessTime",
                                        "99PercentileStalenessTime",
                                        "sum_fresher_versions",
                                        "NumReadLocalItems",
                                        "NumReadLocalStaleItems",
                                        "NumReadRemoteItems",
                                        "NumReadRemoteStaleItems",
                                        "NumReadItemsFromKVStore",
                                        "ServerReadLatencySum",
                                        "NumServerReadLatencyMeasurements",
                                        "ServerReadLatencyAvg",
                                        "ChainLatencySum",
                                        "NumChainLatencyMeasurements",
                                        "ChainLatencyAvg",
                                        "CohordHandleInternalSliceReadLatencySum",
                                        "NumCohordHandleInternalSliceReadLatencyMeasurements",
                                        "CohordHandleInternalSliceReadLatencyAvg",
                                        "CohordSendingReadResultsLatencySum",
                                        "NumCohordSendingReadResultsLatencyMeasurements",
                                        "CohordSendingReadResultsLatencyAvg",
                                        "CoordinatorMappingKeysToShardsLatencySum",
                                        "NumCoordinatorMappingKeysToShardsLatencyMeasurements",
                                        "CoordinatorMappingKeysToShardsLatencyAvg",
                                        "CoordinatorSendingReadReqToShardsLatencySum",
                                        "NumCoordinatorSendingReadReqToShardsLatencyMeasurements",
                                        "CoordinatorSendingReadReqToShardsLatencyAvg",
                                        "CoordinatorReadingLocalKeysLatencySum",
                                        "NumCoordinatorReadingLocalKeysLatencyMeasurements",
                                        "CoordinatorReadingLocalKeysLatencyAvg",
                                        "CoordinatorProcessingOtherShardsReadRepliesLatencySum",
                                        "NumCoordinatorProcessingOtherShardsReadRepliesLatencyMeasurements",
                                        "CoordinatorProcessingOtherShardsReadRepliesLatencyAvg",
                                        "CoordinatorLocalKeyReadsLatencySum",
                                        "NumCoordinatorLocalKeyReadsLatencyMeasurements",
                                        "CoordinatorLocalKeyReadsLatencyAvg",
                                        "CoordinatorWaitingForShardsRepliesLatencySum",
                                        "NumCoordinatorWaitingForShardsRepliesMeasurements",
                                        "CoordinatorWaitingForShardsRepliesLatencyAvg",
                                        "CoordinatorReadReplyHandlingLatencySum",
                                        "NumCoordinatorReadReplyHandlingMeasurements",
                                        "CoordinatorReadReplyHandlingLatencyAvg",
                                        "CoordinatorCommitLatencySum",
                                        "NumCoordinatorCommitMeasurements",
                                        "CoordinatorCommitLatencyAvg",
                                        "CoordinatorCommitMappingKeysToShardsLatencySum",
                                        "NumCoordinatorCommitMappingKeysToShardsLatencyMeasurements",
                                        "CoordinatorCommitMappingKeysToShardsLatencyAvg",
                                        "CoordinatorCommitSendingPrepareReqToShardsLatencySum",
                                        "NumCoordinatorCommitSendingPrepareReqToShardsLatencyMeasurements",
                                        "CoordinatorCommitSendingPrepareReqToShardsLatencyAvg",
                                        "CoordinatorCommitLocalPrepareLatencySum",
                                        "NumCoordinatorCommitLocalPrepareLatencyMeasurements",
                                        "CoordinatorCommitLocalPrepareLatencyAvg",
                                        "CoordinatorCommitWaitingOtherShardsPrepareRepliesLatencySum",
                                        "NumCoordinatorCommitWaitingOtherShardsPrepareRepliesLatencyMeasurements",
                                        "CoordinatorCommitWaitingOtherShardsPrepareRepliesLatencyAvg",
                                        "CoordinatorCommitProcessingPrepareRepliesLatencySum",
                                        "NumCoordinatorCommitProcessingPrepareRepliesLatencyMeasurements",
                                        "CoordinatorCommitProcessingPrepareRepliesLatencyAvg",
                                        "CoordinatorCommitSendingCommitReqLatencySum",
                                        "NumCoordinatorCommitSendingCommitReqLatencyMeasurements",
                                        "CoordinatorCommitSendingCommitReqLatencySum",
                                        "NumSentGSTs",
                                        "NumRecvGSTs",
                                        "NumSentGSTBytes",
                                        "NumRecvGSTBytes",
                                        "NumChannels"
        };

        std::string sname = ">>>;ServerName";
        Utils::appendToCSVString(stateStr, sname);
        for (int i = 0; i < header.size(); i++) {
            Utils::appendToCSVString(stateStr, header[i]);
        }
        std::string strToken;
        for (int i = 0; i < 9; i++) {
            strToken = "Remote_Overall_" + std::to_string(((i + 1) * 10));
            Utils::appendToCSVString(stateStr, strToken);
        }
        strToken = "Remote_Overall_95";
        Utils::appendToCSVString(stateStr, strToken);
        strToken = "Remote_Overall_99";
        Utils::appendToCSVString(stateStr, strToken);

        for (int i = 0; i < 9; i++) {
            strToken = "Remote_Propagation_" + std::to_string(((i + 1) * 10));
            Utils::appendToCSVString(stateStr, strToken);
        }
        strToken = "Remote_Propagation_95";
        Utils::appendToCSVString(stateStr, strToken);
        strToken = "Remote_Propagation_99";
        Utils::appendToCSVString(stateStr, strToken);

        for (int i = 0; i < 9; i++) {
            strToken = "Remote_Visibility_" + std::to_string(((i + 1) * 10));
            Utils::appendToCSVString(stateStr, strToken);
        }
        strToken = "Remote_Visibility_95";
        Utils::appendToCSVString(stateStr, strToken);
        strToken = "Remote_Visibility_99";
        Utils::appendToCSVString(stateStr, strToken);

        for (int i = 0; i < 9; i++) {
            strToken = "Local_Overall_" + std::to_string(((i + 1) * 10));
            Utils::appendToCSVString(stateStr, strToken);
        }
        strToken = "Local_Overall_95";
        Utils::appendToCSVString(stateStr, strToken);
        strToken = "Local_Overall_99";
        Utils::appendToCSVString(stateStr, strToken);

        for (int i = 0; i < 9; i++) {
            strToken = "Local_Propagation_" + std::to_string(((i + 1) * 10));
            Utils::appendToCSVString(stateStr, strToken);
        }
        strToken = "Local_Propagation_95";
        Utils::appendToCSVString(stateStr, strToken);
        strToken = "Local_Propagation_99";
        Utils::appendToCSVString(stateStr, strToken);

        for (int i = 0; i < 9; i++) {
            strToken = "Local_Visibility_" + std::to_string(((i + 1) * 10));
            Utils::appendToCSVString(stateStr, strToken);
        }
        strToken = "Local_Visibility_95";
        Utils::appendToCSVString(stateStr, strToken);
        strToken = "Local_Visibility_99";
        Utils::appendToCSVString(stateStr, strToken);


        stateStr += "\n";
        std::string dummy = ">>>";
        /* Then add server statistics  */
        Utils::appendToCSVString(stateStr, dummy);
        Utils::appendToCSVString(stateStr, serverName);
        Utils::appendToCSVString(stateStr, protocol);
        Utils::appendToCSVString(stateStr, (long) SysConfig::FreshnessParameter);
        Utils::appendToCSVString(stateStr, _localClock);
        Utils::appendToCSVString(stateStr, _replicaClock);
        Utils::appendToCSVString(stateStr, Utils::physicaltv2str(vv));
        Utils::appendToCSVString(stateStr, Utils::logicaltv2str(lvv));
        Utils::appendToCSVString(stateStr, (long) SysStats::NumCommitedTxs);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumPublicTxStartRequests);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumPublicTxReadRequests);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumPublicTxCommitRequests);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumInternalTxSliceReadRequests);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumInternalCommitRequests);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumInternalPrepareRequests);

        Utils::appendToCSVString(stateStr, (long) SysStats::NumTxReadBlocks);
        Utils::appendToCSVString(stateStr, (double) SysStats::TxReadBlockDuration);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumTxReadBlocksAtCoordinator);
        Utils::appendToCSVString(stateStr, (double) SysStats::TxReadBlockDurationAtCoordinator);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumTxReadPhases);

        Utils::appendToCSVString(stateStr, (long) SysStats::NumTxPreparePhaseBlocks);
        Utils::appendToCSVString(stateStr, (double) SysStats::TxPreparePhaseBlockDuration);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumTxPreparePhaseBlocksAtCoordinator);
        Utils::appendToCSVString(stateStr, (double) SysStats::TxPreparePhaseBlockDurationAtCoordinator);

        Utils::appendToCSVString(stateStr, (long) SysStats::NumSentReplicationBytes);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumRecvReplicationBytes);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumRecvUpdateReplicationMsgs);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumRecvUpdateReplications);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumReceivedPropagatedUpdates);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumSentUpdates);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumSentBatches);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumReceivedBatches);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumRecvHeartbeats);
        Utils::appendToCSVString(stateStr, (long) SysStats::timeInPreparedSet.values.size());
        Utils::appendToCSVString(stateStr, (double) SysStats::timeInPreparedSet.average);
        Utils::appendToCSVString(stateStr, (double) SysStats::timeInPreparedSet.min);
        Utils::appendToCSVString(stateStr, (double) SysStats::timeInPreparedSet.max);
        Utils::appendToCSVString(stateStr, (long) SysStats::timeInCommitedSet.values.size());
        Utils::appendToCSVString(stateStr, (double) SysStats::timeInCommitedSet.average);
        Utils::appendToCSVString(stateStr, (double) SysStats::timeInCommitedSet.min);
        Utils::appendToCSVString(stateStr, (double) SysStats::timeInCommitedSet.max);
        Utils::appendToCSVString(stateStr, (double) SysStats::PrepareCondBlocks);
        Utils::appendToCSVString(stateStr, (double) SysStats::PrepBlockDifference);
        Utils::appendToCSVString(stateStr, (double) SysStats::PrepareCondNoBlocks);
        Utils::appendToCSVString(stateStr, (double) SysStats::PrepNoBlockDifference);
        Utils::appendToCSVString(stateStr, (double) SysStats::NumTimesWaitOnPrepareIsEntered);
        Utils::appendToCSVString(stateStr, (double) SysStats::NumGSSisZero);
        Utils::appendToCSVString(stateStr, (double) SysStats::NumDepTimeisZero);
        Utils::appendToCSVString(stateStr, (double) SysStats::PrepareActionDuration);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumTimesPrepareActionIsExecuted);

        Utils::appendToCSVString(stateStr, (double) SysStats::CalcUBTActionDuration);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumTimesCalcUBTIsExecuted);

        Utils::appendToCSVString(stateStr, (double) SysStats::PosDiff_HybToPhys_TxStart);
        Utils::appendToCSVString(stateStr, (int) SysStats::CountPosDiff_HybToPhys_TxStart);
        Utils::appendToCSVString(stateStr, (double) SysStats::NegDiff_HybToPhys_TxStart);
        Utils::appendToCSVString(stateStr, (int) SysStats::CountNegDiff_HybToPhys_TxStart);

        Utils::appendToCSVString(stateStr, (double) SysStats::PosDiff_HybToPhys_TxRead);
        Utils::appendToCSVString(stateStr, (int) SysStats::CountPosDiff_HybToPhys_TxRead);
        Utils::appendToCSVString(stateStr, (double) SysStats::NegDiff_HybToPhys_TxRead);
        Utils::appendToCSVString(stateStr, (int) SysStats::CountNegDiff_HybToPhys_TxRead);

        Utils::appendToCSVString(stateStr, (double) SysStats::PosDiff_HybToVV_TxRead);
        Utils::appendToCSVString(stateStr, (int) SysStats::CountPosDiff_HybToVV_TxRead);
        Utils::appendToCSVString(stateStr, (double) SysStats::NegDiff_HybToVV_TxRead);
        Utils::appendToCSVString(stateStr, (int) SysStats::CountNegDiff_HybToVV_TxRead);

        Utils::appendToCSVString(stateStr, (double) SysStats::PosDiff_HybToVV_TxRead_Block);
        Utils::appendToCSVString(stateStr, (int) SysStats::CountPosDiff_HybToVV_TxRead_Block);
        Utils::appendToCSVString(stateStr, (double) SysStats::NegDiff_HybToVV_TxRead_Block);
        Utils::appendToCSVString(stateStr, (int) SysStats::CountNegDiff_HybToVV_TxRead_Block);

        /* staleness stats */
        Utils::appendToCSVString(stateStr, (long) SysStats::NumReturnedItemVersions);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumReturnedStaleItemVersions);
        Utils::appendToCSVString(stateStr, (double) stat.averageNumFresherVersionsInItemChain);
        Utils::appendToCSVString(stateStr, (double) stat.averageUserPerceivedStalenessTime);
        Utils::appendToCSVString(stateStr, (double) stat.minStalenessTime);
        Utils::appendToCSVString(stateStr, (double) stat.maxStalenessTime);
        Utils::appendToCSVString(stateStr, (double) stat.medianStalenessTime);
        Utils::appendToCSVString(stateStr, (double) stat._90PercentileStalenessTime);
        Utils::appendToCSVString(stateStr, (double) stat._95PercentileStalenessTime);
        Utils::appendToCSVString(stateStr, (double) stat._99PercentileStalenessTime);
        Utils::appendToCSVString(stateStr, (long) std::accumulate(SysStats::NumFresherVersionsInItemChain.begin(),
                                                                  SysStats::NumFresherVersionsInItemChain.end(), 0));

        Utils::appendToCSVString(stateStr, (int) SysStats::NumReadLocalItems);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumReadLocalStaleItems);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumReadRemoteItems);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumReadRemoteStaleItems);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumReadItemsFromKVStore);

        Utils::appendToCSVString(stateStr, (double) SysStats::ServerReadLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumServerReadLatencyMeasurements);

        double serverReadLatAvg = -1;
        if (SysStats::NumServerReadLatencyMeasurements != 0) {
            serverReadLatAvg = SysStats::ServerReadLatencySum / SysStats::NumServerReadLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) serverReadLatAvg);

        Utils::appendToCSVString(stateStr, (double) SysStats::ChainLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumChainLatencyMeasurements);

        double chainLatAvg = -1;
        if (SysStats::NumChainLatencyMeasurements != 0) {
            chainLatAvg = SysStats::ChainLatencySum / SysStats::NumChainLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) chainLatAvg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CohordHandleInternalSliceReadLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCohordHandleInternalSliceReadLatencyMeasurements);
        double avg = -1;
        if (SysStats::NumCohordHandleInternalSliceReadLatencyMeasurements != 0) {
            avg = SysStats::CohordHandleInternalSliceReadLatencySum /
                  SysStats::NumCohordHandleInternalSliceReadLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CohordSendingReadResultsLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCohordSendingReadResultsLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCohordSendingReadResultsLatencyMeasurements != 0) {
            avg = SysStats::CohordSendingReadResultsLatencySum /
                  SysStats::NumCohordSendingReadResultsLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorMapingKeysToShardsLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorMapingKeysToShardsLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorMapingKeysToShardsLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorMapingKeysToShardsLatencySum /
                  SysStats::NumCoordinatorMapingKeysToShardsLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorSendingReadReqToShardsLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorSendingReadReqToShardsLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorSendingReadReqToShardsLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorSendingReadReqToShardsLatencySum /
                  SysStats::NumCoordinatorSendingReadReqToShardsLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorReadingLocalKeysLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorReadingLocalKeysLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorReadingLocalKeysLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorReadingLocalKeysLatencySum /
                  SysStats::NumCoordinatorReadingLocalKeysLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorProcessingOtherShardsReadRepliesLatencySum);
        Utils::appendToCSVString(stateStr,
                                 (int) SysStats::NumCoordinatorProcessingOtherShardsReadRepliesLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorProcessingOtherShardsReadRepliesLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorProcessingOtherShardsReadRepliesLatencySum /
                  SysStats::NumCoordinatorProcessingOtherShardsReadRepliesLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorLocalKeyReadsLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorLocalKeyReadsLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorLocalKeyReadsLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorLocalKeyReadsLatencySum /
                  SysStats::NumCoordinatorLocalKeyReadsLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorWaitingForShardsRepliesLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorWaitingForShardsRepliesMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorWaitingForShardsRepliesMeasurements != 0) {
            avg = SysStats::CoordinatorWaitingForShardsRepliesLatencySum /
                  SysStats::NumCoordinatorWaitingForShardsRepliesMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorReadReplyHandlingLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorReadReplyHandlingMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorReadReplyHandlingMeasurements != 0) {
            avg = SysStats::CoordinatorReadReplyHandlingLatencySum /
                  SysStats::NumCoordinatorReadReplyHandlingMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorCommitLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorCommitMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorCommitMeasurements != 0) {
            avg = SysStats::CoordinatorCommitLatencySum /
                  SysStats::NumCoordinatorCommitMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorCommitMappingKeysToShardsLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorCommitMappingKeysToShardsLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorCommitMappingKeysToShardsLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorCommitMappingKeysToShardsLatencySum /
                  SysStats::NumCoordinatorCommitMappingKeysToShardsLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorCommitSendingPrepareReqToShardsLatencySum);
        Utils::appendToCSVString(stateStr,
                                 (int) SysStats::NumCoordinatorCommitSendingPrepareReqToShardsLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorCommitSendingPrepareReqToShardsLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorCommitSendingPrepareReqToShardsLatencySum /
                  SysStats::NumCoordinatorCommitSendingPrepareReqToShardsLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);


        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorCommitLocalPrepareLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorCommitLocalPrepareLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorCommitLocalPrepareLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorCommitLocalPrepareLatencySum /
                  SysStats::NumCoordinatorCommitLocalPrepareLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr,
                                 (double) SysStats::CoordinatorCommitWaitingOtherShardsPrepareRepliesLatencySum);
        Utils::appendToCSVString(stateStr,
                                 (int) SysStats::NumCoordinatorCommitWaitingOtherShardsPrepareRepliesLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorCommitWaitingOtherShardsPrepareRepliesLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorCommitWaitingOtherShardsPrepareRepliesLatencySum /
                  SysStats::NumCoordinatorCommitWaitingOtherShardsPrepareRepliesLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);


        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorCommitProcessingPrepareRepliesLatencySum);
        Utils::appendToCSVString(stateStr,
                                 (int) SysStats::NumCoordinatorCommitProcessingPrepareRepliesLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorCommitProcessingPrepareRepliesLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorCommitProcessingPrepareRepliesLatencySum /
                  SysStats::NumCoordinatorCommitProcessingPrepareRepliesLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorCommitSendingCommitReqLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorCommitSendingCommitReqLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorCommitSendingCommitReqLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorCommitSendingCommitReqLatencySum /
                  SysStats::NumCoordinatorCommitSendingCommitReqLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::NumSentGSTs);
        Utils::appendToCSVString(stateStr, (double) SysStats::NumRecvGSTs);
        Utils::appendToCSVString(stateStr, (double) SysStats::NumSentGSTBytes);
        Utils::appendToCSVString(stateStr, (double) SysStats::NumRecvGSTBytes);
        Utils::appendToCSVString(stateStr, (double) SysConfig::NumChannelsPartition);

        /* visibility stats */
        for (int i = 0; i < 11; i++) {
            Utils::appendToCSVString(stateStr, (double) vs.overall[i]);
        }

        for (int i = 0; i < 11; i++) {
            Utils::appendToCSVString(stateStr, (double) vs.propagation[i]);
        }

        for (int i = 0; i < 11; i++) {
            Utils::appendToCSVString(stateStr, (double) vs.visibility[i]);
        }

        for (int i = 0; i < 11; i++) {
            Utils::appendToCSVString(stateStr, (double) vsLocal.overall[i]);
        }

        for (int i = 0; i < 11; i++) {
            Utils::appendToCSVString(stateStr, (double) vsLocal.propagation[i]);
        }

        for (int i = 0; i < 11; i++) {
            Utils::appendToCSVString(stateStr, (double) vsLocal.visibility[i]);
        }

        replace(stateStr.begin(), stateStr.end(), ',', separator);

        return true;

    }

    bool MVKVTXStore::DumpLatencyMeasurement(std::string &resultStr) {
        std::lock_guard<std::mutex> lk(_pendingInvisibleVersionsMutex);

        for (unsigned int i = 0; i < _recordedLatencies.size(); ++i) {

            ReplicationLatencyResult &r = _recordedLatencies[i];
            resultStr +=
                    std::to_string(r.OverallLatency.Seconds * 1000 +
                                   r.OverallLatency.NanoSeconds / 1000000.0) + " " +
                    std::to_string(r.PropagationLatency.Seconds * 1000 +
                                   r.PropagationLatency.NanoSeconds / 1000000.0) + " " +
                    std::to_string(r.VisibilityLatency.Seconds * 1000 +
                                   r.VisibilityLatency.NanoSeconds / 1000000.0) + " [ms] " +
                    std::to_string(r.SrcReplica) + "\n";
        }

        return true;
    }


    void MVKVTXStore::ShowVisibilityLatency(VisibilityStatistics &v, bool remoteMode) {
#ifdef MEASURE_VISIBILITY_LATENCY
        std::lock_guard<std::mutex> lk(_pendingInvisibleVersionsMutex);
        std::vector<double> overallLatency;
        std::vector<double> propagationLatency;
        std::vector<double> visibilityLatency;

        std::vector<ReplicationLatencyResult> _latencies;

        if (remoteMode) {
            _latencies = _recordedLatencies;
        } else {
            _latencies = _recordedLocalLatencies;
        }

        SLOG((boost::format("_latencies.size() = %d .") % _latencies.size()).str());
        for (unsigned int i = 0; i < _latencies.size(); ++i) {

            ReplicationLatencyResult &r = _latencies[i];
            overallLatency.push_back(r.OverallLatency.Seconds * 1000 +
                                     r.OverallLatency.NanoSeconds / 1000000.0);
            propagationLatency.push_back(r.PropagationLatency.Seconds * 1000 +
                                         r.PropagationLatency.NanoSeconds / 1000000.0);
            visibilityLatency.push_back(r.VisibilityLatency.Seconds * 1000 +
                                        r.VisibilityLatency.NanoSeconds / 1000000.0);
        }
        std::sort(overallLatency.begin(), overallLatency.end());
        std::sort(propagationLatency.begin(), propagationLatency.end());
        std::sort(visibilityLatency.begin(), visibilityLatency.end());

        //We take 10-20-30-40-50-60-70-80-90-95-99-th percentile
        double size = overallLatency.size() / 100.0;
        int i;


        for (i = 1; i <= 9; i++) {
            v.overall[i - 1] = overallLatency[(int) (i * 10.0 * size)];
        }
        v.overall[9] = overallLatency[(int) (95.0 * size)];
        v.overall[10] = overallLatency[(int) (99.0 * size)];

        for (i = 1; i <= 9; i++) {
            v.propagation[i - 1] = propagationLatency[(int) (i * 10.0 * size)];
        }
        v.propagation[9] = propagationLatency[(int) (95.0 * size)];
        v.propagation[10] = propagationLatency[(int) (99.0 * size)];

        for (i = 1; i <= 9; i++) {
            v.visibility[i - 1] = visibilityLatency[(int) (i * 10.0 * size)];
        }
        v.visibility[9] = visibilityLatency[(int) (95.0 * size)];
        v.visibility[10] = visibilityLatency[(int) (99.0 * size)];
#endif


    }

    int MVKVTXStore::Size() {
        int size = 0;
        for (int i = 0; i < SysConfig::NumItemIndexTables; i++) {
            std::lock_guard<std::mutex> lk(*(_indexTableMutexes[i]));
            size += _indexTables[i]->size();
        }

        return size;
    }

    void MVKVTXStore::Reserve(int numItems) {
        for (int i = 0; i < SysConfig::NumItemIndexTables; i++) {
            std::lock_guard<std::mutex> lk(*(_indexTableMutexes[i]));
            _indexTables[i]->reserve(numItems / SysConfig::NumItemIndexTables + 1);
        }
    }

//////////////////////////////////////////////////////

    bool MVKVTXStore::HandlePropagatedUpdate(std::vector<PropagatedUpdate *> &updates) {
        for (unsigned int i = 0; i < updates.size(); i++) {
            InstallPropagatedUpdate(updates[i]);
        }

#ifdef MEASURE_STATISTICS
        SysStats::NumReceivedPropagatedUpdates += updates.size();
        SysStats::NumReceivedBatches += 1;
#endif

        return true;
    }


    bool MVKVTXStore::HandleHeartbeat(Heartbeat &hb, int srcReplica) {
#if defined(H_CURE) || defined(WREN)
        LOCK_VV(srcReplica);

            assert(hb.PhysicalTime >= _HVV[srcReplica]); //_PVV does not play role in the hybrid clock version
            assert(hb.LogicalTime >= _LVV[srcReplica]);
            _HVV[srcReplica] = hb.PhysicalTime;
            assert(_HVV_latest[srcReplica] <= _HVV[srcReplica]);
            _HVV_latest[srcReplica] = _HVV[srcReplica];
            _LVV[srcReplica] = hb.LogicalTime;
        UNLOCK_VV(srcReplica);
        return true;
#else
        LOCK_VV(srcReplica);
            if (hb.PhysicalTime < _PVV[srcReplica]) {
            }

            assert(hb.PhysicalTime >= _PVV[srcReplica]);
            assert(hb.LogicalTime >= _LVV[srcReplica]);
            _PVV[srcReplica] = hb.PhysicalTime;
            assert(_PVV_latest[srcReplica] <= _PVV[srcReplica]);
            _PVV_latest[srcReplica] = _PVV[srcReplica];
            _LVV[srcReplica] = hb.LogicalTime;
        UNLOCK_VV(srcReplica);
        return true;
#endif
    }

    void MVKVTXStore::InstallPropagatedUpdate(PropagatedUpdate *update) {
        ItemAnchor *itemAnchor = NULL;
        PhysicalTimeSpec depTime;

        int ti = Utils::strhash(update->Key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        {
            std::lock_guard<std::mutex> lk(*(_indexTableMutexes[ti]));
            try {
                itemAnchor = _indexTables[ti]->at(update->Key);
            } catch (std::out_of_range &e) {
                if (SysConfig::Debugging) {
                    SLOG((boost::format("MVKVTXStore: ApplyPropagatedUpdate key %s does not exist.")
                          % update->Key).str());
                    assert(false);
                }
            }
        }

        // create and insert propagated version
        // no need to delete

        ItemVersion *newVersion = new ItemVersion(update->Value);
        newVersion->Key = update->Key;
        newVersion->UT = update->UT;
        newVersion->SrcReplica = update->SrcReplica;
        newVersion->Persisted = false;
        newVersion->LUT = update->LUT;
#ifdef MEASURE_VISIBILITY_LATENCY
        newVersion->CreationTime = update->CreationTime;
#endif
#if defined(H_CURE) || defined(WREN)
        newVersion->RST = update->RST;
#elif defined(CURE)
        newVersion->DV.resize(update->DV.size());
        for (int i = 0; i < update->DV.size(); i++) {
            newVersion->DV[i].Seconds = update->DV[i].Seconds;
            newVersion->DV[i].NanoSeconds = update->DV[i].NanoSeconds;
        }
#endif //CURE

        update->UpdatedItemAnchor = itemAnchor;
        update->UpdatedItemVersion = newVersion;

        newVersion->RIT = Utils::GetCurrentClockTime();// RIT is always physical time

        itemAnchor->InsertVersion(newVersion);

#ifdef MEASURE_STATISTICS
        SysStats::NumPendingPropagatedUpdates += 1;
#endif

        newVersion->Persisted = true;

#if defined(H_CURE) || defined(WREN)
        LOCK_VV(update->SrcReplica);
            _replicaClock += 1;
            _LVV[update->SrcReplica] = update->LUT;
            assert(_HVV[update->SrcReplica] <= update->UT);

            if (_HVV[update->SrcReplica] > update->UT) {
                //inserting not in FIFO //the updates could be from the same transaction with the same commit time
                int64_t u_l, u_p, c_l, c_p;
                HYB_TO_PHYS(u_p, update->UT.Seconds);
                HYB_TO_PHYS(c_p, _HVV[update->SrcReplica].Seconds);
                HYB_TO_LOG(u_l, update->UT.Seconds);
                HYB_TO_LOG(c_l, _HVV[update->SrcReplica].Seconds);
                fprintf(stdout, "[INFO]:Local: %d.%d vs update %d.%d\n", c_p, c_l, u_p, u_l);
                fflush(stdout);
                assert(false);
            }

            _HVV[update->SrcReplica] = update->UT;
            assert(_HVV_latest[update->SrcReplica] <= _HVV[update->SrcReplica]);
            _HVV_latest[update->SrcReplica] = _HVV[update->SrcReplica];
        UNLOCK_VV(update->SrcReplica);

#else //CURE

        LOCK_VV(update->SrcReplica);
            _replicaClock += 1;
            _LVV[update->SrcReplica] = update->LUT;
            if (_PVV[update->SrcReplica] > update->UT) {
                SLOG((boost::format("_PVV[%d] = %s update->UT = %s .\n")
                      % update->SrcReplica % Utils::physicaltime2str(_PVV[update->SrcReplica])
                      % Utils::physicaltime2str(update->UT)).str());
                assert(false);
            }

            assert(_PVV[update->SrcReplica] <= update->UT);
            _PVV[update->SrcReplica] = update->UT; //;MAX(update->PUT,_PVV[update->SrcReplica]);
            assert(_PVV_latest[update->SrcReplica] <= _PVV[update->SrcReplica]);
            _PVV_latest[update->SrcReplica] = _PVV[update->SrcReplica];
        UNLOCK_VV(update->SrcReplica);

#endif

        delete update;

#ifdef MEASURE_VISIBILITY_LATENCY
        {
            std::lock_guard<std::mutex> lk(_pendingInvisibleVersionsMutex);
            _pendingInvisibleVersions[newVersion->SrcReplica].push(newVersion);
        }
#endif
    }


#ifdef CURE

    void MVKVTXStore::CheckAndRecordVisibilityLatency(std::vector<PhysicalTimeSpec> gsv)
#elif defined(H_CURE)

    void MVKVTXStore::CheckAndRecordVisibilityLatency(PhysicalTimeSpec rst)
#elif defined(WREN)

    void MVKVTXStore::CheckAndRecordVisibilityLatency(PhysicalTimeSpec glst, PhysicalTimeSpec grst)
#endif
    {
#ifdef MEASURE_VISIBILITY_LATENCY
        PhysicalTimeSpec currentTime = Utils::GetCurrentClockTime();

        {
            std::lock_guard<std::mutex> lk(_pendingInvisibleVersionsMutex);
            PhysicalTimeSpec rit;
            PhysicalTimeSpec put;
            int j;
            bool vis;
#ifdef WREN
            bool local;
#endif
            for (unsigned int i = 0; i < _pendingInvisibleVersions.size(); ++i) {
#ifdef WREN
                local = false;
                if (i == _replicaId) { local = true; }
#endif
                std::queue<ItemVersion *> &q = _pendingInvisibleVersions[i];
                vis = true;
                while (!q.empty()) {
                    ItemVersion *version = q.front();

#if defined(H_CURE)
                    if (version->UT > rst) {
                        vis = false;
                    }
#elif defined(WREN)
                    if (local) {
                        if (version->UT > glst || version->RST > grst) {
                            vis = false;
                        }
                    } else {
                        if (version->UT > grst || version->RST > glst) {
                            vis = false;
                        }
                    }
#elif defined(CURE)

                    for (j = 0; j < version->DV.size(); j++) {
                        if (version->DV[j] > gsv[j]) {
                            vis = false;
                            break;
                        }
                    }


#endif
                    /*
                     * In GentleRain and Okapi, updates from a replica are made visible in update timestamp order.
                     * In Vec-GR it is not true, since the dependency vector of the client that wrote might be very stale
                     * So, you can create something very new that depends on something very old, making the new update immediately
                     * visible. Something "less new" can depend on something not so old, and become visible afterwards.
                     *
                     * In Cure this is less likely to happen, because the local entry in the DV is set to the the local clock
                     * upon writing (i.e., d.DV[local] =  d.ut). In particular, in Cure, this ensures that if x.ut < y.ut and
                     * x and y are in the same partition, than y becomes visible after x (not true in Vec-GR)
                     *
                     * Anyhow, in our benchmark we always write in closed loop, so we consider that remote updates from a
                     * replica become visible in update timestamp order.
                     *
                     * Hence, we break if !vis
                     *
                     */

                    if (!vis) {
                        break;
                    }

                    _numCheckedVersions += 1;

                    if (_numCheckedVersions % SysConfig::LatencySampleInterval == 0) {
#if defined(CURE)
                        put = version->UT;
#elif defined(WREN) || defined(H_CURE)
                        put = version->CreationTime;
#endif
                        PhysicalTimeSpec put64 = put;
                        FROM_64(put64, put64);

                        ReplicationLatencyResult result;
                        result.OverallLatency = currentTime - put64;
                        result.PropagationLatency = version->RIT - put64;
                        result.VisibilityLatency = currentTime - version->RIT;
                        result.SrcReplica = version->SrcReplica;
#ifdef WREN
                        if (local) {
                            _recordedLocalLatencies.push_back(result);
                        } else {
                            _recordedLatencies.push_back(result);
                        }
#else
                        _recordedLatencies.push_back(result);
#endif
                    }
                    q.pop();
                }
            }
        }
#endif
    }


    void MVKVTXStore::calculateStalenessStatistics(StalenessStatistics &stat) {

        {
            std::lock_guard<std::mutex> lk(SysStats::NumFresherVersionsInItemChainMutex);
            std::vector<int64_t> numFreshVer = SysStats::NumFresherVersionsInItemChain;

            if (numFreshVer.size() == 0) {
                stat.averageNumFresherVersionsInItemChain = 0;
            } else {
                stat.averageNumFresherVersionsInItemChain = std::accumulate(
                        numFreshVer.begin(), numFreshVer.end(), 0.0) / numFreshVer.size();
                assert(stat.averageNumFresherVersionsInItemChain >= 1);
            }
        }

        std::vector<double> usrPercST;

        {
            std::lock_guard<std::mutex> lk(SysStats::UserPerceivedStalenessTimeMutex);
            usrPercST = SysStats::UserPerceivedStalenessTime;
        }

        if (usrPercST.size() == 0) {
            stat.averageUserPerceivedStalenessTime = 0;
            stat.minStalenessTime = 0;
            stat.maxStalenessTime = 0;
            stat.medianStalenessTime = 0;
            stat._90PercentileStalenessTime = 0;
            stat._95PercentileStalenessTime = 0;
            stat._99PercentileStalenessTime = 0;

        } else {

            stat.averageUserPerceivedStalenessTime =
                    std::accumulate(usrPercST.begin(), usrPercST.end(), 0.0) / usrPercST.size();

            std::sort(usrPercST.begin(), usrPercST.end());

            stat.minStalenessTime = SysStats::MinUserPercievedStalenessTime;
            stat.maxStalenessTime = SysStats::MaxUserPercievedStalenessTime;
            stat.medianStalenessTime = usrPercST.at((int) std::round(usrPercST.size() * 0.50));
            stat._90PercentileStalenessTime = usrPercST.at((int) std::round(usrPercST.size() * 0.90) - 1);
            stat._95PercentileStalenessTime = usrPercST.at((int) std::round(usrPercST.size() * 0.95) - 1);
            stat._99PercentileStalenessTime = usrPercST.at((int) std::round(usrPercST.size() * 0.99) - 1);
        }

    }

    void MVKVTXStore::addStalenessTimesToOutput(string &str) {
        {
            str += "\n\nSTALENESS TIMES\n\n";
            std::lock_guard<std::mutex> lk(SysStats::UserPerceivedStalenessTimeMutex);

            for (int i = 0; i < SysStats::UserPerceivedStalenessTime.size(); i++) {
                str += ((boost::format("%lf\n") % SysStats::UserPerceivedStalenessTime[i]).str());
            }

        }

    }

    /* ---------------------  H_CURE specific functions ---------------------  */
#ifdef H_CURE

    PhysicalTimeSpec MVKVTXStore::updateAndGetRST() {
        PhysicalTimeSpec localPartitionRST = getRemoteVVmin();
        LOCK_RST(_partitionId);
            _RST[_partitionId] = localPartitionRST;
        UNLOCK_RST(_partitionId);

#ifdef MEASURE_VISIBILITY_LATENCY
        CheckAndRecordVisibilityLatency(localPartitionRST);
#endif
        return localPartitionRST;
    }

    void MVKVTXStore::updateRSTOnRead(PhysicalTimeSpec clientRST) {
        LOCK_RST(_partitionId);
            _RST[_partitionId] = MAX(clientRST, _RST[_partitionId]);
        UNLOCK_RST();
#ifdef MEASURE_VISIBILITY_LATENCY
        CheckAndRecordVisibilityLatency( _RST[_partitionId]);
#endif
    }

    void MVKVTXStore::HandleRSTFromPeer(PhysicalTimeSpec rst, int peerId) {
        LOCK_RST(peerId);
            _RST[peerId] = rst;
        UNLOCK_RST();
    }

    PhysicalTimeSpec MVKVTXStore::getRSTMin() {
        PhysicalTimeSpec rstMin = _RST.begin()->second;

        for (auto it = _RST.begin(); it != _RST.end(); ++it) {
            PhysicalTimeSpec rst = it->second;
            rstMin = MIN(rstMin, rst);
        }

        return rstMin;
    }

#endif

#ifdef WREN

    bool MVKVTXStore::updateAndGetStabilizationTimes(PhysicalTimeSpec &lst, PhysicalTimeSpec &rst) {

        PhysicalTimeSpec localPartitionRST, localPartitionLST;
        MVKVTXStore::Instance()->getCurrentRemoteAndLocalValues(localPartitionLST, localPartitionRST);

        LOCK_RST(_partitionId);
            _RST[_partitionId] = localPartitionRST;
        UNLOCK_RST();
        rst = localPartitionRST;

        LOCK_LST(_partitionId);
            _LST[_partitionId] = localPartitionLST;
        UNLOCK_LST();

        lst = localPartitionLST;
        return true;
    }

    void MVKVTXStore::updateRSTOnRead(PhysicalTimeSpec clientRST) {
        LOCK_RST(_partitionId);
            _RST[_partitionId] = MAX(clientRST, _RST[_partitionId]);
        UNLOCK_RST();
    }

    void MVKVTXStore::updateLSTOnRead(PhysicalTimeSpec clientLST) {
        LOCK_LST(_partitionId);
            _LST[_partitionId] = MAX(clientLST, _LST[_partitionId]);
        UNLOCK_LST();
    }

    void MVKVTXStore::HandleStabilizationTimesFromPeer(PhysicalTimeSpec lst, PhysicalTimeSpec rst, int peerId) {
        LOCK_LST(peerId);
            _LST[peerId] = lst;
        UNLOCK_LST();
        LOCK_RST(peerId);
            _RST[peerId] = rst;
        UNLOCK_RST();
    }

    PhysicalTimeSpec MVKVTXStore::getRSTMin() {
        PhysicalTimeSpec rstMin = _RST.begin()->second;

        for (auto it = _RST.begin(); it != _RST.end(); ++it) {
            PhysicalTimeSpec rst = it->second;
            rstMin = MIN(rstMin, rst);
        }

        return rstMin;
    }

    PhysicalTimeSpec MVKVTXStore::getLSTMin() {
        PhysicalTimeSpec lstMin = _LST.begin()->second;

        for (auto it = _LST.begin(); it != _LST.end(); ++it) {
            PhysicalTimeSpec lst = it->second;
            lstMin = MIN(lstMin, lst);
        }

        return lstMin;
    }

#endif


    PhysicalTimeSpec MVKVTXStore::GetVV(int replicaId) {

#if defined(H_CURE) || defined(WREN)
        return _HVV[replicaId];
#else

        return _PVV[replicaId];
#endif

    }

    std::vector<PhysicalTimeSpec> MVKVTXStore::GetVV() {
#if defined(H_CURE) || defined(WREN)
        return _HVV;
#else
        return _PVV;
#endif
    }

    std::vector<int64_t> MVKVTXStore::GetLVV() {
        return _LVV;
    }

    int64_t MVKVTXStore::getLocalClock() {
        return _localClock;
    }

#if defined(H_CURE) || defined(WREN)

    void MVKVTXStore::getCurrentRemoteAndLocalValues(PhysicalTimeSpec &local, PhysicalTimeSpec &remote) {
        std::vector<PhysicalTimeSpec> currVV = _HVV;
        PhysicalTimeSpec min(INT64_MAX, INT64_MAX);

        for (int i = 0; i < currVV.size(); i++) {
            if (_replicaId != i) {
                if (currVV[i] < min) {
                    min = currVV[i];
                }
            }
        }

        local = currVV[_replicaId];
        remote = min;
    }

    PhysicalTimeSpec MVKVTXStore::getRemoteVVmin() {
        std::vector<PhysicalTimeSpec> currVV = _HVV;
        PhysicalTimeSpec min(INT64_MAX, INT64_MAX);

        for (int i = 0; i < currVV.size(); i++) {
            if (_replicaId != i) {
                if (currVV[i] < min) {
                    min = currVV[i];
                }
            }
        }

        return min;
    }

#endif

    PhysicalTimeSpec MVKVTXStore::getVVmin() {
        PhysicalTimeSpec min;
#if defined(H_CURE) || defined(WREN)
        min = _HVV[0];
        for (int i = 0; i < _HVV.size(); i++) {
            if (_HVV[i] < min) {
                min = _HVV[i];
            }
        }
#endif
        return min;
    }

    void MVKVTXStore::updateVV(int replicaId, PhysicalTimeSpec t) {
#if defined(H_CURE) || defined(WREN)
        LOCK_VV(replicaId);
            _HVV[replicaId] = MAX(t, _HVV[replicaId]);
            assert(_HVV_latest[replicaId] <= _HVV[replicaId]);
            _HVV_latest[replicaId] = _HVV[replicaId];
        UNLOCK_VV(replicaId);

#elif defined(CURE)
        LOCK_VV(replicaId);
            _PVV[replicaId] = MAX(t, _PVV[replicaId]);
            assert(_PVV_latest[replicaId] <= _PVV[replicaId]);
            _PVV_latest[replicaId] = _PVV[replicaId];
        UNLOCK_VV(replicaId);
#endif

    }

    PhysicalTimeSpec MVKVTXStore::updateAndGetVV(int replicaId, PhysicalTimeSpec t) {
        PhysicalTimeSpec res;

#if defined(H_CURE) || defined(WREN)
        LOCK_VV(replicaId);
            _HVV[replicaId] = MAX(t, _HVV[replicaId]);
            assert(_HVV_latest[replicaId] <= _HVV[replicaId]);
            res = _HVV[replicaId];
            _HVV_latest[replicaId] = _HVV[replicaId];
        UNLOCK_VV(replicaId);

#elif defined(CURE)
        LOCK_VV(replicaId);
            _PVV[replicaId] = MAX(t, _PVV[replicaId]);
            assert(_PVV_latest[replicaId] <= _PVV[replicaId]);
            _PVV_latest[replicaId] = _PVV[replicaId];
            res = _PVV[replicaId];
        UNLOCK_VV(replicaId);

#endif
        return res;

    }

    std::vector<PhysicalTimeSpec> MVKVTXStore::updateAndGetVV() {

        std::vector<PhysicalTimeSpec> ret;
        PhysicalTimeSpec now = Utils::GetCurrentClockTime();
#if defined(H_CURE) || defined(WREN)
        LOCK_VV(_replicaId);
            TO_64(now, now);
            SET_HYB_PHYS(now.Seconds, now.Seconds);
            SET_HYB_LOG(now.Seconds, 0);
            if (now.Seconds > _HVV[_replicaId].Seconds) {
                _HVV[_replicaId] = now;
                assert(_HVV_latest[_replicaId] <= _HVV[_replicaId]);
                _HVV_latest[_replicaId] = _HVV[_replicaId];
            }
            ret = _HVV;
        UNLOCK_VV(_replicaId);
#endif
        return ret;
    }


    PhysicalTimeSpec MVKVTXStore::GetLST() {
        PhysicalTimeSpec lst;
#ifndef SIXTY_FOUR_BIT_CLOCK
        PVV_ALL_LOCK();
#endif
        lst = _PVV[0];


        for (unsigned int i = 1; i < _PVV.size(); ++i) {
            lst = MIN(lst, _PVV[i]);
        }
#ifndef SIXTY_FOUR_BIT_CLOCK
        PVV_ALL_UNLOCK();
#endif

        return lst;
    }

    void MVKVTXStore::SetGST(PhysicalTimeSpec gst) {


#ifndef SIXTY_FOUR_BIT_CLOCK
        {
            std::lock_guard<std::mutex> lk(_GSTMutex);
            ASSERT(gst >= _GST);

            _GST = gst;

        }
#else
        //Assuming there are no problems in the gst ;)
        _GST.Seconds = gst.Seconds;
#endif

    }

    PhysicalTimeSpec MVKVTXStore::GetGST() {
#ifndef SIXTY_FOUR_BIT_CLOCK
        std::lock_guard<std::mutex> lk(_GSTMutex);
#endif
        return _GST;
    }

/* --------------------- CURE specific functions --------------------- */
#ifdef CURE

    std::vector<PhysicalTimeSpec> MVKVTXStore::updateAndGetPVV() {
        std::vector<PhysicalTimeSpec> ret;

        LOCK_VV(_replicaId);
            PhysicalTimeSpec now = Utils::GetCurrentClockTime();
            TO_64(_PVV[_replicaId], now);
            ret = _PVV;
        UNLOCK_VV(_replicaId);
        return ret;
    }

    std::vector<PhysicalTimeSpec> MVKVTXStore::GetPVV() {
        std::vector<PhysicalTimeSpec> ret;
#ifndef SIXTY_FOUR_BIT_CLOCK
        PVV_ALL_LOCK();
#endif
            ret = _PVV;
#ifndef SIXTY_FOUR_BIT_CLOCK
        PVV_ALL_UNLOCK();
#endif
        return ret;
    }

#endif
} // namespace scc
