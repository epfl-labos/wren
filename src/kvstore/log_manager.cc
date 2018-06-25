#include "kvstore/log_manager.h"
#include "kvstore/mv_kvstore.h"
#include "common/sys_config.h"
#include "common/sys_stats.h"
#include "common/utils.h"
#include "common/sys_logger.h"
#include "kvstore/item_anchor.h"
#include "kvstore/item_version.h"
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <messages/op_log_entry.pb.h>

namespace scc {

    LogManager *LogManager::Instance() {
        static LogManager _instance;
        return &_instance;
    }

    LogManager::LogManager() {
        bool usePreallocatedOpLogFile = false;

        // check if the pre-allocated log file exists
        struct stat s;
        if (stat(SysConfig::PreallocatedOpLogFile.c_str(), &s) != -1) {
            usePreallocatedOpLogFile = true;
        }

        if (usePreallocatedOpLogFile) {
            // open log file
            mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
            _logfd = open(SysConfig::PreallocatedOpLogFile.c_str(), O_WRONLY, mode);
        } else {
            // construct full file name
            std::string hostName = Utils::GetHostName();
            std::string pId = std::to_string((long long) getpid());
            std::string logFile = SysConfig::OpLogFilePrefix + hostName + "_" + pId;
            // remove the log file if already exists
            unlink(logFile.c_str());
            // open log file
            mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
            _logfd = open(logFile.c_str(), O_WRONLY | O_CREAT, mode);
        }

        // launch worker thread
        _workerThread = new std::thread(&LogManager::worker, this);

        NumReplicatedBytes = 0;
    }

    LogManager::~LogManager() {
        // join worker thread
        _workerThread->join();
        // close log file
        close(_logfd);
    }

    void LogManager::Initialize(int numReplicas) {
        _replicatedUpdateQueues.resize(numReplicas);

        PersistedPropagatedUpdateQueues.resize(numReplicas);
        for (int i = 0; i < numReplicas; i++) {
            PersistedPropagatedUpdateQueueMutexes.push_back(new std::mutex);
            PersistedPropagatedUpdateEvents.push_back(new WaitHandle);
        }
        ToPropagateLocalUpdateQueuePtr = &ToPropagateLocalUpdateQueue;
    }

    void LogManager::AppendLog(LocalUpdate *update, WaitHandle *persistedEvent) {

        std::lock_guard<std::mutex> lk(_reqQueueMutex);
#ifdef MEASURE_STATISTICS
        SysStats::NumUpdatesStoredInLocalUpdateQueue += 1;
#endif
        _localUpdateQueue.push_back(update);

        _opPersistedEventQueue.push_back(persistedEvent);
        // notify the worker
        _reqAvailable.Set();
    }

    void LogManager::AppendReplicatedUpdate(PropagatedUpdate *update) {
        std::lock_guard<std::mutex> lk(_reqQueueMutex);

        _replicatedUpdateQueues[update->SrcReplica].push_back(update);
        // notify the worker
        _reqAvailable.Set();
    }


    void LogManager::worker() {

        std::vector<WaitHandle *> persistedEvents;
        std::vector<LocalUpdate *> localUpdates;
        std::vector<std::vector<PropagatedUpdate *>> replicatedUpdates;

        while (true) {
            // wait until there is work to do
            _reqAvailable.WaitAndReset();

            // obtain to persist transactions
            {
                std::lock_guard<std::mutex> lk(_reqQueueMutex);

                localUpdates = _localUpdateQueue;
                _localUpdateQueue.clear();

                replicatedUpdates = _replicatedUpdateQueues;
                for (unsigned int i = 0; i < _replicatedUpdateQueues.size(); i++) {
                    _replicatedUpdateQueues[i].clear();
                }

                persistedEvents = _opPersistedEventQueue;
                _opPersistedEventQueue.clear();
            }

            // calculate the number of bytes to write

            int numBytesToPersist = 0;
            for (unsigned int i = 0; i < localUpdates.size(); i++) {
                LocalUpdate *updt = localUpdates[i];
                assert(updt != NULL);
                ASSERT(updt != NULL);

                auto ver = static_cast<ItemVersion *> (localUpdates[i]->UpdatedItemVersion);
                ASSERT(ver != NULL);

                // serialize operation
#if defined(H_CURE) || defined WREN
                PbLogTccWrenSetRecord record;
                record.set_key(ver->Key);
                record.set_value(ver->Value);
                record.mutable_ut()->set_seconds(ver->UT.Seconds);
                record.mutable_ut()->set_nanoseconds(ver->UT.NanoSeconds);
                record.mutable_rst()->set_seconds(ver->RST.Seconds);
                record.mutable_rst()->set_nanoseconds(ver->RST.NanoSeconds);
                record.set_srcreplica(ver->SrcReplica);
#endif

#ifdef CURE
                PbLogTccCureSetRecord record;
                record.set_key(ver->Key);
                record.set_value(ver->Value);
                record.mutable_ut()->set_seconds(ver->UT.Seconds);
                record.mutable_ut()->set_nanoseconds(ver->UT.NanoSeconds);
                record.set_srcreplica(ver->SrcReplica);
                for (int j = 0; j < ver->DV.size(); j++) {

                    PbPhysicalTimeSpec *vv = record.add_dv();

                    vv->set_seconds(ver->DV[j].Seconds);
                    vv->set_nanoseconds(ver->DV[j].NanoSeconds);
                }
#endif //CURE

                localUpdates[i]->SerializedRecord = record.SerializeAsString();
#ifdef DO_PERSISTENCE
                numBytesToPersist += localUpdates[i]->SerializedRecord.length();
#endif //DO_PERSISTENCE
            }

#ifdef DO_PERSISTENCE
            for (unsigned int i = 0; i < replicatedUpdates.size(); i++) {
                for (unsigned int j = 0; j < replicatedUpdates[i].size(); j++) {
                    ASSERT(replicatedUpdates[i][j] != NULL);
                    numBytesToPersist += replicatedUpdates[i][j]->SerializedRecord.length();
                }
            }

            // merge all to-persist transactions to one buffer
            char *toPersistBuf = new char[numBytesToPersist];
            int tmpIndex = 0;
            for (unsigned int i = 0; i < localUpdates.size(); i++) {
                std::string &record = localUpdates[i]->SerializedRecord;
                record.copy(toPersistBuf + tmpIndex, record.length(), 0);
                tmpIndex += record.length();
            }
            NumReplicatedBytes += tmpIndex;
            for (unsigned int i = 0; i < replicatedUpdates.size(); i++) {
                for (unsigned int j = 0; j < replicatedUpdates[i].size(); j++) {
                    std::string &record = replicatedUpdates[i][j]->SerializedRecord;
                    record.copy(toPersistBuf + tmpIndex, record.length(), 0);
                    tmpIndex += record.length();
                }
            }
            // write to disk synchronously
            if (SysConfig::DurabilityOption == DurabilityType::Disk) {
                write(_logfd, toPersistBuf, numBytesToPersist);
                fdatasync(_logfd);
                delete[] toPersistBuf;
            } else if (SysConfig::DurabilityOption == DurabilityType::Memory) {
                _inMemoryLog.push_back(toPersistBuf);
            }
#endif //DO_PERSISTENCE

            //////////local update//////////////////////

            // mark local updates persisted
            for (unsigned int i = 0; i < localUpdates.size(); i++) {
                auto version = static_cast<ItemVersion *> (localUpdates[i]->UpdatedItemVersion);
                version->Persisted = true;
            }

            // notify waiting threads that peroform local updates
            for (unsigned int i = 0; i < persistedEvents.size(); i++) {
                persistedEvents[i]->Set();
            }
            persistedEvents.clear();

            if (!localUpdates.empty()) {
                for (unsigned int i = 0; i < localUpdates.size(); i++) {

                    std::lock_guard<std::mutex> lk(ToPropagateLocalUpdateQueueMutex);

#ifdef MEASURE_STATISTICS
                    SysStats::NumUpdatesStoredInToPropagateLocalUpdateQueue += 1;
#endif

                    //Atomically insert the update to be replicated and update the MAXDV vector
                    ToPropagateLocalUpdateQueuePtr->push_back(localUpdates[i]);
                }
                ReplicationWaitHandle.Set();
            }

            localUpdates.clear();

            //////////replicated update//////////////////////
            // mark replicated updates persisted
            for (unsigned int i = 0; i < replicatedUpdates.size(); i++) {
                for (unsigned int j = 0; j < replicatedUpdates[i].size(); j++) {
                    PropagatedUpdate *update = replicatedUpdates[i][j];
                    auto version = static_cast<ItemVersion *> (update->UpdatedItemVersion);
                    version->Persisted = true;
                    delete update;
#ifdef MEASURE_STATISTICS
                    SysStats::NumPendingPropagatedUpdates -= 1;
#endif
                }
            }

        }
    }

    std::vector<LocalUpdate *> *LogManager::GetCurrUpdates() {

        std::vector<LocalUpdate *> *ret = ToPropagateLocalUpdateQueuePtr;
        if (ToPropagateLocalUpdateQueuePtr == &ToPropagateLocalUpdateQueue) {
            ToPropagateLocalUpdateQueuePtr = &ToPropagateLocalUpdateQueue2;
        } else {
            ToPropagateLocalUpdateQueuePtr = &ToPropagateLocalUpdateQueue;
        }
        return ret;
    }

} // namespace scc
