#ifndef SCC_KVSTORE_MV_KVSTORE_H_
#define SCC_KVSTORE_MV_KVSTORE_H_

#include "common/utils.h"
#include "common/sys_stats.h"
#include "common/sys_config.h"
#include "common/sys_logger.h"
#include "common/types.h"
#include "kvstore/item_version.h"
#include "kvstore/item_anchor.h"
#include "kvstore/log_manager.h"
#include "kvservice/coordinator.h"
#include "messages/op_log_entry.pb.h"
#include <assert.h>
#include <type_traits>
#include <stdexcept>
#include <atomic>
#include <vector>
#include <unordered_map>
#include <queue>
#include <thread>
#include <chrono>
#include <numeric>
#include <iostream>


#if defined(H_CURE) || defined(WREN)

#define LOCK_RST(A)  { std::lock_guard <std::mutex> lk(*(_RSTMutex[A]));

#define UNLOCK_RST(A)  }

#if 0  //Apparently, also here using a spinlock degrades performance w.r.t. using a mutex
#error No spinlock
#define LOCK_HVV(A) do{\
                        while(!__sync_bool_compare_and_swap((volatile unsigned int *)&_hvv_l[A], 0, 1)){\
                                __asm __volatile("pause\n": : :"memory");\
                            }\
                    }while(0)

#define UNLOCK_HVV(A)     *(volatile unsigned int *)&_hvv_l[A] = 0
#else

#define LOCK_HVV(A)  { std::lock_guard <std::mutex> lk(_hvvMutex[A]);

#define UNLOCK_HVV(A)  }

#endif

#ifdef WREN
#define LOCK_LST(A)  { std::lock_guard <std::mutex> lk(*(_LSTMutex[A]));

#define UNLOCK_LST(A)  }
#endif


#define PHYS_BITS 48
#define LOG_BITS 16
#define MAX_LOG 65536

//TODO: when recovering the nsec from a hybrid, you have to take into account that the last log bits from the leas significant 32 bits
//TODO: of the hybrid representation are logical, so you have to set them to 0
#define PHYS_MASK 0xFFFFFFFFFFFFFFFF<<LOG_BITS
#define LOG_MASK  0xFFFFFFFFFFFFFFFF>>PHYS_BITS

#define PHYS_TO_HYB(A, B) A = (B&PHYS_MASK)
#define HYB_TO_PHYS(A, B) PHYS_TO_HYB(A,B)
#define HYB_TO_LOG(A, B) A = (B&LOG_MASK)
#define H_T_P(A) (A&PHYS_MASK)
#define H_T_L(A) (A&LOG_MASK)
#define SET_HYB_PHYS(A, B) A = (B & PHYS_MASK) | (A & LOG_MASK)
#define SET_HYB_LOG(A, B) assert(B < MAX_LOG); A = (B & LOG_MASK) | (A & PHYS_MASK)
#endif //H_CURE


#ifdef CURE
#ifdef SPIN_LOCK //Use spinlocks
#error Spink locks do not perform well. Either they are poorly implemented or simply contention is too high to make them work
unsigned long _gst_lock;
unsigned long _pvv_lock;

#define PVV_LOCK() do{\
                        unsigned int wait;\
                        unsigned int i = 0;\
                        while (1){\
                            wait = 1;\
                            if (__sync_bool_compare_and_swap((volatile unsigned long *)&_pvv_lock, 0, 1))\
                                break;\
                            while(*(volatile unsigned long *)&_pvv_lock){\
                                for (i = 0; i < wait; i++)\
                                    __asm __volatile("pause\n": : :"memory");\
                                wait *= 2;\
                             }\
                        }\
                        (*(volatile unsigned long *) &pvv_counter)++;
             }while(0)

#define PVV_UNLOCK()  (*(volatile unsigned long *) &pvv_counter)++; __asm __volatile("": : :"memory"); (*(volatile uint32_t *)&_pvv_lock) = 0;

//TODO: consider using __asm volatile ("pause" ::: "memory");

#define GSV_LOCK() while (1){\
            if (__sync_bool_compare_and_swap((volatile unsigned long *)&_gsv_lock, 0, 1))\
                break;\
            while(*(volatile unsigned long *)&_pvv_lock){\
                    __asm __volatile("pause\n": : :"memory");\
            }\
        }

#define GSV_UNLOCK()  __asm __volatile("": : :"memory"); (*(volatile uint32_t *)&_gsv_lock) = 0

#else   //Use locks

#define PVV_LOCK(A)  { std::lock_guard <std::mutex> lk(_pvvMutex[A]);
//PVV ALL LOCK is actually doing nothing. It is not a problem for us, since it is only used in ShowState and
// To get the minLST when there is no 64 bit, which is something we are not supporting anymore
#define PVV_ALL_LOCK()  {   for(int i=0;i<32;i++) std::lock_guard <std::mutex> lk(_pvvMutex[i]);

#define GSV_LOCK() { std::lock_guard <std::mutex> lk(_GSTMutex);

#define PVV_UNLOCK(A)  }
#define PVV_ALL_UNLOCK()  }

#define GSV_UNLOCK()  }

#endif //SPIN_LOCK
#endif //CURE

#if defined(H_CURE) || defined(WREN)

#define LOCK_VV(A) LOCK_HVV(A)
#define UNLOCK_VV(A) UNLOCK_HVV(A)

#elif defined(CURE)

#define LOCK_VV(A) PVV_LOCK(A)
#define UNLOCK_VV(A) PVV_UNLOCK(A)

#endif

namespace scc {

    typedef std::unordered_map<std::string, ItemAnchor *> ItemIndexTable;
    typedef std::pair<ItemIndexTable::iterator, bool> IITResult;

    class MVKVTXStore {
    public:
        static MVKVTXStore *Instance();

        MVKVTXStore();

        ~MVKVTXStore();

        void SetPartitionInfo(DBPartition p, int numPartitions, int numReplicasPerPartition);

        void Initialize();

        void GetSysClockTime(PhysicalTimeSpec &physicalTime, int64_t &logicalTime);

        bool Add(const std::string &key, const std::string &value);

        bool AddAgain(const std::string &key, const std::string &value);

#if defined(H_CURE) || defined(WREN)

        bool Write(PhysicalTimeSpec commitTime, TxContex &cdata, const std::string &key, const std::string &value,
                   std::vector<LocalUpdate *> &updatesToBeReplicatedQueue);

#elif defined(CURE)

        bool Write(std::vector<PhysicalTimeSpec> commitTime, TxContex &cdata, const std::string &key,
                   const std::string &value,
                   std::vector<LocalUpdate *> &updatesToBeReplicatedQueue);

#endif

#if defined(H_CURE) || defined(WREN)

        volatile int _hvv_l[32];
        std::mutex _hvvMutex[32];

        int64_t InstallXactTimeIfNeeded(PhysicalTimeSpec &xactTime);

        std::vector<PhysicalTimeSpec> _HVV; // hybrid version vector
        std::vector<PhysicalTimeSpec> _HVV_latest; // hybrid version vector
#endif

#ifdef WREN

        bool LocalTxSliceGet(PhysicalTimeSpec cdataGLST, PhysicalTimeSpec cdataGRST, const std::string &key,
                             std::string &value);


#else
        bool LocalTxSliceGet(TxContex &cdata, const std::string &key, std::string &value);

#endif

        bool ShowItem(const std::string &key, std::string &itemVersions);

        bool ShowDB(std::string &allItemVersions);

        bool ShowTime(std::string &stateStr);

        bool ShowStateTx(std::string &stateStr);

        bool ShowStateTxCSV(std::string &stateStr);

        bool DumpLatencyMeasurement(std::string &resultStr);

        int Size();

        void Reserve(int numItems);

        bool HandlePropagatedUpdate(std::vector<PropagatedUpdate *> &updates);

        bool HandleHeartbeat(Heartbeat &hb, int srcReplica);

        //Staleness time measurement
        std::vector<StalenessTimeMeasurement> stalenessStartTimeStatisticsArrayPerReplica;
        std::mutex _avgStalenessTimesMutex;
        std::vector<double> avgStalenessTimes;
        int stalenessBorder;

        std::mutex _sysTimeMutex;
        std::mutex _pvvMutex[32];
        std::vector<PhysicalTimeSpec> _PVV; // physical version vector
        std::vector<PhysicalTimeSpec> _PVV_latest; // hybrid version vector

        inline int getReplicaId() {
            return _replicaId;
        }

        inline int getPartitionId() {
            return _partitionId;
        }

        void InitializeStalenessMeasurements();

#ifdef H_CURE

        PhysicalTimeSpec updateAndGetRST();

        void updateRSTOnRead(PhysicalTimeSpec clientRST);

        void HandleRSTFromPeer(PhysicalTimeSpec rst, int peerId);

        PhysicalTimeSpec getRSTMin();

#endif

#ifdef WREN

        bool updateAndGetStabilizationTimes(PhysicalTimeSpec &lst, PhysicalTimeSpec &rst);

        void updateRSTOnRead(PhysicalTimeSpec clientRST);

        void updateLSTOnRead(PhysicalTimeSpec clientLST);

        void HandleStabilizationTimesFromPeer(PhysicalTimeSpec lst, PhysicalTimeSpec rst, int peerId);

        PhysicalTimeSpec getRSTMin();

        PhysicalTimeSpec getLSTMin();

#endif

        std::vector<int64_t> GetLVV();

        int64_t getLocalClock();

        PhysicalTimeSpec getRemoteVVmin();

        PhysicalTimeSpec getVVmin();

        PhysicalTimeSpec GetVV(int replicaId);

        std::vector<PhysicalTimeSpec> GetVV();

        void updateVV(int replicaId, PhysicalTimeSpec t);

        std::vector<PhysicalTimeSpec> updateAndGetVV();

        PhysicalTimeSpec updateAndGetVV(int replicaId, PhysicalTimeSpec t);

        PhysicalTimeSpec START_TIME;

        void getCurrentRemoteAndLocalValues(PhysicalTimeSpec &local, PhysicalTimeSpec &remote);

    private:
        std::vector<ItemIndexTable *> _indexTables;
        std::vector<std::mutex *> _indexTableMutexes;
        int _partitionId;
        int _replicaId;
        int _numPartitions;
        int _numReplicasPerPartition;

        // system clock
        int64_t _localClock;
        int64_t _replicaClock;

        std::vector<int64_t> _LVV; // logical version vector (for debugging)
        PhysicalTimeSpec _initItemVersionUT; // PUT of initially loaded item version

    private:
        PhysicalTimeSpec _GST;
#ifdef CURE
#endif

#ifdef H_CURE
        std::unordered_map<int, PhysicalTimeSpec> _RST;
        std::unordered_map<int, std::mutex *> _RSTMutex;
#endif

#ifdef WREN
        std::unordered_map<int, PhysicalTimeSpec> _RST;
        std::unordered_map<int, PhysicalTimeSpec> _LST;
        std::unordered_map<int, std::mutex *> _RSTMutex;
        std::unordered_map<int, std::mutex *> _LSTMutex;
#endif

    public:
        PhysicalTimeSpec GetLST();

        PhysicalTimeSpec GetGST();

        void SetGST(PhysicalTimeSpec gst);

        void SetGSTIfSmaller(PhysicalTimeSpec gst);

        PhysicalTimeSpec GetAndUpdateLSTIfNeeded(PhysicalTimeSpec &target);

#ifdef CURE

        bool waitOnRead(ConsistencyMetadata &cdata, PhysicalTimeSpec &maxPVV);

        std::vector<PhysicalTimeSpec> GetGSV();

        void SetGSVIfSmaller(std::vector<PhysicalTimeSpec> &gsv);

        void SetGSV(std::vector<PhysicalTimeSpec> gsv);

        std::vector<PhysicalTimeSpec> updateAndGetPVV();

        std::vector<PhysicalTimeSpec> GetPVV();

        volatile unsigned long pvv_counter;

#endif


        bool AdvanceLocalPVV();

        PhysicalTimeSpec GetAndUpdateLST();

        // measure replication visibility latency
        std::mutex _pendingInvisibleVersionsMutex;
        std::vector<std::queue<ItemVersion *>> _pendingInvisibleVersions;
        std::vector<ReplicationLatencyResult> _recordedLatencies;
        std::vector<ReplicationLatencyResult> _recordedLocalLatencies;
        int _numCheckedVersions;

#ifdef CURE
        void CheckAndRecordVisibilityLatency(std::vector<PhysicalTimeSpec> GSV);
#elif defined(H_CURE)
        void CheckAndRecordVisibilityLatency(PhysicalTimeSpec rst);
#elif defined(WREN)

        void CheckAndRecordVisibilityLatency(PhysicalTimeSpec glst, PhysicalTimeSpec grst);

#endif

    private:

        void ShowVisibilityLatency(VisibilityStatistics &v, bool remoteMode);

        // update propagation
        void InstallPropagatedUpdate(PropagatedUpdate *update);


        void calculateStalenessStatistics(StalenessStatistics &stat);

        void addStalenessTimesToOutput(string &str);

        template<typename T>
        std::string addToStateStr(const std::string property, T value) {

            std::string str;

            if (std::is_same<T, std::string>::value) {
                str = (boost::format(" %s %s\n") % property % value).str();
            } else {
                str = (boost::format(" %s %s\n") % property % std::to_string(value)).str();
            }

            return str;
        }

        std::string addToStateStr(const std::string property, std::string value) {

            return (boost::format(" %s %s\n") % property % value).str();
        }

    };


} // namespace scc

#endif
