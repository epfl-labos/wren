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

#ifndef SCC_COMMON_TYPES_H
#define SCC_COMMON_TYPES_H

#include "common/wait_handle.h"
#include "common/sys_logger.h"
#include <sys/types.h>
#include <algorithm>
#include <string>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <functional>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cmath>
#include <cassert>
#include <inttypes.h>
#include <limits>

#define WREN
//#define H_CURE
//#define CURE
#define MEASURE_VISIBILITY_LATENCY
#define MEASURE_STATISTICS
#define PROFILE_STATS
//#define SIXTY_FOUR_BIT_CLOCK

namespace scc {

    class Configuration {
    public:
        int NumPartitions;
        int NumReplicasPerPartition;
        int TotalNumItems;
        int NumValueBytes;
        int NumOpsPerThread;
        int NumTxsPerThread;
        int NumThreads;
        int ServingReplicaId;
        int ServingPartitionId;
        int ClientServerId;
        std::string RequestsDistribution;
        double RequestsDistributionParameter;
        std::string GroupServerName;
        int GroupServerPort;
        int ReadRatio;
        int WriteRatio;
        int ReadTxRatio;
        std::string clientServerName;
        std::string expResultsOutputFileName;
        bool reservoirSampling;
        int reservoirSamplingLimit;
        int experimentDuration; //milliseconds
        int warmUpDuration; //milliseconds
        int TotalNumOps;
        int TotalNumTxs;
        int experimentType; //1- experiment1, 2 -experiment2
        int clientResetNumber;
        bool enableClientReset;
        int numTxReadItems;
        int numTxWriteItems;
        int locality;
        int64_t TotalNumReadItems;
        int64_t numItemsReadFromClientWriteSet;
        int64_t numItemsReadFromClientReadSet;
        int64_t numItemsReadFromClientWriteCache;
        int64_t numItemsReadFromStore;
        int64_t totalNumClientReadItems;

    };

    class PhysicalTimeSpec {
    public:

        PhysicalTimeSpec() : Seconds(0), NanoSeconds(0) {
        }

        PhysicalTimeSpec(int64_t seconds, int64_t nanoSeconds)
                : Seconds(seconds),
                  NanoSeconds(nanoSeconds) {
        }

        PhysicalTimeSpec(const PhysicalTimeSpec &other) {
            this->NanoSeconds = other.NanoSeconds;
            this->Seconds = other.Seconds;

        }

        PhysicalTimeSpec &operator=(const PhysicalTimeSpec &other) {
            this->NanoSeconds = other.NanoSeconds;
            this->Seconds = other.Seconds;

        }

        bool Zero() {
            return Seconds == 0 && NanoSeconds == 0;
        }

        int64_t Seconds;
        int64_t NanoSeconds;

        double toMilliSeconds();

        double toMicroSeconds();

        void addMilliSeconds(double ms);

        double toSeconds();

        void setToZero() {
            Seconds = 0;
            NanoSeconds = 0;
        }

    };

    bool operator==(const PhysicalTimeSpec &a, const PhysicalTimeSpec &b) {
        return (a.Seconds == b.Seconds) && (a.NanoSeconds == b.NanoSeconds);
    }

    bool operator!=(const PhysicalTimeSpec &a, const PhysicalTimeSpec &b) {
        return !(a == b);
    }

    bool operator>(const PhysicalTimeSpec &a, const PhysicalTimeSpec &b) {
        return (a.Seconds > b.Seconds) ||
               ((a.Seconds == b.Seconds) && (a.NanoSeconds > b.NanoSeconds));
    }

    bool operator<=(const PhysicalTimeSpec &a, const PhysicalTimeSpec &b) {
        return !(a > b);
    }

    bool operator<(const PhysicalTimeSpec &a, const PhysicalTimeSpec &b) {
        return (a.Seconds < b.Seconds) ||
               ((a.Seconds == b.Seconds) && (a.NanoSeconds < b.NanoSeconds));
    }

    bool operator>=(const PhysicalTimeSpec &a, const PhysicalTimeSpec &b) {
        return !(a < b);
    }

    PhysicalTimeSpec operator-(PhysicalTimeSpec &a, PhysicalTimeSpec &b) {
        PhysicalTimeSpec d;

        d.Seconds = a.Seconds - b.Seconds;
        d.NanoSeconds = a.NanoSeconds - b.NanoSeconds;

        while (d.NanoSeconds < 0) {
            d.Seconds -= 1;
            d.NanoSeconds += 1000000000;
        }

        return d;
    }

    double PhysicalTimeSpec::toMilliSeconds() {
        double res = 0;

        res = (double) this->NanoSeconds / 1000000.0;
        res += this->Seconds * 1000.0;

        return res;
    }

    void PhysicalTimeSpec::addMilliSeconds(double ms) {
        this->NanoSeconds += ms * 1000000;
    }

    double PhysicalTimeSpec::toMicroSeconds() {
        double res = 0;

        res = (double) this->NanoSeconds / 1000.0;
        res += this->Seconds * 1000000.0;

        return res;
    }

    double PhysicalTimeSpec::toSeconds() {
        double res = 0;

        res = this->Seconds + (double) this->NanoSeconds / 1000000000.0;

        return res;
    }

    enum class DurabilityType {
        Disk = 1,
        Memory
    };

    enum class GSTDerivationType {
        TREE = 1,
        BROADCAST,
        SIMPLE_BROADCAST
    };

    class DBPartition {
    public:

        DBPartition()
                : Name("NULL"),
                  PublicPort(-1),
                  PartitionPort(-1),
                  ReplicationPort(-1),
                  PartitionId(-1),
                  ReplicaId(-1) {
        }

        DBPartition(std::string name, int publicPort)
                : Name(name),
                  PublicPort(publicPort),
                  PartitionPort(-1),
                  ReplicationPort(-1),
                  PartitionId(-1),
                  ReplicaId(-1) {
        }

        DBPartition(std::string name,
                    int publicPort,
                    int partitionPort,
                    int replicationPort,
                    int partitionId,
                    int replicaId)
                : Name(name),
                  PublicPort(publicPort),
                  PartitionPort(partitionPort),
                  ReplicationPort(replicationPort),
                  PartitionId(partitionId),
                  ReplicaId(replicaId) {
        }

    public:
        std::string Name;
        int PublicPort;
        int PartitionPort;
        int ReplicationPort;
        int PartitionId;
        int ReplicaId;
    };

    typedef std::vector<int64_t> LogicalTimeVector;
    typedef std::vector<std::vector<int64_t>> LogicalTimeVectorVector;

    typedef std::vector<PhysicalTimeSpec> PhysicalTimeVector;
    typedef std::vector<std::vector<PhysicalTimeSpec>> PhysicalTimeVectorVector;

    bool operator<(const LogicalTimeVectorVector &a,
                   const LogicalTimeVectorVector &b) {
        bool leq = true;
        bool strictLess = false;
        for (unsigned int i = 0; i < a.size(); i++) {
            for (unsigned int j = 0; j < a[i].size(); j++) {
                if (a[i][j] < b[i][j]) {
                    strictLess = true;
                } else if (a[i][j] == b[i][j]) {
                    // do nothing
                } else {
                    leq = false;
                    break;
                }
            }
        }

        return (leq && strictLess);
    }

    bool operator>(const LogicalTimeVectorVector &a,
                   const LogicalTimeVectorVector &b) {
        bool geq = true;
        bool strictGreater = false;
        for (unsigned int i = 0; i < a.size(); i++) {
            for (unsigned int j = 0; j < a[i].size(); j++) {
                if (a[i][j] > b[i][j]) {
                    strictGreater = true;
                } else if (a[i][j] == b[i][j]) {
                    // do nothing
                } else {
                    geq = false;
                    break;
                }
            }
        }

        return (geq && strictGreater);
    }

    enum class ConsistencyType {
        Causal = 1
    };

    class ReadItemVersion {
    public:
        std::string Value;
        int64_t LUT;
        int SrcReplica;
        int SrcPartition;
    };

    class GetTransaction {
    public:
        std::vector<std::string> ToReadKeys;
        std::vector<ReadItemVersion> ReadVersions;
    };

    class ConsistencyMetadata {
    public:

        PhysicalTimeSpec DT;
        PhysicalTimeSpec GST;
        PhysicalTimeSpec DUT;
        PhysicalTimeSpec minLST;
        double waited_xact;
    };

    class PropagatedUpdate {
    public:

        PropagatedUpdate()
                : UpdatedItemAnchor(NULL),
                  UpdatedItemVersion(NULL) {
        }

        std::string SerializedRecord;
        std::string Key;
        std::string Value;
        PhysicalTimeSpec UT;
#ifdef MEASURE_VISIBILITY_LATENCY
        PhysicalTimeSpec CreationTime;
#endif
        int64_t LUT;

#if defined(H_CURE) || defined(WREN)
        PhysicalTimeSpec RST;
#endif

#ifdef CURE
        std::vector<PhysicalTimeSpec> DV;
#endif//CURE

        int SrcPartition;
        int SrcReplica;
        void *UpdatedItemAnchor;
        void *UpdatedItemVersion;

#ifdef MINLSTKEY
        std::string minLSTKey;
#endif //MINLSTKEY
    };

    class Heartbeat {
    public:
        PhysicalTimeSpec PhysicalTime;
        int64_t LogicalTime;
    };

    class LocalUpdate {
    public:

        LocalUpdate()
                : UpdatedItemAnchor(NULL),
                  UpdatedItemVersion(NULL) {
        }

        void *UpdatedItemAnchor;
        void *UpdatedItemVersion;
        std::string SerializedRecord;
        bool delayed;
    };

    class UpdatedItemVersion {
    public:
        int64_t LUT;
        int SrcReplica;
    };

    typedef struct {

        long operator()(const UpdatedItemVersion &v) const {
            return (v.LUT << 4) + (v.SrcReplica);
        }
    } UpdatedVersionHash;

    typedef struct {

        long operator()(const UpdatedItemVersion &a, const UpdatedItemVersion &b) const {
            return (a.LUT == b.LUT) && (a.SrcReplica == b.SrcReplica);
        }
    } UpdatedVersionEq;

    typedef std::unordered_map<UpdatedItemVersion,
            PropagatedUpdate *,
            UpdatedVersionHash,
            UpdatedVersionEq> WaitingPropagatedUpdateMap;

    class ReplicationLatencyResult {
    public:
        PhysicalTimeSpec OverallLatency;
        PhysicalTimeSpec PropagationLatency;
        PhysicalTimeSpec VisibilityLatency;
        int SrcReplica;
    };

    enum class WorkloadType {
        READ_ALL_WRITE_LOCAL = 1,
        READ_WRITE_RATIO,
        WRITE_ROUND_ROBIN,
        RANDOM_READ_WRITE
    };

    class StalenessTimeMeasurement {
    public:
        StalenessTimeMeasurement() : sumTimes(0.0), count(0) {}

        double sumTimes;
        int count;
    };

    class VisibilityStatistics {
    public:
        double overall[12];
        double propagation[12];
        double visibility[12];

        VisibilityStatistics() {
            int i;
            for (i = 0; i < 12; i++) { overall[i] = -1; }
            for (i = 0; i < 12; i++) { propagation[i] = -1; }
            for (i = 0; i < 12; i++) { visibility[i] = -1; }
        }

    };

    class StalenessStatistics {
    public:
        double averageStalenessTime;
        double averageStalenessTimeTotal;
        double maxStalenessTime;
        double minStalenessTime;
        double averageUserPerceivedStalenessTime;
        double averageFirstVisibleItemVersionStalenessTime;
        double averageNumFresherVersionsInItemChain;
        double averageNumFresherVersionsInItemChainTotal;
        double medianStalenessTime;
        double _90PercentileStalenessTime;
        double _95PercentileStalenessTime;
        double _99PercentileStalenessTime;

        StalenessStatistics() : minStalenessTime(1000000.0),
                                maxStalenessTime(0.0),
                                averageStalenessTime(0.0),
                                averageFirstVisibleItemVersionStalenessTime(0.0),
                                averageNumFresherVersionsInItemChain(0.0),
                                averageNumFresherVersionsInItemChainTotal(0.0),
                                averageUserPerceivedStalenessTime(0.0),
                                averageStalenessTimeTotal(0.0),
                                medianStalenessTime(0.0),
                                _90PercentileStalenessTime(0.0),
                                _95PercentileStalenessTime(0.0),
                                _99PercentileStalenessTime(0.0) {}
    };

    class Statistic {
    public:
        std::string type;
        double sum;
        double average;
        double median;
        double min;
        double max;
        double _75Percentile;
        double _90Percentile;
        double _95Percentile;
        double _99Percentile;
        double variance;
        double standardDeviation;
        std::vector<double> values;
        mutable std::mutex valuesMutex;

        Statistic() : sum(0.0),
                      average(0.0),
                      median(0.0),
                      min(100000000.0),
                      max(0.0),
                      _75Percentile(0.0),
                      _90Percentile(0.0),
                      _95Percentile(0.0),
                      _99Percentile(0.0),
                      variance(0.0),
                      standardDeviation(0.0),
                      type("") {}

        Statistic(Statistic &&other) : sum(other.sum),
                                       average(other.average),
                                       median(other.median),
                                       min(other.min),
                                       max(other.max),
                                       _75Percentile(other._75Percentile),
                                       _90Percentile(other._90Percentile),
                                       _95Percentile(other._95Percentile),
                                       _99Percentile(other._99Percentile),
                                       variance(other.variance),
                                       standardDeviation(other.standardDeviation),
                                       type(other.type) {
            {
                std::lock_guard<std::mutex> lock(other.valuesMutex);
                for (int i = 0; i < values.size(); i++) {
                    values[i] = std::move(other.values[i]);
                }
            }
        }

        Statistic(Statistic &other) : sum(other.sum),
                                      average(other.average),
                                      median(other.median),
                                      min(other.min),
                                      max(other.max),
                                      _75Percentile(other._75Percentile),
                                      _90Percentile(other._90Percentile),
                                      _95Percentile(other._95Percentile),
                                      _99Percentile(other._99Percentile),
                                      variance(other.variance),
                                      standardDeviation(other.standardDeviation),
                                      type(other.type) {
            {
                std::lock_guard<std::mutex> lock(other.valuesMutex);
                for (int i = 0; i < values.size(); i++) {
                    values[i] = other.values[i];
                }
            }
        }

        Statistic &operator=(Statistic &&other) {
            sum = other.sum;
            average = other.average;
            median = other.median;
            min = other.min;
            max = other.max;
            _75Percentile = other._75Percentile;
            _90Percentile = other._90Percentile;
            _95Percentile = other._95Percentile;
            _99Percentile = other._99Percentile;
            variance = other.variance;
            standardDeviation = other.standardDeviation;
            type = other.type;

            {
                std::lock(valuesMutex, other.valuesMutex);
            }
            std::lock_guard<std::mutex> self_lock(valuesMutex, std::adopt_lock);
            std::lock_guard<std::mutex> other_lock(other.valuesMutex, std::adopt_lock);
            for (int i = 0; i < values.size(); i++) {
                values[i] = std::move(other.values[i]);
            }
            return *this;
        }

        Statistic &operator=(const Statistic &other) {
            sum = other.sum;
            average = other.average;
            median = other.median;
            min = other.min;
            max = other.max;
            _75Percentile = other._75Percentile;
            _90Percentile = other._90Percentile;
            _95Percentile = other._95Percentile;
            _99Percentile = other._99Percentile;
            variance = other.variance;
            standardDeviation = other.standardDeviation;
            type = other.type;

            {
                std::lock(valuesMutex, other.valuesMutex);
                std::lock_guard<std::mutex> self_lock(valuesMutex, std::adopt_lock);
                std::lock_guard<std::mutex> other_lock(other.valuesMutex, std::adopt_lock);
                for (int i = 0; i < values.size(); i++) {
                    values[i] = other.values[i];
                }
            }
            return *this;
        }


        void setToZero() {
            sum = 0;
            average = 0;
            median = 0;
            _75Percentile = 0;
            _99Percentile = 0;
            _95Percentile = 0;
            _90Percentile = 0;
            variance = 0;
            standardDeviation = 0;
            min = 0;
            max = 0;
        }

        std::string toString() {
            std::string
                    str = (boost::format("sum %s %.5lf\n"
                                                 "average % s % .5lf\n"
                                                 "median %s %.5lf\n"
                                                 "min %s %.5lf\n"
                                                 "max %s %.5lf\n"
                                                 "75 percentile %s %.5lf\n"
                                                 "90 percentile %s %.5lf\n"
                                                 "95 percentile %s %.5lf\n"
                                                 "99 percentile %s %.5lf\n")
                           % type % sum
                           % type % average
                           % type % median
                           % type % min
                           % type % max
                           % type % _75Percentile
                           % type % _90Percentile
                           % type % _95Percentile
                           % type % _99Percentile).str();

            return str;

        }

        void calculate();
    };


    void Statistic::calculate() {
        if (values.size() > 0) {
            sum = std::accumulate(values.begin(), values.end(), 0.0);
            average = sum / values.size();

            min = *(std::min_element(std::begin(values),
                                     std::end(values)));
            max = *(std::max_element(std::begin(values),
                                     std::end(values)));

            median = values.at((int) std::round(values.size() * 0.50));

            _75Percentile = values.at((int) std::round(values.size() * 0.75) - 1);

            _90Percentile = values.at((int) std::round(values.size() * 0.90) - 1);

            _95Percentile = values.at((int) std::round(values.size() * 0.95) - 1);

            _99Percentile = values.at((int) std::round(values.size() * 0.99) - 1);

            variance = 0;

            for (int i = 0; i < values.size(); i++) {
                variance += std::pow(average - values[i], 2);
            }

            variance = variance / values.size();
            standardDeviation = std::sqrt(variance);
        }

    }

    class OptVersionBlockStatistics : public Statistic {
    public:

        OptVersionBlockStatistics() : Statistic() {}

    };

/* >>>>>>>>>>>>>>>>>>> Read-Write Transactional Code <<<<<<<<<<<<<<<<<<<<<<< */
    class TxConsistencyMetadata {
    public:
#ifdef H_CURE
        PhysicalTimeSpec LDT; /* local dependency time */
        PhysicalTimeSpec RST; /* remote stable time */
        PhysicalTimeSpec CT; /* latest commit time */

#elif defined(WREN)
        PhysicalTimeSpec GLST; /* global local stable time */
        PhysicalTimeSpec GRST; /* global remote stable time */
        PhysicalTimeSpec CT; /* latest commit time */

#elif defined(CURE)
        int SrcReplica; /*Source replica id */
        int MaxElId;
        std::vector<PhysicalTimeSpec> DV; /* global stable client dependency vector clock */
        std::vector<PhysicalTimeSpec> CT; /* commit time vector */

#endif

        uint64_t txId; /* transaction id */ //std::atomic<int>
    };

    class Item;
    class TxClientMetadata : public TxConsistencyMetadata {
    public:
        std::unordered_map<std::string, std::string> ReadSet;  /* <key, value> */
        std::unordered_map<std::string, std::string> WriteSet; /* <key, value> */
#ifdef WREN
        std::unordered_map<std::string, std::pair<std::string, PhysicalTimeSpec>> WriteCache; /* <key, value> */
//        std::unordered_map<std::string, Item*> WriteCache; /* <key, item<value, updateTime, srcReplica>> */
#endif
    };


    class ServerMetadata {
    public:
#ifdef H_CURE
        PhysicalTimeSpec RST; /* remote stable time */
        std::mutex rstMutex;
#elif defined(WREN)
        std::mutex gstMutex;
        PhysicalTimeSpec GRST; /* global remote stable time */
        std::mutex grstMutex;
        PhysicalTimeSpec GLST; /* global local stable time */
        std::mutex glstMutex;
#elif defined(CURE)
        std::vector<PhysicalTimeSpec> DV;
        std::mutex dvMutex;
//        std::vector<std::mutex *> dvMutex;

#endif

    };


    class TxContex {
    public:
#ifdef H_CURE
        PhysicalTimeSpec LDT; /* local dependency time */
        PhysicalTimeSpec RST; /* remote stable time */
#elif defined(WREN)
        PhysicalTimeSpec GLST; /* local dependency time */
        PhysicalTimeSpec GRST; /* remote stable time */
        PhysicalTimeSpec HT; /* clock update time for the prepare request */
#elif defined(CURE)
        std::vector<PhysicalTimeSpec> DV;
#endif

        PhysicalTimeSpec startTimePreparedSet;
        PhysicalTimeSpec endTimePreparedSet;
        PhysicalTimeSpec startTimeCommitedSet;
        PhysicalTimeSpec endTimeCommitedSet;

        double waited_xact;
        double timeWaitedOnPrepareXact;

#ifdef H_CURE

        TxContex() : LDT(0, 0), RST(0, 0) {
            waited_xact = 0.0;
            timeWaitedOnPrepareXact = 0.0;
        }

        TxContex(PhysicalTimeSpec ldt, PhysicalTimeSpec rst) {

            this->LDT = ldt;
            this->RST = rst;
            waited_xact = 0.0;
            timeWaitedOnPrepareXact = 0.0;
        }

#elif defined(WREN)

        TxContex() : GLST(0, 0), GRST(0, 0), HT(0, 0) {
            waited_xact = 0.0;
            timeWaitedOnPrepareXact = 0.0;
        }

        TxContex(PhysicalTimeSpec glst, PhysicalTimeSpec grst) {

            this->GLST = glst;
            this->GRST = grst;
            waited_xact = 0.0;
            timeWaitedOnPrepareXact = 0.0;
        }

#elif defined(CURE)

        TxContex() { waited_xact = 0.0; }

        TxContex(int size) {
            PhysicalTimeSpec t(0, 0);
            this->DV.resize(size, t);
            waited_xact = 0.0;
            timeWaitedOnPrepareXact=0.0;
        }

        TxContex(std::vector<PhysicalTimeSpec> gss) {
            assert(this->DV.size() == 0);
            for (int i = 0; i < gss.size(); i++) {
                this->DV.push_back(gss[i]);
            }

            assert(this->DV.size() ==  gss.size());

            waited_xact = 0.0;
            timeWaitedOnPrepareXact=0.0;
        }
#endif


        TxContex(const TxContex &ctx) {
#ifdef H_CURE
            this->LDT = ctx.LDT;
            this->RST = ctx.RST;

#elif defined(WREN)
            this->GLST = ctx.GLST;
            this->GRST = ctx.GRST;
            this->HT = ctx.HT;

#elif defined(CURE)

            this->DV.clear();
            assert(this->DV.size() == 0);
            for (int i = 0; i < ctx.DV.size(); i++) {
                this->DV.push_back(ctx.DV[i]);
            }
            assert(this->DV.size() == ctx.DV.size());
#endif

            waited_xact = ctx.waited_xact;
            timeWaitedOnPrepareXact = ctx.timeWaitedOnPrepareXact;

            startTimePreparedSet = ctx.startTimePreparedSet;
            endTimePreparedSet = ctx.endTimePreparedSet;
            startTimeCommitedSet = ctx.startTimeCommitedSet;
            endTimeCommitedSet = ctx.endTimeCommitedSet;
        }


        TxContex &operator=(const TxContex &ctx) {
#ifdef H_CURE
            this->LDT = ctx.LDT;
            this->RST = ctx.RST;

#elif defined(WREN)
            this->GLST = ctx.GLST;
            this->GRST = ctx.GRST;
            this->HT = ctx.HT;

#elif defined(CURE)
            this->DV.clear();
            assert(this->DV.size() == 0);
            for (int i = 0; i < ctx.DV.size(); i++) {
                this->DV.push_back(ctx.DV[i]);
            }
            assert(this->DV.size() == ctx.DV.size());

#endif

            waited_xact = ctx.waited_xact;
            timeWaitedOnPrepareXact = ctx.timeWaitedOnPrepareXact;

            startTimePreparedSet = ctx.startTimePreparedSet;
            endTimePreparedSet = ctx.endTimePreparedSet;
            startTimeCommitedSet = ctx.startTimeCommitedSet;
            endTimeCommitedSet = ctx.endTimeCommitedSet;

        }


    };

    class Item {
        std::string key;
        std::string value;
        PhysicalTimeSpec updateTime;
        int srcReplica;
    public:

        Item() {
            key = "";
            value = "";
            updateTime.setToZero();
            srcReplica = 0;
        }

        Item(const std::string &key, const std::string &value, const PhysicalTimeSpec &ut, int sr) : key(key),
                                                                                                     value(value),
                                                                                                     srcReplica(sr) {
            this->updateTime = ut;
        }

        Item(const std::string &key) : key(key) {
            this->value = "";
            this->updateTime.setToZero();
            this->srcReplica = 0;
        }


        Item(const Item &rhs) {
            key = rhs.key;
            value = rhs.value;
            updateTime = rhs.updateTime;
            srcReplica = rhs.srcReplica;
        }

        Item &operator=(const Item &rhs) {
            key = rhs.key;
            value = rhs.value;
            updateTime = rhs.updateTime;
            srcReplica = rhs.srcReplica;
        }

        bool operator==(const Item &rhs) const {
            return key == rhs.key &&
                   value == rhs.value &&
                   updateTime == rhs.updateTime &&
                   srcReplica == rhs.srcReplica;
        }


        bool operator!=(const Item &rhs) const {
            return !(rhs == *this);
        }

        const std::string &getKey() const {
            return key;
        }

        void setKey(const  std::string &key) {
            Item::key = key;
        }

        const  std::string &getValue() const {
            return value;
        }

        void setValue(const  std::string &value) {
            Item::value = value;
        }

        const PhysicalTimeSpec &getUpdateTime() const {
            return updateTime;
        }

        void setUpdateTime(const PhysicalTimeSpec &updateTime) {
            Item::updateTime = updateTime;
        }

        int getSrcReplica() const {
            return srcReplica;
        }

        void setSrcReplica(int srcReplica) {
            Item::srcReplica = srcReplica;
        }

    };

} // namespace scc

#endif
