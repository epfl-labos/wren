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

#ifndef GENTLERAIN_TRANSACTION_H
#define GENTLERAIN_TRANSACTION_H

#include "common/types.h"
#include "common/sys_config.h"
#include "common/utils.h"
#include <string>
#include <vector>
#include <map>

#define SUCCESFUL_READ(tx, index) (tx._slices[index]->ret)

namespace scc {

    class PrepareRequest;

    class TxReadSlice;

    class Transaction {
    private:

    public:
        unsigned long txId;
        TxContex *txContex;
        std::mutex mutex;
        std::unordered_map<int, TxReadSlice *> partToReadSliceMap;
        std::unordered_map<int, PrepareRequest *> partToPrepReqMap;
        WaitHandle *prepareReqWaitHandle;
        WaitHandle *readSlicesWaitHandle;

        Transaction(unsigned int tid);

        Transaction(unsigned int tid, TxContex *txc);

        Transaction(const Transaction &t) {
            this->txId = t.txId;
            this->txContex = new TxContex(*t.txContex);
            this->partToReadSliceMap = t.partToReadSliceMap;
            this->partToPrepReqMap = t.partToPrepReqMap;
        }

        Transaction &operator=(const Transaction &t) {
            this->txId = t.txId;
            this->txContex = new TxContex(*t.txContex);
            this->partToReadSliceMap = t.partToReadSliceMap;
            this->partToPrepReqMap = t.partToPrepReqMap;
        }

        ~Transaction(){
            delete txContex;
            for (auto it : partToReadSliceMap) {
                delete it.second;
            }
            partToReadSliceMap.clear();

            for (auto it : partToPrepReqMap) {
                delete it.second;
            }
            partToPrepReqMap.clear();

        }

        void setTxContex(TxContex *txc);

        TxContex *getTxContex();

        unsigned long getTxId() const;

        void setTxId(unsigned long txid);

    };

    class PrepareRequest {

    public:
        unsigned int id;
        TxContex metadata;
        PhysicalTimeSpec prepareTime;
#if defined(H_CURE) || defined(WREN)
        PhysicalTimeSpec commitTime;

#elif defined(CURE)
        std::vector<PhysicalTimeSpec> commitTime;
        int replicaId;

#endif
        std::vector<std::string> keys;
        std::vector<std::string> values;
        std::vector<int> partitionIds;
        bool result;

        double prepareBlockDuration;

        PrepareRequest() { this->prepareBlockDuration = 0; };

        PrepareRequest(unsigned int prid);

        PrepareRequest(int txId, TxContex &cdata, const std::vector<std::string> &wkeys,
                       std::vector<std::string> &wvals) : id(txId), keys(wkeys), values(wvals) {
            this->metadata = cdata;
            this->prepareBlockDuration = 0;

        }

#if defined(H_CURE) || defined(WREN)

        PrepareRequest(PhysicalTimeSpec t) {
            this->commitTime = t;
            this->prepareBlockDuration = 0;
        }

        PrepareRequest(int txId, TxContex &cdata) {
            this->id = txId;
            this->metadata = cdata;
            this->prepareBlockDuration = 0;

        }

#elif defined(CURE)

        PrepareRequest(int numReplicas) {
            PhysicalTimeSpec init(0, 0);
            this->commitTime.resize(numReplicas, init);
            this->prepareBlockDuration = 0;
        }


        PrepareRequest(PhysicalTimeSpec t, int rid, int numReplicas) {
            this->replicaId = rid;
            PhysicalTimeSpec init(0, 0);
            this->commitTime.resize(numReplicas, init);
            this->commitTime[rid] = t;
            this->prepareBlockDuration = 0;
        }

        PrepareRequest(int txId, TxContex &cdata, int numReplicas) {
            this->id = txId;
            this->metadata = cdata;
            PhysicalTimeSpec init(0, 0);
            this->commitTime.resize(numReplicas, init);
            this->prepareBlockDuration = 0;

            assert(metadata.DV.size() == cdata.DV.size());
            assert(metadata.DV.size() == numReplicas);

        }

#endif

        PrepareRequest(const PrepareRequest &req) {
            this->id = req.id;
            this->metadata = req.metadata;
            this->commitTime = req.commitTime;
            this->keys = req.keys;
            this->values = req.values;
            this->prepareTime = req.prepareTime;
            this->prepareBlockDuration = 0;

#ifdef CURE
            this->replicaId = req.replicaId;
#endif

        }

        PrepareRequest &operator=(const PrepareRequest &req) {
            this->id = req.id;
            this->metadata = req.metadata;
#ifdef CURE
            assert(this->metadata.DV.size() == req.metadata.DV.size());
#endif
            this->commitTime = req.commitTime;
            this->prepareTime = req.prepareTime;
            this->keys = req.keys;
            this->values = req.values;
#ifdef CURE
            this->replicaId = req.replicaId;
#endif
            return *this;
        }

        bool operator<(const PrepareRequest &rhs) const {

#if defined(H_CURE) || defined(WREN)
            return (this->commitTime.Seconds < rhs.commitTime.Seconds) ||
                   ((this->commitTime.Seconds == rhs.commitTime.Seconds) &&
                    (this->commitTime.NanoSeconds < rhs.commitTime.NanoSeconds));
#elif defined(CURE)
            return (this->commitTime[this->replicaId].Seconds < rhs.commitTime[rhs.replicaId].Seconds) ||
                               ((this->commitTime[this->replicaId].Seconds == rhs.commitTime[rhs.replicaId].Seconds) &&
                                (this->commitTime[this->replicaId].NanoSeconds < rhs.commitTime[rhs.replicaId].NanoSeconds));
#endif
        };


        friend std::ostream &operator<<(std::ostream &out, const PrepareRequest &obj);
    };

    bool operator==(const PrepareRequest &lhs, const PrepareRequest &rhs) {
#if defined(H_CURE) || defined(WREN)
        return (lhs.id == rhs.id) && (lhs.commitTime == rhs.commitTime) && (lhs.prepareTime == rhs.prepareTime);
#elif defined(CURE)
        return (lhs.id == rhs.id) && (lhs.commitTime[lhs.replicaId] == rhs.commitTime[rhs.replicaId]) && (lhs.prepareTime == rhs.prepareTime);
#endif
    }

    std::ostream &operator<<(std::ostream &out, const PrepareRequest &obj) {
#if defined(H_CURE) || defined(WREN)
        out << "PrepareRequest{ txId: " << obj.id << ", prepareTime: " << Utils::physicaltime2str(obj.prepareTime)
            << ", commitTime: " << Utils::physicaltime2str(obj.commitTime) << "}\n";
#elif defined(CURE)
        out << "PrepareRequest{ txId: " << obj.id << ", prepareTime: " << Utils::physicaltime2str(obj.prepareTime)
            << ", commitTime: " << Utils::physicaltime2str(obj.commitTime[obj.replicaId]) << "}\n";
#endif
        return out;
    }

    class PrepareRequestElement {
    public:
        unsigned int txId;
        PhysicalTimeSpec prepareTime;

        PrepareRequestElement(int id, PhysicalTimeSpec pt) : txId(id), prepareTime(pt) {}
    };

    struct preparedRequestsElementComparator {
        bool operator()(const PrepareRequestElement *lhs, const PrepareRequestElement *rhs) const {

            return (lhs->prepareTime < rhs->prepareTime);
        }
    };

#if defined(H_CURE) || defined(WREN)

    struct commitRequestsComparator {
        bool operator()(const PrepareRequest *lhs, const PrepareRequest *rhs) const {

            return (lhs->commitTime < rhs->commitTime) || ((lhs->commitTime == rhs->commitTime) &&
                                                           (lhs->id < rhs->id));
        }
    };

#elif defined(CURE)

    struct commitRequestsComparator {
        bool operator()(const PrepareRequest *lhs, const PrepareRequest *rhs) const {
            assert(lhs->replicaId == rhs->replicaId);
        return (lhs->commitTime[lhs->replicaId] < rhs->commitTime[rhs->replicaId]) || ((lhs->commitTime[lhs->replicaId] == rhs->commitTime[rhs->replicaId]) &&
                                                           (lhs->id < rhs->id));
        }
    };

#endif

    class TxReadSlice {

    public:
        std::vector<std::string> keys;
        std::vector<std::string> values;
        std::vector<int> positionIds;
        bool sucesses;
        double txWaitOnReadTime;
#ifdef WREN
#endif

        TxReadSlice() {};

    };

}


#endif //GENTLERAIN_TRANSACTION_H
