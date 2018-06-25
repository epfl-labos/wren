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

#ifndef SCC_COMMON_SYS_STATS_H
#define SCC_COMMON_SYS_STATS_H

#include <atomic>
#include <vector>
#include <mutex>
#include "types.h"

namespace scc {

    class SysStats {
    public:
        static std::atomic<int> NumPublicTxStartRequests;
        static std::atomic<int> NumPublicTxReadRequests;
        static std::atomic<int> NumPublicTxWriteRequests;
        static std::atomic<int> NumPublicTxCommitRequests;

        static std::atomic<int> NumInternalTxSliceReadRequests;
        static std::atomic<int> NumInternalCommitRequests;
        static std::atomic<int> NumInternalPrepareRequests;

        static std::atomic<int> NumTxReadBlocks;
        static std::atomic<double> TxReadBlockDuration;
        static std::atomic<int> NumTxReadBlocksAtCoordinator;
        static std::atomic<double> TxReadBlockDurationAtCoordinator;
        static std::atomic<int> NumTxReadPhases;
        static std::atomic<int> NumTransactions; // increased at a tx start phase
        static std::atomic<int> NumCommitedTxs;

        static std::atomic<int> NumDelayedLocalUpdates;
        static std::atomic<long> SumDelayedLocalUpdates;

        static std::atomic<int> NumPendingPropagatedUpdates;
        static std::atomic<int> NumRecvUpdateReplicationMsgs;
        static std::atomic<int> NumRecvUpdateReplications;
        static std::atomic<int> NumReceivedPropagatedUpdates;
        static std::atomic<int> NumSentUpdates;
        static std::atomic<int> NumSentBatches;
        static std::atomic<int> NumReceivedBatches;
        static std::atomic<int> NumRecvHeartbeats;
        static std::atomic<int> NumUpdatesStoredInLocalUpdateQueue;
        static std::atomic<int> NumUpdatesStoredInToPropagateLocalUpdateQueue;

        static std::atomic<int> NumGSTRounds;
        static std::atomic<int> NumRecvGSTs;
        static std::atomic<int> NumSentGSTs;
        static std::atomic<int> NumRecvLSTs;
        static std::atomic<int> NumSentLSTs;
        static std::atomic<int> NumRecvGSTBytes;
        static std::atomic<int> NumSentGSTBytes;
        static std::atomic<int> NumSendInternalLSTBytes;
        static std::atomic<int> NumRecvInternalLSTBytes;

        static std::atomic<int> NumSentRSTs;
        static std::atomic<int> NumRecvRSTs;
        static std::atomic<int> NumSentRSTBytes;
        static std::atomic<int> NumRecvRSTBytes;

        static std::atomic<int> NumSentReplicationBytes;
        static std::atomic<int> NumRecvReplicationBytes;

        static std::atomic<int> NumReturnedItemVersions;
        static std::atomic<int> NumReturnedStaleItemVersions;
        static std::mutex NumFresherVersionsInItemChainMutex;
        static std::vector<int64_t> NumFresherVersionsInItemChain;
        static std::mutex UserPerceivedStalenessTimeMutex;
        static std::vector<double> UserPerceivedStalenessTime;
        static std::atomic<double> MinUserPercievedStalenessTime;
        static std::atomic<double> MaxUserPercievedStalenessTime;

        /* Transaction profiling statistics */
        static Statistic timeInPreparedSet;
        static Statistic timeInCommitedSet;

        static std::atomic<int> NumTxPreparePhaseBlocks;
        static std::atomic<double> TxPreparePhaseBlockDuration;
        static std::atomic<int> NumTxPreparePhaseBlocksAtCoordinator;
        static std::atomic<double> TxPreparePhaseBlockDurationAtCoordinator;

        static std::vector<double> PreparePhaseBlockingTimes;
        static std::mutex PreparePhaseBlockingTimesMutex;

        /* Just for debuging purposes */
        static std::atomic<int> PrepareCondBlocks;
        static std::atomic<int> PrepareCondNoBlocks;
        static std::atomic<double> PrepNoBlockDifference;
        static std::atomic<double> PrepBlockDifference;
        static std::atomic<int> NumDepTimeisZero;
        static std::atomic<int> NumGSSisZero;
        static std::atomic<int> NumTimesWaitOnPrepareIsEntered;

        static std::atomic<double> PrepareActionDuration;
        static std::atomic<int> NumTimesPrepareActionIsExecuted;

        static std::atomic<double> CalcUBTActionDuration;
        static std::atomic<int> NumTimesCalcUBTIsExecuted;

        static std::atomic<double> NegDiff_HybToPhys_TxStart;
        static std::atomic<double> NegDiff_HybToPhys_TxRead;
        static std::atomic<double> NegDiff_HybToVV_TxRead;
        static std::atomic<double> NegDiff_HybToVV_TxRead_Block;

        static std::atomic<double> PosDiff_HybToPhys_TxStart;
        static std::atomic<double> PosDiff_HybToPhys_TxRead;
        static std::atomic<double> PosDiff_HybToVV_TxRead;
        static std::atomic<double> PosDiff_HybToVV_TxRead_Block;

        static std::atomic<int> CountNegDiff_HybToPhys_TxStart;
        static std::atomic<int> CountNegDiff_HybToPhys_TxRead;
        static std::atomic<int> CountNegDiff_HybToVV_TxRead;
        static std::atomic<int> CountNegDiff_HybToVV_TxRead_Block;

        static std::atomic<int> CountPosDiff_HybToPhys_TxStart;
        static std::atomic<int> CountPosDiff_HybToPhys_TxRead;
        static std::atomic<int> CountPosDiff_HybToVV_TxRead;
        static std::atomic<int> CountPosDiff_HybToVV_TxRead_Block;


        /* Cure protocol related statistics */
#ifdef CURE
        static std::vector<double> CommitPhaseBlockingTimes;
        static std::mutex CommitPhaseBlockingTimesMutex;
#endif

        static std::atomic<int> NumReadLocalItems;
        static std::atomic<int> NumReadRemoteItems;
        static std::atomic<int> NumReadLocalStaleItems;
        static std::atomic<int> NumReadRemoteStaleItems;

        static std::atomic<int> NumReadItemsFromKVStore;

        static std::atomic<double> ChainLatencySum;
        static std::atomic<int> NumChainLatencyMeasurements;

        static std::atomic<double> ServerReadLatencySum;
        static std::atomic<int> NumServerReadLatencyMeasurements;

        static std::atomic<double> CohordHandleInternalSliceReadLatencySum;
        static std::atomic<int> NumCohordHandleInternalSliceReadLatencyMeasurements;

        static std::atomic<double> CohordSendingReadResultsLatencySum;
        static std::atomic<int> NumCohordSendingReadResultsLatencyMeasurements;

        static std::atomic<double> CoordinatorMapingKeysToShardsLatencySum;
        static std::atomic<int> NumCoordinatorMapingKeysToShardsLatencyMeasurements;

        static std::atomic<double> CoordinatorSendingReadReqToShardsLatencySum;
        static std::atomic<int> NumCoordinatorSendingReadReqToShardsLatencyMeasurements;

        static std::atomic<double> CoordinatorReadingLocalKeysLatencySum;
        static std::atomic<int> NumCoordinatorReadingLocalKeysLatencyMeasurements;

        static std::atomic<double> CoordinatorProcessingOtherShardsReadRepliesLatencySum;
        static std::atomic<int> NumCoordinatorProcessingOtherShardsReadRepliesLatencyMeasurements;

        static std::atomic<double> CoordinatorLocalKeyReadsLatencySum;
        static std::atomic<int> NumCoordinatorLocalKeyReadsLatencyMeasurements;

        static std::atomic<double> CoordinatorWaitingForShardsRepliesLatencySum;
        static std::atomic<int> NumCoordinatorWaitingForShardsRepliesMeasurements;

        static std::atomic<double> CoordinatorReadReplyHandlingLatencySum;
        static std::atomic<int> NumCoordinatorReadReplyHandlingMeasurements;

        static std::atomic<double> CoordinatorCommitLatencySum;
        static std::atomic<int> NumCoordinatorCommitMeasurements;

        static std::atomic<double> CoordinatorCommitMappingKeysToShardsLatencySum;
        static std::atomic<int> NumCoordinatorCommitMappingKeysToShardsLatencyMeasurements;

        static std::atomic<double> CoordinatorCommitSendingPrepareReqToShardsLatencySum;
        static std::atomic<int> NumCoordinatorCommitSendingPrepareReqToShardsLatencyMeasurements;

        static std::atomic<double> CoordinatorCommitLocalPrepareLatencySum;
        static  std::atomic<int> NumCoordinatorCommitLocalPrepareLatencyMeasurements;

        static std::atomic<double> CoordinatorCommitWaitingOtherShardsPrepareRepliesLatencySum;
        static  std::atomic<int> NumCoordinatorCommitWaitingOtherShardsPrepareRepliesLatencyMeasurements;

        static std::atomic<double> CoordinatorCommitProcessingPrepareRepliesLatencySum;
        static std::atomic<int> NumCoordinatorCommitProcessingPrepareRepliesLatencyMeasurements;

        static std::atomic<double> CoordinatorCommitSendingCommitReqLatencySum;
        static std::atomic<int> NumCoordinatorCommitSendingCommitReqLatencyMeasurements;

    };

}

#endif
