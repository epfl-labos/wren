#include "common/sys_stats.h"

namespace scc {

    std::atomic<int> SysStats::NumPublicTxStartRequests(0);
    std::atomic<int> SysStats::NumPublicTxReadRequests(0);
    std::atomic<int> SysStats::NumPublicTxWriteRequests(0);
    std::atomic<int> SysStats::NumPublicTxCommitRequests(0);

    std::atomic<int> SysStats::NumInternalTxSliceReadRequests(0);
    std::atomic<int> SysStats::NumInternalCommitRequests(0);
    std::atomic<int> SysStats::NumInternalPrepareRequests(0);

    std::atomic<int> SysStats::NumTxReadBlocks(0);
    std::atomic<double> SysStats::TxReadBlockDuration(0.0);
    std::atomic<int> SysStats::NumTxReadBlocksAtCoordinator(0);
    std::atomic<double> SysStats::TxReadBlockDurationAtCoordinator(0.0);
    std::atomic<int> SysStats::NumTxReadPhases(0);
    std::atomic<int> SysStats::NumTransactions(0);
    std::atomic<int> SysStats::NumCommitedTxs(0);

    std::atomic<int> SysStats::NumDelayedLocalUpdates(0);
    std::atomic<long> SysStats::SumDelayedLocalUpdates(0);

    std::atomic<int> SysStats::NumPendingPropagatedUpdates(0);
    std::atomic<int> SysStats::NumRecvUpdateReplicationMsgs(0);
    std::atomic<int> SysStats::NumRecvUpdateReplications(0);
    std::atomic<int> SysStats::NumReceivedPropagatedUpdates(0);
    std::atomic<int> SysStats::NumSentUpdates(0);
    std::atomic<int> SysStats::NumSentBatches(0);
    std::atomic<int> SysStats::NumReceivedBatches(0);
    std::atomic<int> SysStats::NumRecvHeartbeats(0);
    std::atomic<int> SysStats::NumUpdatesStoredInLocalUpdateQueue(0);
    std::atomic<int> SysStats::NumUpdatesStoredInToPropagateLocalUpdateQueue(0);

    std::atomic<int> SysStats::NumGSTRounds(0);
    std::atomic<int> SysStats::NumRecvGSTs(0);
    std::atomic<int> SysStats::NumSentGSTs(0);

    std::atomic<int> SysStats::NumRecvLSTs(0);
    std::atomic<int> SysStats::NumSentLSTs(0);
    std::atomic<int> SysStats::NumRecvGSTBytes(0);
    std::atomic<int> SysStats::NumSentGSTBytes(0);
    std::atomic<int> SysStats::NumSendInternalLSTBytes(0);
    std::atomic<int> SysStats::NumRecvInternalLSTBytes(0);

    std::atomic<int> SysStats::NumSentRSTs(0);
    std::atomic<int> SysStats::NumRecvRSTs(0);
    std::atomic<int> SysStats::NumSentRSTBytes(0);
    std::atomic<int> SysStats::NumRecvRSTBytes(0);

    std::atomic<int> SysStats::NumSentReplicationBytes(0);
    std::atomic<int> SysStats::NumRecvReplicationBytes(0);

    std::atomic<int> SysStats::NumReturnedItemVersions(0);
    std::atomic<int> SysStats::NumReturnedStaleItemVersions(0);
    std::mutex SysStats::NumFresherVersionsInItemChainMutex;
    std::vector<int64_t> SysStats::NumFresherVersionsInItemChain(0);
    std::mutex SysStats::UserPerceivedStalenessTimeMutex;
    std::vector<double> SysStats::UserPerceivedStalenessTime(0.0);
    std::atomic<double> SysStats::MinUserPercievedStalenessTime(1000000.0);
    std::atomic<double> SysStats::MaxUserPercievedStalenessTime(0.0);

    Statistic SysStats::timeInPreparedSet;
    Statistic SysStats::timeInCommitedSet;

    std::atomic<int> SysStats::NumTxPreparePhaseBlocks(0);
    std::atomic<double> SysStats::TxPreparePhaseBlockDuration(0.0);
    std::atomic<int> SysStats::NumTxPreparePhaseBlocksAtCoordinator(0);
    std::atomic<double> SysStats::TxPreparePhaseBlockDurationAtCoordinator(0.0);

    /* Just for debuging purposes */
    std::atomic<int> SysStats::PrepareCondBlocks(0);
    std::atomic<int> SysStats::PrepareCondNoBlocks(0);
    std::atomic<double> SysStats::PrepNoBlockDifference(0.0);
    std::atomic<double> SysStats::PrepBlockDifference(0.0);
    std::atomic<int> SysStats::NumDepTimeisZero(0);
    std::atomic<int> SysStats::NumGSSisZero(0);
    std::atomic<int> SysStats::NumTimesWaitOnPrepareIsEntered(0);

    std::atomic<double> SysStats::PrepareActionDuration(0.0);
    std::atomic<int> SysStats::NumTimesPrepareActionIsExecuted(0.0);

    std::atomic<double> SysStats::CalcUBTActionDuration(0.0);
    std::atomic<int> SysStats::NumTimesCalcUBTIsExecuted(0.0);

    std::atomic<double> SysStats::NegDiff_HybToPhys_TxStart(0.0);
    std::atomic<double> SysStats::NegDiff_HybToPhys_TxRead(0.0);
    std::atomic<double> SysStats::NegDiff_HybToVV_TxRead(0.0);
    std::atomic<double> SysStats::NegDiff_HybToVV_TxRead_Block(0.0);

    std::atomic<double> SysStats::PosDiff_HybToPhys_TxStart(0.0);
    std::atomic<double> SysStats::PosDiff_HybToPhys_TxRead(0.0);
    std::atomic<double> SysStats::PosDiff_HybToVV_TxRead(0.0);
    std::atomic<double> SysStats::PosDiff_HybToVV_TxRead_Block(0.0);

    std::atomic<int> SysStats::CountNegDiff_HybToPhys_TxStart(0);
    std::atomic<int> SysStats::CountNegDiff_HybToPhys_TxRead(0);
    std::atomic<int> SysStats::CountNegDiff_HybToVV_TxRead(0);
    std::atomic<int> SysStats::CountNegDiff_HybToVV_TxRead_Block(0);

    std::atomic<int> SysStats::CountPosDiff_HybToPhys_TxStart(0);
    std::atomic<int> SysStats::CountPosDiff_HybToPhys_TxRead(0);
    std::atomic<int> SysStats::CountPosDiff_HybToVV_TxRead(0);
    std::atomic<int> SysStats::CountPosDiff_HybToVV_TxRead_Block(0);

#ifdef CURE
    std::vector<double> SysStats::CommitPhaseBlockingTimes;
    std::mutex SysStats::CommitPhaseBlockingTimesMutex;
#endif

    std::atomic<int> SysStats::NumReadLocalItems(0);
    std::atomic<int> SysStats::NumReadRemoteItems(0);
    std::atomic<int> SysStats::NumReadLocalStaleItems(0);
    std::atomic<int> SysStats::NumReadRemoteStaleItems(0);
    std::atomic<int> SysStats::NumReadItemsFromKVStore(0);

    std::atomic<double> SysStats::ChainLatencySum(0.0);
    std::atomic<int> SysStats::NumChainLatencyMeasurements(0);
    std::atomic<double> SysStats::ServerReadLatencySum(0.0);
    std::atomic<int> SysStats::NumServerReadLatencyMeasurements(0);

     std::atomic<double> SysStats::CohordHandleInternalSliceReadLatencySum(0.0);
     std::atomic<int> SysStats::NumCohordHandleInternalSliceReadLatencyMeasurements(0);

     std::atomic<double> SysStats::CohordSendingReadResultsLatencySum(0.0);
     std::atomic<int> SysStats::NumCohordSendingReadResultsLatencyMeasurements(0);

     std::atomic<double> SysStats::CoordinatorMapingKeysToShardsLatencySum(0.0);
     std::atomic<int> SysStats::NumCoordinatorMapingKeysToShardsLatencyMeasurements(0);

     std::atomic<double> SysStats::CoordinatorSendingReadReqToShardsLatencySum(0.0);
     std::atomic<int> SysStats::NumCoordinatorSendingReadReqToShardsLatencyMeasurements(0);

     std::atomic<double> SysStats::CoordinatorReadingLocalKeysLatencySum(0.0);
     std::atomic<int> SysStats::NumCoordinatorReadingLocalKeysLatencyMeasurements(0);

     std::atomic<double> SysStats::CoordinatorProcessingOtherShardsReadRepliesLatencySum(0.0);
     std::atomic<int> SysStats::NumCoordinatorProcessingOtherShardsReadRepliesLatencyMeasurements(0);

    std::atomic<double> SysStats::CoordinatorLocalKeyReadsLatencySum(0.0);
    std::atomic<int> SysStats::NumCoordinatorLocalKeyReadsLatencyMeasurements(0);

    std::atomic<double> SysStats::CoordinatorWaitingForShardsRepliesLatencySum(0.0);
    std::atomic<int> SysStats::NumCoordinatorWaitingForShardsRepliesMeasurements(0);

    std::atomic<double> SysStats::CoordinatorReadReplyHandlingLatencySum(0.0);
    std::atomic<int> SysStats::NumCoordinatorReadReplyHandlingMeasurements(0);

    std::atomic<double> SysStats::CoordinatorCommitLatencySum(0.0);
    std::atomic<int> SysStats::NumCoordinatorCommitMeasurements(0);

    std::atomic<double> SysStats::CoordinatorCommitMappingKeysToShardsLatencySum(0.0);
    std::atomic<int> SysStats::NumCoordinatorCommitMappingKeysToShardsLatencyMeasurements(0);

    std::atomic<double> SysStats::CoordinatorCommitSendingPrepareReqToShardsLatencySum(0.0);
    std::atomic<int> SysStats::NumCoordinatorCommitSendingPrepareReqToShardsLatencyMeasurements(0);

    std::atomic<double> SysStats::CoordinatorCommitLocalPrepareLatencySum(0.0);
    std::atomic<int> SysStats::NumCoordinatorCommitLocalPrepareLatencyMeasurements(0);

    std::atomic<double> SysStats::CoordinatorCommitWaitingOtherShardsPrepareRepliesLatencySum(0.0);
    std::atomic<int> SysStats::NumCoordinatorCommitWaitingOtherShardsPrepareRepliesLatencyMeasurements(0);

    std::atomic<double> SysStats::CoordinatorCommitProcessingPrepareRepliesLatencySum(0.0);
    std::atomic<int> SysStats::NumCoordinatorCommitProcessingPrepareRepliesLatencyMeasurements(0);

    std::atomic<double> SysStats::CoordinatorCommitSendingCommitReqLatencySum(0.0);
    std::atomic<int> SysStats::NumCoordinatorCommitSendingCommitReqLatencyMeasurements(0);



} // namespace scc
