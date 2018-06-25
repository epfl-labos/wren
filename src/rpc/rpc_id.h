#ifndef SCC_RPC_RPC_ID_H
#define SCC_RPC_RPC_ID_H

namespace scc {

    enum class RPCMethod {
        // key-value server
                EchoTest = 1,
        GetServerConfig,
        Add,
        Get,
        Set,
        TxGet,
        ShowItem, // show the version chain of one key
        ShowDB, // show the version chains of all items
        ShowState,
        ShowStateCSV,
        DumpLatencyMeasurement,

        // partiton
                InternalGet,
        InternalSet,
        InternalTxSliceGet,
        InternalShowItem,
        InitializePartitioning,
        InternalDependencyCheck,

        // replication,
                InitializeReplication,
        ReplicateUpdate,
        SendHeartbeat,
#ifdef USV
        PushGSV,
#endif

        // GST
                SendLST,
        SendGST,
        SendGSTRequest,
#ifdef DEP_VECTORS
        SendPVV,
      SendGSV,
      SendGSVRequest,
#endif

        InternalTxSliceGetResult,
        ParallelInternalTxSliceGetResult,
        InternalTxSliceReadKeys,
        InternalTxSliceReadKeysResult,
        InternalPrepareRequest,
        InternalPrepareReply,
        InternalCommitRequest,

        // manager
                RegisterPartition,
        GetRegisteredPartitions,
        NotifyReadiness,

// read-write transactions
                TxStart,
        TxCommit,
        TxWrite,
        TxRead

//stabilization protocol
#if defined(H_CURE)
        ,SendRST
#endif
#if  defined(WREN)
        ,SendStabilizationTimes
#endif
#ifdef CURE
        ,SendPVV
        ,SendGSV
#endif


    };

    static const char *RPCMethodS[] = {
            "EchoTest",
            "GetServerConfig",
            "Add",
            "Get",
            "Set",
            "TxGet",
            "ShowItem",
            "ShowDB",
            "ShowState",
            "ShowStateCSV",
            "DumpLatencyMeasurement",

            "InternalGet",
            "InternalSet",
            "InternalTxSliceGet",
            "InternalShowItem",
            "InitializePartitioning",
            "InternalDependencyCheck",

            "InitializeReplication",
            "ReplicateUpdate",
            "SendHeartbeat",
#ifdef USV
            "PushGSV",
#endif
            "SendLST",
            "SendGST",
            "SendGSTRequest",
#ifdef DEP_VECTORS
    "SendPVV",
    "SendGSV",
    "SendGSVRequest",
#endif

            "InternalTxSliceGetResult",
            "ParallelInternalTxSliceGetResult",
            "InternalTxSliceReadKeys",
            "InternalTxSliceReadKeysResult",
            "InternalPrepareRequest",
            "InternalPrepareReply",
            "InternalCommitRequest",


            "RegisterPartition",
            "GetRegisteredPartitions",
            "NotifyReadiness",

            "TxStart",
            "TxCommit",
            "TxWrite",
            "TxRead"
#if defined(H_CURE)
            ,"SendRST"
#endif
#if  defined(WREN)
            ,"SendStabilizationTimes"
#endif
#ifdef CURE
            ,"SendPVV"
            ,"SendGSV"
#endif

    };

    const char *getTextForEnum(RPCMethod rid) {
        return RPCMethodS[((int) rid) - 1];
    }

}

#endif