#ifndef SCC_KVSERVICE_PARTITION_KV_TX_CLIENT_H
#define SCC_KVSERVICE_PARTITION_KV_TX_CLIENT_H

#include "common/types.h"
#include "rpc/async_rpc_client.h"
#include "rpc/sync_rpc_client.h"
#include <string>
#include <vector>

namespace scc {

    class PartitionKVClient {
    public:
        PartitionKVClient(std::string serverName, int serverPort);

        ~PartitionKVClient();

        bool ShowItem(const std::string &key, std::string &itemVersions);

        void InitializePartitioning(DBPartition source); // done by a different connection

        void SendLST(PhysicalTimeSpec lst, int round);

        void SendGST(PhysicalTimeSpec gst);

#ifdef WREN

        void SendGST(PhysicalTimeSpec local, PhysicalTimeSpec remote);

        void SendLST(PhysicalTimeSpec lst, PhysicalTimeSpec rst, int round);

#endif

        void TxSliceReadKeys(unsigned int txId, TxContex &cdata, const std::vector<std::string> &keys, int src);

        void PrepareRequest(int txId, TxContex &cdata, const std::vector<std::string> &keys,
                            std::vector<std::string> &values, int srcPartition);

#if defined(H_CURE) || defined(WREN)

        void CommitRequest(unsigned int txId, PhysicalTimeSpec ct);

#elif defined(CURE)
        void CommitRequest(unsigned int txId, std::vector<PhysicalTimeSpec> ct);
#endif


        template<class Result>
        void SendInternalTxSliceReadKeysResult(Result &opResult) {
            std::string serializedArg = opResult.SerializeAsString();
            _asyncRpcClient->CallWithNoState(RPCMethod::InternalTxSliceReadKeysResult, serializedArg);
        }

        template<class Result>
        void SendPrepareReply(Result &opResult) {
            std::string serializedArg = opResult.SerializeAsString();
            _asyncRpcClient->CallWithNoState(RPCMethod::InternalPrepareReply, serializedArg);
        }

        /* Stabilization protocol */
#ifdef H_CURE

        void SendRST(PhysicalTimeSpec rst, int partitionId);

#endif

#ifdef WREN

        void SendStabilizationTimesToPeers(PhysicalTimeSpec lst, PhysicalTimeSpec rst, int partitionId);

#endif


#ifdef CURE
        void SendPVV(std::vector<PhysicalTimeSpec> pvv, int round);

        void SendGSV(std::vector<PhysicalTimeSpec> gsv);

#endif

    private:
        std::string _serverName;
        int _serverPort;
        AsyncRPCClient *_asyncRpcClient;
        SyncRPCClient *_syncRpcClient;



    };
}

#endif
