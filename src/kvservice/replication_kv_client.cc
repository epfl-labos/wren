#include "kvstore/item_version.h"
#include "kvservice/replication_kv_client.h"
#include "common/sys_config.h"
#include "common/sys_stats.h"
#include "common/types.h"


namespace scc {

    ReplicationKVClient::ReplicationKVClient(std::string serverName, int replicationPort, int replicaId)
            : _serverName(serverName), _replicaId(replicaId),
              _serverPort(replicationPort) {
        _rpcClient = new SyncRPCClient(_serverName, _serverPort);
    }

    ReplicationKVClient::~ReplicationKVClient() {
        delete _rpcClient;
    }

    void ReplicationKVClient::InitializeReplication(DBPartition source) {
        PbPartition opArg;
        opArg.set_name(source.Name);
        opArg.set_publicport(source.PublicPort);
        opArg.set_partitionport(source.PartitionPort);
        opArg.set_replicationport(source.ReplicationPort);
        opArg.set_partitionid(source.PartitionId);
        opArg.set_replicaid(source.ReplicaId);

        std::string serializedArg = opArg.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::InitializeReplication, serializedArg);
    }

    bool ReplicationKVClient::SendUpdate(std::vector<LocalUpdate *> &updates) {
        PbRpcReplicationArg opArg;
        for (unsigned int i = 0; i < updates.size(); i++) {
            ItemVersion *ver = (ItemVersion *) updates[i]->UpdatedItemVersion;
            opArg.add_updaterecord(updates[i]->SerializedRecord);
        }

        std::string serializedArg = opArg.SerializeAsString();
#ifdef MEASURE_STATISTICS
        SysStats::NumSentReplicationBytes+=serializedArg.size();
#endif
        // call server
        _rpcClient->Call(RPCMethod::ReplicateUpdate, serializedArg);
        return true;
    }

    bool ReplicationKVClient::SendHeartbeat(Heartbeat &hb) {
        PbRpcHeartbeat pb_hb;

        pb_hb.mutable_physicaltime()->set_seconds(hb.PhysicalTime.Seconds);
        pb_hb.mutable_physicaltime()->set_nanoseconds(hb.PhysicalTime.NanoSeconds);
        pb_hb.set_logicaltime(hb.LogicalTime);

        std::string serializedArg = pb_hb.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::SendHeartbeat, serializedArg);

        return true;
    }

}
