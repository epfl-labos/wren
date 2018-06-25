#include "groupservice/group_client.h"
#include "messages/rpc_messages.pb.h"

namespace scc {

  GroupClient::GroupClient(std::string host, int port)
  : _rpcClient(host, port) { }

  void GroupClient::Echo(const std::string& text, std::string& echoText) {
    PbRpcEchoTest arg;
    std::string serializedArg;
    PbRpcEchoTest result;
    std::string serializedResult;

    // prepare argument
    arg.set_text(text);
    serializedArg = arg.SerializeAsString();

    // call server
    _rpcClient.Call(RPCMethod::EchoTest, serializedArg, serializedResult);

    // parse result
    result.ParseFromString(serializedResult);
    echoText = result.text();
  }

  bool GroupClient::RegisterPartition(DBPartition& p) {
    PbPartition arg;
    std::string serializedArg;
    PbRpcGroupServiceResult result;
    std::string serializedResult;

    // prepare argument
    arg.set_name(p.Name);
    arg.set_publicport(p.PublicPort);
    arg.set_partitionport(p.PartitionPort);
    arg.set_replicationport(p.ReplicationPort);
    arg.set_partitionid(p.PartitionId);
    arg.set_replicaid(p.ReplicaId);
    serializedArg = arg.SerializeAsString();

    // call server
    _rpcClient.Call(RPCMethod::RegisterPartition, serializedArg, serializedResult);

    // parse result
    result.ParseFromString(serializedResult);

    return result.succeeded();
  }

  std::vector<std::vector<DBPartition>> GroupClient::GetRegisteredPartitions() {
    std::string emptyArg;
    PbRegisteredPartitions result;
    std::string serializedResult;
    std::vector<std::vector < DBPartition>> partitions;

    // call server
    _rpcClient.Call(RPCMethod::GetRegisteredPartitions, emptyArg, serializedResult);

    // parse result
    result.ParseFromString(serializedResult);
    int numPartitions = result.numpartitions();
    int numReplicasPerPartition = result.numreplicasperpartition();

    partitions.resize(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      for (int j = 0; j < numReplicasPerPartition; j++) {
        PbPartition rp = result.partition(i * numReplicasPerPartition + j);
        DBPartition p(rp.name(), rp.publicport(), rp.partitionport(),
          rp.replicationport(), rp.partitionid(), rp.replicaid());
        partitions[i].push_back(p);
      }
    }

    return partitions;
  }

  bool GroupClient::NotifyReadiness(DBPartition& p) {
    PbPartition arg;
    std::string serializedArg;
    PbRpcGroupServiceResult result;
    std::string serializedResult;

    // prepare argument
    arg.set_name(p.Name);
    arg.set_publicport(p.PublicPort);
    arg.set_partitionport(p.PartitionPort);
    arg.set_replicationport(p.ReplicationPort);
    arg.set_partitionid(p.PartitionId);
    arg.set_replicaid(p.ReplicaId);
    serializedArg = arg.SerializeAsString();

    // call server
    _rpcClient.Call(RPCMethod::NotifyReadiness, serializedArg, serializedResult);

    // parse result
    result.ParseFromString(serializedResult);

    return result.succeeded();
  }

} // namespace scc
