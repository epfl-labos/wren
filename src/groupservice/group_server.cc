#include "groupservice/group_server.h"
#include "common/exceptions.h"
#include "common/sys_logger.h"
#include "rpc/rpc_server.h"
#include "rpc/rpc_id.h"
#include <unistd.h>

namespace scc {

  GroupServer::GroupServer(unsigned short port, int numPartitions, int numReplicasPerPartition)
  : _serverPort(port),
  _numPartitions(numPartitions),
  _numReplicasPerPartition(numReplicasPerPartition),
  _totalNumPartitions(numPartitions*numReplicasPerPartition),
  _numRegisteredPartitions(0),
  _allPartitionsRegistered(false),
  _numReadyPartitions(0),
  _allPartitionsReady(false) {

    fprintf(stdout,"Constructing GroupServer!\n");


    _registeredPartitions.resize(_numPartitions);
    for (int i = 0; i < _numPartitions; i++) {
      _registeredPartitions[i].resize(_numReplicasPerPartition);
    }
  }

  void GroupServer::Run() {
    fprintf(stdout,"GroupServer:RUN()\n");
    ServeConnection();
  }

  void GroupServer::ServeConnection() {
    try {
      TCPServerSocket serverSocket(_serverPort);
      while (true) {
        TCPSocket* clientSocket = serverSocket.accept();
        std::thread t(&GroupServer::HandleRequest, this, clientSocket);
        t.detach();
      }
    } catch (SocketException& e) {
      fprintf(stdout, "Server socket exception: %s\n", e.what());
      fflush(stdout);
    }
  }

  void GroupServer::HandleRequest(TCPSocket* clientSocket) {
    try {
      RPCServer rpcServer(clientSocket);
      while (true) {
        // get rpc request
        PbRpcRequest rpcRequest;
        PbRpcReply rpcReply;
        rpcServer.RecvRequest(rpcRequest);

        switch (static_cast<RPCMethod> (rpcRequest.methodid())) {
        case RPCMethod::EchoTest:
        {
          // get arguments 
          PbRpcEchoTest echoRequest;
          PbRpcEchoTest echoReply;
          echoRequest.ParseFromString(rpcRequest.arguments());
          echoReply.set_text(echoRequest.text());
          // send rpc reply
          rpcReply.set_msgid(rpcRequest.msgid());
          rpcReply.set_results(echoReply.SerializeAsString());
          rpcServer.SendReply(rpcReply);
          break;
        }
        case RPCMethod::RegisterPartition:
        {
          // get op argument
          PbPartition arg;
          arg.ParseFromString(rpcRequest.arguments());
          PbRpcGroupServiceResult result;
          // execute op
          DBPartition p(arg.name(), arg.publicport(),
            arg.partitionport(), arg.replicationport(),
            arg.partitionid(), arg.replicaid());

          {
            std::lock_guard<std::mutex> lk(_registrationMutex);

            _registeredPartitions[p.PartitionId][p.ReplicaId] = p;
            _numRegisteredPartitions += 1;

            SLOG((boost::format("Partition %d:%d at %s:%d registered.")
              % p.PartitionId % p.ReplicaId % p.Name % p.PublicPort).str());

            if (_numRegisteredPartitions == _totalNumPartitions) {
              _allPartitionsRegistered = true;
              SLOG("All partitions registered.");
            }
          }

          result.set_succeeded(true);
          // send rpc reply
          rpcReply.set_msgid(rpcRequest.msgid());
          rpcReply.set_results(result.SerializeAsString());
          rpcServer.SendReply(rpcReply);
          break;
        }
        case RPCMethod::GetRegisteredPartitions:
        {
          // wait until all partitions registered
          while (!_allPartitionsRegistered) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
          }

          PbRegisteredPartitions opResult;
          // execute op
          opResult.set_numpartitions(_numPartitions);
          opResult.set_numreplicasperpartition(_numReplicasPerPartition);
          for (int i = 0; i < _numPartitions; i++) {
            for (int j = 0; j < _numReplicasPerPartition; j++) {
              DBPartition& p = _registeredPartitions[i][j];
              PbPartition* rp = opResult.add_partition();
              rp->set_name(p.Name);
              rp->set_publicport(p.PublicPort);
              rp->set_partitionport(p.PartitionPort);
              rp->set_replicationport(p.ReplicationPort);
              rp->set_partitionid(p.PartitionId);
              rp->set_replicaid(p.ReplicaId);
            }
          }
          // send rpc reply
          rpcReply.set_msgid(rpcRequest.msgid());
          rpcReply.set_results(opResult.SerializeAsString());
          rpcServer.SendReply(rpcReply);
          break;
        }
        case RPCMethod::NotifyReadiness:
        {
          // get op argument
          PbPartition arg;
          arg.ParseFromString(rpcRequest.arguments());
          PbRpcGroupServiceResult result;

          // execute op
          DBPartition p(arg.name(), arg.publicport(),
            arg.partitionport(), arg.replicationport(),
            arg.partitionid(), arg.replicaid());
          {
            std::lock_guard<std::mutex> lk(_readinessMutex);

            _numReadyPartitions++;
            SLOG((boost::format("Partition %d:%d at %s:%d ready.")
              % p.PartitionId % p.ReplicaId % p.Name % p.PublicPort).str());

            if (_numReadyPartitions == _totalNumPartitions) {
              _allPartitionsReady = true;
              SLOG("All partitions are ready to serve client requests.");
            }
          }
          result.set_succeeded(true);

          // wait until all partitions ready
          while (!_allPartitionsReady) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
          }

          // send rpc reply
          rpcReply.set_msgid(rpcRequest.msgid());
          rpcReply.set_results(result.SerializeAsString());
          rpcServer.SendReply(rpcReply);
          break;
        }
        default:
          throw KVOperationException("Unsupported operation.");
        }
      }
    } catch (SocketException& e) {
      fprintf(stdout, "GroupServer:Client serving thread socket exception: %s\n", e.what());
      fflush(stdout);
    }
  }

}
