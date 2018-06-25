#ifndef SCC_RPC_SYNC_RPC_CLIENT_H
#define SCC_RPC_SYNC_RPC_CLIENT_H

#include "rpc/abstract_rpc_client.h"
#include "rpc/message_channel.h"
#include "messages/rpc_messages.pb.h"
#include <string>

namespace scc {

class SyncRPCClient : public AbstractRPCClient {
public:
  SyncRPCClient(std::string host, int port);
  void Call(RPCMethod rid, std::string& args, std::string& results);
  void Call(RPCMethod rid, std::string& args);
private:
  MessageChannelPtr _msgChannel;
  void _Call(RPCMethod rid, std::string& args,
      std::string& results, bool recvResults);
};

}

#endif
