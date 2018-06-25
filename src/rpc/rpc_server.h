#ifndef SCC_RPC_RPC_SERVER_H
#define SCC_RPC_RPC_SERVER_H

#include "rpc/message_channel.h"
#include "messages/rpc_messages.pb.h"
#include <memory>

namespace scc {

class RPCServer {
public:
  RPCServer(TCPSocket* socket);
  bool RecvRequest(PbRpcRequest& request);
  bool SendReply(PbRpcReply& reply);
private:
  MessageChannel _mc;
  std::string _clientName;
  int _clientPort;
};

typedef std::shared_ptr<RPCServer> RPCServerPtr;

}

#endif
