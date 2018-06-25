#include "rpc/rpc_server.h"

namespace scc {

  RPCServer::RPCServer(TCPSocket* socket)
  : _mc(socket) { }

  bool RPCServer::RecvRequest(PbRpcRequest& request) {
    // receive request
    _mc.Recv(request);
    return true;
  }

  bool RPCServer::SendReply(PbRpcReply& reply) {
    // send reply
    _mc.Send(reply);
    return true;
  }

}
