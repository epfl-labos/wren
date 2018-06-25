#include "rpc/sync_rpc_client.h"
#include <unistd.h>
#include "common/sys_logger.h"

namespace scc {

  SyncRPCClient::SyncRPCClient(std::string host, int port)
  : AbstractRPCClient(),
  _msgChannel(new MessageChannel(host, port)) {  }

  void SyncRPCClient::Call(RPCMethod rid, std::string& args, std::string& results) {
    _Call(rid, args, results, true);
  }

  void SyncRPCClient::Call(RPCMethod rid, std::string& args) {
    std::string results;
    _Call(rid, args, results, false);
  }

  void SyncRPCClient::_Call(RPCMethod rid, std::string& args,
    std::string& results, bool recvResults) {
    PbRpcRequest request;

    request.set_msgid(GetMessageId());
    request.set_methodid(static_cast<int32_t> (rid));
    request.set_arguments(args);

    // send request 
    _msgChannel->Send(request);

    if (recvResults) {
      // receive reply
      PbRpcReply reply;
      _msgChannel->Recv(reply);

      results = reply.results();
    }
  }

} // namespace scc
