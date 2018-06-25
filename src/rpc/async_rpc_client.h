#ifndef SCC_RPC_ASYNC_RPC_CLIENT_H
#define SCC_RPC_ASYNC_RPC_CLIENT_H

#include "rpc/abstract_rpc_client.h"
#include "rpc/message_channel.h"
#include "messages/rpc_messages.pb.h"
#include "common/wait_handle.h"
#include <vector>
#include <unordered_map>
#include <thread>
#include <memory>
#include <pthread.h>
#include <atomic>

namespace scc {

class AsyncState {
public:
  WaitHandle FinishEvent;
  std::string Results;
};

class AsyncRPCClient : public AbstractRPCClient {
public:
  AsyncRPCClient(std::string host, int port, int numChannels);
  ~AsyncRPCClient();
  void Call(RPCMethod rid, std::string& args, AsyncState& state);
    void CallWithNoState(RPCMethod rid, std::string& args);
private:
  typedef std::unordered_map<int64_t, AsyncState*> PendingCallMap;
  typedef std::shared_ptr<PendingCallMap> PendingCallMapPtr;
  typedef std::shared_ptr<std::mutex> MutexPtr;

  // initialization synchronization
  std::vector<WaitHandle*> _replyHandlingThreadReadyEvents;
  WaitHandle _allReplyHandlingThreadsReady;
  int _numChannels;
  std::vector<MessageChannelPtr> _msgChannels;
  std::vector<PendingCallMapPtr> _pendingCallSets;
  std::vector<MutexPtr> _pendingCallSetMutexes;
  void HandleReplyMessage(int channelId);
};

typedef std::shared_ptr<AsyncRPCClient> AsyncRPCClientPtr;

} // namespace scc

#endif
