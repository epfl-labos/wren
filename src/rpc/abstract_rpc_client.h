#ifndef SCC_RPC_ABSTRACT_RPC_CLIENT_H
#define SCC_RPC_ABSTRACT_RPC_CLIENT_H

#include "rpc/message_channel.h"
#include "rpc/rpc_id.h"
#include <string>
#include <thread>
#include <atomic>

namespace scc {

class AbstractRPCClient {
public:

  AbstractRPCClient()
    : _msgIdCounter(0) {
  }

protected:
  std::atomic<int64_t> _msgIdCounter;

  int64_t GetMessageId() {
    return _msgIdCounter.fetch_add(1);
  }
};

}

#endif
