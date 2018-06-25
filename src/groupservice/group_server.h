/*
 * WREN 
 *
 * Copyright 2018 Operating Systems Laboratory EPFL
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SCC_GROUPSERVICE_TIME_SERVER_H
#define SCC_GROUPSERVICE_TIME_SERVER_H

#include "common/types.h"
#include "rpc/socket.h"
#include "messages/rpc_messages.pb.h"
#include <string>
#include <vector>
#include <thread>

namespace scc {

class GroupServer {
public:
  GroupServer(unsigned short port, int numPartitions, int numReplicasPerPartition);
  void Run();
  void HandleRequest(TCPSocket* clientSocket);
  void ServeConnection();
private:
  unsigned short _serverPort;
  int _numPartitions;
  int _numReplicasPerPartition;
  int _totalNumPartitions;
  std::vector<std::vector<DBPartition>> _registeredPartitions;
  int _numRegisteredPartitions;
  bool _allPartitionsRegistered;
  std::mutex _registrationMutex;
  int _numReadyPartitions;
  bool _allPartitionsReady;
  std::mutex _readinessMutex;
};

}

#endif
