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

#ifndef SCC_KVSERVICE_REPLICATION_KV_CLIENT_H
#define SCC_KVSERVICE_REPLICATION_KV_CLIENT_H

#include "common/types.h"
#include "rpc/sync_rpc_client.h"
#include <vector>

namespace scc {

    class ReplicationKVClient {
    public:
        ReplicationKVClient(std::string serverName, int serverPort, int replicaId);

        ~ReplicationKVClient();

        void InitializeReplication(DBPartition source);

        bool SendUpdate(std::vector<LocalUpdate *> &updates);

        bool SendHeartbeat(Heartbeat &hb);

    private:
        std::string _serverName;
        int _serverPort;
        SyncRPCClient *_rpcClient;
        int _replicaId;
        PhysicalTimeSpec latestSentUpdateCommitTime;
        std::mutex latestSentUpdateCommitTimeMutex;
        PhysicalTimeSpec latestSentHeartBeat;
        std::mutex latestSentHeartBeatMutex;
    };

} // namespace scc

#endif
