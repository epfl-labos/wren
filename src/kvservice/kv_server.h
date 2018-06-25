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

#ifndef SCC_KV_TX_SERVICE_KV_SERVER_H
#define SCC_KV_TX_SERVICE_KV_SERVER_H

#include "common/sys_stats.h"
#include "common/sys_logger.h"
#include "common/sys_config.h"
#include "common/types.h"
#include "common/utils.h"
#include "common/exceptions.h"
#include "kvservice/coordinator.h"
#include "rpc/socket.h"
#include "rpc/rpc_server.h"
#include "rpc/rpc_id.h"
#include "messages/rpc_messages.pb.h"
#include "messages/tx_messages.pb.h"
#include "messages/op_log_entry.pb.h"
#include <gperftools/profiler.h>
#include <google/protobuf/text_format.h>
#include <thread>
#include <chrono>
#include <assert.h>
#include <unistd.h>
#include <vector>
#include <iostream>
#include <boost/thread.hpp>


namespace scc {

    class KVTxServer {
    public:
        KVTxServer(std::string name, unsigned short publicPort, int totalNumKeys);

        KVTxServer(std::string name,
                   unsigned short publicPort,
                   unsigned short partitionPort,
                   unsigned short replicationPort,
                   int partitionId,
                   int replicaId,
                   int totalNumKeys,
                   std::string groupServerName,
                   int groupServerPort);

        ~KVTxServer();

        void Run();

    private:
        std::string _serverName;
        int _publicPort;
        int _partitionPort;
        int _replicationPort;
        int _replicaId;
        int _partitionId;
        CoordinatorTx *_coordinator;

        typedef boost::shared_lock<boost::shared_mutex> Shared;
        typedef boost::unique_lock<boost::shared_mutex> Exclusive;

    private:
        void ServePublicConnection();

        void HandlePublicRequest(TCPSocket *clientSocket);

        void ServePartitionConnection();
        void ServePartitionsWriteConnection();

        void HandlePartitionRequest(TCPSocket *partitionSocket);
        void HandlePartitionsWriteRequest(TCPSocket *partitionSocket);


        void ServeReplicationConnection();

        void HandleReplicationRequest(TCPSocket *replicaSocket);

        void HandleGetPess(PbRpcKVPublicGetArg &opArg, PbRpcKVPublicGetResult &opResult);

        void HandleInternalGetPess(PbRpcKVInternalGetArg &opArg, PbRpcKVInternalGetResult &opResult);

        void HandleTxGetPess(PbRpcKVPublicTxGetArg &opArg, PbRpcKVPublicTxGetResult &opResult);

        template<class Argument, class Result>
        void HandleTxStart(Argument &opArg, Result &opResult);

        template<class Argument, class Result>
        void HandleTxRead(Argument &opArg, Result &opResult);

        void HandleTxCommit(PbRpcKVPublicCommitArg &opArg, PbRpcKVPublicCommitResult &opResult);
        void HandleTxCommit(PbRpcTccNBWrenKVPublicCommitArg &opArg, PbRpcTccNBWrenKVPublicCommitResult &opResult);
        void HandleTxCommit(PbRpcTccCureKVPublicCommitArg &opArg, PbRpcTccCureKVPublicCommitResult &opResult);

        template<class Argument, class Result>
        void HandleInternalTxSliceReadKeys(Argument &opArg, Result &opResult);

        template<class Argument>
        void HandleInternalPrepareRequest(Argument &opArg);

    };

} // namespace scc

#endif
