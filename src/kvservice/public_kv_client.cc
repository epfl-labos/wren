#include "kvservice/public_kv_client.h"
#include "messages/rpc_messages.pb.h"
#include "messages/tx_messages.pb.h"
#include "common/sys_logger.h"
#include "common/utils.h"
#include "common/sys_stats.h"
#include "common/sys_config.h"
#include "common/exceptions.h"
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <time.h>
#include <iostream>

#ifndef USE_GSV_AS_DV
#error Tx only with USE_GSV_AS_DV
#endif

namespace scc {

    PublicTxClient::PublicTxClient(std::string serverName, int publicPort)
            : _serverName(serverName),
              _serverPort(publicPort) {
        _rpcClient = new SyncRPCClient(_serverName, _serverPort);
        GetServerConfig();
        _sessionData.txId = 1;
        _sessionData.WriteSet.clear();
        _sessionData.ReadSet.clear();

#ifdef H_CURE
        _sessionData.LDT.setToZero();
        _sessionData.RST.setToZero();
        _sessionData.CT.setToZero();

#elif defined(WREN)
        _sessionData.GLST.setToZero();
        _sessionData.GRST.setToZero();
        _sessionData.CT.setToZero();

#elif defined(CURE)
        PhysicalTimeSpec t(0, 0);
        _sessionData.DV.resize(_numReplicasPerPartition, t);
        _sessionData.CT.resize(_numReplicasPerPartition, t);

#endif
        numItemsReadFromStore = 0;
        numItemsReadFromClientWriteCache = 0;
        numItemsReadFromClientReadSet = 0;
        numItemsReadFromClientWriteSet = 0;
        totalNumReadItems = 0;
    }

    PublicTxClient::~PublicTxClient() {
        delete _rpcClient;
    }

    bool PublicTxClient::GetServerConfig() {
        std::string serializedArg;
        PbRpcTxPublicGetServerConfigResult result;
        std::string serializedResult;

        // call server
        _rpcClient->Call(RPCMethod::GetServerConfig, serializedArg, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (!result.succeeded()) {
            SLOG("Could not get server config!");
            return false;
        }

        _numPartitions = result.numpartitions();
        _numReplicasPerPartition = result.numreplicasperpartition();
        _replicaId = result.replicaid();

        return true;
    }

    int PublicTxClient::getLocalReplicaId() {
        return _replicaId;
    }

    int PublicTxClient::getNumReplicas() {
        return _numReplicasPerPartition;
    }

    void PublicTxClient::Echo(const std::string &input, std::string &output) {
        PbRpcEchoTest arg;
        std::string serializedArg;
        PbRpcEchoTest result;
        std::string serializedResult;

        // prepare argument
        arg.set_text(input);
        serializedArg = arg.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::EchoTest, serializedArg, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        output = result.text();
    }

    void PublicTxClient::setTotalNumKeyInKVStore(int count) {
        totalNumKeyInKVStore = count;

    }

    void PublicTxClient::resetSession() {
#ifdef H_CURE
        _sessionData.LDT.setToZero();
        _sessionData.RST.setToZero();
        _sessionData.CT.setToZero();

#endif
#ifdef WREN
        _sessionData.GLST.setToZero();
        _sessionData.GRST.setToZero();
        _sessionData.CT.setToZero();
#endif

#ifdef CURE
        _sessionData.DV.clear();
#endif
        _sessionData.ReadSet.clear();
        _sessionData.WriteSet.clear();
        _sessionData.txId = 1;

    }

#ifdef H_CURE

    bool PublicTxClient::TxStart() {

        std::string serializedArgs;
        std::string serializedResult;

        // prepare arguments
        PbRpcTccWrenPublicStartArg args;
        PbRpcTccWrenPublicStartResult result;

        args.mutable_ldt()->set_seconds(_sessionData.LDT.Seconds);
        args.mutable_ldt()->set_nanoseconds(_sessionData.LDT.NanoSeconds);

        args.mutable_rst()->set_seconds(_sessionData.RST.Seconds);
        args.mutable_rst()->set_nanoseconds(_sessionData.RST.NanoSeconds);


        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::TxStart, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (result.succeeded()) {

            _sessionData.txId = result.id();


            PhysicalTimeSpec resultRST(result.rst().seconds(), result.rst().nanoseconds());
            assert(resultRST >= _sessionData.RST);
            _sessionData.RST = MAX(_sessionData.RST, resultRST);
//            _sessionData.RST.Seconds = result.rst().seconds();
//            _sessionData.RST.NanoSeconds = result.rst().nanoseconds();

            _sessionData.ReadSet.clear();
            _sessionData.WriteSet.clear();
        }

        return result.succeeded();
    }

#endif

#ifdef CURE

    bool PublicTxClient::TxStart() {

        std::string serializedArgs;
        std::string serializedResult;

        // prepare arguments
        PbRpcTccCurePublicStartArg args;
        PbRpcTccCurePublicStartResult result;
        int i;

        for (i = 0; i < _numReplicasPerPartition; i++) {
            PbPhysicalTimeSpec *vv = args.add_dv();
            vv->set_seconds(_sessionData.DV[i].Seconds);
            vv->set_nanoseconds(_sessionData.DV[i].NanoSeconds);
        }

        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::TxStart, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (result.succeeded()) {

            _sessionData.txId = result.id();
            for (i = 0; i < _numReplicasPerPartition; i++) {
                if (i != _replicaId) {
                    const PbPhysicalTimeSpec &vv = result.dv(i);
                    PhysicalTimeSpec p(vv.seconds(), vv.nanoseconds());
                    _sessionData.DV[i] = MAX(p, _sessionData.DV[i]);
                }
            }

            _sessionData.ReadSet.clear();
            _sessionData.WriteSet.clear();
        }

        return result.succeeded();
    }

#endif

#ifdef WREN

    bool PublicTxClient::TxStart() {

        std::string serializedArgs;
        std::string serializedResult;

        // prepare arguments
        PbRpcTccNBWrenPublicStartArg args;
        PbRpcTccNBWrenPublicStartResult result;

        args.mutable_glst()->set_seconds(_sessionData.GLST.Seconds);
        args.mutable_glst()->set_nanoseconds(_sessionData.GLST.NanoSeconds);

        args.mutable_grst()->set_seconds(_sessionData.GRST.Seconds);
        args.mutable_grst()->set_nanoseconds(_sessionData.GRST.NanoSeconds);

        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::TxStart, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (result.succeeded()) {

            _sessionData.txId = result.id();

            PhysicalTimeSpec glstResult(result.glst().seconds(), result.glst().nanoseconds());
            assert(glstResult >= _sessionData.GLST);
            _sessionData.GLST.Seconds = result.glst().seconds();
            _sessionData.GLST.NanoSeconds = result.glst().nanoseconds();

            PhysicalTimeSpec grstResult(result.grst().seconds(), result.grst().nanoseconds());
            assert(grstResult >= _sessionData.GRST);
            _sessionData.GRST.Seconds = result.grst().seconds();
            _sessionData.GRST.NanoSeconds = result.grst().nanoseconds();

            _sessionData.ReadSet.clear();
            _sessionData.WriteSet.clear();

            trimWriteCache(_sessionData.GLST);
        }


        return result.succeeded();
    }

#endif

#ifdef CURE

    bool PublicTxClient::TxRead(const std::vector<std::string> &keySet, std::vector<std::string> &valueSet) {
        std::string serializedArgs;
        std::string serializedResult;
        std::string PLACEHOLDER = "";
        unsigned int i;

        // prepare arguments

        PbRpcTccCureKVPublicReadArg args;
        PbRpcTccCureKVPublicReadResult result;

        std::vector<std::string> unseenKeys;
        std::vector<int> positions;

        totalNumReadItems += keySet.size();

        for (i = 0; i < keySet.size(); ++i) {
            valueSet.push_back(PLACEHOLDER);

            /* If the key has been written/updated in this same transaction, read that value. */
            if (_sessionData.WriteSet.find(keySet[i]) != _sessionData.WriteSet.end()) {
                valueSet[i] = _sessionData.WriteSet[keySet[i]];
                numItemsReadFromClientWriteSet++;

            } else if (_sessionData.ReadSet.find(keySet[i]) != _sessionData.ReadSet.end()) {
                /* If the key has been read before in this same transaction, read that value (enabling repeatable reads). */
                valueSet[i] = _sessionData.ReadSet[keySet[i]];
                numItemsReadFromClientReadSet++;

            } else {

                unseenKeys.push_back(keySet[i]);
                positions.push_back(i);
            }
        }

        // prepare tx id argument
        args.set_id(_sessionData.txId);

        for (i = 0; i < unseenKeys.size(); ++i) {
            args.add_key(unseenKeys[i]);
        }

        numItemsReadFromStore += unseenKeys.size();

        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::TxRead, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (result.succeeded()) {
            ASSERT(unseenKeys.size() == result.value_size());

            for (int i = 0; i < result.value_size(); ++i) {
                _sessionData.ReadSet[keySet[positions[i]]] = result.value(i);
                valueSet[positions[i]] = result.value(i);
            }
        }

        return result.succeeded();
    }

#endif

#ifdef H_CURE

    bool PublicTxClient::TxRead(const std::vector<std::string> &keySet, std::vector<std::string> &valueSet) {
        std::string serializedArgs;
        std::string serializedResult;
        std::string PLACEHOLDER = "";
        unsigned int i;

        // prepare arguments
        PbRpcTccWrenKVPublicReadArg args;
        PbRpcTccWrenKVPublicReadResult result;


        std::vector<std::string> unseenKeys;
        std::vector<int> positions;

        totalNumReadItems += keySet.size();

        for (i = 0; i < keySet.size(); ++i) {
            valueSet.push_back(PLACEHOLDER);

            /* If the key has been written/updated in this same transaction, read that value. */
            if (_sessionData.WriteSet.find(keySet[i]) != _sessionData.WriteSet.end()) {
                valueSet[i] = _sessionData.WriteSet[keySet[i]];
                numItemsReadFromClientWriteSet++;

            } else if (_sessionData.ReadSet.find(keySet[i]) != _sessionData.ReadSet.end()) {
                /* If the key has been read before in this same transaction, read that value (enabling repeatable reads). */
                valueSet[i] = _sessionData.ReadSet[keySet[i]];
                numItemsReadFromClientReadSet++;
            } else {

                unseenKeys.push_back(keySet[i]);
                positions.push_back(i);
            }
        }

        // prepare tx id argument
        args.set_id(_sessionData.txId);

        for (i = 0; i < unseenKeys.size(); ++i) {
            args.add_key(unseenKeys[i]);
        }
        numItemsReadFromStore += unseenKeys.size();

        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::TxRead, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (result.succeeded()) {
            ASSERT(unseenKeys.size() == result.value_size());
            for (int i = 0; i < result.value_size(); ++i) {
                _sessionData.ReadSet[keySet[positions[i]]] = result.value(i);
                valueSet[positions[i]] = result.value(i);
            }
        }

        return result.succeeded();
    }

#endif

#ifdef WREN

    bool PublicTxClient::TxRead(const std::vector<std::string> &keySet, std::vector<std::string> &valueSet) {
        std::string serializedArgs;
        std::string serializedResult;
        std::string PLACEHOLDER = "";
        unsigned int i;

        // prepare arguments

        PbRpcTccWrenKVPublicReadArg args;
        PbRpcTccWrenKVPublicReadResult result;

        std::vector<std::string> unseenKeys;
        std::vector<int> positions;

        totalNumReadItems += keySet.size();

        for (i = 0; i < keySet.size(); ++i) {
            valueSet.push_back(PLACEHOLDER);

            /* If the key has been written/updated in this same transaction, read that value. */
            if (_sessionData.WriteSet.find(keySet[i]) != _sessionData.WriteSet.end()) {
                valueSet[i] = _sessionData.WriteSet[keySet[i]];
                numItemsReadFromClientWriteSet++;

            } else if (_sessionData.ReadSet.find(keySet[i]) != _sessionData.ReadSet.end()) {
                /* If the key has been read before in this same transaction, read that value (enabling repeatable reads). */
                valueSet[i] = _sessionData.ReadSet[keySet[i]];
                numItemsReadFromClientReadSet++;

            } else {

                /* The key has been writen from the client in a previous transaction */
                /* If the key is still present in the write cache, its value is newer */
                /* the snapshot time, and that is the value that needs to be returned */

                auto it = _sessionData.WriteCache.find(keySet[i]);
                bool readFromCache;

                readFromCache = it != _sessionData.WriteCache.end();

                if (readFromCache) {
                    valueSet[i] = it->second.first;
                    numItemsReadFromClientWriteCache++;
                } else {
                    unseenKeys.push_back(keySet[i]);
                    positions.push_back(i);
                }
            }
        }

        // prepare tx id argument
        args.set_id(_sessionData.txId);

        for (i = 0; i < unseenKeys.size(); ++i) {
            args.add_key(unseenKeys[i]);
        }

        numItemsReadFromStore += unseenKeys.size();

        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::TxRead, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (result.succeeded()) {
            ASSERT(unseenKeys.size() == result.value_size());
            for (int i = 0; i < result.value_size(); ++i) {
                _sessionData.ReadSet[keySet[positions[i]]] = result.value(i);
                assert(_sessionData.ReadSet.find(keySet[positions[i]]) != _sessionData.ReadSet.end());
                valueSet[positions[i]] = result.value(i);

            }
        } else {
            SLOG("[ERROR]:UNSUCCESSFUL READ!");
        }

        return result.succeeded();
    }

#endif

    bool PublicTxClient::TxWrite(const std::vector<std::string> &keySet, std::vector<std::string> &valueSet) {
        assert(keySet.size() == valueSet.size());
        for (unsigned int i = 0; i < keySet.size(); ++i) {
            /* Update an item that has already been updated within the same transaction. */
            _sessionData.WriteSet[keySet[i]] = valueSet[i];
            assert(_sessionData.WriteSet.find(keySet[i]) != _sessionData.WriteSet.end());
        }

        return true;
    }

#ifdef CURE

    bool PublicTxClient::TxCommit() {

        PbRpcTccCureKVPublicCommitArg args;
        PbRpcTccCureKVPublicCommitResult result;

        std::string serializedArgs;
        std::string serializedResult;

        // prepare arguments
        args.set_id(_sessionData.txId);

        if (!_sessionData.WriteSet.empty()) {
            for (auto it = _sessionData.WriteSet.begin(); it != _sessionData.WriteSet.end(); ++it) {
                args.add_key(it->first);
                args.add_value(it->second);
            }
        }

        serializedArgs = args.SerializeAsString();

        _rpcClient->Call(RPCMethod::TxCommit, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (!_sessionData.WriteSet.empty() && result.succeeded()) {

            const PbPhysicalTimeSpec &vv = result.ct(_replicaId);

            for (int i = 0; i < _sessionData.DV.size(); i++) {
                const PbPhysicalTimeSpec &v = result.ct(i);
                PhysicalTimeSpec p(v.seconds(), v.nanoseconds());
                _sessionData.DV[i] = MAX(p, _sessionData.DV[i]);
            }

        }

        return true;
    }

#endif

#ifdef H_CURE

    bool PublicTxClient::TxCommit() {

        PbRpcKVPublicCommitArg args;
        PbRpcKVPublicCommitResult result;

        std::string serializedArgs;
        std::string serializedResult;

        // prepare arguments
        args.set_id(_sessionData.txId);

        if (!_sessionData.WriteSet.empty()) {
            for (auto it = _sessionData.WriteSet.begin(); it != _sessionData.WriteSet.end(); ++it) {
                args.add_key(it->first);
                args.add_value(it->second);
            }
        }

        serializedArgs = args.SerializeAsString();

        _rpcClient->Call(RPCMethod::TxCommit, serializedArgs, serializedResult);
        // parse result
        result.ParseFromString(serializedResult);

        if (!_sessionData.WriteSet.empty() && result.succeeded()) {

            /* Update freshest local visible snapshot with the commit time */
            PhysicalTimeSpec ctResult;
            ctResult.Seconds = result.ct().seconds();
            ctResult.NanoSeconds = result.ct().nanoseconds();
            assert(ctResult > _sessionData.LDT);
            _sessionData.LDT.Seconds = MAX(_sessionData.LDT.Seconds, ctResult.Seconds);
            _sessionData.LDT.NanoSeconds = MAX(_sessionData.LDT.NanoSeconds, ctResult.NanoSeconds);
        }

        return true;
    }

#endif


#ifdef WREN

    bool PublicTxClient::TxCommit() {

        PbRpcTccNBWrenKVPublicCommitArg args;
        PbRpcTccNBWrenKVPublicCommitResult result;

        std::string serializedArgs;
        std::string serializedResult;

        // prepare arguments
        args.set_id(_sessionData.txId);

        if (!_sessionData.WriteSet.empty()) {
            for (auto it = _sessionData.WriteSet.begin(); it != _sessionData.WriteSet.end(); ++it) {
                args.add_key(it->first);
                args.add_value(it->second);
            }

            args.mutable_lct()->set_seconds(_sessionData.CT.Seconds);
            args.mutable_lct()->set_nanoseconds(_sessionData.CT.NanoSeconds);
        }

        serializedArgs = args.SerializeAsString();

        _rpcClient->Call(RPCMethod::TxCommit, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (!_sessionData.WriteSet.empty() && result.succeeded()) {

            PhysicalTimeSpec ctResult;
            ctResult.Seconds = result.ct().seconds();
            ctResult.NanoSeconds = result.ct().nanoseconds();
            assert(ctResult > _sessionData.CT);
            _sessionData.CT.Seconds = result.ct().seconds();
            _sessionData.CT.NanoSeconds = result.ct().nanoseconds();

            updateWriteCache(_sessionData.CT);

        }

        return true;
    }

#endif

    bool PublicTxClient::ShowItem(const std::string &key, std::string &itemVersions) {
        PbRpcKVPublicShowArg args;
        std::string serializedArgs;
        PbRpcKVPublicShowResult result;
        std::string serializedResult;

        // prepare arguments
        args.set_key(key);
        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::ShowItem, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        if (result.succeeded()) {
            itemVersions = result.returnstring();
        }

        return result.succeeded();
    }

    bool PublicTxClient::ShowDB(std::string &allItemVersions) {
        std::string serializedArgs;
        PbRpcKVPublicShowResult result;
        std::string serializedResult;

        // call server
        _rpcClient->Call(RPCMethod::ShowDB, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        if (result.succeeded()) {
            allItemVersions = result.returnstring();
        }

        return result.succeeded();
    }

    bool PublicTxClient::ShowState(std::string &stateStr) {
        std::string serializedArgs;
        PbRpcKVPublicShowResult result;
        std::string serializedResult;

        // call server
        _rpcClient->Call(RPCMethod::ShowState, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        if (result.succeeded()) {
            stateStr = result.returnstring();
        }

        return result.succeeded();
    }

    bool PublicTxClient::ShowStateCSV(std::string &stateStr) {
        std::string serializedArgs;
        PbRpcKVPublicShowResult result;
        std::string serializedResult;

        // call server
        _rpcClient->Call(RPCMethod::ShowStateCSV, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        if (result.succeeded()) {
            stateStr = result.returnstring();
        }

        return result.succeeded();
    }

    bool PublicTxClient::DumpLatencyMeasurement(std::string &resultStr) {
        std::string serializedArgs;
        PbRpcKVPublicShowResult result;
        std::string serializedResult;

        // call server
        _rpcClient->Call(RPCMethod::DumpLatencyMeasurement, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        if (result.succeeded()) {
            resultStr = result.returnstring();
        }

        return result.succeeded();
    }

#ifdef WREN

    void PublicTxClient::trimWriteCache(PhysicalTimeSpec t) {

        for (auto it = begin(_sessionData.WriteCache); it != end(_sessionData.WriteCache);) {
            PhysicalTimeSpec updateTime = it->second.second;

            if (updateTime <= t) {
                it = _sessionData.WriteCache.erase(it);
            } else {
                ++it;
            }
        }
    }

    void PublicTxClient::updateWriteCache(PhysicalTimeSpec ct) {
        for (auto it = begin(_sessionData.WriteSet); it != end(_sessionData.WriteSet); ++it) {
            std::string writeSetKey = it->first;
            std::string writeSetValue = it->second;

            _sessionData.WriteCache[writeSetKey] = std::make_pair(writeSetValue, ct);
        }
    }

#endif


    int PublicTxClient::getNumItemsReadFromClientWriteSet() {
        return numItemsReadFromClientWriteSet;
    }

    int PublicTxClient::getNumItemsReadFromClientReadSet() {
        return numItemsReadFromClientReadSet;
    }

    int PublicTxClient::getNumItemsReadFromClientWriteCache() {
        return numItemsReadFromClientWriteCache;
    }

    int PublicTxClient::getNumItemsReadFromStore() {
        return numItemsReadFromStore;
    }

    int PublicTxClient::getTotalNumReadItems() {
        return totalNumReadItems;
    }


} // namespace scc