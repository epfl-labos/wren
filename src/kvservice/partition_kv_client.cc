#include "common/utils.h"
#include "kvservice/partition_kv_client.h"
#include "common/sys_config.h"
#include "messages/rpc_messages.pb.h"
#include "messages/tx_messages.pb.h"
#include "common/sys_logger.h"
#include "common/sys_stats.h"

namespace scc {

    PartitionKVClient::PartitionKVClient(std::string serverName, int serverPort)
            : _serverName(serverName),
              _serverPort(serverPort),
              _asyncRpcClient(NULL),
              _syncRpcClient(NULL) {
        _asyncRpcClient = new AsyncRPCClient(serverName, serverPort, SysConfig::NumChannelsPartition);
        _syncRpcClient = new SyncRPCClient(serverName, serverPort);
    }

    PartitionKVClient::~PartitionKVClient() {
        delete _asyncRpcClient;
    }

    bool PartitionKVClient::ShowItem(const std::string &key, std::string &itemVersions) {
        PbRpcKVInternalShowItemArg args;
        std::string serializedArgs;
        PbRpcKVInternalShowItemResult result;
        AsyncState state;

        // prepare arguments
        args.set_key(key);
        serializedArgs = args.SerializeAsString();

        // call server
        _asyncRpcClient->Call(RPCMethod::InternalShowItem, serializedArgs, state);
        state.FinishEvent.WaitAndReset();

        // parse result
        result.ParseFromString(state.Results);
        if (result.succeeded()) {
            itemVersions = result.itemversions();
        }

        return result.succeeded();
    }

    void PartitionKVClient::InitializePartitioning(DBPartition source) {
        PbPartition opArg;
        opArg.set_name(source.Name);
        opArg.set_publicport(source.PublicPort);
        opArg.set_partitionport(source.PartitionPort);
        opArg.set_replicationport(source.ReplicationPort);
        opArg.set_partitionid(source.PartitionId);
        opArg.set_replicaid(source.ReplicaId);

        std::string serializedArg = opArg.SerializeAsString();

        // call server
        _syncRpcClient->Call(RPCMethod::InitializePartitioning, serializedArg);
    }


    void PartitionKVClient::PrepareRequest(int txId, TxContex &cdata, const std::vector<std::string> &keys,
                                           std::vector<std::string> &values, int srcPartition) {

#ifdef H_CURE
        PbRpcTccWrenPartitionClientPrepareRequestArg opArg;

        // Set the key to be read and the src node
        opArg.set_id(txId);

        opArg.mutable_ldt()->set_seconds(cdata.LDT.Seconds);
        opArg.mutable_ldt()->set_nanoseconds(cdata.LDT.NanoSeconds);

        opArg.mutable_rst()->set_seconds(cdata.RST.Seconds);
        opArg.mutable_rst()->set_nanoseconds(cdata.RST.NanoSeconds);

        for (int i = 0; i < keys.size(); i++) {
            opArg.add_key(keys[i]);
            opArg.add_value(values[i]);
        }

        opArg.set_src(srcPartition);

        std::string serializedArg = opArg.SerializeAsString();

        _asyncRpcClient->CallWithNoState(RPCMethod::InternalPrepareRequest, serializedArg);

#elif defined(WREN)
        PbRpcTccNBWrenPartitionClientPrepareRequestArg opArg;

        // Set the key to be read and the src node
        opArg.set_id(txId);

        opArg.mutable_lst()->set_seconds(cdata.GLST.Seconds);
        opArg.mutable_lst()->set_nanoseconds(cdata.GLST.NanoSeconds);

        opArg.mutable_rst()->set_seconds(cdata.GRST.Seconds);
        opArg.mutable_rst()->set_nanoseconds(cdata.GRST.NanoSeconds);

        opArg.mutable_ht()->set_seconds(cdata.HT.Seconds);
        opArg.mutable_ht()->set_nanoseconds(cdata.HT.NanoSeconds);

        for (int i = 0; i < keys.size(); i++) {
            opArg.add_key(keys[i]);
            opArg.add_value(values[i]);
        }

        opArg.set_src(srcPartition);

        std::string serializedArg = opArg.SerializeAsString();

        _asyncRpcClient->CallWithNoState(RPCMethod::InternalPrepareRequest, serializedArg);

#elif defined(CURE)
        PbRpcTccCurePartitionClientPrepareRequestArg opArg;

        // Set the key to be read and the src node
        opArg.set_id(txId);

        for (int i = 0; i < cdata.DV.size(); i++) {
            PbPhysicalTimeSpec *p = opArg.add_dv();
            p->set_seconds(cdata.DV[i].Seconds);
            p->set_nanoseconds(cdata.DV[i].NanoSeconds);
        }

        for (int i = 0; i < keys.size(); i++) {
            opArg.add_key(keys[i]);
            opArg.add_value(values[i]);
        }

        opArg.set_src(srcPartition);

        std::string serializedArg = opArg.SerializeAsString();

        _asyncRpcClient->CallWithNoState(RPCMethod::InternalPrepareRequest, serializedArg);

#endif
    }

#if defined(H_CURE) || defined(WREN)

    void PartitionKVClient::CommitRequest(unsigned int txId, PhysicalTimeSpec ct) {


        PbRpCommitRequestArg opArg;

        opArg.set_id(txId);

        opArg.mutable_ct()->set_seconds(ct.Seconds);
        opArg.mutable_ct()->set_nanoseconds(ct.NanoSeconds);

        std::string serializedArg = opArg.SerializeAsString();

        assert(_asyncRpcClient != NULL);
        _asyncRpcClient->CallWithNoState(RPCMethod::InternalCommitRequest, serializedArg);

    }

#elif defined(CURE)

    void PartitionKVClient::CommitRequest(unsigned int txId, std::vector<PhysicalTimeSpec> ct) {

        PbRpTccCureCommitRequestArg opArg;

        opArg.set_id(txId);

        for (int i = 0; i < ct.size(); i++) {
            PbPhysicalTimeSpec *p = opArg.add_ct();
            p->set_seconds(ct[i].Seconds);
            p->set_nanoseconds(ct[i].NanoSeconds);
        }

        std::string serializedArg = opArg.SerializeAsString();

        assert(_asyncRpcClient != NULL);
        _asyncRpcClient->CallWithNoState(RPCMethod::InternalCommitRequest, serializedArg);

    }

#endif

#ifdef WREN

    void PartitionKVClient::TxSliceReadKeys(unsigned int txId, TxContex &cdata, const std::vector<std::string> &keys,
                                            int src) {

        PbRpcTccNBWrenKVInternalTxSliceReadKeysArg opArg;
        PbRpcTccNBWrenKVInternalTxSliceReadKeysResult opResult;
        // Set the key to be read and the src node
        for (int i = 0; i < keys.size(); i++) {
            opArg.add_key(keys[i]);
        }
        opArg.set_src(src);
        opArg.set_id(txId);
        opArg.mutable_lst()->set_seconds(cdata.GLST.Seconds);
        opArg.mutable_lst()->set_nanoseconds(cdata.GLST.NanoSeconds);
        opArg.mutable_rst()->set_seconds(cdata.GRST.Seconds);
        opArg.mutable_rst()->set_nanoseconds(cdata.GRST.NanoSeconds);

        std::string serializedArg = opArg.SerializeAsString();

        _asyncRpcClient->CallWithNoState(RPCMethod::InternalTxSliceReadKeys, serializedArg);

    }

#endif

#ifdef H_CURE

    void PartitionKVClient::TxSliceReadKeys(unsigned int txId, TxContex &cdata, const std::vector<std::string> &keys,
                                            int src) {
        PbRpcTccWrenKVInternalTxSliceReadKeysArg opArg;
        PbRpcTccWrenKVInternalTxSliceReadKeysResult opResult;
        // Set the key to be read and the src node
        for (int i = 0; i < keys.size(); i++) {
            opArg.add_key(keys[i]);
        }
        opArg.set_src(src);
        opArg.set_id(txId);
        opArg.mutable_ldt()->set_seconds(cdata.LDT.Seconds);
        opArg.mutable_ldt()->set_nanoseconds(cdata.LDT.NanoSeconds);
        opArg.mutable_rst()->set_seconds(cdata.RST.Seconds);
        opArg.mutable_rst()->set_nanoseconds(cdata.RST.NanoSeconds);

        std::string serializedArg = opArg.SerializeAsString();

        _asyncRpcClient->CallWithNoState(RPCMethod::InternalTxSliceReadKeys, serializedArg);

    }
#endif

#ifdef CURE

    void PartitionKVClient::TxSliceReadKeys(unsigned int txId, TxContex &cdata, const std::vector<std::string> &keys,
                                            int src) {

        PbRpcTccCureKVInternalTxSliceReadKeysArg opArg;
        PbRpcTccCureKVInternalTxSliceReadKeysResult opResult;
        // Set the key to be read and the src node
        for (int i = 0; i < keys.size(); i++) {
            opArg.add_key(keys[i]);
        }
        opArg.set_src(src);
        opArg.set_id(txId);

        for (int j = 0; j < cdata.DV.size(); j++) {
            PbPhysicalTimeSpec *pt = opArg.add_dv();
            pt->set_seconds(cdata.DV[j].Seconds);
            pt->set_nanoseconds(cdata.DV[j].NanoSeconds);
        }

        std::string serializedArg = opArg.SerializeAsString();

        _asyncRpcClient->CallWithNoState(RPCMethod::InternalTxSliceReadKeys, serializedArg);

    }

#endif


    void PartitionKVClient::SendLST(PhysicalTimeSpec lst, int round) {
        PbRpcLST pb_lst;
        pb_lst.mutable_time()->set_seconds(lst.Seconds);
        pb_lst.mutable_time()->set_nanoseconds(lst.NanoSeconds);
        pb_lst.set_round(round);

        std::string serializedArg = pb_lst.SerializeAsString();

#ifdef MEASURE_STATISTICS
        SysStats::NumSentGSTBytes += serializedArg.size();
        SysStats::NumSentGSTs += 1;
#endif
        // call server
        _syncRpcClient->Call(RPCMethod::SendLST, serializedArg);
    }

#ifdef WREN

    void PartitionKVClient::SendLST(PhysicalTimeSpec lst, PhysicalTimeSpec rst, int round) {
        PbRpcST pb_st;
        pb_st.mutable_lst()->set_seconds(lst.Seconds);
        pb_st.mutable_lst()->set_nanoseconds(lst.NanoSeconds);
        pb_st.mutable_rst()->set_seconds(rst.Seconds);
        pb_st.mutable_rst()->set_nanoseconds(rst.NanoSeconds);
        pb_st.set_round(round);

        std::string serializedArg = pb_st.SerializeAsString();

#ifdef MEASURE_STATISTICS

        SysStats::NumSentGSTBytes += serializedArg.size();
        SysStats::NumSentGSTs += 1;
#endif
        // call server
        _syncRpcClient->Call(RPCMethod::SendLST, serializedArg);
    }

#endif


#if defined(H_CURE)
    void PartitionKVClient::SendGST(PhysicalTimeSpec gst) {
        PbRpcGST pb_gst;
        pb_gst.mutable_time()->set_seconds(gst.Seconds);
        pb_gst.mutable_time()->set_nanoseconds(gst.NanoSeconds);

        std::string serializedArg = pb_gst.SerializeAsString();

#ifdef MEASURE_STATISTICS
        SysStats::NumSentGSTBytes += serializedArg.size();
        SysStats::NumSentGSTs += 1;
#endif

        // call server
        _syncRpcClient->Call(RPCMethod::SendGST, serializedArg);
    }
#endif

#ifdef WREN

    void PartitionKVClient::SendGST(PhysicalTimeSpec local, PhysicalTimeSpec remote) {
        PbRpcStabilizationTime pb_st;

        pb_st.mutable_glst()->set_seconds(local.Seconds);
        pb_st.mutable_glst()->set_nanoseconds(local.NanoSeconds);

        pb_st.mutable_grst()->set_seconds(remote.Seconds);
        pb_st.mutable_grst()->set_nanoseconds(remote.NanoSeconds);

        std::string serializedArg = pb_st.SerializeAsString();

#ifdef MEASURE_STATISTICS
        SysStats::NumSentGSTBytes += serializedArg.size();
        SysStats::NumSentGSTs += 1;
#endif

        // call server
        _syncRpcClient->Call(RPCMethod::SendGST, serializedArg);
    }

#endif

#ifdef CURE

    void PartitionKVClient::SendPVV(std::vector<PhysicalTimeSpec> pvv, int round) {
        PbRpcPVV pb_lst;
        for (int i = 0; i < pvv.size(); i++) {
            PbPhysicalTimeSpec *p = pb_lst.add_pvv();
            p->set_seconds(pvv[i].Seconds);
            p->set_nanoseconds(pvv[i].NanoSeconds);
        }
        pb_lst.set_round(round);

        std::string serializedArg = pb_lst.SerializeAsString();

#ifdef MEASURE_STATISTICS
          SysStats::NumSentGSTBytes += serializedArg.size();
          SysStats::NumSentGSTs += 1;

#endif
        // call server
        _syncRpcClient->Call(RPCMethod::SendPVV, serializedArg);
    }

    void PartitionKVClient::SendGSV(std::vector<PhysicalTimeSpec> gsv) {
        PbRpcGSV pb_gsv;
        for (int i = 0; i < gsv.size(); i++) {
            PbPhysicalTimeSpec *p = pb_gsv.add_gsv();
            p->set_seconds(gsv[i].Seconds);
            p->set_nanoseconds(gsv[i].NanoSeconds);
        }

        std::string serializedArg = pb_gsv.SerializeAsString();

#ifdef MEASURE_STATISTICS

        SysStats::NumSentGSTBytes += serializedArg.size();
#endif

        // call server
        _syncRpcClient->Call(RPCMethod::SendGSV, serializedArg);
    }

#endif

#ifdef H_CURE
    void PartitionKVClient::SendRST(PhysicalTimeSpec rst, int partitionId) {

        PbRpcRST pb_rst;
        pb_rst.mutable_time()->set_seconds(rst.Seconds);
        pb_rst.mutable_time()->set_nanoseconds(rst.NanoSeconds);
        pb_rst.set_srcpartition(partitionId);

        std::string serializedArg = pb_rst.SerializeAsString();

#ifdef MEASURE_STATISTICS
        SysStats::NumSentRSTs ++;
        SysStats::NumSentRSTBytes += serializedArg.size();
#endif

        _asyncRpcClient->CallWithNoState(RPCMethod::SendRST, serializedArg);
    }

#endif

#ifdef WREN

    void PartitionKVClient::SendStabilizationTimesToPeers(PhysicalTimeSpec lst, PhysicalTimeSpec rst, int partitionId) {

        PbRpcPeerStabilizationTimes pb_st;
        pb_st.mutable_lst()->set_seconds(lst.Seconds);
        pb_st.mutable_lst()->set_nanoseconds(lst.NanoSeconds);
        pb_st.mutable_rst()->set_seconds(rst.Seconds);
        pb_st.mutable_rst()->set_nanoseconds(rst.NanoSeconds);
        pb_st.set_srcpartition(partitionId);

        std::string serializedArg = pb_st.SerializeAsString();

#ifdef MEASURE_STATISTICS
        SysStats::NumSentRSTs++;
        SysStats::NumSentRSTBytes += serializedArg.size();
#endif
        _asyncRpcClient->CallWithNoState(RPCMethod::SendStabilizationTimes, serializedArg);
    }

#endif


}
