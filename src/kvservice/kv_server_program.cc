#include "kvservice/kv_server.h"
#include "common/sys_config.h"
#include "common/utils.h"
#include "common/types.h"
#include <stdlib.h>
#include <iostream>

using namespace scc;
const int ARG_NUM = 19;

int main(int argc, char *argv[]) {

    fprintf(stdout, "[INFO]: Starting kv_server.\n");
    if (argc == ARG_NUM) {
        SysConfig::Consistency = Utils::str2consistency(argv[1]);
        std::string serverName = argv[2];
        int publicPort = atoi(argv[3]);
        int partitionPort = atoi(argv[4]);
        int replicationPort = atoi(argv[5]);
        int partitionId = atoi(argv[6]);
        int replicaId = atoi(argv[7]);
        int totalNumKeys = atoi(argv[8]);
        std::string duraStr = argv[9];

        if (duraStr == "Disk") {
            SysConfig::DurabilityOption = DurabilityType::Disk;
        } else if (duraStr == "Memory") {
            SysConfig::DurabilityOption = DurabilityType::Memory;
        } else {
            fprintf(stdout, "<Durability: Disk/Memory>\n");
            exit(1);
        }
        std::string groupServerName = argv[10];
        int groupServerPort = atoi(argv[11]);

        std::string gstStr = argv[12];
        if (gstStr == "tree") {
            SysConfig::GSTDerivationMode = GSTDerivationType::TREE;
            cout << "[INFO]: GSTDerivationMode set to TREE." << endl;
        } else {
            cout << "[INFO]: GSTDerivationMode currently not supported!" << endl;
            fprintf(stdout, "<GSTDerivationMode: tree>\n");
            exit(1);
        }

#if defined(H_CURE) || defined(WREN)
        SysConfig::RSTComputationInterval = atoi(argv[13]);
        SysConfig::GSTComputationInterval = atoi(argv[13]);
#else
        SysConfig::GSTComputationInterval = atoi(argv[13]);
        SysConfig::RSTComputationInterval = atoi(argv[13]);
#endif

        SysConfig::UpdatePropagationBatchTime = atoi(argv[14]);
        SysConfig::ReplicationHeartbeatInterval = atoi(argv[15]);

        SysConfig::GetSpinTime = atoi(argv[16]);

        SysConfig::FreshnessParameter = atoi(argv[17]);
        SysConfig::NumChannelsPartition = atoi(argv[18]);

        // start server
        KVTxServer server(serverName, publicPort, partitionPort, replicationPort, partitionId, replicaId, totalNumKeys,
                          groupServerName, groupServerPort);

        server.Run();

    } else {
        cout << "[INFO]: You provided " << argc << " arguments instead of " << ARG_NUM << " !\n";
        fprintf(stdout,
                "Usage: %s<Causal> <KVServerName> <PublicPort> <PartitionPort> <ReplicationPort> <PartitionId> <ReplicaId> <TotalNumKeys> <Durability: Memory> <GroupServerName> <GroupServerPort> <GSTDerivationMode: tree> <GSTInterval (us)> <UpdatePropagationBatchTime (us)> <ReplicationHeartbeatInterval (us)>",
                argv[0]);
        exit(1);
    }
}