#include "common/sys_config.h"

namespace scc {

    ConsistencyType SysConfig::Consistency = ConsistencyType::Causal;
    GSTDerivationType SysConfig::GSTDerivationMode = GSTDerivationType::TREE;
    int SysConfig::NumItemIndexTables = 8;
    std::string SysConfig::PreallocatedOpLogFile = "/data/txlog.bin";
    std::string SysConfig::OpLogFilePrefix = "/data/txlog.bin.";
    DurabilityType SysConfig::DurabilityOption = DurabilityType::Memory;
    std::string SysConfig::SysLogFilePrefix = "/tmp/syslog.txt.";
    std::string SysConfig::SysDebugFilePrefix = "/root/grain/tools/debug/sysdebug_";
    bool SysConfig::SysLogEchoToConsole = true;
    bool SysConfig::SysDebugEchoToConsole = false;
    bool SysConfig::Debugging = false;
    int SysConfig::NumChannelsPartition = 1;
    int SysConfig::UpdatePropagationBatchTime = 5000; // microseconds

    int SysConfig::ReplicationHeartbeatInterval = 10; // miliseconds

    int SysConfig::NumChildrenOfTreeNode = 2;
    int SysConfig::GSTComputationInterval = 5000; // microseconds
    int SysConfig::RSTComputationInterval = 5000; // microseconds

    bool SysConfig::MeasureVisibilityLatency = false;
    int SysConfig::LatencySampleInterval = 1; // number of operations

    int SysConfig::GetSpinTime = 500;
    int SysConfig::FreshnessParameter = 1; // milliseconds

} // namespace scc
