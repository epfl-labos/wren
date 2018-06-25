#include "kvservice/public_kv_client.h"
#include "kvservice/experiment.h"
#include "groupservice/group_client.h"
#include "common/utils.h"
#include "common/types.h"
#include "common/wait_handle.h"
#include "common/sys_config.h"
#include "common/generators.h"
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <string>
#include <numeric>
#include <time.h>
#include <math.h>
#include <thread>
#include <chrono>
#include <algorithm>
#include <iostream>

using namespace scc;

void showUsage(char *program) {
    fprintf(stdout,
            "Usage: %s <Causal> <RequestDistribution> <RequestDistributionParameter> <ReadRatio> <WriteRatio> <ManagerName> <ManagerPort> <TotalNumItems> <WriteValueSize> <NumOpsPerThread> <NumThreads> <ServingReplicaId> <ClientServerId> <LatencyOutputFileName> <ReservoirSampling> <ReservoirSamplingLimit> <ExperimentDuration[ms]> <ClientSessionResetLimit> <EnableClientSessionResetLimit> <NumTxReadItems> <numTxReadItems>\n",
            program);
}

int main(int argc, char *argv[]) {
    try {
        if (argc != 24) {
            fprintf(stdout, "argc=%d\n", argc);
            showUsage(argv[0]);
            exit(1);
        }

        SysConfig::Consistency = Utils::str2consistency(argv[1]);

        Experiment exp(argv);

        try {
            exp.runExperiment();
        } catch (SocketException &e) {
            fprintf(stdout, "SocketException: %s\n", e.what());
            exit(1);
        } catch (...) {
            fprintf(stdout, "Exception occurred during running the experiment\n");
            exit(1);
        }

    } catch (...) {
        fprintf(stdout, "Exception occurred during running the experiment\n");
        exit(1);
    }
}
