#include "kvservice/public_kv_client.h"
#include "groupservice/group_client.h"
#include "common/utils.h"
#include "common/types.h"
#include "common/wait_handle.h"
#include "common/sys_config.h"
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <string>
#include <numeric>
#include <time.h>
#include <thread>
#include <chrono>

using namespace scc;

typedef struct {
  int ThreadId;
  std::string ServerName;
  int ServerPort;
  int PartitionId;
  int NumPartitions;
  int TotalNumItems;
  int NumOpsPerThread;
  std::string OpType;
  int NumValueBytes;
  WaitHandle OpReadyEvent;
  WaitHandle OpStartEvent;
  WaitHandle OpEndEvent;
  std::vector<double> OpLatencies;
} ThreadArg;

std::string GetRandomKeyAtPartition(int partitionId, int numPartitions, int totalNumItems) {
  int key;

  do {
    key = abs(random()) % totalNumItems;
  } while (key % numPartitions != partitionId);

  return std::to_string(key);
}

void runTx(ThreadArg* arg) {
  try {
    srand(Utils::GetThreadId());
    std::this_thread::sleep_for(std::chrono::milliseconds(10 * arg->ThreadId));

    // connect to kv server
    std::string setValue(arg->NumValueBytes, 'y');
    std::string getValue;
    std::vector<std::string> txGetValues;
    PublicKVClient client(arg->ServerName, arg->ServerPort);

    // generate keys
    std::vector<std::string> opKeys;
    for (int i = 0; i < arg->NumOpsPerThread; i++) {
      std::string key = GetRandomKeyAtPartition(arg->PartitionId, arg->NumPartitions, arg->TotalNumItems);
      opKeys.push_back(key);
    }

    // notify readiness
    arg->OpReadyEvent.Set();

    // wait to start
    arg->OpStartEvent.WaitAndReset();

    for (int i = 0; i < arg->NumOpsPerThread; i++) {
      // execute operation at the server
      auto startTime = std::chrono::high_resolution_clock::now();
      if (arg->OpType == "Get") {
        bool ret = client.Get(opKeys[i], getValue);
        if (!ret) {
          fprintf(stdout, "Get %s failed\n", opKeys[i].c_str());
        }
      } else if (arg->OpType == "Set") {
        bool ret = client.Set(opKeys[i], setValue);
        if (!ret) {
          fprintf(stdout, "Set %s failed\n", opKeys[i].c_str());
        }
      } else if (arg->OpType == "Mixed") {
        int r = random() % 5;
        if (r == 0 || r == 1 || r == 2 || r == 3) {
          bool ret = client.Get(opKeys[i], getValue);
          if (!ret) {
            fprintf(stdout, "Get %s failed\n", opKeys[i].c_str());
          }
        } else if (r == 4) {
          bool ret = client.Set(opKeys[i], setValue);
          if (!ret) {
            fprintf(stdout, "Set %s failed\n", opKeys[i].c_str());
          }
        }
      }
      auto endTime = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

      arg->OpLatencies.push_back(duration.count());
    }

    // notify stopping
    arg->OpEndEvent.Set();

  } catch (SocketException& e) {
    fprintf(stdout, "SocketException: %s\n", e.what());
    exit(1);
  }
}

void showUsage(char* program) {
  fprintf(stdout, "Usage: %s <Causal> <KVService> <ServerName> <ServerPort> <PartitionId> <NumPartitions> <TotalNumItems> <Get/Set/Mixed> <ValueSize> <NumOpsPerThread> <NumThreads>\n", program);
  fprintf(stdout, "Usage: %s <Causal> <GroupService> <ServerName> <ServerPort> <TotalNumItems> <Get/Set/Mixed> <ValueSize> <NumOpsPerThread> <NumThreads>\n", program);
}

int main(int argc, char* argv[]) {
  if (argc != 12 && argc != 10) {
    showUsage(argv[0]);
    exit(1);
  }

  SysConfig::Consistency = Utils::str2consistency(argv[1]);

  std::string serverName;
  int serverPort;
  int partitionId;
  int numPartitions;
  int totalNumItems;
  std::string opType;
  int numValueBytes;
  int numOpsPerThread;
  int numThreads;

  std::vector<std::vector < DBPartition>> allPartitions;
  int numReplicasPerPartition = -1;

  if (strcmp(argv[2], "KVService") == 0) {
    serverName = argv[3];
    serverPort = atoi(argv[4]);
    partitionId = atoi(argv[5]);
    numPartitions = atoi(argv[6]);
    totalNumItems = atoi(argv[7]);
    opType = argv[8];
    if (opType != "Get" && opType != "Set" && opType != "Mixed") {
      showUsage(argv[0]);
      exit(1);
    }
    numValueBytes = atoi(argv[9]);
    numOpsPerThread = atoi(argv[10]);
    numThreads = atoi(argv[11]);
  } else if (strcmp(argv[2], "GroupService") == 0) {
    std::string groupServerName = argv[3];
    int groupServerPort = atoi(argv[4]);

    // choose a partition randomly by asking the group service
    GroupClient groupClient(groupServerName, groupServerPort);
    allPartitions = groupClient.GetRegisteredPartitions();
    numPartitions = allPartitions.size();
    numReplicasPerPartition = allPartitions[0].size();

    srand(time(NULL));
    DBPartition p = allPartitions[random() % numPartitions][random() % numReplicasPerPartition];

    serverName = p.Name;
    serverPort = p.PublicPort;
    partitionId = p.PartitionId;
    numPartitions = allPartitions.size();
    totalNumItems = atoi(argv[5]);
    opType = argv[6];
    if (opType != "Get" && opType != "Set" && opType != "Mixed") {
      showUsage(argv[0]);
      exit(1);
    }
    numValueBytes = atoi(argv[7]);
    numOpsPerThread = atoi(argv[8]);
    numThreads = atoi(argv[9]);
  } else {
    showUsage(argv[0]);
    exit(1);
  }

  try {
    // build thread arguments
    std::vector<ThreadArg*> threadArgs;
    std::vector<std::thread*> threads;
    std::list<WaitHandle*> opReadyEvents;
    std::list<WaitHandle*> opEndEvents;
    for (int i = 0; i < numThreads; i++) {
      ThreadArg* arg = new ThreadArg;
      arg->ThreadId = i;
      if (strcmp(argv[2], "KVService") == 0) {
        arg->ServerName = serverName;
        arg->ServerPort = serverPort;
        arg->PartitionId = partitionId;
      } else if (strcmp(argv[2], "GroupService") == 0) {
        int partitionIndex = i % (numPartitions * numReplicasPerPartition) / numReplicasPerPartition;
        int replicaIndex = i % (numPartitions * numReplicasPerPartition) % numReplicasPerPartition;
        DBPartition p = allPartitions[partitionIndex][replicaIndex];
        arg->ServerName = p.Name;
        arg->ServerPort = p.PublicPort;
        arg->PartitionId = p.PartitionId;
      }
      arg->NumPartitions = numPartitions;
      arg->TotalNumItems = totalNumItems;
      arg->NumOpsPerThread = numOpsPerThread;
      arg->OpType = opType;
      arg->NumValueBytes = numValueBytes;

      threadArgs.push_back(arg);
      opReadyEvents.push_back(&arg->OpReadyEvent);
      opEndEvents.push_back(&arg->OpEndEvent);
    }

    // launch benchmarking client threads
    for (int i = 0; i < numThreads; i++) {
      std::thread* t = new std::thread(runTx, threadArgs[i]);
      t->detach();
      threads.push_back(t);
    }

    fprintf(stdout, "launched all threads.\n");

    // wait for all threads ready to run
    WaitHandle::WaitAll(opReadyEvents);

    // record start time
    auto startTime = std::chrono::high_resolution_clock::now();

    // signal threads to start
    for (int i = 0; i < numThreads; i++) {
      threadArgs[i]->OpStartEvent.Set();
    }

    // wait for all threads finishing
    WaitHandle::WaitAll(opEndEvents);

    // record end time
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    // calculate perf numbers
    double totalOps = numThreads * numOpsPerThread;
    double totalTime = duration.count();
    double opRate = totalOps / totalTime;
    double totalLatency = 0.0;
    for (int i = 0; i < numThreads; i++) {
      totalLatency += std::accumulate(threadArgs[i]->OpLatencies.begin(),
        threadArgs[i]->OpLatencies.end(), 0);
    }
    double averageLatency = totalLatency / (numThreads * numOpsPerThread);

    fprintf(stdout, "Finished %.1f operations in %.1f milliseconds.\n", totalOps, totalTime);
    fprintf(stdout, "Operation throughput is %.1f ops/ms.\n", opRate);
    fprintf(stdout, "Operation average latency is %.1f milliseconds.\n", averageLatency);

    // free resources
    for (int i = 0; i < numThreads; i++) {
      delete threadArgs[i];
      delete threads[i];
    }
  } catch (SocketException& e) {
    fprintf(stdout, "SocketException: %s\n", e.what());
    exit(1);
  }
}
