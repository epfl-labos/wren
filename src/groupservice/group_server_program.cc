#include "groupservice/group_server.h"
#include <stdlib.h>

using namespace scc;

int main(int argc, char* argv[]) {
  if (argc != 4) {
    fprintf(stdout,"Number of args: %d ", argc);

    for (int i = 0; i< argc; i++){
      fprintf(stdout," %s ", argv[i]);
    }
    fprintf(stdout, "Usage: %s <Port> <NumPartitions> <NumReplicasPerPartition>\n", argv[0]);
    exit(1);
  }

  int port = atoi(argv[1]);
  int numPartitions = atoi(argv[2]);
  int numReplicaPerPartition = atoi(argv[3]);

  fprintf(stdout,"GroupServer program... \n");
  GroupServer server(port, numPartitions, numReplicaPerPartition);
  fprintf(stdout,"GroupServer connected. \n");
  server.Run();
}

//opt/gentlerain/build/group_server_program 2000 2 2