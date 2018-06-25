# Wren

## What is Wren?

Wren is the first transactional causally consistent system that at the same time has nonblocking reads and small constant metadata. Wren introduces new protocols for transaction execution, dependency tracking and stabilization. The transaction protocol supports nonblocking reads by providing a transaction with a snapshot that is the union of a fresh causal snapshot S installed by every partition in the local data center and a client-side cache for writes that are not yet included in S. The dependency tracking and stabilization protocols require only two scalar timestamps, resulting in efficient resource utilization and providing scalability in terms of replication sites. In return for these benefits, Wren slightly increases the visibility latency of updates.
You can read more about Wren in the paper " Wren: Nonblocking Reads in a Partitioned Transactional Causally Consistent Data Store", presented at the IEEE/IFIP International Conference on Dependable Systems and Networks, DSN'18 (25-28 June 2018, Luxembourg).

You can find the paper at:
https://infoscience.epfl.ch/record/254970/files/sdw_wren_infoscience.pdf

## Compilation

In order to compile this project\'s code, you need to install gcc4.8, protobuf-2.6.2, boost-1.63.0 and gtest-1.7.0.
The default project and build directories are `/wren` and `/wren/build`. You can change them by changing the PROJECT and BUILD values in the Makefile.
The code is then compiled simply by running twice:

```
    $make all
```

## Running Wren

In order to run Wren, you need to run the group manager, the server and client programs.

The group manager program can be run with:

```
    $/wren/build/group_server_program <Port> <NumPartitions> <NumReplicasPerPartition>
```
 
A server program can be run with:

```
    $/wren/build/kv_server_program <Causal> <KVServerName> <PublicPort> <PartitionPort> <ReplicationPort> <PartitionId> <ReplicaId> <TotalNumKeys> <Durability: Memory> <GroupServerName> <GroupServerPort> <GSTDerivationMode: tree> <GSTInterval (us)> <UpdatePropagationBatchTime (us)> <ReplicationHeartbeatInterval (us)>"
```
              
A client program can be run with:

```
$/wren/build/run_experiments <Causal> <RequestDistribution: zipfian/uniform> <RequestDistributionParameter> <ReadRatio> <WriteRatio> <GroupManagerName> <GroupManagerPort> <TotalNumItems> <WriteValueSize> <NumOpsPerThread: default 0> <NumThreads> <ServingPartitionId> <ServingReplicaId> <ClientServerName> <OutputResultsFileName> <ReservoirSampling: default false> <ReservoirSamplingLimit> <ExperimentDuration: (ms)> <ExperimentType> <ClientSessionResetLimit> <EnableClientSessionResetLimit: default false> <NumTxReadItems> <NumTxWriteItems> <SpinTime us> <FreshnessParameter: default 0> <NumChannelsPartition: default 4>\n"
```

Additionally, you can run an interactive client program with:

```
    $/wren/build/interactive_kv_client_program <Causal> <ServerName> <ServerPort>
```
    
and provide it with the command, or
```
    $/wren/build/interactive_kv_client_program <Causal> <ServerName> <ServerPort> <Command>
```

where 'Command' can be one of: 

- TxStart
- Read k1,k2, ..,kn
- Write k1,k2, ..,kn v1,v2, ..,vn
- TxCommit

You can run three protocols with this code. Wren is the default protocol. If you want to run Cure or H-Cure you need to set `-DCURE` or `-DH_CURE` in the make file instead of `-DWREN` or define them in the code using the `#define` directive.

## Licensing

**Wren is released under the Apache License, Version 2.0.**

Please see the LICENSE file.
                                
## Contact

- Kristina Spirovska <kristina.spirovska@epfl.ch>
- Diego Didona <diego.didona@epfl.ch>

## Contributors

- Kristina Spirovska
- Diego Didona
- Jiaqing Du
- Calin Iorgulescu
- Amitabha Roy
- Sameh Elnikety


