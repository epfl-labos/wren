#include "kvservice/public_kv_client.h"
#include "common/exceptions.h"
#include "common/sys_config.h"
#include "common/utils.h"
#include <string>
#include <stdlib.h>
#include <iostream>
#include <boost/algorithm/string.hpp>

using namespace scc;

void showUsage();

int main(int argc, char *argv[]) {

    if (argc != 4 && argc != 5) {
        fprintf(stdout, "Usage: %s <Causal> <ServerName> <ServerPort>\n", argv[0]);
        fprintf(stdout, "Usage: %s <Causal> <ServerName> <ServerPort> <command>\n", argv[0]);
        fprintf(stdout, "Found %d args\n", argc);
        exit(1);
    }

    SysConfig::Consistency = Utils::str2consistency(argv[1]);
    std::string serverName = argv[2];
    unsigned short serverPort = atoi(argv[3]);
    std::string cmd;
    if (argc == 5) {
        cmd = argv[4];
    }

    try {
        // connect to the server
        PublicTxClient client(serverName, serverPort);
        do {
            std::string input;

            if (argc == 5) {
                input = argv[4];
            } else {
                std::cout << ">"; // wait for user input
                std::getline(std::cin, input);
            }

            std::vector<string> splitedInput;
            boost::split(splitedInput, input, boost::is_any_of(" "));
            bool inputInvalid = true;

            if (splitedInput[0] == "Echo") {
                if (splitedInput.size() == 2) {
                    std::string input = splitedInput[1];
                    std::string output;
                    client.Echo(input, output);
                    cout << output << "\n";
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "TxStart") {
                //Usage e.g. TxStart
                if (splitedInput.size() == 1) {
                    std::string stateStr;
                    bool r = client.TxStart();
                    if (r) {
                        cout << "[INFO]:Transaction started \n";
                    } else {
                        cout << "[INFO]:TxStart operation failed.\n";
                    }
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "Read") {
                //Usage e.g. Read 1,2,3,4,5
                if (splitedInput.size() == 2) {
                    std::vector<string> keyList, valueList;
                    boost::split(keyList, splitedInput[1], boost::is_any_of(","));

                    bool r = client.TxRead(keyList, valueList);
                    if (r) {
                        cout << "Got: \n";
                        for (int i = 0; i < keyList.size(); i++) {
                            cout << keyList[i] << " = " << valueList[i] << "\n";
                        }
                    } else {
                        std::cout << "Operation failed.\n";
                    }
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "Check") {
                //Usage e.g. Check 1,2,3 one,two,three
                if (splitedInput.size() == 3) {
                    std::vector<string> keyList, expectedValueList, valueList;
                    boost::split(keyList, splitedInput[1], boost::is_any_of(","));
                    boost::split(expectedValueList, splitedInput[2], boost::is_any_of(","));

                    bool r = client.TxRead(keyList, valueList);
                    if (r) {

                        cout << "Checking: \n";
                        for (int i = 0; i < keyList.size(); i++) {
                            if (expectedValueList[i] == valueList[i]) {
                                cout << "[OK] " << keyList[i] << " == " << valueList[i] << endl;
                            } else {
                                cout << "ERROR! Expected [" << expectedValueList[i] << "] but found [" << valueList[i]
                                     << "]" << endl;
                            }
                        }

                    } else {
                        cout << "Check operation failed.\n";
                    }
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "Write") {
                //Usage e.g. Write 1,2,3 one,two,three
                if (splitedInput.size() == 3) {
                    std::vector<string> keyList, valueList;
                    boost::split(keyList, splitedInput[1], boost::is_any_of(","));
                    boost::split(valueList, splitedInput[2], boost::is_any_of(","));
                    for (int i = 0; i < keyList.size(); i++) {
                        cout << "Writing "<< keyList[i] << " = " << valueList[i] << endl;
                    }

                    bool r = client.TxWrite(keyList, valueList);
                    if (r) {
                        cout << "Assigned: \n";
                        for (int i = 0; i < keyList.size(); i++) {
                            cout << keyList[i] << " = " << valueList[i] << endl;
                        }
                    } else {
                        cout << "Write operation failed.\n";
                    }
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "TxCommit") {
                //Usage e.g. TxCommit
                if (splitedInput.size() == 1) {
                    bool r = client.TxCommit();
                    if (r) {
                        cout << "[INFO]:Transaction commited \n";
                    } else {
                        cout << "[INFO]:Commit operation failed.\n";
                    }
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "ShowItem") {
                if (splitedInput.size() == 2) {
                    std::string key = splitedInput[1];
                    std::string itemVersions;
                    bool r = client.ShowItem(key, itemVersions);
                    if (r) {
                        cout << itemVersions << "\n";
                    } else {
                        cout << "Operation failed.\n";
                    }
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "ShowDB") {
                if (splitedInput.size() == 1) {
                    std::string allItemVersions;
                    bool r = client.ShowDB(allItemVersions);
                    if (r) {
                        cout << allItemVersions << "\n";
                    } else {
                        cout << "Operation failed.\n";
                    }
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "ShowState") {
                if (splitedInput.size() == 1) {
                    std::string stateStr;
                    bool r = client.ShowState(stateStr);
                    if (r) {
                        cout << stateStr << "\n";
                    } else {
                        cout << "Operation failed.\n";
                    }
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "ShowStateCSV") {
                if (splitedInput.size() == 1) {
                    std::string stateStr;
                    bool r = client.ShowStateCSV(stateStr);
                    if (r) {
                        cout << stateStr << "\n";
                    } else {
                        cout << "Operation failed.\n";
                    }
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "DumpLatency") {
                if (splitedInput.size() == 1) {
                    std::string resultStr;
                    bool r = client.DumpLatencyMeasurement(resultStr);
                    if (r) {
                        cout << resultStr << "\n";
                    } else {
                        cout << "Operation failed.\n";
                    }
                    inputInvalid = false;
                }
            } else if (splitedInput[0] == "PartitionId") {
                if (splitedInput.size() == 3) {
                    std::string key = splitedInput[1];
                    int numPartitions = atoi(splitedInput[2].c_str());
                    std::cout << Utils::strhash(key) % numPartitions << "\n";
                    inputInvalid = false;
                }
            }

            if (inputInvalid) {
                std::cout << "Invalid input.\n";
                showUsage();
            }
        } while (argc == 4);

    } catch (SocketException &e) {
        fprintf(stdout, "SocketException: %s\n", e.what());
        exit(1);
    }

}

void showUsage() {
    std::string usage = "Echo <text>\n"
            "TxStart\n"
            "Read <keys>\n"
            "Write <keys> <values>\n"
            "Check <keys> <values>\n"
            "TxCommit\n"
            "ShowItem <key>\n"
            "ShowDB\n"
            "ShowState\n"
            "DumpLatency\n"
            "PartitionId <key> <number of partitions>\n";
    fprintf(stdout, "%s", usage.c_str());
}
