#include "sys_logger.h"
#include "common/utils.h"
#include "common/sys_config.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <thread>
#include <mutex>
#include <boost/format.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/lexical_cast.hpp>

namespace scc {

    SysLogger* SysLogger::Instance() {
        static SysLogger _instance;
        return &_instance;
    }

    SysLogger::SysLogger() {
        std::string hostName = Utils::GetHostName();
        std::string execFileName = Utils::GetCurrentExecFileName();
        std::string pId = std::to_string((long long) getpid());
        
        std::string logFile = SysConfig::SysLogFilePrefix +
                hostName + "_" + execFileName + "_" + pId;
        _logStream = fopen(logFile.c_str(), "w");
        
        std::string debugFile = SysConfig::SysDebugFilePrefix +
                hostName + "_" + execFileName + ".txt";
        _debugStream = fopen(debugFile.c_str(), "w");
    }

    SysLogger::~SysLogger() {
        fclose(_logStream);
//        fclose(_debugStream);
        
    }

    void SysLogger::Log(std::string msg) {
        boost::posix_time::ptime now = boost::posix_time::microsec_clock::local_time();
        std::string time = boost::posix_time::to_simple_string(now);
        time.erase(0, 15); // no need to show YEAR, DATE, and HOUR in the log
        unsigned int tid = Utils::GetThreadId();
        std::string logMsg = (boost::format("[%s] %-6u %s") % time % tid % msg).str();
        std::lock_guard<std::mutex> lk(_logStreamMutex);
        // echo to the console
        if (SysConfig::SysLogEchoToConsole) {
            fprintf(stdout, "%s\n", logMsg.c_str());
            fflush(stdout);
        }
        // write to log file
        fprintf(_logStream, "%s\n", logMsg.c_str());
        fflush(_logStream);
    }
    void SysLogger::Debug(std::string msg) {
        boost::posix_time::ptime now = boost::posix_time::microsec_clock::local_time();
        std::string time = boost::posix_time::to_simple_string(now);
        time.erase(0, 15); // no need to show YEAR, DATE, and HOUR in the log
        unsigned int tid = Utils::GetThreadId();
        std::string debugMsg = (boost::format("[%s] %-6u %s") % time % tid % msg).str();
        std::lock_guard<std::mutex> lk(_debugStreamMutex);
        // echo to the console
        if (SysConfig::SysDebugEchoToConsole) {
            fprintf(stdout, "%s\n", debugMsg.c_str());
            fflush(stdout);
        }
        // write to log file
        fprintf(_debugStream, "%s\n", debugMsg.c_str());
        fflush(_debugStream);
    }

} // namespace scc
