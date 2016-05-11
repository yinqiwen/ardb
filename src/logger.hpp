/*
 *Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 * 
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 * 
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 * 
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS 
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
 *THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef LOGGER_MACROS_HPP_
#define LOGGER_MACROS_HPP_

#include <string>

namespace ardb
{
    enum LogLevel
    {
        NONE_LOG_LEVEL = 0,
        FATAL_LOG_LEVEL = 1,
        ERROR_LOG_LEVEL = 2,
        WARN_LOG_LEVEL = 3,
        INFO_LOG_LEVEL = 4,
        DEBUG_LOG_LEVEL = 5,
        TRACE_LOG_LEVEL = 6,
        ALL_LOG_LEVEL = 7,

        INVALID_LOG_LEVEL = 100
    };
    typedef void ArdbLogHandler(LogLevel level, const char* filename,
                    const char* function, int line, const char* format, ...);
    typedef bool IsLogEnable(LogLevel level);
    typedef FILE* GetLogStream();

    struct LoggerSetting
    {
            ArdbLogHandler* handler;
            IsLogEnable* enable;
            GetLogStream* logstream;
            LoggerSetting() :
                            handler(NULL), enable(NULL), logstream(NULL)
            {
            }
    };

    struct ArdbLogger
    {
            static ArdbLogHandler* GetLogHandler();
            static IsLogEnable* GetLogChecker();
            static void InstallLogHandler(LoggerSetting& setting);
            static void InitDefaultLogger(const std::string& level,
                            const std::string& logfile);
            static void SetLogLevel(const std::string& level);
            static void DestroyDefaultLogger();
            static FILE* GetLogStream();
    };
}

#define DEBUG_ENABLED() ((ardb::ArdbLogger::GetLogChecker())(ardb::DEBUG_LOG_LEVEL))
#define TRACE_ENABLED() ((ardb::ArdbLogger::GetLogChecker())(ardb::TRACE_LOG_LEVEL))
#define ERROR_ENABLED() ((ardb::ArdbLogger::GetLogChecker())(ardb::ERROR_LOG_LEVEL))
#define INFO_ENABLED()  ((ardb::ArdbLogger::GetLogChecker())(ardb::INFO_LOG_LEVEL))
#define FATAL_ENABLED() ((ardb::ArdbLogger::GetLogChecker())(ardb::FATAL_LOG_LEVEL))
#define WARN_ENABLED() ((ardb::ArdbLogger::GetLogChecker())(ardb::WARN_LOG_LEVEL))
#define LOG_ENABLED(level) ((ardb::ArdbLogger::GetLogChecker())(level))

#define DEBUG_LOG(...) do {\
   if(DEBUG_ENABLED())\
   {                 \
	   (*(ardb::ArdbLogger::GetLogHandler()))(ardb::DEBUG_LOG_LEVEL, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
   }\
}while(0)

#define WARN_LOG(...) do {\
	if(WARN_ENABLED())\
    {                 \
		(*(ardb::ArdbLogger::GetLogHandler()))(ardb::WARN_LOG_LEVEL, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
	}\
}while(0)

#define TRACE_LOG(...) do {\
	if(TRACE_ENABLED())\
	{                 \
		(*(ardb::ArdbLogger::GetLogHandler()))(ardb::TRACE_LOG_LEVEL, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
	}\
}while(0)

#define ERROR_LOG(...) do {\
	if(ERROR_ENABLED())\
	{                 \
		(*(ardb::ArdbLogger::GetLogHandler()))(ardb::ERROR_LOG_LEVEL, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
	}\
}while(0)

#define FATAL_LOG(...) do {\
	if(FATAL_ENABLED())\
    {                 \
		(*(ardb::ArdbLogger::GetLogHandler()))(ardb::FATAL_LOG_LEVEL, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
		abort();\
	}\
}while(0)

#define INFO_LOG(...)do {\
		if(INFO_ENABLED())\
	    {                 \
			(*(ardb::ArdbLogger::GetLogHandler()))(ardb::INFO_LOG_LEVEL, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
		}\
	}while(0)

#define LOG_WITH_LEVEL(level, ...) do {\
   if(LOG_ENABLED(level))\
   {                 \
	   (*(ardb::ArdbLogger::GetLogHandler()))(level, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
   }\
}while(0)

#endif /* LOGGER_MACROS_HPP_ */
