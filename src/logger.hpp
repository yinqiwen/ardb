/*
 * logger_macros.hpp
 *
 *  Created on: 2011-4-20
 *      Author: qiyingwang
 */

#ifndef LOGGER_MACROS_HPP_
#define LOGGER_MACROS_HPP_

namespace rddb
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
	typedef void RDDBLogHandler(LogLevel level, const char* filename,
			const char* function, int line, const char* format, ...);
	typedef bool IsLogEnable(LogLevel level);

	struct RDDBLogger
	{
			static RDDBLogHandler* GetLogHandler();
			static IsLogEnable* GetLogChecker();
			static void InstallLogHandler(RDDBLogHandler* h, IsLogEnable* c);
	};
}

#define DEBUG_ENABLED() ((rddb::RDDBLogger::GetLogChecker())(rddb::DEBUG_LOG_LEVEL))
#define TRACE_ENABLED() ((rddb::RDDBLogger::GetLogChecker())(rddb::TRACE_LOG_LEVEL))
#define ERROR_ENABLED() ((rddb::RDDBLogger::GetLogChecker())(rddb::ERROR_LOG_LEVEL))
#define INFO_ENABLED()  ((rddb::RDDBLogger::GetLogChecker())(rddb::INFO_LOG_LEVEL))
#define FATAL_ENABLED() ((rddb::RDDBLogger::GetLogChecker())(rddb::FATAL_LOG_LEVEL))
#define WARN_ENABLED() ((rddb::RDDBLogger::GetLogChecker())(rddb::WARN_LOG_LEVEL))

#define DEBUG_LOG( ...) do {\
   if(DEBUG_ENABLED())\
   {                 \
	   (*(rddb::RDDBLogger::GetLogHandler()))(rddb::DEBUG_LOG_LEVEL, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
   }\
}while(0)

#define WARN_LOG(...) do {\
	if(WARN_ENABLED())\
    {                 \
		(*(rddb::RDDBLogger::GetLogHandler()))(rddb::WARN_LOG_LEVEL, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
	}\
}while(0)

#define TRACE_LOG(...) do {\
	if(TRACE_ENABLED())\
	{                 \
		(*(rddb::RDDBLogger::GetLogHandler()))(rddb::TRACE_LOG_LEVEL, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
	}\
}while(0)

#define ERROR_LOG(...) do {\
	if(ERROR_ENABLED())\
	{                 \
		(*(rddb::RDDBLogger::GetLogHandler()))(rddb::ERROR_LOG_LEVEL, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
	}\
}while(0)

#define FATAL_LOG(...) do {\
	if(FATAL_ENABLED())\
    {                 \
		(*(rddb::RDDBLogger::GetLogHandler()))(rddb::FATAL_LOG_LEVEL, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
	}\
}while(0)

#define INFO_LOG(...) do {\
	if(INFO_ENABLED())\
	{                 \
		(*(rddb::RDDBLogger::GetLogHandler()))(rddb::INFO_LOG_LEVEL, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \                 \
	}\
}while(0)

#endif /* LOGGER_MACROS_HPP_ */
