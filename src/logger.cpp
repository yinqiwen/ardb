/*
 * logger.cpp
 *
 *  Created on: 2013-4-8
 *      Author: wqy
 */

#include "logger.hpp"
#include "util/helpers.hpp"
#include <stdarg.h>
#include <stdio.h>
namespace ardb
{
	static ARDBLogHandler* kLogHandler = 0;
	static IsLogEnable* kLogChecker = 0;
	static const char* kLogLevelNames[] = { "FATAL", "ERROR", "WARN", "INFO",
			"DEBUG", "TRACE" };
	static const unsigned int k_default_log_line_buf_size = 256;
	static void default_loghandler(LogLevel level, const char* filename,
			const char* function, int line, const char* format, ...)
	{
		const char* levelstr = 0;
		uint64_t timestamp = get_current_epoch_millis();
		if (level > 0 && level < ALL_LOG_LEVEL)
		{
			levelstr = kLogLevelNames[level - 1];
		} else
		{
			levelstr = "???";
		}
		size_t log_line_size = k_default_log_line_buf_size;
		va_list args;
		std::string record;
		va_start(args, format);
		for (;;)
		{
			char content[log_line_size + 1];
#ifndef va_copy
#define va_copy(dst, src)   memcpy(&(dst), &(src), sizeof(va_list))
#endif
			va_list aq;
			va_copy(aq, args);
			int sz = vsnprintf(content, log_line_size, format, aq);
			va_end(aq);
			if (sz < 0)
			{
				return;
			}
			if ((size_t) sz < log_line_size)
			{
				record = content;
				break;
			}
			log_line_size <<= 1;
		}

		uint32 mills = timestamp % 1000;
		char timetag[256];
		struct tm& tm = get_current_tm();
		sprintf(timetag, "%u-%02u-%02u %02u:%02u:%02u", tm.tm_year + 1900,
				tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
		FILE* out = stdout;
		if (level <= WARN_LOG_LEVEL)
		{
			out = stderr;
		}
		fprintf(out, "[%s,%03u][%s][%u][%s:%u]%s\n", timetag, mills, levelstr,
				getpid(), get_basename(filename).c_str(), line, record.c_str());
	}

	static bool default_logchcker(LogLevel level)
	{
		return true;
	}

	ARDBLogHandler* ARDBLogger::GetLogHandler()
	{
		if (!kLogHandler)
		{
			kLogHandler = default_loghandler;
		}
		return kLogHandler;
	}
	IsLogEnable* ARDBLogger::GetLogChecker()
	{
		if (!kLogChecker)
		{
			kLogChecker = default_logchcker;
		}
		return kLogChecker;
	}
	void ARDBLogger::InstallLogHandler(ARDBLogHandler* h, IsLogEnable* c)
	{
		kLogHandler = h;
		kLogChecker = c;
	}
}

