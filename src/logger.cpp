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
#include <sstream>
namespace ardb
{
	static ArdbLogHandler* kLogHandler = 0;
	static IsLogEnable* kLogChecker = 0;
	static const char* kLogLevelNames[] = { "FATAL", "ERROR", "WARN", "INFO",
	        "DEBUG", "TRACE" };
	static const unsigned int k_default_log_line_buf_size = 256;
	static LogLevel kDeafultLevel = DEBUG_LOG_LEVEL;
	static FILE* kLogFile = stdout;
	static std::string kLogFilePath;
	static const uint32 k_max_file_size = 20 * 1024 * 1024;
	static const uint32 k_max_rolling_index = 100;

	static void reopen_default_logfile()
	{
		if (!kLogFilePath.empty())
		{
			kLogFile = fopen(kLogFilePath.c_str(), "a+");
			if (NULL == kLogFile)
			{
				WARN_LOG("Failed to open log file:%s, use stdout instead.");
				kLogFile = stdout;
				return;
			}
		}
	}

	static void rollover_default_logfile()
	{
		if (NULL != kLogFile)
		{
			fclose(kLogFile);
			kLogFile = stdout;
		}
		std::stringstream oldest_file(
		        std::stringstream::in | std::stringstream::out);
		oldest_file << kLogFilePath << "." << k_max_rolling_index;
		remove(oldest_file.str().c_str());

		for (int i = k_max_rolling_index - 1; i >= 1; --i)
		{
			std::stringstream source_oss(
			        std::stringstream::in | std::stringstream::out);
			std::stringstream target_oss(
			        std::stringstream::in | std::stringstream::out);

			source_oss << kLogFilePath << "." << i;
			target_oss << kLogFilePath << "." << (i + 1);
			if(is_file_exist(source_oss.str()))
			{
				remove(target_oss.str().c_str());
				rename(source_oss.str().c_str(), target_oss.str().c_str());
			}
			//loglog_renaming_result(*loglog, source, target, ret);
		}
		std::stringstream ss(std::stringstream::in | std::stringstream::out);
		ss << kLogFilePath << ".1";
		string path = ss.str();
		rename(kLogFilePath.c_str(), path.c_str());
	}

	static void default_loghandler(LogLevel level, const char* filename,
	        const char* function, int line, const char* format, ...)
	{
		const char* levelstr = 0;
		uint64_t timestamp = get_current_epoch_millis();
		if (level > 0 && level < ALL_LOG_LEVEL)
		{
			levelstr = kLogLevelNames[level - 1];
		}
		else
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
		sprintf(timetag, "%02u-%02u %02u:%02u:%02u", tm.tm_mon + 1, tm.tm_mday,
		        tm.tm_hour, tm.tm_min, tm.tm_sec);
		fprintf(kLogFile, "[%u] %s,%03u %s %s\n", getpid(), timetag, mills,
		        levelstr, record.c_str());
		fflush(kLogFile);
		if (!kLogFilePath.empty() && kLogFile != stdout)
		{
			long file_size = ftell(kLogFile);
			if (file_size < 0)
			{
				reopen_default_logfile();
			}
			else if (file_size >= k_max_file_size)
			{
				rollover_default_logfile();
				reopen_default_logfile();
			}
		}
	}

	static bool default_logchcker(LogLevel level)
	{
		return level <= kDeafultLevel;
	}

	ArdbLogHandler* ArdbLogger::GetLogHandler()
	{
		if (!kLogHandler)
		{
			kLogHandler = default_loghandler;
		}
		return kLogHandler;
	}
	IsLogEnable* ArdbLogger::GetLogChecker()
	{
		if (!kLogChecker)
		{
			kLogChecker = default_logchcker;
		}
		return kLogChecker;
	}
	void ArdbLogger::InstallLogHandler(ArdbLogHandler* h, IsLogEnable* c)
	{
		kLogHandler = h;
		kLogChecker = c;
	}

	void ArdbLogger::InitDefaultLogger(const std::string& level,
	        const std::string& logfile)
	{
		if(!logfile.empty() && (logfile != "stdout" && logfile != "stderr"))
		{
			kLogFilePath = logfile;
			reopen_default_logfile();
		}
		std::string level_str = string_toupper(level);
		for (uint32 i = 0; (i + 1) < ALL_LOG_LEVEL; i++)
		{
			if (level_str == kLogLevelNames[i])
			{
				kDeafultLevel = (LogLevel)(i + 1);
			}
		}
	}

	void ArdbLogger::DestroyDefaultLogger()
	{
		if(kLogFile != stdout)
		{
			fclose(kLogFile);
		}
	}
}

