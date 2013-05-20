/*
 * system_helper.cpp
 *
 *  Created on: 2011-3-1
 *      Author: qiyingwang
 */
#include "system_helper.hpp"
#include <execinfo.h>
#if  __APPLE__
#include <sys/param.h>
#include <sys/sysctl.h>
#endif

namespace ardb
{
	uint32 available_processors()
	{
		uint32 ret = 1;
#ifdef __linux__
		int num = sysconf(_SC_NPROCESSORS_ONLN);
		if (num < 0)
		{
			ret = 1;
		}
		else
		{
			ret = static_cast<uint32>(num);
		}
#elif __APPLE__
		int nm[2];
		size_t len = 4;
		uint32_t count;

		nm[0] = CTL_HW; nm[1] = HW_AVAILCPU;
		sysctl(nm, 2, &count, &len, NULL, 0);

		if(count < 1)
		{
			nm[1] = HW_NCPU;
			sysctl(nm, 2, &count, &len, NULL, 0);
			if(count < 1)
			{	count = 1;}
		}
		return count;
#endif
		return ret;
	}

	int print_stacktrace(std::string& buf)
	{
#define MAX_TRACE_SIZE 100
		int j, nptrs;
		void *buffer[MAX_TRACE_SIZE];
		char **strings;

		nptrs = backtrace(buffer, MAX_TRACE_SIZE);
		//printf("backtrace() returned %d addresses\n", nptrs);

		/* The call backtrace_symbols_fd(buffer, nptrs, STDOUT_FILENO)
		 would produce similar output to the following: */

		strings = backtrace_symbols(buffer, nptrs);
		if (strings == NULL)
		{
			buf += "backtrace_symbols";
			return -1;
		}

		for (j = 0; j < nptrs; j++)
		{
			buf += strings[j];
			if (j != nptrs - 1)
			{
				buf += "\n";
			}
		}
		free(strings);
		return 0;
	}
}
