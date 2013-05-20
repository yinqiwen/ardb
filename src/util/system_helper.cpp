/*
 * system_helper.cpp
 *
 *  Created on: 2011-3-1
 *      Author: qiyingwang
 */
#include "system_helper.hpp"
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
}
