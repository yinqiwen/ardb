/*
 * helpers.hpp
 *
 *  Created on: 2013-3-28
 *      Author: wqy
 */

#ifndef HELPERS_HPP_
#define HELPERS_HPP_
#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <stdlib.h>
#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif

namespace rddb
{
	inline uint64_t get_current_epoch_nanos()
	{
		struct timespec timeValue;
#ifdef __MACH__ // OS X does not have clock_gettime, use clock_get_time
		clock_serv_t cclock;
		mach_timespec_t mts;
		host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
		clock_get_time(cclock, &mts);
		mach_port_deallocate(mach_task_self(), cclock);
		timeValue.tv_sec = mts.tv_sec;
		timeValue.tv_nsec = mts.tv_nsec;
#else
		clock_gettime(CLOCK_REALTIME, &timeValue);
#endif
		uint64_t ret = ((uint64_t) timeValue.tv_sec) * 1000000000;
		ret += timeValue.tv_nsec;
		return ret;
	}

	inline bool str_toint64(const char* str, int64_t& value)
	{
		if (NULL == str)
		{
			return false;
		}
		char *endptr = NULL;
		long long int val = strtoll(str, &endptr, 10);
		if (NULL == endptr || *endptr != '\0')
		{
			return false;
		}
		value = val;
		return true;
	}

	inline bool raw_toint64(const void* raw, uint32_t len, int64_t& value)
	{
		if (NULL == raw)
		{
			return false;
		}
		const char* str = (const char*) raw;
		char *endptr = NULL;
		long long int val = strtoll(str, &endptr, 10);
		if (NULL == endptr || (endptr - str) != len)
		{
			return false;
		}

		value = val;
		return true;
	}

	inline bool raw_todouble(const void* raw, uint32_t len, double& value)
	{
		if (NULL == raw)
		{
			return false;
		}
		const char* str = (const char*) raw;
		char *endptr = NULL;
		double val = strtod(str, &endptr);
		if (NULL == endptr || (endptr - str) != len)
		{
			return false;
		}

		value = val;
		return true;
	}
}

#endif /* HELPERS_HPP_ */
