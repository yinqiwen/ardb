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
}

#endif /* HELPERS_HPP_ */
