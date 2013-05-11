/*
 * TimeHelper.hpp
 *
 *  Created on: 2011-1-12
 *      Author: wqy
 */

#ifndef NOVA_TIMEHELPER_HPP_
#define NOVA_TIMEHELPER_HPP_
#include "common.hpp"
#include "time_unit.hpp"
#include <sys/time.h>
#include <time.h>
namespace ardb
{
	void init_timespec(uint64 time, TimeUnit unit, struct timespec& val);
	void init_timeval(uint64 time, TimeUnit unit, struct timeval& val);
	uint64 nanostime(uint64 time, TimeUnit unit);
	uint64 microstime(uint64 time, TimeUnit unit);
	uint64 millistime(uint64 time, TimeUnit unit);

	void add_nanos(struct timespec& val, uint64 nanos);
	void add_micros(struct timespec& val, uint64 micros);
	void add_millis(struct timespec& val, uint64 millis);

	void add_micros(struct timeval& val, uint64 micros);
	void add_millis(struct timeval& val, uint64 millis);

	inline void get_current_epoch_time(struct timeval& val)
	{
		gettimeofday(&val, NULL);
	}

	uint64 get_current_epoch_millis();
	uint64 get_current_epoch_micros();
	uint32 get_current_epoch_seconds();

	uint32 get_current_year_day();
	uint32 get_current_hour();
	uint32 get_current_minute();
	uint32 get_current_year();
	uint8 get_current_month();
	uint8 get_current_month_day();
	uint8 get_current_minute_secs();
	struct tm& get_current_tm();
}

#endif /* TIMEHELPER_HPP_ */
