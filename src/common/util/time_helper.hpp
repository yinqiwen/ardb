 /*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
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

	uint32 get_current_year_day(time_t now = 0);
	uint32 get_current_hour(time_t now = 0);
	uint32 get_current_minute(time_t now = 0);
	uint32 get_current_year(time_t now = 0);
	uint8 get_current_month(time_t now = 0);
	uint8 get_current_month_day(time_t now = 0);
	uint8 get_current_minute_secs(time_t now = 0);
	struct tm& get_current_tm(time_t now = 0);
}

#endif /* TIMEHELPER_HPP_ */
