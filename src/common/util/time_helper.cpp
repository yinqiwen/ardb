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

#include "util/time_helper.hpp"
#include <limits.h>

//static const uint32 kmax_uint32 = 0xffffffff;
namespace ardb
{
    static struct tm k_tm;

    static uint32 k_current_day_start_ts = 0;
    static uint32 k_tomorrow_day_start_ts = 0;
    static const uint32 k_one_day_secs = 24 * 3600;

    static void reinit_tm(time_t now)
    {
        //struct tm t;
        localtime_r(&now, &k_tm);
        k_current_day_start_ts = now - (k_tm.tm_hour * 3600) - (k_tm.tm_min * 60) - k_tm.tm_sec;
        k_tomorrow_day_start_ts = k_current_day_start_ts + k_one_day_secs;
    }

    static uint32 init_tm(time_t now)
    {
        static bool k_inited = false;
        if(0 == now)
        {
            now = time(NULL);
        }
        if (!k_inited || now >= k_tomorrow_day_start_ts)
        {
            reinit_tm(now);
            k_inited = true;
        }
        return now;
    }

    void init_timeval(uint64 time, TimeUnit unit, struct timeval& val)
    {
        uint64 micros = 0;
        switch (unit)
        {
            case NANOS:
            {
                micros = time / 1000;
                break;
            }
            case MICROS:
            {
                micros = time;
                break;
            }
            case MILLIS:
            {
                micros = time * 1000;
                break;
            }
            case SECONDS:
            {
                micros = time * 1000 * 1000;
                break;
            }
            case MINUTES:
            {
                micros = time * 1000 * 1000 * 60;
                break;
            }
            case HOURS:
            {
                micros = time * 1000 * 1000 * 60 * 60;
                break;
            }
            case DAYS:
            {
                micros = time * 1000 * 1000 * 60 * 60 * 24;
                break;
            }
            default:
                break;
        }
        if (micros < 1000000)
        {
            val.tv_sec = 0;
            val.tv_usec = (long) micros;
        }
        else
        {
            val.tv_sec = micros / 1000000;
            val.tv_usec = micros % 1000000;
        }
    }

    void init_timespec(uint64 time, TimeUnit unit, struct timespec& val)
    {
        uint64 nanos = 0;
        switch (unit)
        {
            case NANOS:
            {
                nanos = time;
                break;
            }
            case MICROS:
            {
                nanos = time * 1000;
                break;
            }
            case MILLIS:
            {
                nanos = time * 1000 * 1000;
                break;
            }
            case SECONDS:
            {
                nanos = time * 1000 * 1000 * 1000;
                break;
            }
            case MINUTES:
            {
                nanos = time * 1000 * 1000 * 1000 * 60;
                break;
            }
            case HOURS:
            {
                nanos = time * 1000 * 1000 * 1000 * 60 * 60;
                break;
            }
            case DAYS:
            {
                nanos = time * 1000 * 1000 * 1000 * 60 * 60 * 24;
                break;
            }
            default:
                break;
        }
        if (nanos < 1000000000)
        {
            val.tv_sec = 0;
            val.tv_nsec = (long) nanos;
        }
        else
        {
            val.tv_sec = nanos / 1000000000;
            val.tv_nsec = nanos % 1000000000;
        }
    }

    void add_nanos(struct timespec& val, uint64 nanos)
    {
        uint64 added_nanos = nanos + val.tv_nsec;
        if (added_nanos >= 1000000000)
        {
            val.tv_sec += added_nanos / 1000000000;
            val.tv_nsec = added_nanos % 1000000000;
        }
        else
        {
            val.tv_nsec = added_nanos;
        }
    }

    void add_micros(struct timespec& val, uint64 micros)
    {
        uint64 added_nanos = micros * 1000 + val.tv_nsec;
        if (added_nanos >= 1000000000)
        {
            val.tv_sec += added_nanos / 1000000000;
            val.tv_nsec = added_nanos % 1000000000;
        }
        else
        {
            val.tv_nsec = added_nanos;
        }
    }
    void add_millis(struct timespec& val, uint64 millis)
    {
        uint64 added_nanos = millis * 1000 * 1000 + val.tv_nsec;
        if (added_nanos >= 1000000000)
        {
            val.tv_sec += added_nanos / 1000000000;
            val.tv_nsec = added_nanos % 1000000000;
        }
        else
        {
            val.tv_nsec = added_nanos;
        }
    }

    void add_micros(struct timeval& val, uint64 micros)
    {
        uint64 added_micros = micros + val.tv_usec;
        if (added_micros >= 1000000)
        {
            val.tv_sec += added_micros / 1000000;
            val.tv_usec = added_micros % 1000000;
        }
        else
        {
            val.tv_usec = added_micros;
        }
    }
    void add_millis(struct timeval& val, uint64 millis)
    {
        uint64 added_micros = millis * 1000 + val.tv_usec;
        if (added_micros >= 1000000)
        {
            val.tv_sec += added_micros / 1000000;
            val.tv_usec = added_micros % 1000000;
        }
        else
        {
            val.tv_usec = added_micros;
        }
    }

    uint64 nanostime(uint64 time, TimeUnit unit)
    {
        switch (unit)
        {
            case NANOS:
            {
                return time;
            }
            case MICROS:
            {
                return time * 1000;
            }
            case MILLIS:
            {
                return time * 1000 * 1000;
            }
            case SECONDS:
            {
                return time * 1000 * 1000 * 1000;
            }
            case MINUTES:
            {
                return time * 1000 * 1000 * 1000 * 60;
            }
            case HOURS:
            {
                return time * 1000 * 1000 * 1000 * 60 * 60;
            }
            case DAYS:
            {
                return time * 1000 * 1000 * 1000 * 60 * 60 * 24;
            }
            default:
            {
                return 0;
            }
        }
    }

    uint64 microstime(uint64 time, TimeUnit unit)
    {
        switch (unit)
        {
            case NANOS:
            {
                uint64 ret = time / 1000;
                if (time % 1000 >= 500)
                {
                    ret++;
                }
                return ret;
            }
            case MICROS:
            {
                return time;
            }
            case MILLIS:
            {
                return time * 1000;
            }
            case SECONDS:
            {
                return time * 1000 * 1000;
            }
            case MINUTES:
            {
                return time * 1000 * 1000 * 60;
            }
            case HOURS:
            {
                return time * 1000 * 1000 * 60 * 60;
            }
            case DAYS:
            {
                return time * 1000 * 1000 * 60 * 60 * 24;
            }
            default:
            {
                return 0;
            }
        }
    }

    uint64 millistime(uint64 time, TimeUnit unit)
    {
        switch (unit)
        {
            case NANOS:
            {
                uint64 ret = time / 1000000;
                if (time % 1000000 >= 500000)
                {
                    ret++;
                }
                return ret;
            }
            case MICROS:
            {
                uint64 ret = time / 1000;
                if (time % 1000 >= 500)
                {
                    ret++;
                }
                return ret;
            }
            case MILLIS:
            {
                return time;
            }
            case SECONDS:
            {
                return time * 1000;
            }
            case MINUTES:
            {
                return time * 1000 * 60;
            }
            case HOURS:
            {
                return time * 1000 * 60 * 60;
            }
            case DAYS:
            {
                return time * 1000 * 60 * 60 * 24;
            }
            default:
            {
                return 0;
            }
        }
    }

    uint64 get_current_epoch_millis()
    {
        struct timeval timeValue;
        get_current_epoch_time(timeValue);
        uint64 ret = ((uint64) timeValue.tv_sec) * 1000;
        ret += ((timeValue.tv_usec) / 1000);
        return ret;
    }
    uint64 get_current_epoch_micros()
    {
        struct timeval timeValue;
        get_current_epoch_time(timeValue);
        uint64 micros = ((uint64) timeValue.tv_sec) * 1000000;
        micros += (timeValue.tv_usec);
        return micros;
    }

    uint32 get_current_epoch_seconds()
    {
        return time(NULL);
    }

    struct tm& get_current_tm(time_t now)
    {
        now = init_tm(now);
        k_tm.tm_hour = (now - k_current_day_start_ts) / 3600;
        k_tm.tm_min = ((now - k_current_day_start_ts) % 3600) / 60;
        k_tm.tm_sec = ((now - k_current_day_start_ts) % 3600) % 60;
        return k_tm;
    }

    uint32 get_current_year_day(time_t now)
    {
        init_tm(now);
        return k_tm.tm_yday;
    }
    uint32 get_current_hour(time_t now)
    {
         now = init_tm(now);
        return (now - k_current_day_start_ts) / 3600;
    }
    uint32 get_current_minute(time_t now)
    {
        now = init_tm(now);
        uint32 minsecs = (now - k_current_day_start_ts) % 3600;
        return minsecs / 60;
    }

    uint32 get_current_year(time_t now)
    {
        init_tm(now);
        return k_tm.tm_year + 1900;
    }
    uint8 get_current_month(time_t now)
    {
        init_tm(now);
        return k_tm.tm_mon + 1;
    }
    uint8 get_current_month_day(time_t now)
    {
        init_tm(now);
        return k_tm.tm_mday;
    }
    uint8 get_current_minute_secs(time_t now)
    {
        now = init_tm(now);
        uint32 minsecs = (now - k_current_day_start_ts) % 3600;
        return minsecs % 60;
    }
}
