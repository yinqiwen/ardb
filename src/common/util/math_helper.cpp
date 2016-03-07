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

#include "util/math_helper.hpp"
#include <stdlib.h>
#include <time.h>

namespace ardb
{
    uint32 upper_power_of_two(uint32 v)
    {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v;
    }

    int32 random_int32()
    {
        srandom(time(NULL));
        return random();
    }

    int32 random_between_int32(int32 min, int32 max)
    {
        if (min == max)
        {
            return min;
        }
        srandom(time(NULL));
        int diff = max - min;
        return (int) (((double) (diff + 1) / RAND_MAX) * rand() + min);
    }

    static const uint64 P12 = 10000LL * 10000LL * 10000LL;
    static const uint64 P11 = 1000000LL * 100000LL;
    static const uint64 P10 = 100000LL * 100000LL;
    static const uint64 P09 = 100000L * 10000L;
    static const uint64 P08 = 10000L * 10000L;
    static const uint64 P07 = 10000L * 1000L;
    static const uint64 P06 = 1000L * 1000L;
    static uint32 lludigits10(uint64 v)
    {
        if (v < 10)
            return 1;
        if (v < 100)
            return 2;
        if (v < 1000)
            return 3;
        if (v < P12)
        {
            if (v < P08)
            {
                if (v < P06)
                {
                    if (v < 10000)
                        return 4;
                    return 5 + (v >= 100000);
                }
                return 7 + (v >= P07);
            }
            if (v < P10)
            {
                return 9 + (v >= P09);
            }
            return 11 + (v >= P11);
        }
        return 12 + digits10(v / P12);
    }

    uint32 digits10(int64 v)
    {
        uint32 len = lludigits10(std::abs(v));
        if (v < 0)
            len++;
        return len;
    }
}
