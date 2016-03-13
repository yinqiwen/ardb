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

#ifndef HELPERS_HPP_
#define HELPERS_HPP_
#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include <inttypes.h>
#include <string>
#include <stdio.h>
#include "common.hpp"
#include "time_helper.hpp"
#include "file_helper.hpp"
#include "string_helper.hpp"
#include "math_helper.hpp"
#include "network_helper.hpp"
#include "system_helper.hpp"
#include "version_helper.hpp"

namespace ardb
{
    inline bool raw_toint64(const void* raw, uint32_t slen, int64_t& value)
    {
        if (NULL == raw)
        {
            return false;
        }
        const char *p = (const char*) raw;
        size_t plen = 0;
        int negative = 0;
        unsigned long long v;

        if (plen == slen)
            return false;

        /* Special case: first and only digit is 0. */
        if (slen == 1 && p[0] == '0')
        {
            value = 0;
            return true;
        }

        if (p[0] == '-')
        {
            negative = 1;
            p++;
            plen++;

            /* Abort on only a negative sign. */
            if (plen == slen)
                return false;
        }

        /* First digit should be 1-9, otherwise the string should just be 0. */
        if (p[0] >= '1' && p[0] <= '9')
        {
            v = p[0] - '0';
            p++;
            plen++;
        }
        else if (p[0] == '0' && slen == 1)
        {
            value = 0;
            return true;
        }
        else
        {
            return false;
        }

        while (plen < slen && p[0] >= '0' && p[0] <= '9')
        {
            if (v > (ULLONG_MAX / 10)) /* Overflow. */
                return false;
            v *= 10;

            if (v > (ULLONG_MAX - (p[0] - '0'))) /* Overflow. */
                return false;
            v += p[0] - '0';

            p++;
            plen++;
        }

        /* Return if not all bytes were used. */
        if (plen < slen)
            return false;

        if (negative)
        {
            if (v > ((unsigned long long) (-(LLONG_MIN + 1)) + 1)) /* Overflow. */
                return false;
            value = -v;
        }
        else
        {
            if (v > LLONG_MAX) /* Overflow. */
                return false;
            value = v;
        }
        return true;
    }

    inline std::string double_tostring(double d)
    {
        double min = -4503599627370495LL; /* (2^52)-1 */
        double max = 4503599627370496LL; /* -(2^52) */
        int64 iv = (int64) d;
        if (d > min && d < max && d == ((double) iv))
        {
            char tmp[256];
            sprintf(tmp, "%" PRId64, iv);
            return tmp;
        }
        else
        {
            char tmp[256];
            snprintf(tmp, sizeof(tmp) - 1, "%.17g", d);
            return tmp;
        }
    }

    inline bool raw_todouble(const void* raw, uint32_t slen, double& value)
    {
        if (NULL == raw || slen == 0)
        {
            return false;
        }
        const char* str = (const char*) raw;
        const char* p = str;
        uint32_t plen = slen;

        if (p[0] == '-')
        {
            p++;
            plen--;
        }
        if (plen == 0)
        {
            return false;
        }
        if (p[0] == '0')
        {
            if (plen > 1)
            {
                if (p[1] != '.')
                {
                    return false;
                }
            }
        }
        char *endptr = NULL;
        double val = strtod(str, &endptr);
        if (NULL == endptr || (uint32) (endptr - str) != slen)
        {
            return false;
        }
        bool founddot = false;
        for (uint32 i = 0; i < slen; i++)
        {
            if (str[i] == '.')
            {
                founddot = true;
                break;
            }
        }
        if (founddot)
        {
            value = val;
        }
        return founddot;
    }
}

#endif /* HELPERS_HPP_ */
