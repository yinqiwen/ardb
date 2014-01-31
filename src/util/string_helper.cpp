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

#include "util/string_helper.hpp"
#include "util/math_helper.hpp"
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <stdexcept>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include "util/sha1.h"

namespace ardb
{

    std::string get_basename(const std::string& filename)
    {
#if defined(_WIN32)
        char dir_sep('\\');
#else
        char dir_sep('/');
#endif

        std::string::size_type pos = filename.rfind(dir_sep);
        if (pos != std::string::npos)
            return filename.substr(pos + 1);
        else
            return filename;
    }

    char* trim_str(char* s, const char* cset)
    {
        RETURN_NULL_IF_NULL(s);
        if (NULL == cset)
        {
            return s;
        }
        char *start, *end, *sp, *ep;
        size_t len;

        sp = start = s;
        ep = end = s + strlen(s) - 1;
        while (*sp && strchr(cset, *sp))
            sp++;
        while (ep > sp && strchr(cset, *ep))
            ep--;
        len = (sp > ep) ? 0 : ((ep - sp) + 1);
        if (start != sp)
            memmove(start, sp, len);
        start[len] = '\0';
        return start;
    }

    std::string trim_string(const std::string& str, const std::string& cset)
    {
        if (str.empty())
        {
            return str;
        }
        size_t start = 0;
        size_t str_len = str.size();
        size_t end = str_len - 1;
        while (start < str_len && cset.find(str.at(start)) != std::string::npos)
        {
            start++;
        }
        while (end > start && end < str_len && cset.find(str.at(end)) != std::string::npos)
        {
            end--;
        }
        return str.substr(start, end - start + 1);
    }

    std::vector<char*> split_str(char* str, const char* sep)
    {
        std::vector<char*> ret;
        char* start = str;
        char* found = NULL;
        uint32 sep_len = strlen(sep);
        while (*start && (found = strstr(start, sep)) != NULL)
        {
            *found = 0;
            if (*start)
            {
                ret.push_back(start);
            }
            start = (found + sep_len);
        }
        if (*start)
        {
            ret.push_back(start);
        }
        return ret;
    }

    std::vector<std::string> split_string(const std::string& str, const std::string& sep)
    {
        std::vector<std::string> ret;
        size_t start = 0;
        size_t str_len = str.size();
        size_t found = std::string::npos;
        while (start < str_len && (found = str.find(sep, start)) != std::string::npos)
        {
            if (found > start)
            {
                ret.push_back(str.substr(start, found - start));
            }
            start = (found + sep.size());
        }
        if (start < str_len)
        {
            ret.push_back(str.substr(start));
        }
        return ret;
    }

    void split_string(const std::string& strs, const std::string& sp, std::vector<std::string>& res)
    {
        std::string::size_type pos1, pos2;

        pos2 = strs.find(sp);
        pos1 = 0;

        while (std::string::npos != pos2)
        {
            res.push_back(strs.substr(pos1, pos2 - pos1));
            pos1 = pos2 + sp.length();
            pos2 = strs.find(sp, pos1);
        }

        res.push_back(strs.substr(pos1));
    }

    int string_replace(std::string& str, const std::string& pattern, const std::string& newpat)
    {
        int count = 0;
        const size_t nsize = newpat.size();
        const size_t psize = pattern.size();

        for (size_t pos = str.find(pattern, 0); pos != std::string::npos; pos = str.find(pattern, pos + nsize))
        {
            str.replace(pos, psize, newpat);
            count++;
        }

        return count;
    }

    char* str_tolower(char* str)
    {
        char* start = str;
        while (*start)
        {
            *start = tolower(*start);
            start++;
        }
        return str;
    }
    char* str_toupper(char* str)
    {
        char* start = str;
        while (*start)
        {
            *start = toupper(*start);
            start++;
        }
        return str;
    }

    void lower_string(std::string& str)
    {
        uint32 i = 0;
        std::string ret;
        while (i < str.size())
        {
            str[i] = tolower(str.at(i));
            i++;
        }
    }
    void upper_string(std::string& str)
    {
        uint32 i = 0;
        std::string ret;
        while (i < str.size())
        {
            str[i] = toupper(str.at(i));
            i++;
        }
    }

    std::string string_tolower(const std::string& str)
    {
        uint32 i = 0;
        std::string ret;
        while (i < str.size())
        {
            char ch = tolower(str.at(i));
            ret.push_back(ch);
            i++;
        }
        return ret;
    }
    std::string string_toupper(const std::string& str)
    {
        uint32 i = 0;
        std::string ret;
        while (i < str.size())
        {
            char ch = toupper(str.at(i));
            ret.push_back(ch);
            i++;
        }
        return ret;
    }

    bool str_toint64(const char* str, int64& value)
    {
        RETURN_FALSE_IF_NULL(str);
        char *endptr = NULL;
        long long int val = strtoll(str, &endptr, 10);
        if (NULL == endptr || 0 != *endptr)
        {
            return false;
        }
        value = val;
        return true;
    }

    bool str_touint64(const char* str, uint64& value)
    {
        int64 temp;
        if (!str_toint64(str, temp))
        {
            return false;
        }
        value = (uint64) temp;
        return true;
    }

    bool str_tofloat(const char* str, float& value)
    {
        RETURN_FALSE_IF_NULL(str);
        char *endptr = NULL;
        float val = strtof(str, &endptr);
        if (NULL == endptr || 0 != *endptr)
        {
            return false;
        }
        value = val;
        return true;
    }

    bool str_todouble(const char* str, double& value)
    {
        RETURN_FALSE_IF_NULL(str);
        char *endptr = NULL;
        double val = strtod(str, &endptr);
        if (NULL == endptr || 0 != *endptr)
        {
            return false;
        }
        value = val;
        return true;
    }

    bool has_prefix(const std::string& str, const std::string& prefix)
    {
        if (prefix.empty())
        {
            return true;
        }
        if (str.size() < prefix.size())
        {
            return false;
        }
        return str.find(prefix) == 0;
    }
    bool has_suffix(const std::string& str, const std::string& suffix)
    {
        if (suffix.empty())
        {
            return true;
        }
        if (str.size() < suffix.size())
        {
            return false;
        }
        return str.rfind(suffix) == str.size() - suffix.size();
    }

    static inline void strreverse(char* begin, char* end)
    {
        char aux;
        while (end > begin)
            aux = *end, *end-- = *begin, *begin++ = aux;
    }

    static void ensure_space(char*& str, int& slen, char*& wstr)
    {
        if (wstr - str == slen)
        {
            int pos = slen;
            slen *= 2;
            str = (char*) realloc(str, slen);
            wstr = str + pos;
        }
    }

    void fast_dtoa(double value, int prec, std::string& result)
    {
        static const double powers_of_10[] =
            { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000 };
        /* Hacky test for NaN
         * under -fast-math this won't work, but then you also won't
         * have correct nan values anyways.  The alternative is
         * to link with libmath (bad) or hack IEEE double bits (bad)
         */

        if (!(value == value))
        {
//            str[0] = 'n';
//            str[1] = 'a';
//            str[2] = 'n';
//            str[3] = '\0';
            result = "nan";
            return;
        }
        int slen = 256;
        char* str = (char*) malloc(slen);
        /* if input is larger than thres_max, revert to exponential */
        const double thres_max = (double) (0x7FFFFFFF);

        int count;
        double diff = 0.0;
        char* wstr = str;

        if (prec < 0)
        {
            prec = 0;
        }
        else if (prec > 9)
        {
            /* precision of >= 10 can lead to overflow errors */
            prec = 9;
        }

        /* we'll work in positive values and deal with the
         negative sign issue later */
        int neg = 0;
        if (value < 0)
        {
            neg = 1;
            value = -value;
        }

        int whole = (int) value;
        double tmp = (value - whole) * powers_of_10[prec];
        uint32_t frac = (uint32_t) (tmp);
        diff = tmp - frac;

        if (diff > 0.5)
        {
            ++frac;
            /* handle rollover, e.g.  case 0.99 with prec 1 is 1.0  */
            if (frac >= powers_of_10[prec])
            {
                frac = 0;
                ++whole;
            }
        }
        else if (diff == 0.5 && ((frac == 0) || (frac & 1)))
        {
            /* if halfway, round up if odd, OR
             if last digit is 0.  That last part is strange */
            ++frac;
        }

        /* for very large numbers switch back to native sprintf for exponentials.
         anyone want to write code to replace this? */
        /*
         normal printf behavior is to print EVERY whole number digit
         which can be 100s of characters overflowing your buffers == bad
         */
        if (value > thres_max)
        {
            while (snprintf(str, slen, "%e", neg ? -value : value) < 0)
            {
                slen *= 2;
                str = (char*) realloc(str, slen);
            }
            result.assign(str, (wstr - str));
            free(str);
            return;
        }

        if (prec == 0)
        {
            diff = value - whole;
            if (diff > 0.5)
            {
                /* greater than 0.5, round up, e.g. 1.6 -> 2 */
                ++whole;
            }
            else if (diff == 0.5 && (whole & 1))
            {
                /* exactly 0.5 and ODD, then round up */
                /* 1.5 -> 2, but 2.5 -> 2 */
                ++whole;
            }

            //vvvvvvvvvvvvvvvvvvv  Diff from modp_dto2
        }
        else if (frac)
        {
            count = prec;
            // now do fractional part, as an unsigned number
            // we know it is not 0 but we can have leading zeros, these
            // should be removed
            while (!(frac % 10))
            {
                --count;
                frac /= 10;
            }
            //^^^^^^^^^^^^^^^^^^^  Diff from modp_dto2

            // now do fractional part, as an unsigned number
            do
            {
                --count;
                ensure_space(str, slen, wstr);
                *wstr++ = (char) (48 + (frac % 10));
            } while (frac /= 10);
            // add extra 0s
            while (count-- > 0)
            {
                ensure_space(str, slen, wstr);
                *wstr++ = '0';
            }
            // add decimal
            ensure_space(str, slen, wstr);
            *wstr++ = '.';
        }

        // do whole part
        // Take care of sign
        // Conversion. Number is reversed.
        do
        {
            ensure_space(str, slen, wstr);
            *wstr++ = (char) (48 + (whole % 10));
        } while (whole /= 10);
        if (neg)
        {
            ensure_space(str, slen, wstr);
            *wstr++ = '-';
        }
        ensure_space(str, slen, wstr);
        *wstr = '\0';
        strreverse(str, wstr - 1);
        result.assign(str, (wstr - str));
        free(str);
        return;
    }

    int fast_itoa(char* dst, uint32 dstlen, uint64 value)
    {
        static const char digits[201] = "0001020304050607080910111213141516171819"
                "2021222324252627282930313233343536373839"
                "4041424344454647484950515253545556575859"
                "6061626364656667686970717273747576777879"
                "8081828384858687888990919293949596979899";
        uint32_t const length = digits10(value);
        if (length >= dstlen)
        {
            return -1;
        }
        uint32_t next = length - 1;
        while (value >= 100)
        {
            uint32 i = (value % 100) * 2;
            value /= 100;
            dst[next] = digits[i + 1];
            dst[next - 1] = digits[i];
            next -= 2;
        }
        // Handle last 1-2 digits
        if (value < 10)
        {
            dst[next] = '0' + uint32_t(value);
        }
        else
        {
            uint32 i = uint32(value) * 2;
            dst[next] = digits[i + 1];
            dst[next - 1] = digits[i];
        }
        return length;
    }

    std::string random_between_string(const std::string& min, const std::string& max)
    {
        if (min == max)
        {
            return min;
        }
        std::string random_key;
        if (min.size() == max.size())
        {
            random_key.resize(max.size());
            bool fill_by_random = false;
            bool fill_by_random_gt_min = false;
            for (uint32 i = 0; i < min.size(); i++)
            {
                if (fill_by_random_gt_min)
                {
                    random_key[i] = (char) random_between_int32(min[i], 127);
                    if (random_key[i] > min[i])
                    {
                        fill_by_random = true;
                    }
                    else
                    {
                        fill_by_random_gt_min = true;
                    }
                    continue;
                }
                if (fill_by_random)
                {
                    random_key[i] = (char) random_between_int32(0, 127);
                }
                else
                {
                    if (min[i] < max[i])
                    {
                        random_key[i] = (char) random_between_int32(min[i], max[i]);
                        if (random_key[i] > min[i])
                        {
                            fill_by_random = true;
                        }
                        else
                        {
                            fill_by_random_gt_min = true;
                        }
                    }
                    else
                    {
                        random_key[i] = min[i];
                    }
                }

            }
        }
        else
        {
            uint32 randomsize = (uint32)random_between_int32(min.size(), max.size());
            bool gt_than_min = (randomsize == min.size());
            bool less_than_max = (randomsize == max.size());
            if (!gt_than_min && !less_than_max)
            {
                random_key = random_string(randomsize);
            }
            else
            {
                random_key.resize(randomsize);
                bool fill_by_random = false;
                for (uint32 i = 0; i < randomsize; i++)
                {
                    if (fill_by_random)
                    {
                        random_key[i] = (char) random_between_int32(0, 127);
                    }
                    else
                    {
                        if (gt_than_min)
                        {
                            random_key[i] = (char) random_between_int32(min[i], 127);
                            if (random_key[i] > min[i])
                            {
                                fill_by_random = true;
                            }
                        }
                        else
                        {
                            random_key[i] = (char) random_between_int32(-128, max[i]);
                            if (random_key[i] < max[i])
                            {
                                fill_by_random = true;
                            }
                        }
                    }
                }
            }
        }
        return random_key;
    }

    std::string random_string(uint32 len)
    {
        static const char alphanum[] = "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";
        srand(time(NULL));
        char buf[len];
        for (uint32 i = 0; i < len; ++i)
        {
            buf[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
        }
        return std::string(buf, len);
    }

    /* Generate the Redis "Run ID", a SHA1-sized random number that identifies a
     * given execution of Redis, so that if you are talking with an instance
     * having run_id == A, and you reconnect and it has run_id == B, you can be
     * sure that it is either a different instance or it was restarted. */
    std::string random_hex_string(uint32 len)
    {
        char p[len];
        FILE *fp = fopen("/dev/urandom", "r");
        const char *charset = "0123456789abcdef";
        unsigned int j;

        if (fp == NULL || fread(p, len, 1, fp) == 0)
        {
            /* If we can't read from /dev/urandom, do some reasonable effort
             * in order to create some entropy, since this function is used to
             * generate run_id and cluster instance IDs */
            char *x = p;
            unsigned int l = len;
            struct timeval tv;
            pid_t pid = getpid();

            /* Use time and PID to fill the initial array. */
            gettimeofday(&tv, NULL);
            if (l >= sizeof(tv.tv_usec))
            {
                memcpy(x, &tv.tv_usec, sizeof(tv.tv_usec));
                l -= sizeof(tv.tv_usec);
                x += sizeof(tv.tv_usec);
            }
            if (l >= sizeof(tv.tv_sec))
            {
                memcpy(x, &tv.tv_sec, sizeof(tv.tv_sec));
                l -= sizeof(tv.tv_sec);
                x += sizeof(tv.tv_sec);
            }
            if (l >= sizeof(pid))
            {
                memcpy(x, &pid, sizeof(pid));
                l -= sizeof(pid);
                x += sizeof(pid);
            }
            /* Finally xor it with rand() output, that was already seeded with
             * time() at startup. */
            for (j = 0; j < len; j++)
                p[j] ^= rand();
        }
        /* Turn it into hex digits taking just 4 bits out of 8 for every byte. */
        for (j = 0; j < len; j++)
            p[j] = charset[p[j] & 0x0F];
        fclose(fp);
        return std::string(p, len);
    }

    std::string sha1_sum(const std::string& str)
    {
        SHA1_CTX ctx;
        unsigned char hash[20];
        const char *cset = "0123456789abcdef";
        int j;

        SHA1Init(&ctx);
        SHA1Update(&ctx, (const unsigned char*) str.data(), str.size());
        SHA1Final(hash, &ctx);
        char digest[40];
        for (j = 0; j < 20; j++)
        {
            digest[j * 2] = cset[((hash[j] & 0xF0) >> 4)];
            digest[j * 2 + 1] = cset[(hash[j] & 0xF)];
        }
        return std::string(digest, 40);
    }

    std::string sha1_sum_data(const void* data, size_t len)
    {
        SHA1_CTX ctx;
        unsigned char hash[20];
        const char *cset = "0123456789abcdef";
        int j;

        SHA1Init(&ctx);
        SHA1Update(&ctx, (const unsigned char*) data, len);
        SHA1Final(hash, &ctx);
        char digest[40];
        for (j = 0; j < 20; j++)
        {
            digest[j * 2] = cset[((hash[j] & 0xF0) >> 4)];
            digest[j * 2 + 1] = cset[(hash[j] & 0xF)];
        }
        return std::string(digest, 40);
    }

    /* Convert a string into a long long. Returns 1 if the string could be parsed
     * into a (non-overflowing) long long, 0 otherwise. The value will be set to
     * the parsed value when appropriate. */
    int string2ll(const char *s, size_t slen, long long *value)
    {
        const char *p = s;
        size_t plen = 0;
        int negative = 0;
        unsigned long long v;

        if (plen == slen)
            return 0;

        /* Special case: first and only digit is 0. */
        if (slen == 1 && p[0] == '0')
        {
            if (value != NULL)
                *value = 0;
            return 1;
        }

        if (p[0] == '-')
        {
            negative = 1;
            p++;
            plen++;

            /* Abort on only a negative sign. */
            if (plen == slen)
                return 0;
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
            *value = 0;
            return 1;
        }
        else
        {
            return 0;
        }

        while (plen < slen && p[0] >= '0' && p[0] <= '9')
        {
            if (v > (ULLONG_MAX / 10)) /* Overflow. */
                return 0;
            v *= 10;

            if (v > (ULLONG_MAX - (p[0] - '0'))) /* Overflow. */
                return 0;
            v += p[0] - '0';

            p++;
            plen++;
        }

        /* Return if not all bytes were used. */
        if (plen < slen)
            return 0;

        if (negative)
        {
            if (v > ((unsigned long long) (-(LLONG_MIN + 1)) + 1)) /* Overflow. */
                return 0;
            if (value != NULL)
                *value = -v;
        }
        else
        {
            if (v > LLONG_MAX) /* Overflow. */
                return 0;
            if (value != NULL)
                *value = v;
        }
        return 1;
    }

    int ll2string(char *s, size_t len, long long value)
    {
        char buf[32], *p;
        unsigned long long v;
        size_t l;

        if (len == 0)
            return 0;
        v = (value < 0) ? -value : value;
        p = buf + 31; /* point to the last character */
        do
        {
            *p-- = '0' + (v % 10);
            v /= 10;
        } while (v);
        if (value < 0)
            *p-- = '-';
        p++;
        l = 32 - (p - buf);
        if (l + 1 > len)
            l = len - 1; /* Make sure it fits, including the nul term */
        memcpy(s, p, l);
        s[l] = '\0';
        return l;
    }

    std::string stringfromll(int64 value)
    {
        size_t l;
        char buf[32];
        char* p = NULL;
        unsigned long long v;

        v = (value < 0) ? -value : value;
        p = buf + 31; /* point to the last character */
        do
        {
            *p-- = '0' + (v % 10);
            v /= 10;
        } while (v);
        if (value < 0)
            *p-- = '-';
        p++;
        l = 32 - (p - buf);
        return std::string(p, l);
    }
}
