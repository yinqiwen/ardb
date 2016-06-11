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

#ifndef STRING_HELPER_HPP_
#define STRING_HELPER_HPP_
#include "common.hpp"
#include <string>
#include <vector>
#include <limits.h>
#include <sstream>

namespace ardb
{
    std::string get_basename(const std::string& filename);

    char* trim_str(char* str, const char* cset);
    std::string trim_string(const std::string& str, const std::string& set = " \t\r\n");

    std::vector<char*> split_str(char* str, const char* sep);
    std::vector<std::string> split_string(const std::string& str, const std::string& sep);
    int split_uint32_array(const std::string& str, const std::string& sep, std::vector<uint32>& array);
    void split_string(const std::string& strs, const std::string& sp, std::vector<std::string>& res);

    int string_replace(std::string& str, const std::string& pattern, const std::string& newpat);

    char* str_tolower(char* str);
    char* str_toupper(char* str);
    std::string string_tolower(const std::string& str);
    std::string string_toupper(const std::string& str);
    void lower_string(std::string& str);
    void upper_string(std::string& str);

    bool str_toint64(const char* str, int64& value);
    bool str_touint64(const char* str, uint64& value);
    inline bool string_toint64(const std::string& str, int64& value)
    {
        return str_toint64(str.c_str(), value);
    }
    inline bool string_touint64(const std::string& str, uint64& value)
    {
        return str_touint64(str.c_str(), value);
    }

    inline bool str_toint32(const char* str, int32& value)
    {
        int64 temp;
        if (str_toint64(str, temp))
        {
            if (temp > INT_MAX || temp < INT_MIN)
            {
                return false;
            }
            value = static_cast<int32>(temp);
            return true;
        }
        return false;
    }
    inline bool string_toint32(const std::string& str, int32& value)
    {
        return str_toint32(str.c_str(), value);
    }

    inline bool str_touint32(const char* str, uint32& value)
    {
        int64 temp;
        if (str_toint64(str, temp))
        {
            if (temp < 0 || temp > 0xffffffff)
            {
                return false;
            }
            value = static_cast<uint32>(temp);
            return true;
        }
        return false;
    }
    inline bool string_touint32(const std::string& str, uint32& value)
    {
        return str_touint32(str.c_str(), value);
    }

    bool str_tofloat(const char* str, float& value);
    inline bool string_tofloat(const std::string& str, float& value)
    {
        return str_tofloat(str.c_str(), value);
    }

    bool str_todouble(const char* str, double& value);
    inline bool string_todouble(const std::string& str, double& value)
    {
        return str_todouble(str.c_str(), value);
    }

    void fast_dtoa(double value, int prec, std::string& result);
    int fast_itoa(char* str, uint32 strlen, uint64 i);

    bool has_prefix(const std::string& str, const std::string& prefix);
    bool has_suffix(const std::string& str, const std::string& suffix);

    std::string random_string(uint32 len);
    std::string random_between_string(const std::string& min, const std::string& max);
    std::string random_hex_string(uint32 len);

    std::string ascii_codes(const std::string& str);

    std::string sha1_sum(const std::string& str);
    std::string sha1_sum_data(const void* data, size_t len);

    int string2ll(const char *s, size_t slen, int64_t *value);
    int ll2string(char *s, size_t len, long long value);
    int lf2string(char* s, size_t len, double value);

    std::string stringfromll(int64 v);
    std::string base16_stringfromllu(uint64 v);

    template<typename T>
    std::string string_join_container(const T& container, const std::string& sep)
    {
        typename T::const_iterator it = container.begin();
        std::stringstream buffer;
        while (it != container.end())
        {
            buffer<< (*it);
            if (*it != *(container.rbegin()))
            {
                buffer << sep;
            }
            it++;
        }
        return buffer.str();
    }

    int stringmatchlen(const char *pattern, int patternLen, const char *string, int stringLen, int nocase);
    int stringmatch(const char *pattern, const char *string, int nocase);
    bool is_pattern_string(const std::string& str);
}

#endif /* STRING_HELPER_HPP_ */
