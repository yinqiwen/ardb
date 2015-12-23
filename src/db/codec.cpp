/*
 *Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
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

#include "codec.hpp"
#include "buffer/buffer_helper.hpp"
#include "buffer/struct_codec_macros.hpp"
#include <cmath>

OP_NAMESPACE_BEGIN

    enum DataEncoding
    {
        E_INT64 = 1, E_FLOAT64 = 2, E_CSTR = 3, E_SDS = 4,
    };

    Data::Data() :
            encoding(0), len(0)
    {
        //value.iv = 0;
    }
    Data::Data(const std::string& v, bool try_int_encoding) :
            encoding(0)
    {
        SetString(v, try_int_encoding);
    }
    Data::~Data()
    {
        Clear();
    }

    Data::Data(const Data& other) :
            encoding(other.encoding), len(other.len)
    {
        Clone(other);
    }

    Data& Data::operator=(const Data& data)
    {
        return *this;
    }

    void Data::Encode(Buffer& buf) const
    {
        buf.WriteByte((char) encoding);
        switch (encoding)
        {
            case E_INT64:
            {
                BufferHelper::WriteVarInt64(buf, GetInt64());
                return;
            }
            case E_CSTR:
            case E_SDS:
            {
                BufferHelper::WriteVarUInt32(buf, StringLength());
                const char* ptr = CStr();
                buf.Write(ptr, StringLength());
                return;
            }
            default:
            {
                return;
            }
        }
    }
    bool Data::Decode(Buffer& buf, bool clone_str)
    {
        char header = 0;
        if (!buf.ReadByte(header))
        {
            return false;
        }
        encoding = (uint8) header;
        switch (encoding)
        {
            case E_INT64:
            {
                int64_t v;
                if (!BufferHelper::ReadVarInt64(buf, v))
                {
                    return false;
                }
                SetInt64(v);
                return true;
            }
            case E_CSTR:
            case E_SDS:
            {
                uint32_t strlen = 0;
                if (!BufferHelper::ReadVarUInt32(buf, strlen))
                {
                    return false;
                }
                const char* ss = buf.GetRawReadBuffer();
                Clear();
                len = strlen;
                if (clone_str)
                {
                    void* s = malloc(strlen);
                    memcpy(s, (char*) ss, strlen);
                    *(void**) data = s;
                    encoding = E_SDS;
                }
                else
                {
                    *(void**) data = ss;
                    encoding = E_CSTR;
                }
                return true;
            }
            default:
            {
                return false;
            }
        }
    }

    void Data::SetString(const std::string& str, bool try_int_encoding)
    {
        long long int_val;
        if (str.size() <= 21 && string2ll(str.data(), str.size(), &int_val))
        {
            SetInt64((int64) int_val);
            return;
        }
        Clear();
        *(void**) data = str.data();
        len = str.size();
        encoding = E_CSTR;
    }
    void Data::SetInt64(int64 v)
    {
        Clear();
        encoding = E_INT64;
        memcpy(data, &v, sizeof(int64));
        len = digits10(std::abs(v));
        if (v < 0)
            len++;
    }
    int64 Data::GetInt64() const
    {
        int64 v = 0;
        if (IsInteger())
        {
            memcpy(&v, data, sizeof(int64));
        }
        return v;
    }

    void Data::Clone(const Data& other)
    {
        Clear();
        encoding = other.encoding;
        len = other.len;
        memcpy(data, other.data, sizeof(data));
        if (encoding == E_SDS)
        {
            void* s = malloc(other.len);
            memcpy(s, (char*) other.data, other.len);
            *(void**) data = s;
        }
    }
    int Data::Compare(const Data& right, bool alpha_cmp = false) const
    {
        if (!alpha_cmp)
        {
            if (IsInteger() && right.IsInteger())
            {
                return GetInt64() - right.GetInt64();
            }
            //integer is always less than text value in non alpha comparator
            if (IsInteger())
            {
                return -1;
            }
            if (right.IsInteger())
            {
                return 1;
            }
        }
        size_t min_len = len < right.len ? len : right.len;
        const char* other_raw_data = right.CStr();
        const char* raw_data = CStr();
        if (encoding == E_INT64)
        {
            char* data_buf = (char*) alloca(len);
            ll2string(data_buf, len, GetInt64());
            raw_data = data_buf;
        }
        if (right.encoding == E_INT64)
        {
            char* data_buf = (char*) alloca(right.len);
            ll2string(data_buf, right.len, right.GetInt64());
            other_raw_data = data_buf;
        }
        int ret = memcmp(raw_data, other_raw_data, min_len);
        if (ret < 0)
        {
            return -1;
        }
        else if (ret > 0)
        {
            return 1;
        }
        return len - right.len;
    }

    bool Data::IsInteger() const
    {
        return encoding == E_INT64;
    }
    uint32 Data::StringLength() const
    {
        return len;
    }
    void Data::Clear()
    {
        if (encoding == E_SDS)
        {
            free((char*) data);
        }
        encoding = 0;
        len = 0;
    }
    const char* Data::CStr() const
    {
        switch (encoding)
        {
            case E_INT64:
            {
                return NULL;
            }
            case E_CSTR:
            case E_SDS:
            {
                void* ptr = *(void**) data;
                return (const char*) ptr;
            }
            default:
            {
                return NULL;
            }
        }
    }
    const std::string& Data::ToString(std::string& str) const
    {
        switch (encoding)
        {
            case E_INT64:
            {
                str.resize(len);
                ll2string(&(str[0]), len, *((long long*) data));
                break;
            }
            case E_CSTR:
            case E_SDS:
            {
                str.assign(CStr(), len);
                break;
            }
            default:
            {
                break;
            }
        }
        return str;
    }

    size_t DataHash::operator()(const Data& t) const
    {
        if(t.IsInteger())
        {
            return (size_t)t.GetInt64();
        }

    }

    bool DataEqual::operator()(const Data& s1, const Data& s2) const
    {
        return s1.Compare(s2, false) == 0;
    }

OP_NAMESPACE_END

