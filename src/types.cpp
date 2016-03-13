/*
 * types.cpp
 *
 *  Created on: 2016Äê3ÔÂ2ÈÕ
 *      Author: wangqiying
 */

#include "types.hpp"
#include "buffer/buffer_helper.hpp"
#include "util/murmur3.h"
#include "util/string_helper.hpp"
#include "util/math_helper.hpp"
#include <cmath>

OP_NAMESPACE_BEGIN

    enum DataEncoding
    {
        E_INT64 = 1, E_FLOAT64 = 2, E_CSTR = 3, E_SDS = 4,
    };

    Data::Data() :
            data(0), len(0), encoding(0)
    {
        //value.iv = 0;
    }
    Data::Data(const std::string& v, bool try_int_encoding) :
            data(0), len(0), encoding(0)
    {
        SetString(v, try_int_encoding);
    }

    Data::Data(int64_t v) :
            data(v), len(0), encoding(E_INT64)
    {
    }
    Data::Data(long double v) :
            len(0), encoding(E_FLOAT64)
    {
        memcpy(&data, &v, sizeof(data));
    }
    Data::~Data()
    {
        Clear();
    }

    Data::Data(const Data& other) :
            len(other.len), encoding(other.encoding)
    {
        Clone(other);
    }

    Data& Data::operator=(const Data& data)
    {
        Clone(data);
        return *this;
    }

    void* Data::ReserveStringSpace(size_t size)
    {
        if (encoding != E_SDS)
        {
            Clear();
        }
        if (StringLength() < size)
        {
            void* s = malloc(size);
            if (IsString())
            {
                memcpy(s, CStr(), StringLength());
                Clear();
            }
            data = (int64_t) s;
            encoding = E_SDS;
        }
        return (void*) data;
    }

    void Data::Encode(Buffer& buf) const
    {
        buf.WriteByte((char) encoding);
        switch (encoding)
        {
            case E_FLOAT64:
            case E_INT64:
            {
                buf.Write(&data, sizeof(data));
                //BufferHelper::WriteVarInt64(buf, GetInt64());
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
            case E_FLOAT64:
            {
//                int64_t v;
                data = *(int64_t*) buf.GetRawReadBuffer();
                buf.AdvanceReadIndex(sizeof(data));
//                buf.Read(&data, sizeof(data));
//                if (!BufferHelper::ReadVarInt64(buf, v))
//                {
//                    return false;
//                }
//                SetInt64(v);
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
                    data = (int64_t) s;
                    memcpy(s, (char*) ss, strlen);
                    //*(void**) (&data) = s;
                    encoding = E_SDS;
                }
                else
                {
                    memcpy(&data, &ss, sizeof(const char*));
                    //*(void**) (&data) = ss;
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
        int64_t int_val;
        if (str.size() <= 21 && string2ll(str.data(), str.size(), &int_val))
        {
            SetInt64((int64) int_val);
            return;
        }
        Clear();
        data = (int64_t) str.data();
        //*(void**) (&data) = str.data();
        len = str.size();
        encoding = E_CSTR;
    }
    void Data::SetInt64(int64 v)
    {
        Clear();
        encoding = E_INT64;
        data = v;
    }
    void Data::SetFloat64(long double v)
    {
        Clear();
        encoding = E_FLOAT64;
        *(long double*) (&data) = v;
    }
    int64 Data::GetInt64() const
    {
        if (IsInteger())
        {
            return data;
        }
        return 0;
    }

    long double Data::GetFloat64() const
    {
        long double v = 0;
        if (IsFloat())
        {
            v = *(long double*) (&data);
        }
        else if (IsInteger())
        {
            v = data;
        }
        return v;
    }

    void Data::Clone(const Data& other)
    {
        Clear();
        encoding = other.encoding;
        len = other.len;
        if (encoding == E_SDS)
        {
            void* s = malloc(other.len);
            data = (int64_t) s;
//            memcpy(s, (char*) (&other.data), other.len);
//            *(void**) (&data) = s;
        }
        else
        {
            memcpy(&data, &other.data, sizeof(data));
        }
    }
    int Data::Compare(const Data& right, bool alpha_cmp) const
    {
        if (!alpha_cmp)
        {
            if (IsInteger() && right.IsInteger())
            {
                return GetInt64() - right.GetInt64();
            }
            if (IsNumber() && right.IsNumber())
            {
                long double v1, v2;
                v1 = GetFloat64();
                v2 = right.GetFloat64();
                return v1 > v2 ? 1 : (v1 < v2 ? -1 : 0);
            }
            //number is always less than text value in non alpha comparator
            if (IsNumber())
            {
                return -1;
            }
            if (right.IsNumber())
            {
                return 1;
            }
        }

        const char* other_raw_data = right.CStr();
        const char* raw_data = CStr();
        uint32 left_len = len, right_len = right.len;
        if (encoding == E_INT64)
        {
            left_len = digits10(data);
            char* data_buf = (char*) alloca(left_len);
            ll2string(data_buf, left_len, GetInt64());
            raw_data = data_buf;
        }
        else if (encoding == E_FLOAT64)
        {
            left_len = 256;
            char* data_buf = (char*) alloca(left_len);
            left_len = lf2string(data_buf, left_len, GetFloat64());
            raw_data = data_buf;
        }
        if (right.encoding == E_INT64)
        {
            right_len = digits10(right.data);
            char* data_buf = (char*) alloca(right_len);
            ll2string(data_buf, right_len, right.GetInt64());
            other_raw_data = data_buf;
        }
        else if (right.encoding == E_FLOAT64)
        {
            right_len = 256;
            char* data_buf = (char*) alloca(right_len);
            right_len = lf2string(data_buf, right_len, right.GetFloat64());
            other_raw_data = data_buf;
        }
        size_t min_len = left_len < right_len ? left_len : right_len;
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
    bool Data::IsFloat() const
    {
        return encoding == E_FLOAT64;
    }
    bool Data::IsNil() const
    {
        return encoding == 0;
    }
    bool Data::IsString() const
    {
        return encoding == E_SDS || encoding == E_CSTR;
    }

    uint32 Data::StringLength() const
    {
        if (encoding == E_INT64)
        {
            return digits10(data);
        }
        return len;
    }
    void Data::Clear()
    {
        if (encoding == E_SDS)
        {
            free((char*) (data));
        }
        encoding = 0;
        len = 0;
        data = 0;
    }
    const char* Data::CStr() const
    {
        switch (encoding)
        {
            case E_INT64:
            case E_FLOAT64:
            {
                return NULL;
            }
            case E_CSTR:
            case E_SDS:
            {
                void* ptr = (void*) data;
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
        uint32 slen = 0;
        switch (encoding)
        {
            case E_INT64:
            {
                slen = StringLength() + 1;
                str.resize(slen);
                slen = ll2string(&(str[0]), slen, data);
                str.resize(slen);
                break;
            }
            case E_FLOAT64:
            {
                slen = 256;
                str.resize(slen);
                slen = lf2string(&(str[0]), slen - 1, GetFloat64());
                str.resize(slen);
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
        if (t.IsInteger())
        {
            return (size_t) t.GetInt64();
        }
        return 0;
    }

    bool DataEqual::operator()(const Data& s1, const Data& s2) const
    {
        return s1.Compare(s2, false) == 0;
    }

OP_NAMESPACE_END
