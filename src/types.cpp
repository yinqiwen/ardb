/*
 * types.cpp
 *
 *  Created on: 2016-03-02
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
        E_INT64 = 1, E_FLOAT64 = 2, E_CSTR = 3, E_SDS = 4
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
    Data::Data(double v) :
            len(0), encoding(E_FLOAT64)
    {
        memcpy(&data, &v, sizeof(data));
    }
    Data Data::WrapCStr(const std::string& str)
    {
        Data data;
        data.SetString(str.data(), str.size(), false);
        return data;
    }
    Data::~Data()
    {
        Clear();
    }

    Data::Data(const Data& other) :
            data(0), len(0), encoding(0)
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
        ToMutableStr();
        if (StringLength() < size)
        {
            void* s = malloc(size);
            memset((char*) s + StringLength(), 0, size - StringLength());
            if (IsString())
            {
                memcpy(s, CStr(), StringLength());
                Clear();
            }
            data = (int64_t) s;
            encoding = E_SDS;
            this->len = size;
        }
        return (void*) data;
    }

    void Data::Encode(Buffer& buf) const
    {
        switch (encoding)
        {
            case E_FLOAT64:
            case E_INT64:
            {
                buf.WriteByte((char) encoding);
                BufferHelper::WriteVarInt64(buf, data);
                return;
            }
            case E_CSTR:
            case E_SDS:
            {
                /*
                 * all string encode as SDS
                 */
                buf.WriteByte((char) E_SDS);
                BufferHelper::WriteVarUInt32(buf, StringLength());
                const char* ptr = CStr();
                buf.Write(ptr, StringLength());
                return;
            }
            default:
            {
                buf.WriteByte((char) encoding);
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
        Clear();
        encoding = (uint8) header;
        switch (encoding)
        {
            case 0:
            {
                return true;
            }
            case E_INT64:
            case E_FLOAT64:
            {
                return BufferHelper::ReadVarInt64(buf, data);

//                if (buf.ReadableBytes() < sizeof(data))
//                {
//                    return false;
//                }
//                memcpy(&data, buf.GetRawReadBuffer(), sizeof(data));
//                buf.AdvanceReadIndex(sizeof(data));
//                return true;
            }
            case E_CSTR:
            case E_SDS:
            {
                uint32_t strlen = 0;
                if (!BufferHelper::ReadVarUInt32(buf, strlen))
                {
                    return false;
                }
                if (buf.ReadableBytes() < strlen)
                {
                    return false;
                }
                const char* ss = buf.GetRawReadBuffer();
                buf.AdvanceReadIndex(strlen);
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
                    data = (int64_t) ss;
                    //memcpy(&data, &ss, sizeof(const char*));
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

    void Data::SetString(const char* str, size_t slen, bool clone)
    {
        Clear();
        if (clone)
        {
            void* s = malloc(slen);
            data = (int64_t) s;
            memcpy(s, (char*) str, slen);
            encoding = E_SDS;
        }
        else
        {
            data = (int64_t)str;
            //memcpy(&data, &str, sizeof(const char*));
            encoding = E_CSTR;
        }
        len = slen;
    }

    void Data::SetString(const std::string& str, bool try_int_encoding, bool clone)
    {
        Clear();
        int64_t int_val;
        if (try_int_encoding && str.size() <= 21 && string2ll(str.data(), str.size(), &int_val))
        {
            SetInt64((int64) int_val);
            return;
        }
        SetString(str.data(), str.size(), clone);
    }

    void Data::SetString(const std::string& str, bool try_int_encoding)
    {
        SetString(str, try_int_encoding, true);
    }
    void Data::SetInt64(int64 v)
    {
        Clear();
        encoding = E_INT64;
        data = v;
    }
    void Data::SetFloat64(double v)
    {
        Clear();
        encoding = E_FLOAT64;
        memcpy(&data, &v, sizeof(data));
    }
    int64 Data::GetInt64() const
    {
        if (IsInteger())
        {
            return data;
        }
        return 0;
    }

    double Data::GetFloat64() const
    {
        double v = 0;
        if (IsFloat())
        {
            memcpy(&v, &data, sizeof(data));
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
        if (other.encoding == E_SDS)
        {
            void* s = malloc(other.len);
            data = (int64_t) s;
            memcpy(s, other.CStr(), other.StringLength());
            encoding = E_SDS;
        }
        else
        {
            data = other.data;
        }
    }
    int Data::Compare(const Data& right, bool alpha_cmp) const
    {
        if (IsNil() || right.IsNil())
        {
            return right.IsNil() - IsNil();
        }
        if (!alpha_cmp)
        {
            if (IsInteger() && right.IsInteger())
            {
                return GetInt64() - right.GetInt64();
            }
            if (IsNumber() && right.IsNumber())
            {
                double v1, v2;
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
        if(0 == min_len)
        {
            return left_len - right_len;
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
        return left_len - right_len;
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

    bool Data::IsCStr() const
    {
        return encoding == E_CSTR;
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
            void* s = (void*) data;
            free(s);
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
        str.clear();
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

    char* Data::ToMutableStr()
    {
        switch (encoding)
        {
            case E_INT64:
            case E_FLOAT64:
            case E_CSTR:
            {
                std::string ss;
                ToString(ss);
                SetString(ss, false);
                return const_cast<char*>(CStr());
            }
            case E_SDS:
            {
                return const_cast<char*>(CStr());
            }
            default:
            {
                return NULL;
            }
        }
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
