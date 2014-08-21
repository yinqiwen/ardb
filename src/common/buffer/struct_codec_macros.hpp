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
/*
 * struct_codec_macros.hpp
 *
 *  Created on: 2011-9-15
 *      Author: yinqiwen
 */

#ifndef STRUCT_CODEC_MACROS_HPP_
#define STRUCT_CODEC_MACROS_HPP_
#include "buffer/buffer_helper.hpp"
#include "buffer/struct_code_templates.hpp"
#include <string>
#include <list>
#include <map>
#include <vector>
#include <deque>
#include <utility>
#include <btree_set.h>
#include <btree_map.h>

using ardb::Buffer;

#define ENCODE_DEFINE(...) \
    public:                  \
    bool Encode(Buffer& buf)  \
    { \
        return ardb::encode_arg(buf,__VA_ARGS__); \
    } \

#define DECODE_DEFINE(...) \
    public:                  \
    bool Decode(Buffer& buf)  \
    { \
        return ardb::decode_arg(buf, __VA_ARGS__); \
    } \

#define ENCODE2_DEFINE(...) \
    public:                  \
    bool Encode(Buffer& buf)  \
    { \
        return ardb::encode_arg_with_tag(buf,__VA_ARGS__); \
    } \

#define DECODE2_DEFINE(...) \
    public:                  \
    bool Decode(Buffer& buf)  \
    { \
        return ardb::decode_arg_with_tag(buf, __VA_ARGS__); \
    } \

#define CODEC2_DEFINE(...) \
            ENCODE2_DEFINE(__VA_ARGS__)\
            DECODE2_DEFINE(__VA_ARGS__)

#define ENCODE_DEFINE_P(...) \
    public:                  \
    bool Encode(Buffer* buf)  \
    { \
        RETURN_FALSE_IF_NULL(buf); \
        return ardb::encode_arg(*buf,__VA_ARGS__); \
    }

#define DECODE_DEFINE_P(...) \
    public:                  \
    bool Decode(Buffer* buf)  \
    { \
        RETURN_FALSE_IF_NULL(buf); \
        return ardb::decode_arg(*buf, __VA_ARGS__); \
    }

#define FIX_ENCODE_DEFINE_P(...) \
    public:                  \
    bool Encode(Buffer* buf)  \
    { \
        RETURN_FALSE_IF_NULL(buf); \
        return ardb::fix_encode_arg(*buf,__VA_ARGS__); \
    }

#define FIX_DECODE_DEFINE_P(...) \
    public:                  \
    bool Decode(Buffer* buf)  \
    { \
        RETURN_FALSE_IF_NULL(buf); \
        return ardb::fix_decode_arg(*buf, __VA_ARGS__); \
    }

#define CODEC_DEFINE(...) \
        ENCODE_DEFINE(__VA_ARGS__)\
        DECODE_DEFINE(__VA_ARGS__)

#define CODEC_DEFINE_P(...) \
        ENCODE_DEFINE_P(__VA_ARGS__)\
        DECODE_DEFINE_P(__VA_ARGS__)

#define FIX_CODEC_DEFINE_P(...) \
        FIX_ENCODE_DEFINE_P(__VA_ARGS__)\
        FIX_DECODE_DEFINE_P(__VA_ARGS__)

namespace ardb
{
    inline bool encode_arg(Buffer& buf)
    {
        return true;
    }

    inline bool encode_arg(Buffer& buf, std::string& a0)
    {
        BufferHelper::WriteVarString(buf, a0);
        return true;
    }

    inline bool encode_arg(Buffer& buf, float& a0)
    {
        BufferHelper::WriteFixFloat(buf, a0);
        return true;
    }

    inline bool encode_arg(Buffer& buf, const double& a0)
    {
        BufferHelper::WriteFixDouble(buf, a0);
        return true;
    }

    inline bool encode_arg(Buffer& buf, uint8& a0)
    {
        BufferHelper::WriteFixUInt8(buf, a0);
        return true;
    }
    inline bool encode_arg(Buffer& buf, const uint8& a0)
    {
        BufferHelper::WriteFixUInt8(buf, a0);
        return true;
    }
    inline bool encode_arg(Buffer& buf, int8& a0)
    {
        BufferHelper::WriteFixInt8(buf, a0);
        return true;
    }

    inline bool encode_arg(Buffer& buf, const int8& a0)
    {
        BufferHelper::WriteFixInt8(buf, a0);
        return true;
    }

    inline bool encode_arg(Buffer& buf, bool& a0)
    {
        BufferHelper::WriteBool(buf, a0);
        return true;
    }
    inline bool encode_arg(Buffer& buf, const bool& a0)
    {
        BufferHelper::WriteBool(buf, a0);
        return true;
    }
    inline bool encode_arg(Buffer& buf, uint16& a0)
    {
        BufferHelper::WriteVarUInt16(buf, a0);
        return true;
    }
    inline bool encode_arg(Buffer& buf, const uint16& a0)
    {
        BufferHelper::WriteVarUInt16(buf, a0);
        return true;
    }
    inline bool encode_arg(Buffer& buf, int16& a0)
    {
        BufferHelper::WriteVarInt16(buf, a0);
        return true;
    }
    inline bool encode_arg(Buffer& buf, const int16& a0)
    {
        BufferHelper::WriteVarInt16(buf, a0);
        return true;
    }
    inline bool encode_arg(Buffer& buf, uint32& a0)
    {
        BufferHelper::WriteVarUInt32(buf, a0);
        return true;
    }
    inline bool encode_arg(Buffer& buf, const uint32& a0)
    {
        BufferHelper::WriteVarUInt32(buf, a0);
        return true;
    }
    inline bool encode_arg(Buffer& buf, int32& a0)
    {
        BufferHelper::WriteVarInt32(buf, a0);
        return true;
    }
    inline bool encode_arg(Buffer& buf, const int32& a0)
    {
        BufferHelper::WriteVarInt32(buf, a0);
        return true;
    }
    inline bool encode_arg(Buffer& buf, uint64& a0)
    {
        BufferHelper::WriteVarUInt64(buf, a0);
        return true;
    }
    inline bool encode_arg(Buffer& buf, const uint64& a0)
    {
        BufferHelper::WriteVarUInt64(buf, a0);
        return true;
    }
    inline bool encode_arg(Buffer& buf, int64& a0)
    {
        BufferHelper::WriteVarInt64(buf, a0);
        return true;
    }
    inline bool encode_arg(Buffer& buf, const int64& a0)
    {
        BufferHelper::WriteVarInt64(buf, a0);
        return true;
    }
    inline bool encode_arg(Buffer& buf, const std::string& a0)
    {
        BufferHelper::WriteVarString(buf, a0);
        return true;
    }

    inline bool encode_arg(Buffer& buf, const char*& a0)
    {
        BufferHelper::WriteVarString(buf, a0);
        return true;
    }

    //fix encode
    inline bool fix_encode_arg(Buffer& buf)
    {
        return true;
    }

    template<typename A0>
    inline bool fix_encode_arg(Buffer& buf, A0& a0)
    {
        return a0.Encode(buf);
    }

    template<typename T>
    inline bool fix_encode_arg(Buffer& buf, std::list<T>& a0)
    {
        BufferHelper::WriteFixUInt32(buf, a0.size());
        typename std::list<T>::iterator iter = a0.begin();
        while (iter != a0.end())
        {
            fix_encode_arg(buf, *iter);
            iter++;
        }
        return true;
    }

    template<typename T>
    inline bool fix_encode_arg(Buffer& buf, std::vector<T>& a0)
    {
        BufferHelper::WriteFixUInt32(buf, a0.size());
        typename std::vector<T>::iterator iter = a0.begin();
        while (iter != a0.end())
        {
            fix_encode_arg(buf, *iter);
            iter++;
        }
        return true;
    }

    template<typename T>
    inline bool fix_encode_arg(Buffer& buf, std::deque<T>& a0)
    {
        BufferHelper::WriteFixUInt32(buf, a0.size());
        typename std::deque<T>::iterator iter = a0.begin();
        while (iter != a0.end())
        {
            fix_encode_arg(buf, *iter);
            iter++;
        }
        return true;
    }

    template<typename K, typename V>
    inline bool fix_encode_arg(Buffer& buf, std::map<K, V>& a0)
    {
        BufferHelper::WriteFixUInt32(buf, a0.size());
        typename std::map<K, V>::iterator iter = a0.begin();
        while (iter != a0.end())
        {
            fix_encode_arg(buf, iter->first);
            fix_encode_arg(buf, iter->second);
            iter++;
        }
        return true;
    }

    inline bool fix_encode_arg(Buffer& buf, float& a0)
    {
        BufferHelper::WriteFixFloat(buf, a0);
        return true;
    }
    inline bool fix_encode_arg(Buffer& buf, double& a0)
    {
        BufferHelper::WriteFixDouble(buf, a0);
        return true;
    }

    inline bool fix_encode_arg(Buffer& buf, uint8_t& a0)
    {
        BufferHelper::WriteFixUInt8(buf, a0);
        return true;
    }
    inline bool fix_encode_arg(Buffer& buf, const uint8_t& a0)
    {
        BufferHelper::WriteFixUInt8(buf, a0);
        return true;
    }
    inline bool fix_encode_arg(Buffer& buf, int8_t& a0)
    {
        BufferHelper::WriteFixInt8(buf, a0);
        return true;
    }

    inline bool fix_encode_arg(Buffer& buf, const int8_t& a0)
    {
        BufferHelper::WriteFixInt8(buf, a0);
        return true;
    }

    inline bool fix_encode_arg(Buffer& buf, bool& a0)
    {
        BufferHelper::WriteBool(buf, a0);
        return true;
    }
    inline bool fix_encode_arg(Buffer& buf, const bool& a0)
    {
        BufferHelper::WriteBool(buf, a0);
        return true;
    }
    inline bool fix_encode_arg(Buffer& buf, uint16_t& a0)
    {
        BufferHelper::WriteFixUInt16(buf, a0);
        return true;
    }
    inline bool fix_encode_arg(Buffer& buf, const uint16_t& a0)
    {
        BufferHelper::WriteFixUInt16(buf, a0);
        return true;
    }
    inline bool fix_encode_arg(Buffer& buf, int16_t& a0)
    {
        BufferHelper::WriteFixInt16(buf, a0);
        return true;
    }
    inline bool fix_encode_arg(Buffer& buf, const int16_t& a0)
    {
        BufferHelper::WriteFixInt16(buf, a0);
        return true;
    }
    inline bool fix_encode_arg(Buffer& buf, uint32_t& a0)
    {
        BufferHelper::WriteFixUInt32(buf, a0);
        return true;
    }
    inline bool fix_encode_arg(Buffer& buf, const uint32_t& a0)
    {
        BufferHelper::WriteFixUInt32(buf, a0);
        return true;
    }
    inline bool fix_encode_arg(Buffer& buf, int32_t& a0)
    {
        BufferHelper::WriteFixInt32(buf, a0);
        return true;
    }
    inline bool fix_encode_arg(Buffer& buf, const int32_t& a0)
    {
        BufferHelper::WriteFixInt32(buf, a0);
        return true;
    }
    inline bool fix_encode_arg(Buffer& buf, uint64_t& a0)
    {
        BufferHelper::WriteFixUInt64(buf, a0);
        return true;
    }
    inline bool fix_encode_arg(Buffer& buf, const uint64_t& a0)
    {
        BufferHelper::WriteFixUInt64(buf, a0);
        return true;
    }
    inline bool fix_encode_arg(Buffer& buf, int64_t& a0)
    {
        BufferHelper::WriteFixInt64(buf, a0);
        return true;
    }
    inline bool fix_encode_arg(Buffer& buf, const int64_t& a0)
    {
        BufferHelper::WriteFixInt64(buf, a0);
        return true;
    }
    inline bool fix_encode_arg(Buffer& buf, std::string& a0)
    {
        BufferHelper::WriteFixString(buf, a0);
        return true;
    }
    inline bool fix_encode_arg(Buffer& buf, const std::string& a0)
    {
        BufferHelper::WriteFixString(buf, a0);
        return true;
    }

    inline bool fix_encode_arg(Buffer& buf, const char*& a0)
    {
        BufferHelper::WriteFixString(buf, a0);
        return true;
    }

    inline bool decode_arg(Buffer& buf)
    {
        return true;
    }

    template<typename A0>
    inline bool decode_arg(Buffer& buf, A0& a0)
    {
        return a0.Decode(buf);
    }

    //template<>
    inline bool decode_arg(Buffer& buf, float& a0)
    {
        return BufferHelper::ReadFixFloat(buf, a0);
    }
    inline bool decode_arg(Buffer& buf, double& a0)
    {
        return BufferHelper::ReadFixDouble(buf, a0);
    }

    inline bool decode_arg(Buffer& buf, uint8& a0)
    {
        return BufferHelper::ReadFixUInt8(buf, a0);
    }
    inline bool decode_arg(Buffer& buf, int8& a0)
    {
        return BufferHelper::ReadFixInt8(buf, a0);
    }

    inline bool decode_arg(Buffer& buf, bool& a0)
    {
        return BufferHelper::ReadBool(buf, a0);
    }

    //template<>
    inline bool decode_arg(Buffer& buf, uint16& a0)
    {
        return BufferHelper::ReadVarUInt16(buf, a0);
    }
    inline bool decode_arg(Buffer& buf, int16& a0)
    {
        return BufferHelper::ReadVarInt16(buf, a0);
    }

    //template<>
    inline bool decode_arg(Buffer& buf, uint32& a0)
    {
        return BufferHelper::ReadVarUInt32(buf, a0);
    }
    inline bool decode_arg(Buffer& buf, int32& a0)
    {
        return BufferHelper::ReadVarInt32(buf, a0);
    }
    //template<>
    inline bool decode_arg(Buffer& buf, uint64& a0)
    {
        return BufferHelper::ReadVarUInt64(buf, a0);
    }
    inline bool decode_arg(Buffer& buf, int64& a0)
    {
        return BufferHelper::ReadVarInt64(buf, a0);
    }
    //template<>
    inline bool decode_arg(Buffer& buf, std::string& a0)
    {
        return BufferHelper::ReadVarString(buf, a0);
    }
    //template<>
    inline bool decode_arg(Buffer& buf, char*& a0)
    {
        return BufferHelper::ReadVarString(buf, a0);
    }

    //Fix decode
    template<typename A0>
    inline bool fix_decode_arg(Buffer& buf, A0& a0)
    {
        return a0.Decode(buf);
    }

    template<typename T>
    inline bool fix_decode_arg(Buffer& buf, std::list<T>& a0)
    {
        uint32 size;
        if (!BufferHelper::ReadFixUInt32(buf, size))
        {
            return false;
        }
        uint32 i = 0;
        while (i < size)
        {
            T t;
            if (!fix_decode_arg(buf, t))
            {
                return false;
            }
            a0.push_back(t);
            i++;
        }
        return true;
    }

    template<typename T>
    inline bool fix_decode_arg(Buffer& buf, std::vector<T>& a0)
    {
        uint32 size;
        if (!BufferHelper::ReadFixUInt32(buf, size))
        {
            return false;
        }
        uint32 i = 0;
        while (i < size)
        {
            T t;
            if (!fix_decode_arg(buf, t))
            {
                return false;
            }
            a0.push_back(t);
            i++;
        }
        return true;
    }

    template<typename T>
    inline bool fix_decode_arg(Buffer& buf, std::deque<T>& a0)
    {
        uint32 size;
        if (!BufferHelper::ReadFixUInt32(buf, size))
        {
            return false;
        }
        uint32 i = 0;
        while (i < size)
        {
            T t;
            if (!fix_decode_arg(buf, t))
            {
                return false;
            }
            a0.push_back(t);
            i++;
        }
        return true;
    }

    template<typename K, typename V>
    inline bool fix_decode_arg(Buffer& buf, std::map<K, V>& a0)
    {
        uint32 size;
        if (!BufferHelper::ReadFixUInt32(buf, size))
        {
            return false;
        }
        uint32 i = 0;
        while (i < size)
        {
            K k;
            V v;
            if (!fix_decode_arg(buf, k) || !fix_decode_arg(buf, v))
            {
                return false;
            }
            a0.insert(std::make_pair(k, v));
            i++;
        }
        return true;
    }

    //template<>
    inline bool fix_decode_arg(Buffer& buf, uint8_t& a0)
    {
        return BufferHelper::ReadFixUInt8(buf, a0);
    }
    inline bool fix_decode_arg(Buffer& buf, int8_t& a0)
    {
        return BufferHelper::ReadFixInt8(buf, a0);
    }

    inline bool fix_decode_arg(Buffer& buf, bool& a0)
    {
        return BufferHelper::ReadBool(buf, a0);
    }

    //template<>
    inline bool fix_decode_arg(Buffer& buf, uint16_t& a0)
    {
        return BufferHelper::ReadFixUInt16(buf, a0);
    }
    inline bool fix_decode_arg(Buffer& buf, int16_t& a0)
    {
        return BufferHelper::ReadFixInt16(buf, a0);
    }

    //template<>
    inline bool fix_decode_arg(Buffer& buf, uint32_t& a0)
    {
        return BufferHelper::ReadFixUInt32(buf, a0);
    }
    inline bool fix_decode_arg(Buffer& buf, int32_t& a0)
    {
        return BufferHelper::ReadFixInt32(buf, a0);
    }
    //template<>
    inline bool fix_decode_arg(Buffer& buf, uint64_t& a0)
    {
        return BufferHelper::ReadFixUInt64(buf, a0);
    }
    inline bool fix_decode_arg(Buffer& buf, int64_t& a0)
    {
        return BufferHelper::ReadFixInt64(buf, a0);
    }
    //template<>
    inline bool fix_decode_arg(Buffer& buf, std::string& a0)
    {
        return BufferHelper::ReadFixString(buf, a0);
    }
    //template<>
    inline bool fix_decode_arg(Buffer& buf, char*& a0)
    {
        return BufferHelper::ReadFixString(buf, a0);
    }

    template<typename A0>
    inline bool encode_arg(Buffer& buf, A0* a0)
    {
        return a0->Encode(buf);
    }

    template<typename A0>
    inline bool encode_arg(Buffer& buf, A0& a0)
    {
        return a0.Encode(buf);
    }

    template<typename T>
    inline bool encode_arg(Buffer& buf, std::list<T>& a0)
    {
        BufferHelper::WriteVarUInt32(buf, a0.size());
        typename std::list<T>::iterator iter = a0.begin();
        while (iter != a0.end())
        {
            encode_arg(buf, *iter);
            iter++;
        }
        return true;
    }

    template<typename T>
    inline bool encode_arg(Buffer& buf, std::set<T>& a0)
    {
        BufferHelper::WriteVarUInt32(buf, a0.size());
        typename std::set<T>::iterator iter = a0.begin();
        while (iter != a0.end())
        {
            encode_arg(buf, *iter);
            iter++;
        }
        return true;
    }

    template<typename T>
    inline bool encode_arg(Buffer& buf, btree::btree_set<T>& a0)
    {
        BufferHelper::WriteVarUInt32(buf, a0.size());
        typename btree::btree_set<T>::iterator iter = a0.begin();
        while (iter != a0.end())
        {
            encode_arg(buf, *iter);
            iter++;
        }
        return true;
    }

    template<typename T>
    inline bool encode_arg(Buffer& buf, std::vector<T>& a0)
    {
        BufferHelper::WriteVarUInt32(buf, a0.size());
        typename std::vector<T>::iterator iter = a0.begin();
        while (iter != a0.end())
        {
            encode_arg(buf, *iter);
            iter++;
        }
        return true;
    }

    template<typename T>
    inline bool encode_arg(Buffer& buf, std::deque<T>& a0)
    {
        BufferHelper::WriteVarUInt32(buf, a0.size());
        typename std::deque<T>::iterator iter = a0.begin();
        while (iter != a0.end())
        {
            encode_arg(buf, *iter);
            iter++;
        }
        return true;
    }

    template<typename K, typename V>
    inline bool encode_arg(Buffer& buf, std::map<K, V>& a0)
    {
        BufferHelper::WriteVarUInt32(buf, a0.size());
        typename std::map<K, V>::iterator iter = a0.begin();
        while (iter != a0.end())
        {
            encode_arg(buf, iter->first);
            encode_arg(buf, iter->second);
            iter++;
        }
        return true;
    }

    template<typename K, typename V>
    inline bool encode_arg(Buffer& buf, btree::btree_map<K, V>& a0)
    {
        BufferHelper::WriteVarUInt32(buf, a0.size());
        typename btree::btree_map<K, V>::iterator iter = a0.begin();
        while (iter != a0.end())
        {
            encode_arg(buf, iter->first);
            encode_arg(buf, iter->second);
            iter++;
        }
        return true;
    }

    template<typename T>
    inline bool decode_arg(Buffer& buf, std::list<T>& a0)
    {
        uint32 size;
        if (!BufferHelper::ReadVarUInt32(buf, size))
        {
            return false;
        }
        uint32 i = 0;
        while (i < size)
        {
            T t;
            if (!decode_arg(buf, t))
            {
                return false;
            }
            a0.push_back(t);
            i++;
        }
        return true;
    }

    template<typename T>
    inline bool decode_arg(Buffer& buf, std::set<T>& a0)
    {
        uint32 size;
        if (!BufferHelper::ReadVarUInt32(buf, size))
        {
            return false;
        }
        uint32 i = 0;
        while (i < size)
        {
            T t;
            if (!decode_arg(buf, t))
            {
                return false;
            }
            a0.insert(t);
            i++;
        }
        return true;
    }

    template<typename T>
    inline bool decode_arg(Buffer& buf, btree::btree_set<T>& a0)
    {
        uint32 size;
        if (!BufferHelper::ReadVarUInt32(buf, size))
        {
            return false;
        }
        uint32 i = 0;
        while (i < size)
        {
            T t;
            if (!decode_arg(buf, t))
            {
                return false;
            }
            a0.insert(t);
            i++;
        }
        return true;
    }

    template<typename T>
    inline bool decode_arg(Buffer& buf, std::vector<T>& a0)
    {
        uint32 size;
        if (!BufferHelper::ReadVarUInt32(buf, size))
        {
            return false;
        }
        uint32 i = 0;
        while (i < size)
        {
            T t;
            if (!decode_arg(buf, t))
            {
                return false;
            }
            a0.push_back(t);
            i++;
        }
        return true;
    }

    template<typename T>
    inline bool decode_arg(Buffer& buf, std::deque<T>& a0)
    {
        uint32 size;
        if (!BufferHelper::ReadVarUInt32(buf, size))
        {
            return false;
        }
        uint32 i = 0;
        while (i < size)
        {
            T t;
            if (!decode_arg(buf, t))
            {
                return false;
            }
            a0.push_back(t);
            i++;
        }
        return true;
    }

    template<typename K, typename V>
    inline bool decode_arg(Buffer& buf, std::map<K, V>& a0)
    {
        uint32 size;
        if (!BufferHelper::ReadVarUInt32(buf, size))
        {
            return false;
        }
        uint32 i = 0;
        while (i < size)
        {
            K k;
            V v;
            if (!decode_arg(buf, k) || !decode_arg(buf, v))
            {
                return false;
            }
            a0.insert(std::make_pair(k, v));
            i++;
        }
        return true;
    }

    template<typename K, typename V>
    inline bool decode_arg(Buffer& buf, btree::btree_map<K, V>& a0)
    {
        uint32 size;
        if (!BufferHelper::ReadVarUInt32(buf, size))
        {
            return false;
        }
        uint32 i = 0;
        while (i < size)
        {
            K k;
            V v;
            if (!decode_arg(buf, k) || !decode_arg(buf, v))
            {
                return false;
            }
            a0.insert(std::make_pair(k, v));
            i++;
        }
        return true;
    }

}

#endif /* STRUCT_CODEC_MACROS_HPP_ */
