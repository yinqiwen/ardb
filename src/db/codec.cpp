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
#include "util/murmur3.h"
#include "channel/all_includes.hpp"
#include <cmath>
#include <float.h>

static const uint8 kCurrentMetaFormat = 0;

OP_NAMESPACE_BEGIN

    void KeyObject::SetType(uint8 t)
    {
        type = t;
        switch (type)
        {
            case KEY_SET_MEMBER:
            case KEY_LIST_ELEMENT:
            case KEY_ZSET_SCORE:
            case KEY_HASH_FIELD:
            {
                elements.resize(1);
                break;
            }
            case KEY_ZSET_SORT:
            {
                elements.resize(2);
                break;
            }
            case KEY_TTL_SORT:
            {
                /*
                 * 0: ttl 1:namespace 2:ttl key
                 */
                elements.resize(3);
                break;
            }
            default:
            {
                break;
            }
        }
    }

    bool KeyObject::DecodeNS(Buffer& buffer, bool clone_str)
    {
        return ns.Decode(buffer, clone_str);
    }

    int KeyObject::ComparePrefix(const KeyObject& other) const
    {
        int ret = ns.Compare(other.ns, false);
        if (ret != 0)
        {
            return ret;
        }
        ret = key.Compare(other.key, false);
        if (ret != 0)
        {
            return ret;
        }
		return 0;
    }

    int KeyObject::Compare(const KeyObject& other) const
    {
        int ret = ComparePrefix(other);
        if (ret != 0)
        {
            return ret;
        }
        ret = (int) type - (int) other.type;
        if (ret != 0)
        {
            return ret;
        }
        ret = elements.size() - other.elements.size();
        if (ret != 0)
        {
            return ret;
        }
        for (size_t i = 0; i < elements.size(); i++)
        {
            ret = elements[i].Compare(other.elements[i], false);
            if (ret != 0)
            {
                return ret;
            }
        }
        return 0;
    }

//    bool KeyObject::DecodeType(Buffer& buffer)
//    {
//        char tmp;
//        if (!buffer.ReadByte(tmp))
//        {
//            return false;
//        }
//        type = (uint8) tmp;
//        return true;
//    }
//    bool KeyObject::DecodeKey(Buffer& buffer, bool clone_str)
//    {
//        return key.Decode(buffer, clone_str);
//    }

    bool KeyObject::DecodeKey(Buffer& buffer, bool clone_str)
    {
        uint32 keylen;
        if (!BufferHelper::ReadVarUInt32(buffer, keylen))
        {
        	ERROR_LOG("Read length header failed.");
            return false;
        }
        if (buffer.ReadableBytes() < (keylen))
        {
        	ERROR_LOG("No space for key content with size:%u", keylen);
            return false;
        }
        key.SetString(buffer.GetRawReadBuffer(), keylen, clone_str);
        buffer.AdvanceReadIndex(keylen);
        return true;
    }

    bool KeyObject::DecodeType(Buffer& buffer)
    {
        if (!buffer.Readable())
        {
            return false;
        }
        type = (uint8) (buffer.GetRawReadBuffer()[0]);
        buffer.AdvanceReadIndex(1);
        return true;
    }

    bool KeyObject::DecodePrefix(Buffer& buffer, bool clone_str)
    {
        if (!DecodeKey(buffer, clone_str))
        {
            return false;
        }
        return DecodeType(buffer);
    }

    int KeyObject::DecodeElementLength(Buffer& buffer)
    {
        char len;
        if (!buffer.ReadByte(len))
        {
            return 0;
        }
        if (len < 0 || len > 127)
        {
            return -1;
        }
        if (len > 0)
        {
            elements.resize(len);
        }
        return (int) len;
    }
    bool KeyObject::DecodeElement(Buffer& buffer, bool clone_str, int idx)
    {
        if (elements.size() <= idx)
        {
            elements.resize(idx + 1);
        }
        return elements[idx].Decode(buffer, clone_str);
    }
    bool KeyObject::Decode(Buffer& buffer, bool clone_str, bool with_ns)
    {
        Clear();
        if (with_ns)
        {
            if (!ns.Decode(buffer, clone_str))
            {
                return false;
            }
        }
        if (!DecodePrefix(buffer, clone_str))
        {
            return false;
        }
//        if (!DecodeKey(buffer, clone_str))
//        {
//            return false;
//        }
        int elen1 = DecodeElementLength(buffer);
        if (elen1 > 0)
        {
            for (int i = 0; i < elen1; i++)
            {
                if (!DecodeElement(buffer, clone_str, i))
                {
                    return false;
                }
            }
        }
        return true;
    }

    void KeyObject::EncodePrefix(Buffer& buffer) const
    {
        BufferHelper::WriteVarUInt32(buffer, key.StringLength());
        buffer.Write(key.CStr(), key.StringLength());
        buffer.WriteByte((char) type);
    }
    Slice KeyObject::Encode(Buffer& buffer, bool verify, bool with_ns) const
    {
        if (verify && !IsValid())
        {
        	ERROR_LOG("Invalid key:%u", GetType());
            return Slice();
        }
        size_t mark = buffer.GetWriteIndex();
        if (with_ns)
        {
            ns.Encode(buffer);
        }
        EncodePrefix(buffer);
        buffer.WriteByte((char) elements.size());
        for (size_t i = 0; i < elements.size(); i++)
        {
            elements[i].Encode(buffer);
        }
        return Slice(buffer.GetRawBuffer() + mark, buffer.GetWriteIndex() - mark);
    }
    void KeyObject::CloneStringPart()
    {
        if (ns.IsString())
        {
            ns.ToMutableStr();
        }
        if (key.IsString())
        {
            key.ToMutableStr();
        }
        for (size_t i = 0; i < elements.size(); i++)
        {
            if (elements[i].IsString())
            {
                elements[i].ToMutableStr();
            }
        }
    }
    bool KeyObject::IsValid() const
    {
        switch (type)
        {
            case KEY_META:
            case KEY_STRING:
            case KEY_HASH:
            case KEY_LIST:
            case KEY_SET:
            case KEY_ZSET:
            case KEY_HASH_FIELD:
            case KEY_LIST_ELEMENT:
            case KEY_SET_MEMBER:
            case KEY_ZSET_SORT:
            case KEY_ZSET_SCORE:
            case KEY_TTL_SORT:
            {
                return true;
            }
            default:
            {
                return false;
            }
        }
    }
    MetaObject::MetaObject() :
            format(kCurrentMetaFormat), ttl(0), size(-1), list_sequential(true)
    {

    }
    void MetaObject::Clear()
    {
        format = kCurrentMetaFormat;
        ttl = 0;
        size = -1;
        list_sequential = true;
    }
    void MetaObject::Encode(Buffer& buffer, uint8 type) const
    {
        buffer.WriteByte((char) format);
        BufferHelper::WriteVarInt64(buffer, ttl);
        switch (type)
        {
            case KEY_STRING:
            {
                //do nothing
                break;
            }
            case KEY_LIST:
            case KEY_SET:
            case KEY_ZSET:
            case KEY_HASH:
            {
                BufferHelper::WriteVarInt64(buffer, size);
                if (type == KEY_LIST)
                {
                    buffer.WriteByte(list_sequential ? 1 : 0);
                }
                break;
            }
            default:
            {
                break;
            }
        }
    }
    bool MetaObject::Decode(Buffer& buffer, uint8 type)
    {
        char tmp;
        if (!buffer.ReadByte(tmp))
        {
            return false;
        }
        format = (uint8) tmp;
        if (!BufferHelper::ReadVarInt64(buffer, ttl))
        {
            return false;
        }
        switch (type)
        {
            case KEY_STRING:
            {
                //do nothing
                break;
            }
            case KEY_LIST:
            case KEY_SET:
            case KEY_ZSET:
            case KEY_HASH:
            {
                if (!BufferHelper::ReadVarInt64(buffer, size))
                {
                    return false;
                }
                if (type == KEY_LIST)
                {
                    if (!buffer.ReadByte(tmp))
                    {
                        return false;
                    }
                    list_sequential = (bool) tmp;
                }
                break;
            }
            default:
            {
                FATAL_LOG("Invalid type for mata object");
            }
        }
        return true;
    }

    MetaObject& ValueObject::GetMetaObject()
    {
        return meta;
    }

    int64_t ValueObject::GetTTL()
    {
        return GetMetaObject().ttl;
    }
    void ValueObject::SetTTL(int64_t v)
    {
        GetMetaObject().ttl = v;
    }
    void ValueObject::ClearMinMaxData()
    {
        GetMin().Clear();
        GetMax().Clear();
    }
    bool ValueObject::SetMinData(const Data& v, bool overwite)
    {
        bool replaced = false;
        if (vals.size() < 2)
        {
            vals.resize(2);
            replaced = true;
        }
        if (overwite || GetMin() > v || GetMin().IsNil())
        {
            GetMin() = v;
            replaced = true;
        }
        return replaced;
    }
    bool ValueObject::SetMaxData(const Data& v, bool overwite)
    {
        bool replaced = false;
        if (vals.size() < 2)
        {
            vals.resize(2);
            replaced = true;
        }
        if (overwite || GetMax() < v || GetMax().IsNil())
        {
            GetMax() = v;
            replaced = true;
        }
        return replaced;
    }
    bool ValueObject::SetMinMaxData(const Data& v)
    {
        bool replaced = false;
        if (vals.size() < 2)
        {
            vals.resize(2);
            replaced = true;
        }
        if (GetMin().IsNil() || GetMin() > v)
        {
            GetMin() = v;
            replaced = true;
        }
        if (GetMax().IsNil() || GetMax() < v)
        {
            GetMax() = v;
            replaced = true;
        }
        return replaced;
    }

    static void encode_value_object(Buffer& encode_buffer, uint8 type, uint16 merge_op, const DataArray& args, const MetaObject* meta)
    {
        encode_buffer.WriteByte((char) type);
        switch (type)
        {
            case KEY_MERGE:
            {
                BufferHelper::WriteFixUInt16(encode_buffer, merge_op, false);
                break;
            }
            case KEY_STRING:
            case KEY_LIST:
            case KEY_SET:
            case KEY_ZSET:
            case KEY_HASH:
            {
                meta->Encode(encode_buffer, type);
                break;
            }
            default:
            {
                break;
            }
        }
        encode_buffer.WriteByte((char) args.size());
        for (size_t i = 0; i < args.size(); i++)
        {
            args[i].Encode(encode_buffer);
        }
    }

    void ValueObject::SetType(uint8 t)
    {
        type = t;
    }

    Slice ValueObject::Encode(Buffer& encode_buffer) const
    {
        if (0 == type)
        {
            return Slice();
        }
        encode_value_object(encode_buffer, type, merge_op, vals, &meta);
        return Slice(encode_buffer.GetRawReadBuffer(), encode_buffer.ReadableBytes());
    }

    bool ValueObject::DecodeMeta(Buffer& buffer)
    {
        Clear();
        if (!buffer.Readable())
        {
            return true;
        }
        char tmp;
        if (!buffer.ReadByte(tmp))
        {
            return false;
        }
        type = (uint8) tmp;
        switch (type)
        {
            case KEY_MERGE:
            {
                if (!BufferHelper::ReadFixUInt16(buffer, merge_op, false))
                {
                    return false;
                }
                break;
            }
            case KEY_STRING:
            case KEY_LIST:
            case KEY_SET:
            case KEY_ZSET:
            case KEY_HASH:
            {
                if (!meta.Decode(buffer, type))
                {
                    return false;
                }
                break;
            }
            default:
            {
                break;
            }
        }
        return true;
    }

    bool ValueObject::Decode(Buffer& buffer, bool clone_str)
    {
        if (!DecodeMeta(buffer))
        {
            return false;
        }
        char lench;
        if (!buffer.ReadByte(lench))
        {
            return false;
        }
        uint8 len = (uint8) lench;
        if (len > 0)
        {
            vals.resize(len);
            for (uint8 i = 0; i < len; i++)
            {
                if (!vals[i].Decode(buffer, clone_str))
                {
                    return false;
                }
            }
        }
        return true;
    }

    void ValueObject::CloneStringPart()
    {
        for (size_t i = 0; i < vals.size(); i++)
        {
            if (vals[i].IsString())
            {
                vals[i].ToMutableStr();
            }
        }
    }

    static int parse_score(const std::string& score_str, double& score, bool& contain)
    {
        contain = true;
        const char* str = score_str.c_str();
        if (score_str.at(0) == '(')
        {
            contain = false;
            str++;
        }
        if (strcasecmp(str, "-inf") == 0)
        {
            score = -DBL_MAX;
        }
        else if (strcasecmp(str, "+inf") == 0)
        {
            score = DBL_MAX;
        }
        else
        {
            if (!str_todouble(str, score))
            {
                return -1;
            }
        }
        return 0;
    }
    bool ZRangeSpec::Parse(const std::string& minstr, const std::string& maxstr)
    {
        double min_d, max_d;
        int ret = parse_score(minstr, min_d, contain_min);
        if (0 == ret)
        {
            ret = parse_score(maxstr, max_d, contain_max);
        }
        if (ret == 0 && min_d > max_d)
        {
            return false;
        }
        min.SetFloat64(min_d);
        max.SetFloat64(max_d);
        return ret == 0;
    }
    static bool verify_lexrange_para(const std::string& str)
    {
        if (str == "-" || str == "+")
        {
            return true;
        }
        if (str.empty())
        {
            return false;
        }

        if (str[0] != '(' && str[0] != '[')
        {
            return false;
        }
        return true;
    }
    bool ZLexRangeSpec::Parse(const std::string& minstr, const std::string& maxstr)
    {
        if (!verify_lexrange_para(minstr) || !verify_lexrange_para(maxstr))
        {
            return false;
        }
        if (minstr[0] == '(')
        {
            include_min = false;
        }
        if (minstr[0] == '[')
        {
            include_min = true;
        }
        if (maxstr[0] == '(')
        {
            include_max = false;
        }
        if (maxstr[0] == '[')
        {
            include_max = true;
        }
        if (minstr == "-")
        {
            min.clear();
            include_min = true;
        }
        else
        {
            min = minstr.substr(1);
        }
        if (maxstr == "+")
        {
            max.clear();
            include_max = true;
        }
        else
        {
            max = maxstr.substr(1);
        }
        if (min > max && !max.empty())
        {
            return false;
        }
        return true;
    }

    int encode_merge_operation(Buffer& buffer, uint16_t op, const DataArray& args)
    {
        encode_value_object(buffer, KEY_MERGE, op, args, NULL);
        return 0;
    }

    KeyType element_type(KeyType type)
    {
        switch (type)
        {
            case KEY_HASH:
            {
                return KEY_HASH_FIELD;
            }
            case KEY_LIST:
            {
                return KEY_LIST_ELEMENT;
            }
            case KEY_SET:
            {
                return KEY_SET_MEMBER;
            }
            case KEY_ZSET:
            {
                return KEY_ZSET_SCORE;
            }
            default:
            {
                abort();
            }
        }
    }

OP_NAMESPACE_END

