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

    Data::Data() :
            encoding(0)
    {
        value.iv = 0;
    }
    Data::Data(const Slice& v, bool try_int_encoding) :
            encoding(0)
    {
        SetString(v, try_int_encoding);
    }
    Data::~Data()
    {
        Clear();
    }

    Data::Data(const Data& data) :
            encoding(0)
    {
        value.iv = 0;
        Clone(data);
    }

    bool Data::Encode(Buffer& buf) const
    {
        buf.WriteByte((char) encoding);
        switch (encoding)
        {
            case STRING_ENCODING_NIL:
            {
                break;
            }
            case STRING_ENCODING_INT64:
            {
                BufferHelper::WriteVarInt64(buf, value.iv);
                break;
            }
            case STRING_ENCODING_DOUBLE:
            {
                BufferHelper::WriteFixDouble(buf, value.dv);
                break;
            }
            case STRING_ENCODING_RAW:
            {
                size_t len = sdslen(value.sv);
                BufferHelper::WriteVarInt64(buf, len);
                if (len > 0)
                {
                    buf.Write(value.sv, len);
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
    bool Data::Decode(Buffer& buf)
    {
        Clear();
        char tmp;
        if (!buf.ReadByte(tmp))
        {
            return false;
        }
        encoding = (uint8) tmp;
        switch (encoding)
        {
            case STRING_ENCODING_NIL:
            {
                break;
            }
            case STRING_ENCODING_INT64:
            {
                return BufferHelper::ReadVarInt64(buf, value.iv);
            }
            case STRING_ENCODING_DOUBLE:
            {
                return BufferHelper::ReadFixDouble(buf, value.dv);
            }
            case STRING_ENCODING_RAW:
            {
                int64 len = 0;
                if (!BufferHelper::ReadVarInt64(buf, len))
                {
                    return false;
                }
                if (len > 0)
                {
                    value.sv = sdsnewlen(NULL, len);
                    if (buf.Read(value.sv, len) < 0)
                    {
                        sdsfree(value.sv);
                        value.sv = NULL;
                        return false;
                    }
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

    void Data::Clone(const Data& data)
    {
        Clear();
        encoding = data.encoding;
        switch (data.encoding)
        {
            case STRING_ENCODING_NIL:
            {
                break;
            }
            case STRING_ENCODING_INT64:
            {
                value.iv = data.value.iv;
                break;
            }
            case STRING_ENCODING_DOUBLE:
            {
                value.dv = data.value.dv;
                break;
            }
            case STRING_ENCODING_RAW:
            {
                if (NULL != data.value.sv)
                {
                    value.sv = sdsnewlen(data.value.sv, sdslen(data.value.sv));
                }
                break;
            }
            default:
            {
                abort();
                break;
            }
        }
    }

    void Data::SetString(const Slice& str, bool try_int_encoding)
    {
        if (encoding == STRING_ENCODING_RAW && NULL != value.sv)
        {
            sdsfree(value.sv);
        }
        if (str.size() == 0)
        {
            encoding = STRING_ENCODING_NIL;
            return;
        }
        if (try_int_encoding)
        {
            if (string2ll(str.data(), str.size(), &(value.iv)) > 0)
            {
                encoding = STRING_ENCODING_INT64;
                return;
            }
        }
        encoding = STRING_ENCODING_RAW;
        value.sv = sdsnewlen(str.data(), str.size());
    }
    bool Data::SetNumber(const std::string& str)
    {
        if (encoding == STRING_ENCODING_RAW && NULL != value.sv)
        {
            sdsfree(value.sv);
        }
        if (string2ll(str.data(), str.size(), &(value.iv)) > 0)
        {
            encoding = STRING_ENCODING_INT64;
            return true;
        }
        if (string_todouble(str, value.dv))
        {
            encoding = STRING_ENCODING_DOUBLE;
            return true;
        }
        return false;
    }
    double Data::NumberValue() const
    {
        double v;
        GetDouble(v);
        return v;
    }
    void Data::SetInt64(int64 v)
    {
        encoding = STRING_ENCODING_INT64;
        value.iv = v;
    }
    void Data::SetDouble(double v)
    {
        encoding = STRING_ENCODING_DOUBLE;
        value.dv = v;
    }

    bool Data::GetDouble(double& v) const
    {
        switch (encoding)
        {
            case STRING_ENCODING_INT64:
            {
                v = value.iv;
                return true;
            }
            case STRING_ENCODING_DOUBLE:
            {
                v = value.dv;
                return true;
            }
            case STRING_ENCODING_NIL:
            {
                v = 0;
                return true;
            }
            case STRING_ENCODING_RAW:
            {
                if (str_todouble(value.sv, v))
                {
                    return true;
                }
                return false;
            }
            default:
            {
                return false;
            }
        }
    }

    Data& Data::IncrBy(const Data& v)
    {
        switch (v.encoding)
        {
            case STRING_ENCODING_INT64:
            {
                return IncrBy(v.value.iv);
            }
            case STRING_ENCODING_DOUBLE:
            {
                if (encoding == STRING_ENCODING_NIL)
                {
                    encoding = STRING_ENCODING_DOUBLE;
                    value.dv = 0;
                }
                if (encoding == STRING_ENCODING_DOUBLE || encoding == STRING_ENCODING_INT64)
                {
                    encoding = STRING_ENCODING_DOUBLE;
                }
                value.dv = NumberValue() + v.value.dv;
                break;
            }
            default:
            {
                break;
            }
        }
        return *this;
    }

    Data& Data::IncrBy(int64 v)
    {
        switch (encoding)
        {
            case STRING_ENCODING_NIL:
            {
                encoding = STRING_ENCODING_INT64;
                value.iv = 0;
                value.iv += v;
                break;
            }
            case STRING_ENCODING_INT64:
            {
                value.iv += v;
                break;
            }
            case STRING_ENCODING_DOUBLE:
            {
                value.dv += v;
                break;
            }
            default:
            {
                break;
            }
        }
        return *this;
    }

    bool Data::IsNumber() const
    {
        return encoding == STRING_ENCODING_INT64 || encoding == STRING_ENCODING_DOUBLE;
    }

    int Data::Compare(const Data& other) const
    {
        if (encoding == STRING_ENCODING_NIL || other.encoding == STRING_ENCODING_NIL)
        {
            if (encoding != other.encoding)
            {
                int a = encoding, b = other.encoding;
                return a - b;
            }
            return 0;
        }
        if (IsNumber() && other.IsNumber())
        {
            double a = NumberValue();
            double b = other.NumberValue();
            return a > b ? 1 : (a < b ? -1 : 0);
        }
        else if (encoding != other.encoding)
        {
            int a = encoding, b = other.encoding;
            return a - b;
        }
        else
        {
            return sdscmp(value.sv, other.value.sv);
        }
    }

    uint32 Data::StringLength() const
    {
        switch (encoding)
        {
            case STRING_ENCODING_INT64:
            case STRING_ENCODING_DOUBLE:
            {
                return 0;
            }
            default:
            {
                if (NULL != value.sv)
                {
                    return sdslen(value.sv);
                }
                return 0;
            }
        }
    }

    const std::string& Data::GetDecodeString(std::string& str) const
    {
        switch (encoding)
        {
            case STRING_ENCODING_NIL:
            {
                str.clear();
                break;
            }
            case STRING_ENCODING_INT64:
            {
                str = stringfromll(value.iv);
                break;
            }
            case STRING_ENCODING_DOUBLE:
            {
                if (std::isinf(value.dv))
                {
                    /* Libc in odd systems (Hi Solaris!) will format infinite in a
                     * different way, so better to handle it in an explicit way. */
                    if (value.dv > 0)
                    {
                        str = "inf";
                    }
                    else
                    {
                        str = "-inf";
                    }
                }
                else
                {
                    char dbuf[128];
                    int dlen = snprintf(dbuf, sizeof(dbuf), "%.17g", value.dv);
                    str.assign(dbuf, dlen);
                }
                break;
            }
            default:
            {
                if (NULL != value.sv)
                {
                    str.assign(value.sv, sdslen(value.sv));
                }
                break;
            }
        }
        return str;
    }
    const sds Data::ToString()
    {
        switch (encoding)
        {
            case STRING_ENCODING_INT64:
            {
                value.sv = sdscatprintf(NULL, "%" PRId64, value.iv);
                break;
            }
            case STRING_ENCODING_DOUBLE:
            {
                if (std::isinf(value.dv))
                {
                    /* Libc in odd systems (Hi Solaris!) will format infinite in a
                     * different way, so better to handle it in an explicit way. */
                    if (value.dv > 0)
                    {
                        value.sv = sdscatprintf(NULL, "inf");
                    }
                    else
                    {
                        value.sv = sdscatprintf(NULL, "-inf");
                    }
                }
                else
                {
                    value.sv = sdscatprintf(NULL, "%.17g", value.dv);
                }
                break;
            }
            default:
            {
                break;
            }
        }
        encoding = STRING_ENCODING_RAW;
        return value.sv;
    }

    void KeyObject::Encode()
    {
        encode_buf.Clear();
        uint32 header = (uint32) (db << 8) + type;
        encode_buf.Write(&header, sizeof(header));
        //BufferHelper::WriteFixUInt32(encode_buf, header);
        BufferHelper::WriteVarSlice(encode_buf, key);
        switch (type)
        {
            case KEY_META:
            case SCRIPT:
            {
                break;
            }
            case SET_ELEMENT:
            case ZSET_ELEMENT_VALUE:
            case HASH_FIELD:
            {
                element.Encode(encode_buf);
                break;
            }
            case ZSET_ELEMENT_SCORE:
            case LIST_ELEMENT:
            {
                score.Encode(encode_buf);
                element.Encode(encode_buf);
                break;
            }
            case BITSET_ELEMENT:
            case KEY_EXPIRATION_ELEMENT:
            {
                score.Encode(encode_buf);
                break;
            }
            default:
            {
                abort();
                break;
            }
        }
    }
    bool KeyObject::Decode(Buffer& buf)
    {
        uint32 header = 0;
        if (!buf.Read(&header, sizeof(header)))
        {
            return false;
        }
        Clear();
        type = (uint8) (header & 0xFF);
        db = header >> 8;
        if (!BufferHelper::ReadVarSlice(buf, key))
        {
            return false;
        }
        switch (type)
        {
            case KEY_META:
            case SCRIPT:
            {
                break;
            }
            case SET_ELEMENT:
            case ZSET_ELEMENT_VALUE:
            case HASH_FIELD:
            {
                if (!element.Decode(buf))
                {
                    return false;
                }
                break;
            }
            case ZSET_ELEMENT_SCORE:
            {
                if (!score.Decode(buf))
                {
                    return false;
                }
                if (!element.Decode(buf))
                {
                    return false;
                }
                break;
            }
            case BITSET_ELEMENT:
            case LIST_ELEMENT:
            case KEY_EXPIRATION_ELEMENT:
            {
                if (!score.Decode(buf))
                {
                    return false;
                }
                break;
            }
            default:
            {
                abort();
                break;
            }
        }
        return true;
    }

    int64 MetaValue::Length()
    {
        switch (Encoding())
        {
            case COLLECTION_ENCODING_ZIPLIST:
            {
                return ziplist.size();
            }
            case COLLECTION_ENCODING_ZIPMAP:
            case COLLECTION_ENCODING_ZIPZSET:
            {
                return zipmap.size();
            }
            case COLLECTION_ENCODING_ZIPSET:
            {
                return zipset.size();
            }
            default:
            {
                return len;
            }
        }
    }

    void MetaValue::Encode(Buffer& buf, uint8 type)
    {
        BufferHelper::WriteVarUInt64(buf, expireat);
        switch (type)
        {
            case STRING_META:
            {
                str_value.Encode(buf);
                break;
            }
            case HASH_META:
            {
                buf.WriteByte((char) attribute);
                if (Encoding() == COLLECTION_ENCODING_ZIPMAP)
                {
                    encode_arg(buf, zipmap);
                }
                else
                {
                    BufferHelper::WriteVarInt64(buf, len);
                }
                break;
            }
            case SET_META:
            {
                buf.WriteByte((char) attribute);
                if (Encoding() == COLLECTION_ENCODING_ZIPSET)
                {
                    encode_arg(buf, zipset);
                }
                else
                {
                    BufferHelper::WriteVarInt64(buf, len);
                    min_index.Encode(buf);
                    max_index.Encode(buf);
                }
                break;
            }
            case LIST_META:
            {
                buf.WriteByte((char) attribute);
                if (Encoding() == COLLECTION_ENCODING_ZIPLIST)
                {
                    encode_arg(buf, ziplist);
                }
                else
                {
                    BufferHelper::WriteVarInt64(buf, len);
                    min_index.Encode(buf);
                    max_index.Encode(buf);
                }
                break;
            }

            case ZSET_META:
            {
                buf.WriteByte((char) attribute);
                if (Encoding() == COLLECTION_ENCODING_ZIPZSET)
                {
                    encode_arg(buf, zipmap);
                }
                else
                {
                    BufferHelper::WriteVarInt64(buf, len);
                    min_index.Encode(buf);
                    max_index.Encode(buf);
                    //encode_arg(buf, zset_score_range);
                }
                break;
            }
            case BITSET_META:
            {
                BufferHelper::WriteVarInt64(buf, len);
                min_index.Encode(buf);
                max_index.Encode(buf);
                break;
            }
            default:
            {
                abort();
                break;
            }
        }
    }
    bool MetaValue::Decode(Buffer& buf, uint8 type)
    {
        Clear();
        if (!BufferHelper::ReadVarUInt64(buf, expireat))
        {
            return false;
        }
        switch (type)
        {
            case STRING_META:
            {
                if (!str_value.Decode(buf))
                {
                    return false;
                }
                break;
            }
            case HASH_META:
            {
                char tmp;
                if (!buf.ReadByte(tmp))
                {
                    return false;
                }
                attribute = (uint8) tmp;
                if (Encoding() == COLLECTION_ENCODING_ZIPMAP)
                {
                    if (!decode_arg(buf, zipmap))
                    {
                        return false;
                    }
                }
                else
                {
                    if (!BufferHelper::ReadVarInt64(buf, len))
                    {
                        return false;
                    }
                }
                break;
            }
            case SET_META:
            {
                char tmp;
                if (!buf.ReadByte(tmp))
                {
                    return false;
                }
                attribute = (uint8) tmp;
                if (Encoding() == COLLECTION_ENCODING_ZIPSET)
                {
                    if (!decode_arg(buf, zipset))
                    {
                        return false;
                    }
                }
                else
                {
                    if (!BufferHelper::ReadVarInt64(buf, len) || !min_index.Decode(buf) || !max_index.Decode(buf))
                    {
                        return false;
                    }
                }
                break;
            }
            case LIST_META:
            {
                char tmp;
                if (!buf.ReadByte(tmp))
                {
                    return false;
                }
                attribute = (uint8) tmp;
                if (Encoding() == COLLECTION_ENCODING_ZIPLIST)
                {
                    if (!decode_arg(buf, ziplist))
                    {
                        return false;
                    }
                }
                else
                {
                    if (!BufferHelper::ReadVarInt64(buf, len) || !min_index.Decode(buf) || !max_index.Decode(buf))
                    {
                        return false;
                    }
                }
                break;
            }

            case ZSET_META:
            {
                char tmp;
                if (!buf.ReadByte(tmp))
                {
                    return false;
                }
                attribute = (uint8) tmp;
                if (Encoding() == COLLECTION_ENCODING_ZIPZSET)
                {
                    if (!decode_arg(buf, zipmap))
                    {
                        return false;
                    }
                }
                else
                {
                    if (!BufferHelper::ReadVarInt64(buf, len) || !min_index.Decode(buf) || !max_index.Decode(buf))
                    {
                        return false;
                    }
                }
                break;
            }
            case BITSET_META:
            {
                if (BufferHelper::ReadVarInt64(buf, len) && min_index.Decode(buf) && max_index.Decode(buf))
                {
                    return true;
                }
                else
                {
                    return false;
                }
                break;
            }
            default:
            {
                abort();
                break;
            }
        }
        return true;
    }

    void ValueObject::Encode()
    {
        encode_buf.Clear();
        encode_buf.WriteByte((char) type);
        switch (type)
        {
            case STRING_META:
            case HASH_META:
            case LIST_META:
            case SET_META:
            case ZSET_META:
            case BITSET_META:
            {
                meta.Encode(encode_buf, type);
                break;
            }
            case LIST_ELEMENT:
            case HASH_FIELD:
            case SCRIPT:
            {
                element.Encode(encode_buf);
                break;
            }
            case SET_ELEMENT:
            case ZSET_ELEMENT_SCORE:
            case KEY_EXPIRATION_ELEMENT:
            {
                break;
            }
            case ZSET_ELEMENT_VALUE:
            {
                score.Encode(encode_buf);
                break;
            }
            case BITSET_ELEMENT:
            {
                element.Encode(encode_buf);
                score.Encode(encode_buf);
                break;
            }
            default:
            {
                abort();
            }
        }
    }
    bool ValueObject::Decode(Buffer& buf)
    {
        Clear();
        char tmp;
        if (!buf.ReadByte(tmp))
        {
            return false;
        }
        type = (uint8) tmp;
        switch (type)
        {
            case STRING_META:
            case HASH_META:
            case LIST_META:
            case SET_META:
            case ZSET_META:
            case BITSET_META:
            {
                return meta.Decode(buf, type);
            }
            case LIST_ELEMENT:
            case HASH_FIELD:
            case SCRIPT:
            {
                return element.Decode(buf);
            }
            case SET_ELEMENT:
            case ZSET_ELEMENT_SCORE:
            case KEY_EXPIRATION_ELEMENT:
            {
                return true;
            }
            case ZSET_ELEMENT_VALUE:
            {
                return score.Decode(buf);
            }
            case BITSET_ELEMENT:
            {
                return element.Decode(buf) && score.Decode(buf);
            }
            default:
            {
                abort();
            }
        }
        return false;
    }

    bool decode_key(const Slice& kbuf, KeyObject& key)
    {
        key.Clear();
        Buffer buffer(const_cast<char*>(kbuf.data()), 0, kbuf.size());
        return key.Decode(buffer);
    }

    bool decode_value(const Slice& kbuf, ValueObject& value)
    {
        value.Clear();
        Buffer buffer(const_cast<char*>(kbuf.data()), 0, kbuf.size());
        return value.Decode(buffer);
    }
OP_NAMESPACE_END

