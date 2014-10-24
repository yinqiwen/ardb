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

#ifndef TYPES_HPP_
#define TYPES_HPP_

#include "common/common.hpp"
#include "buffer/buffer.hpp"
#include "util/sds.h"
#include "util/string_helper.hpp"
#include "slice.hpp"
#include <deque>

#define STRING_ENCODING_NIL    0
#define STRING_ENCODING_INT64  1
#define STRING_ENCODING_DOUBLE 2
#define STRING_ENCODING_RAW    3

#define COLLECTION_ENCODING_RAW      0
#define COLLECTION_ENCODING_ZIPLIST  1
#define COLLECTION_ENCODING_ZIPMAP   2
#define COLLECTION_ENCODING_ZIPSET   3
#define COLLECTION_ENCODING_ZIPZSET  4

#define COLLECTION_FLAG_NORMAL      0
#define COLLECTION_FLAG_SEQLIST     1   //indicate that list is only sequentially pushed/poped at head/tail

#define ARDB_GLOBAL_DB 0xFFFFFF

OP_NAMESPACE_BEGIN

    enum KeyType
    {
        KEY_META = 0, STRING_META = 1, BITSET_META = 2, SET_META = 3, ZSET_META = 4, HASH_META = 5, LIST_META = 6,
        /*
         * Reserver 20 types
         */

        SET_ELEMENT = 20,

        ZSET_ELEMENT_SCORE = 30, ZSET_ELEMENT_VALUE = 31,

        HASH_FIELD = 40,

        LIST_ELEMENT = 50,

        BITSET_ELEMENT = 70,

        KEY_EXPIRATION_ELEMENT = 100, SCRIPT = 102,

        KEY_END = 255, /* max value for 1byte */
    };

    typedef uint32 DBID;

    typedef std::vector<DBID> DBIDArray;
    typedef TreeSet<DBID>::Type DBIDSet;

    struct Data
    {
            union
            {
                    int64 iv;
                    double dv;
                    sds sv;
            } value;
            //std::string str;
            uint8 encoding;

            Data();
            Data(const Slice& v, bool try_int_encoding = true);
            Data(const Data& data);
            Data& operator=(const Data& data)
            {
                Clone(data);
                return *this;
            }
            ~Data();

            bool Encode(Buffer& buf) const;
            bool Decode(Buffer& buf);

            void SetString(const Slice& str, bool try_int_encoding);
            bool SetNumber(const std::string& str);
            double NumberValue() const;
            void SetInt64(int64 v);
            void SetDouble(double v);
            bool GetInt64(int64& v) const
            {
                if (encoding == STRING_ENCODING_INT64)
                {
                    v = value.iv;
                    return true;
                }
                else if (encoding == STRING_ENCODING_NIL)
                {
                    v = 0;
                    return true;
                }
                return false;
            }
            bool GetDouble(double& v) const;

            void Clone(const Data& data);

            Data& IncrBy(int64 v);
            Data& IncrBy(const Data& v);

            int Compare(const Data& other) const;
            bool operator <(const Data& other) const
            {
                return Compare(other) < 0;
            }
            bool operator <=(const Data& other) const
            {
                return Compare(other) <= 0;
            }
            bool operator ==(const Data& other) const
            {
                return Compare(other) == 0;
            }
            bool operator >=(const Data& other) const
            {
                return Compare(other) >= 0;
            }
            bool operator >(const Data& other) const
            {
                return Compare(other) > 0;
            }
            bool operator !=(const Data& other) const
            {
                return Compare(other) != 0;
            }

            bool IsNumber() const;
            bool IsNil() const
            {
                return encoding == STRING_ENCODING_NIL;
            }

            uint32 StringLength() const;

            void Clear()
            {
                if (encoding == STRING_ENCODING_RAW && NULL != value.sv)
                {
                    sdsfree(value.sv);
                }
                value.iv = 0;
                encoding = STRING_ENCODING_NIL;
            }
            sds RawString()
            {
                if (encoding == STRING_ENCODING_RAW)
                {
                    return value.sv;
                }
                return NULL;
            }

            const std::string& GetDecodeString(std::string& str) const;
            const sds ToString();
    };
    typedef std::deque<Data> DataArray;
    typedef TreeMap<Data, Data>::Type DataMap;
    typedef TreeSet<Data>::Type DataSet;

    struct Location
    {
            double x, y;
            Location() :
                    x(0), y(0)
            {
            }
    };
    typedef std::deque<Location> LocationDeque;

    struct KeyObject
    {
        public:
            Buffer encode_buf;

            DBID db;
            uint8 type;
            Slice key;
            Data element;
            Data score;

            uint8 meta_type;
            KeyObject() :
                    db(0), type(0), meta_type(0)
            {
            }
            void Encode();
            bool Decode(Buffer& buf);

            void Clear()
            {
                db = 0;
                type = 0;
                meta_type = 0;
                element.Clear();
                score.Clear();
            }

            KeyObject(const KeyObject& other) :
                    db(0), type(0), meta_type(0)
            {
                db = other.db;
                type = other.type;
                key = other.key;
                element = other.element;
                score = other.score;
            }
            KeyObject& operator=(const KeyObject& other)
            {
                Clear();
                db = other.db;
                type = other.type;
                key = other.key;
                element = other.element;
                score = other.score;
                return *this;
            }

            ~KeyObject()
            {
                Clear();
            }

    };

    typedef TreeMap<double, uint32>::Type ScoreRanges;
    struct MetaValue
    {
            uint64 expireat;
            uint8 attribute;
            Data str_value;
            int64 len; //-1 means unknown

            DataArray ziplist;
            DataMap zipmap;
            DataSet zipset;

            Data min_index;  //min index value in collection(list/set)
            Data max_index;  //max index value in collection(list/set)


            MetaValue() :
                    expireat(0), attribute(0), len(0)
            {
            }
            uint8 Encoding()
            {
                return attribute & 0xF;
            }
            uint8 Flag()
            {
                return attribute >> 4;
            }
            void SetEncoding(uint8 enc)
            {
                attribute = ((attribute & 0xF0) | enc);
            }
            void SetFlag(uint8 f)
            {
                attribute = ((f << 4) | (attribute & 0x0F));
            }
            bool IsSequentialList()
            {
                return Flag()  == COLLECTION_FLAG_SEQLIST;
            }

            int64 Length();
            void Encode(Buffer& buf, uint8 type);
            bool Decode(Buffer& buf, uint8 type);
            void Clear()
            {
                ziplist.clear();
                zipmap.clear();
                zipset.clear();
                min_index.Clear();
                max_index.Clear();
                str_value.Clear();
            }
            ~MetaValue()
            {
                Clear();
            }
    };

    struct AttachOptions
    {
            Location loc;  //decoded location
            bool fetch_loc;
            bool force_zipsave;
            AttachOptions() :
                    fetch_loc(false),force_zipsave(false)
            {
            }
    };

    struct ValueObject
    {
            KeyObject key;
            Buffer encode_buf;

            uint8 type;
            MetaValue meta;
            Data element;
            Data score;

            AttachOptions attach;

            ValueObject(uint8 t = KEY_END) :
                    type(t)
            {
            }
            ValueObject(const ValueObject& other)
            {
                type = other.type;
                meta = other.meta;
                element = other.element;
                score = other.score;
                key = other.key;
            }
            const ValueObject& operator=(const ValueObject& other)
            {
                type = other.type;
                meta = other.meta;
                element = other.element;
                score = other.score;
                key = other.key;
                return *this;
            }
            void Encode();
            bool Decode(Buffer& buf);
            void Clear()
            {
                element.Clear();
                score.Clear();
                meta.Clear();
            }
            ~ValueObject()
            {
                Clear();
            }
    };

    typedef std::vector<int64> Int64Array;
    typedef std::vector<double> DoubleArray;
    typedef std::vector<std::string> StringArray;
    typedef std::vector<ValueObject> ValueObjectArray;
    typedef TreeSet<std::string>::Type StringSet;
    typedef TreeMap<std::string, ValueObject>::Type ValueObjectMap;

    typedef std::pair<Data, Data> ZSetElement;
    typedef std::vector<ZSetElement> ZSetElementArray;

    typedef std::vector<Slice> SliceArray;

    bool decode_key(const Slice& kbuf, KeyObject& key);
    bool decode_value(const Slice& kbuf, ValueObject& value);

OP_NAMESPACE_END

#endif /* TYPES_HPP_ */
