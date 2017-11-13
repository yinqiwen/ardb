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
#include "types.hpp"
#include <assert.h>
#include <deque>

OP_NAMESPACE_BEGIN

    enum KeyType
    {
        KEY_UNKNOWN,

        KEY_META = 1,

        KEY_STRING = 2,

        KEY_HASH = 3, KEY_HASH_FIELD = 4,

        KEY_LIST = 5, KEY_LIST_ELEMENT = 6,

        KEY_SET = 7, KEY_SET_MEMBER = 8,

        KEY_ZSET = 9, KEY_ZSET_SORT = 10, KEY_ZSET_SCORE = 11,

        /*
         * Reserver 20 types
         */
        KEY_TTL_SORT = 29,
        KEY_MERGE = 30,
        KEY_END = 31, /* max value for 1byte */
    };

    struct KeyObject
    {
        private:
            Data ns; //namespace
            uint8 type;
            Data key;
            DataArray elements;

            Data& getElement(uint32_t idx)
            {
                if (elements.size() <= idx)
                {
                    elements.resize(idx + 1);
                }
                return elements[idx];
            }
            void setElement(const Data& data, uint32_t idx)
            {
                getElement(idx).Clone(data);
            }
        public:
            KeyObject(uint8 t = 0) :
                    type(t)
            {
                SetType(t);
            }
            KeyObject(const Data& nns, uint8 t, const std::string& data) :
                    ns(nns), type(0)
            {
                key.SetString(data, false);
                SetType(t);
            }
            KeyObject(const Data& nns, uint8 t, const Data& key_data) :
                    ns(nns), type(0), key(key_data)
            {
                SetType(t);
            }
            void Clear()
            {
                type = 0;
                ns.Clear();
                key.Clear();
                elements.clear();
            }
            const Data& GetNameSpace() const
            {
                return ns;
            }
            void SetNameSpace(const Data& nns)
            {
                ns = nns;
            }
            const Data& GetElement(int idx) const
            {
                return elements.at(idx);
            }
            const Data& GetKey() const
            {
                return key;
            }
            KeyType GetType() const
            {
                return (KeyType) type;
            }
            void SetType(uint8 t);
            void SetKey(const Data& d)
            {
                key = d;
            }
            void SetKey(const std::string& v)
            {
                key.SetString(v, false);
            }
            void SetHashField(const std::string& v)
            {
                getElement(0).SetString(v, true);
            }
            void SetHashField(const Data& v)
            {
                getElement(0).Clone(v);
                //setElement(v, 0);
            }
            const Data& GetHashField() const
            {
                return GetElement(0);
            }
            const Data& GetSetMember() const
            {
                return GetElement(0);
            }
            const Data& GetZSetMember() const
            {
                if (type == KEY_ZSET_SCORE)
                {
                    return GetElement(0);
                }
                else
                {
                    return GetElement(1);
                }
            }
            void SetListIndex(int64_t idx)
            {
                Data v;
                v.SetInt64(idx);
                SetListIndex(v);
            }
            void SetListIndex(double idx)
            {
                Data v;
                v.SetFloat64(idx);
                SetListIndex(v);
            }
            void SetListIndex(const Data& idx)
            {
                setElement(idx, 0);
            }
            double GetListIndex() const
            {
                return GetElement(0).GetFloat64();
            }
            void SetSetMember(const Data& v)
            {
                setElement(v, 0);
            }
            void SetSetMember(const std::string& v)
            {
                getElement(0).SetString(v, true);
            }
            void SetZSetMember(const Data& v)
            {
                if (type == KEY_ZSET_SCORE)
                {
                    setElement(v, 0);
                }
                else
                {
                    setElement(v, 1);
                }
            }
            void SetZSetMember(const std::string& v)
            {
                Data d;
                d.SetString(v, false);
                SetZSetMember(d);
            }
            void SetZSetScore(double score)
            {
                setElement(score, 0);
            }
            double GetZSetScore() const
            {
                return GetElement(0).GetFloat64();
            }
            void SetTTLKeyNamespace(const Data& ns)
            {
                setElement(ns, 1);
            }
            void SetTTLKey(const std::string& key)
            {
                getElement(2).SetString(key, false);
            }
            const Data& GetTTLKey() const
            {
                return GetElement(0);
            }
            void SetTTL(int64 ttl)
            {
                getElement(0).SetInt64(ttl);
            }
            int64 GetTTL()
            {
                return GetElement(0).GetInt64();
            }
            void SetMember(const Data& data, uint32 idx)
            {
                getElement(idx).Clone(data);
            }

            bool IsValid() const;
            int Compare(const KeyObject& other) const;
            // compare (namespace, key)
            int ComparePrefix(const KeyObject& other) const;
            Slice Encode(Buffer& buffer, bool verify = true, bool with_ns = false) const;
            void EncodePrefix(Buffer& buffer) const;
            bool DecodeNS(Buffer& buffer, bool clone_str);
            bool DecodeKey(Buffer& buffer, bool clone_str);
            bool DecodeType(Buffer& buffer);
            bool DecodePrefix(Buffer& buffer, bool clone_str);
            int DecodeElementLength(Buffer& buffer);
            bool DecodeElement(Buffer& buffer, bool clone_str, int idx);
            bool Decode(Buffer& buffer, bool clone_str, bool with_ns = false);

            void CloneStringPart();

            ~KeyObject()
            {
            }
    };

    struct Meta
    {
            int64_t ttl;
            char reserved[8];
            Meta() :
                    ttl(0)
            {
            }
    };

    struct MetaObject
    {
            uint8 format;          //meta format version
            int64_t ttl;
            int64_t size;
            bool list_sequential;  //indicate that list is sequential ot not
            MetaObject();
            void Encode(Buffer& buffer, uint8 type) const;
            bool Decode(Buffer& buffer, uint8 type);
            void Clear();
    };



    class ValueObject
    {
        private:
            uint8 type;
            uint16 merge_op;
            MetaObject meta;
            DataArray vals;
            Data& getElement(uint32_t idx)
            {
                if (vals.size() <= idx)
                {
                    vals.resize(idx + 1);
                }
                return vals[idx];
            }
        public:
            ValueObject() :
                    type(0), merge_op(0)
            {
            }
            void Clear()
            {
                type = 0;
                merge_op = 0;
                vals.clear();
                meta.Clear();
            }
            uint8 GetType() const
            {
                return type;
            }
            uint16& GetMergeOp()
            {
                return merge_op;
            }
            void SetType(uint8 t);
            void SetMergeOp(uint16 op)
            {
                merge_op = op;
            }
            MetaObject& GetMetaObject();
            int64 GetObjectLen()
            {
                if (type == 0)
                    return 0;
                return GetMetaObject().size;
            }
            void SetObjectLen(int64 v)
            {
                GetMetaObject().size = v;
            }
            Data& GetStringValue()
            {
                return getElement(0);
            }
            void SetStringValue(const std::string& v)
            {
                GetStringValue().SetString(v, true);
            }
            void SetHashValue(const Data& v)
            {
                getElement(0).Clone(v);
            }
            void SetHashValue(const std::string& v)
            {
                getElement(0).SetString(v, true);
            }
            void SetListElement(const std::string& v)
            {
                getElement(0).SetString(v, true);
            }
            void SetListElement(const Data& v)
            {
                getElement(0).Clone(v);
            }
            DataArray& GetMergeArgs()
            {
                return vals;
            }
            bool SetMinMaxData(const Data& v);
            bool SetMinData(const Data& v, bool overwite = false);
            bool SetMaxData(const Data& v, bool overwite = false);
            void ClearMinMaxData();
            Data& GetMin()
            {
                return getElement(0);
            }
            Data& GetMax()
            {
                return getElement(1);
            }
            int64 GetListMinIdx()
            {
                return GetMin().GetInt64();
            }
            int64 GetListMaxIdx()
            {
                return GetMax().GetInt64();
            }
            void SetListMaxIdx(int64 v)
            {
                GetMax().SetInt64(v);
            }
            void SetListMinIdx(int64 v)
            {
                GetMin().SetInt64(v);
            }
            Data& GetHashValue()
            {
                return getElement(0);
            }
            Data& GetListElement()
            {
                return getElement(0);
            }
            int64_t GetTTL();
            void SetTTL(int64_t v);
            double GetZSetScore()
            {
                return getElement(0).GetFloat64();
            }
            void SetZSetScore(double s)
            {
                getElement(0).SetFloat64(s);
            }
            void SetMergeArgs(const DataArray& args)
            {
                vals = args;
            }
            const DataArray& GetMergeArgs() const
            {
                return vals;
            }
            Slice Encode(Buffer& buffer) const;
            bool DecodeMeta(Buffer& buffer);
            bool Decode(Buffer& buffer, bool clone_str);
            void CloneStringPart();
    };

    struct ZRangeSpec
    {
            Data min, max;
            bool contain_min, contain_max;
            ZRangeSpec() :
                    contain_min(false), contain_max(false)
            {
                min.SetInt64(0);
                max.SetInt64(0);
            }
            bool Parse(const std::string& minstr, const std::string& maxstr);

            int InRange(const Data& d) const
            {
                int inrange = 0;
                if (d >= min)
                {
                    if (d <= max)
                    {
                        inrange = 0;
                    }
                    else
                    {
                        inrange = 1;
                    }
                }
                else
                {
                    inrange = -1;
                }
                if (0 == inrange)
                {
                    if (!contain_min && d == min)
                    {
                        inrange = -1;
                    }
                    if (!contain_max && d == max)
                    {
                        inrange = 1;
                    }
                }
                return inrange;
            }
            int InRange(double d) const
            {
                Data data;
                data.SetFloat64(d);
                return InRange(data);
            }
    };
    struct ZLexRangeSpec
    {
            std::string min;
            std::string max;
            bool include_min;
            bool include_max;
            ZLexRangeSpec() :
                    include_min(false), include_max(false)
            {
            }
            bool Parse(const std::string& minstr, const std::string& maxstr);
            int InRange(const std::string& str) const
            {
                int in_range = -1;
                if (max.empty())
                {
                    in_range = str >= min ? 0 : -1;
                }
                else
                {
                    if (str >= min)
                    {
                        if (str <= max)
                        {
                            in_range = 0;
                        }
                        else
                        {
                            in_range = 1;
                        }
                    }
                    else
                    {
                        in_range = -1;
                    }
                }

                if (0 == in_range)
                {
                    if (!include_min && str == min)
                    {
                        return -1;
                    }
                    if (!include_max && str == max)
                    {
                        return 1;
                    }
                }
                return in_range;
            }
    };

    int encode_merge_operation(Buffer& buffer, uint16_t op, const DataArray& args);
    KeyType element_type(KeyType type);

    typedef std::vector<KeyObject> KeyObjectArray;
    typedef std::vector<ValueObject> ValueObjectArray;

    typedef std::vector<int> ErrCodeArray;

OP_NAMESPACE_END

#endif /* TYPES_HPP_ */
