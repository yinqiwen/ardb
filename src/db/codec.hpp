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

OP_NAMESPACE_BEGIN

    enum KeyType
    {
        KEY_UNKNOWN, KEY_STRING = 2, KEY_HASH = 4, KEY_LIST_META = 5, KEY_LIST = 6, KEY_SET = 8, KEY_ZSET_SORT = 9, KEY_ZSET_SCORE = 10,
        /*
         * Reserver 20 types
         */

        KEY_TTL_SORT = 33, KEY_TTL_VALUE = 34,

        KEY_END = 255, /* max value for 1byte */
    };

    struct Data
    {
            char data[8];
            unsigned encoding :3;
            unsigned len :29;

            Data();
            Data(const std::string& v, bool try_int_encoding);
            Data(const Data& data);
            Data& operator=(const Data& data);
            ~Data();

            void Encode(Buffer& buf) const;
            bool Decode(Buffer& buf, bool clone_str);

            void SetString(const std::string& str, bool try_int_encoding);
            void SetInt64(int64 v);
            void SetDouble(double v);
            int64 GetInt64() const;
            bool GetDouble(double& v) const;

            void Clone(const Data& data);
            int Compare(const Data& other, bool alpha_cmp = false) const;
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
            bool IsInteger() const;
            uint32 StringLength() const;
            void Clear();
            const char* CStr() const;
            const std::string& ToString(std::string& str) const;
    };

    struct KeyObject
    {
        private:
            Data ns; //namespace
            uint8 type;
            Data key;
            Data element;
            double score;
            uint8 ttl_key_type;

            Buffer encode_buffer;
            inline void clearEncodeBuffer()
            {
                encode_buffer.Clear();
            }
            void setElement(const Data& data)
            {
                element = data;
            }
        public:
            KeyObject(const Data& nns, uint8 t = KEY_STRING, const std::string& data = "") :
                    ns(nns), type(t), score(0), ttl_key_type(0)
            {
                key.SetString(data, false);
            }
            KeyObject(const Data& nns, uint8 t = KEY_STRING, const Data& key_data) :
                    ns(nns), type(t), key(key_data), score(0), ttl_key_type(0)
            {
            }
            const Data& GetStringKey() const
            {
                return key;
            }
            KeyType GetKeyType() const
            {
                return (KeyType) type;
            }

            void SetHashField(const Data& v)
            {
                setElement(v);
            }
            void SetListIndex(double idx)
            {
                this->score = idx;
            }
            void SetSetMember(const Data& v)
            {
                setElement(v);
            }
            void SetZSetMember(const Data& v)
            {
                setElement(v);
            }
            void SetZSetScore(double score)
            {
                this->score = score;
            }
            void SetTTLKeyType(KeyType t)
            {
                ttl_key_type = t;
            }
            int Compare(const KeyObject& other);

            Buffer& Encode();
            bool Decode(Buffer& buffer);

            void Clear();
            ~KeyObject()
            {
            }
    };

    struct ListMeta
    {
            int64_t min_idx;
            int64_t max_idx;
            size_t size;
            bool sequencial;
    };

    class ValueObject
    {
        private:
            Data value;
            ListMeta list_meta;
            double score;
            uint8 key_type;
        public:
            ValueObject(uint8 ktype = 0) :
                    score(0), key_type(ktype)
            {
            }
            Data& GetStringValue();
            void SetStringValue(const std::string& str);
            Data& GetHashValue();
            Data& GetListElement();
            double GetZSetScore();
            ListMeta& GetListMeta();
    };

    typedef std::vector<Data> DataArray;
    struct MergeOperation
    {
            uint16 op;
            DataArray values;
            Data& Add()
            {
                values.resize(values.size() + 1);
                return values[values.size() - 1];
            }
            bool Decode(Buffer& buffer);
            void Encode(Buffer& buffer);
    };

    typedef std::vector<KeyObject> KeyObjectArray;
    typedef std::vector<ValueObject> ValueObjectArray;

    typedef std::vector<int> ErrCodeArray;

OP_NAMESPACE_END

#endif /* TYPES_HPP_ */
