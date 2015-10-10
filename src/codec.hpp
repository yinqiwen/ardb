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
        KEY_UNKNOWN, KEY_META = 1, KEY_STRING = 2, KEY_HASH = 3, KEY_LIST = 4, KEY_SET = 5, KEY_ZSET_DATA = 6, KEY_ZSET_SCORE = 7,
        /*
         * Reserver 20 types
         */

        KEY_TTL_DATA = 100, KEY_TTL_SORT = 101,

        KEY_END = 255, /* max value for 1byte */
    };

    typedef uint16 DBID;

    typedef std::vector<DBID> DBIDArray;
    typedef TreeSet<DBID>::Type DBIDSet;

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
            bool Decode(Buffer& buf);

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
    typedef std::vector<Data> DataArray;
    typedef TreeMap<Data, Data>::Type DataMap;
    typedef TreeSet<Data>::Type DataSet;

    struct KeyObject
    {
        public:
            const char* ns; //namespace
            uint32 db;
            uint8 type;
            Data elements[3];

            KeyObject(uint32 dbid = 0, uint8 t = KEY_STRING, const std::string& data = "") :
                    ns(NULL), db(dbid), type(t)
            {
                elements[0].SetString(data, false);
            }
            void Encode(Buffer& buf) const;
            bool Decode(Buffer& buf);

            void Clear()
            {
                ns = NULL;
                db = 0;
                type = 0;
                elements[0].Clear();
                elements[1].Clear();
                elements[2].Clear();
            }
            ~KeyObject()
            {
                Clear();
            }

    };

    struct ValueObject
    {
            Data value;
            ValueObject()
            {
            }
            void Encode(Buffer& buf) const;
            bool Decode(Buffer& buf);
    };

    typedef std::vector<int64> Int64Array;
    typedef std::vector<int> IntArray;
    typedef std::vector<double> DoubleArray;
    typedef std::vector<std::string> StringArray;
    typedef TreeSet<std::string>::Type StringSet;

    typedef std::vector<Slice> SliceArray;
    typedef std::vector<KeyObject> KeyObjectArray;
    typedef std::vector<ValueObject> ValueObjectArray;

OP_NAMESPACE_END

#endif /* TYPES_HPP_ */
