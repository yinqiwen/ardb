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
        KEY_META = 0, V_STRING = 1, V_HASH = 2, V_LIST = 3, V_SET = 4, V_ZSET = 5,
        /*
         * Reserver 20 types
         */


        V_TTL = 100,

        KEY_END = 255, /* max value for 1byte */
    };

    typedef uint32 DBID;

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

            bool Encode(Buffer& buf) const;
            bool Decode(Buffer& buf);

            void SetString(const std::string& str, bool try_int_encoding);
            double NumberValue() const;
            void SetInt64(int64 v);
            void SetDouble(double v);
            bool GetInt64(int64& v) const;
            bool GetDouble(double& v) const;

            void Clone(const Data& data);
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
            bool IsInteger() const;
            uint32 StringLength() const;
            void Clear();
            const std::string& ToString(std::string& str)const;
    };
    typedef std::vector<Data> DataArray;
    typedef TreeMap<Data, Data>::Type DataMap;
    typedef TreeSet<Data>::Type DataSet;

    struct KeyObject
    {
        public:
            DBID db;
            uint8 type;
            DataArray elements;

            KeyObject(DBID id = 0, uint8 t = V_STRING, const Data& data = "") :
                    db(0), type(t)
            {
                elements.push_back(data);
            }
            void Encode(Buffer& buf);
            bool Decode(Buffer& buf);
            bool Decode(Slice& slice);

            void Clear()
            {
                db = 0;
                type = 0;
                elements.clear();
            }

//            KeyObject(const KeyObject& other) :
//                    db(0), type(0)
//            {
//                db = other.db;
//                type = other.type;
//                elements = other.elements;
//            }
//            KeyObject& operator=(const KeyObject& other)
//            {
//                Clear();
//                db = other.db;
//                type = other.type;
//                elements = other.elements;
//                return *this;
//            }

            ~KeyObject()
            {
                Clear();
            }

    };

    enum MergeOpType
    {
        M_INCR = 1, M_INCRFLOAT = 2, M_APPEND = 3, M_HINCR = 4,
    };

    struct MergeOp
    {
            uint8 op;
            Data val;
            void Encode(Buffer& buf);
    };

    typedef std::vector<int64> Int64Array;
    typedef std::vector<double> DoubleArray;
    typedef std::vector<std::string> StringArray;
    typedef TreeSet<std::string>::Type StringSet;

    typedef std::vector<Slice> SliceArray;
    typedef std::vector<KeyObject> KeyObjectArray;

OP_NAMESPACE_END

#endif /* TYPES_HPP_ */
