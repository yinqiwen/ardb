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
 * data_format.hpp
 *
 *  Created on: 2013/12/24
 *      Author: wqy
 */

#ifndef DATA_FORMAT_HPP_
#define DATA_FORMAT_HPP_
#include <stdint.h>
#include <float.h>
#include <math.h>
#include <list>
#include <vector>
#include <deque>
#include <string>
#include "common.hpp"
#include "slice.hpp"
#include "buffer/struct_codec_macros.hpp"
#include "util/helpers.hpp"

#define ARDB_META_VERSION 0

#define ARDB_GLOBAL_DB 0xFFFFFF

#define COMPARE_NUMBER(a, b)  (a == b?0:(a>b?1:-1))

//#define COMPARE_SLICE(a, b)  (a.size() == b.size()?(a.compare(b)):(a.size()>b.size()?1:-1))

#define ZSET_ENCODING_ZIPLIST  1
#define ZSET_ENCODING_HASH     2

#define COMPARE_EXIST(a, b)  do{ \
	if(!a && !b) return 0;\
	if(a != b) return COMPARE_NUMBER(a,b); \
}while(0)

#define RETURN_NONEQ_RESULT(a, b)  do{ \
	if(a != b) return COMPARE_NUMBER(a,b); \
}while(0)

namespace ardb
{
    /*
     * 3 bytes, value from [0, 0xFFFFFF]
     */
    typedef uint32 DBID;

    typedef TreeSet<DBID>::Type DBIDSet;
    typedef std::vector<DBID> DBIDArray;

    enum KeyType
    {
        KEY_META = 0, STRING_META = 1, BITSET_META = 2, SET_META = 3, ZSET_META = 4, HASH_META = 5, LIST_META = 6,
        /*
         * Reserver 20 types
         */

        SET_ELEMENT = 20,

        ZSET_ELEMENT_NODE = 30, ZSET_ELEMENT = 31,

        HASH_FIELD = 40,

        LIST_ELEMENT = 50,

        BITSET_ELEMENT = 70,

        KEY_EXPIRATION_ELEMENT = 100, SCRIPT = 102,

        KEY_END = 255, /* max value for 1byte */
    };

    struct DBItemKey
    {
            DBID db;
            std::string key;
            DBItemKey(const DBID& id = 0, const Slice& k = Slice()) :
                    db(id)
            {
                if (k.size() > 0)
                {
                    key.assign(k.data(), k.size());
                }
            }
            bool operator<(const DBItemKey& other) const
            {
                if (db > other.db)
                {
                    return false;
                }
                if (db == other.db)
                {
                    return key.compare(other.key) < 0;
                }
                return true;
            }
    };

    struct DBItemStackKey
    {
            DBID db;
            Slice key;
            DBItemStackKey(const DBID& id = 0, const Slice& k = "") :
                    db(id), key(k)
            {
            }
            bool operator<(const DBItemStackKey& other) const
            {
                if (db < other.db)
                {
                    return true;
                }
                if (db == other.db)
                {
                    return key.compare(other.key) < 0;
                }
                return false;
            }
    };

    struct KeyObject
    {
            DBID db;
            KeyType type;
            Slice key;

            KeyObject(const Slice& k, KeyType t, DBID id) :
                    db(id), type(t), key(k)
            {
            }
            virtual ~KeyObject()
            {
            }
    };

    struct ValueObject
    {
            virtual ~ValueObject()
            {
            }
    };

    enum ValueDataType
    {
        EMPTY_VALUE = 0, INTEGER_VALUE = 1, DOUBLE_VALUE = 2, BYTES_VALUE = 3,

        MAX_VALUE_TYPE = 127
    };

    struct ValueData
    {
            uint8 type;
            int64 integer_value;
            double double_value;
            std::string bytes_value;

            Slice slice_value;

            ValueData(const Slice& slice) :
                    type(EMPTY_VALUE), integer_value(0), double_value(0)
            {
                SetValue(slice, true);
            }
            ValueData(double v) :
                    type(DOUBLE_VALUE), integer_value(0), double_value(v)
            {
            }
            ValueData(int64 v) :
                    type(INTEGER_VALUE), integer_value(v), double_value(0)
            {
            }
            void SetValue(const Slice& value, bool auto_convert);
            void SetIntValue(int64 value)
            {
                type = INTEGER_VALUE;
                integer_value = value;
            }
            void SetDoubleValue(double value)
            {
                type = DOUBLE_VALUE;
                double_value = value;
            }
            ValueData() :
                    type(EMPTY_VALUE), integer_value(0), double_value(0)
            {
            }
            inline bool Empty() const
            {
                return type == EMPTY_VALUE || type == MAX_VALUE_TYPE;
            }
            inline bool operator<(const ValueData& other) const
            {
                return Compare(other) < 0;
            }
            inline bool operator>(const ValueData& other) const
            {
                return Compare(other) > 0;
            }
            inline bool operator==(const ValueData& other) const
            {
                return Compare(other) == 0;
            }
            inline bool operator>=(const ValueData& other) const
            {
                return Compare(other) >= 0;
            }
            inline bool operator<=(const ValueData& other) const
            {
                return Compare(other) <= 0;
            }
            inline bool operator!=(const ValueData& other) const
            {
                return Compare(other) != 0;
            }
            ValueData& operator+=(const ValueData& other);
            ValueData& operator/=(uint32 i);
            double NumberValue() const;
            bool Encode(Buffer& buf) const;
            bool Decode(Buffer& buf, bool to_slice= false);
            std::string& ToString(std::string& str) const;
            std::string AsString() const;
            int ToNumber();
            int ToBytes();
            int Incrby(int64 value);
            int IncrbyFloat(double value);
            int Compare(const ValueData& other) const;
            void Clear();
    };



    struct CommonValueObject: public ValueObject
    {
            ValueData data;
            CommonValueObject()
            {
            }
            CommonValueObject(const ValueData& v) :
                    data(v)
            {
            }
        CODEC_DEFINE(data)
            ;
    };
    struct EmptyValueObject: public ValueObject
    {
            bool Encode(Buffer& buf)
            {
                return true;
            }
            bool Decode(Buffer& buf)
            {
                return true;
            }
    };

    typedef std::deque<ValueData> ValueDataArray;
    typedef std::deque<ValueData> ValueDataDeque;
    typedef std::pair<double, uint32> ScoreCount;
    typedef TreeMap<ValueData, double>::Type ValueScoreMap;
    typedef TreeMap<ValueData, ScoreCount>::Type ValueScoreCountMap;
    typedef TreeMap<ValueData, ValueData>::Type HashFieldMap;
    typedef TreeSet<ValueData>::Type ValueSet;
    typedef std::vector<double> DoubleArray;

    typedef std::deque<Slice> SliceArray;
    typedef std::deque<std::string> StringArray;
    typedef std::vector<int64> Int64Array;
    typedef std::vector<uint32> UInt32Array;
    typedef TreeMap<std::string, Slice>::Type StringSliceMap;
    typedef TreeSet<std::string>::Type StringSet;
    typedef TreeSet<Slice>::Type SliceSet;
    typedef std::vector<uint32_t> WeightArray;
    typedef std::vector<double> ZSetScoreArray;
    typedef TreeMap<std::string, std::string>::Type StringStringMap;
    typedef TreeMap<uint64, std::string>::Type UInt64StringMap;

    struct ZSetElement
    {
            ValueData score;
            ValueData value;
            ValueData attr;
            ZSetElement() :
                    score((int64) 0)
            {
            }
            ZSetElement(const Slice& v, const ValueData& s) :
                    score(s), value(v)
            {
            }
            ZSetElement(const Slice& v, int64 s) :
                    score(s), value(v)
            {
            }
            inline bool operator<(const ZSetElement& x)
            {
                if (score < x.score)
                    return true;
                if (score == x.score)
                    return value < x.value;
                return false;
            }
            ZSetElement(const Slice& v, double s) :
                    score(s), value(v)
            {
                uint64 c = (uint64) s;
                if ((double) c == s)
                {
                    score.SetIntValue(c);
                }
            }
            bool Empty()
            {
                return score.Empty() && value.Empty();
            }
            void Clear()
            {
                score.Clear();
                value.Clear();
                attr.Clear();
            }
        CODEC_DEFINE(score, value, attr)
    };

    struct ZRangeSpec
    {
            ValueData min, max;
            bool contain_min, contain_max;
            ZRangeSpec() :
                    contain_min(false), contain_max(false)
            {
            }
    };

    struct ZScoreRangeCounter
    {
            double min, max;
            uint32 count;CODEC_DEFINE(min, max, count)
            ZScoreRangeCounter() :
                    min(0), max(0), count(0)
            {
            }
            inline bool operator<(const ZScoreRangeCounter& x)
            {
                if (min < x.min)
                    return true;
                if (min == x.min)
                    return max < x.max;
                return false;
            }
    };

    struct ZSetNodeValueObject: public ValueObject
    {
            ValueData score;
            ValueData attr;CODEC_DEFINE(score, attr)
            ;
    };

    typedef std::deque<ZScoreRangeCounter> ZScoreRangeCounterArray;
    typedef std::deque<ZSetElement> ZSetElementDeque;
    typedef std::vector<ZSetElementDeque> ZSetElementDequeArray;

    struct ZSetKeyObject: public KeyObject
    {
            ValueData score;
            ValueData value;
            ZSetKeyObject(const Slice& k, const ZSetElement& ee, DBID id);
            ZSetKeyObject(const Slice& k, const Slice& v, const ValueData& s, DBID id);
            ZSetKeyObject(const Slice& k, const ValueData& v, const ValueData& s, DBID id);
    };
    struct ZSetNodeKeyObject: public KeyObject
    {
            ValueData value;
            ZSetNodeKeyObject(const Slice& k, const Slice& v, DBID id);
            ZSetNodeKeyObject(const Slice& k, const ValueData& v, DBID id);
    };

    struct MetaValueHeader
    {
            uint8 version;
            KeyType type;
            uint64 expireat;
            MetaValueHeader() :
                    version(ARDB_META_VERSION), type(KEY_META), expireat(0)
            {
            }
    };
    struct CommonMetaValue: public ValueObject
    {
            MetaValueHeader header;
            virtual ~CommonMetaValue()
            {
            }
    };
    struct StringMetaValue: public CommonMetaValue
    {
            ValueData value;
            bool Encode(Buffer& buf)
            {
                return value.Encode(buf);
            }
            bool Decode(Buffer& buf)
            {
                return value.Decode(buf);
            }
            StringMetaValue()
            {
                header.type = STRING_META;
            }
    };

    struct ZSetMetaValue: public CommonMetaValue
    {
            uint8 encoding;
            uint8 sort_func;
            uint32 size;
            ZSetElementDeque zipvs;
            ZScoreRangeCounterArray ranges;CODEC_DEFINE(encoding, sort_func, size,zipvs, ranges)
            ;
            ZSetMetaValue() :
                    encoding(true), sort_func(0), size(0)
            {
                header.type = ZSET_META;
            }
    };
    struct SetMetaValue: public CommonMetaValue
    {
            bool ziped;
            bool dirty;
            uint32_t size;
            ValueData min;
            ValueData max;
            ValueSet zipvs;CODEC_DEFINE(size,ziped, dirty, min, max, zipvs)
            ;
            SetMetaValue() :
                    ziped(true), dirty(false), size(0)
            {
                header.type = SET_META;
            }
    };

    struct HashMetaValue: public CommonMetaValue
    {
            bool ziped;
            bool dirty;
            uint32_t size;
            HashFieldMap values;CODEC_DEFINE(ziped, dirty, size, values)
            ;
            HashMetaValue() :
                    ziped(true), dirty(false), size(0)
            {
                header.type = HASH_META;
            }
    };
    struct BitSetMetaValue: public CommonMetaValue
    {
            uint64 bitcount;
            uint64 min;
            uint64 max;CODEC_DEFINE(bitcount,min,max)
            ;
            BitSetMetaValue() :
                    bitcount(0), min(0), max(0)
            {
                header.type = BITSET_META;
            }
    };
    struct ListMetaValue: public CommonMetaValue
    {
            uint32_t size;
            bool ziped;
            ValueData min_score;
            ValueData max_score;
            ValueDataDeque zipvs;CODEC_DEFINE(size,ziped,min_score,max_score,zipvs)
            ;
            ListMetaValue() :
                    size(0), ziped(false), min_score((int64) 0), max_score((int64) 0)
            {
                header.type = LIST_META;
            }
    };

    struct SetKeyObject: public KeyObject
    {
            ValueData value;
            SetKeyObject(const Slice& k, const Slice& v, DBID id);
            SetKeyObject(const Slice& k, const ValueData& v, DBID id);
    };

    struct BitSetKeyObject: public KeyObject
    {
            uint64 index;
            BitSetKeyObject(const Slice& k, uint64 v, DBID id) :
                    KeyObject(k, BITSET_ELEMENT, id), index(v)
            {
            }
    };

    struct BitSetElementValue: public ValueObject
    {
            uint32 bitcount;
            uint32 start;
            uint32 limit;
            std::string vals;CODEC_DEFINE(bitcount,start,limit,vals)
            ;
            BitSetElementValue() :
                    bitcount(0), start(0), limit(0)
            {
            }
    };

    struct HashKeyObject: public KeyObject
    {
            ValueData field;
            HashKeyObject(const Slice& k, const ValueData& f, DBID id) :
                    KeyObject(k, HASH_FIELD, id), field(f)
            {
            }
            HashKeyObject(const Slice& k, const Slice& f, DBID id) :
                    KeyObject(k, HASH_FIELD, id)
            {
                field.SetValue(f, true);
            }
    };
    struct ListKeyObject: public KeyObject
    {
            ValueData score;
            ListKeyObject(const Slice& k, const ValueData& s, DBID id) :
                    KeyObject(k, LIST_ELEMENT, id), score(s)
            {
            }
            ListKeyObject(const Slice& k, const double& s, DBID id) :
                    KeyObject(k, LIST_ELEMENT, id), score(s)
            {
            }
            ListKeyObject(const Slice& k, const int64& s, DBID id) :
                    KeyObject(k, LIST_ELEMENT, id), score(s)
            {
            }
    };

    /*
     * All key expiratat value would be stored together for checking
     */
    struct ExpireKeyObject: public KeyObject
    {
            uint64 expireat;
            ExpireKeyObject(const Slice& k, uint64 ts, DBID id) :
                    KeyObject(k, KEY_EXPIRATION_ELEMENT, id), expireat(ts)
            {
            }
    };

    struct HashIterContext
    {
            //ValueData field;
            //ValueData value;
            StringArray* fields;
            StringArray* values;
            HashIterContext() :
                    fields(NULL), values(NULL)
            {
            }
    };

    enum AggregateType
    {
        AGGREGATE_EMPTY = 0,
        AGGREGATE_SUM = 1,
        AGGREGATE_MIN = 2,
        AGGREGATE_MAX = 3,
        AGGREGATE_COUNT = 4,
        AGGREGATE_AVG = 5,
    };

    struct ZSetQueryOptions
    {
            bool withscores;
            bool withlimit;
            bool withattr;
            int limit_offset;
            int limit_count;
            ZSetQueryOptions() :
                    withscores(false), withlimit(false), withattr(false), limit_offset(0), limit_count(0)
            {
            }
            bool Parse(const StringArray& cmd, uint32 idx);
    };

    struct SortOptions
    {
            const char* by;
            bool with_limit;
            int32 limit_offset;
            int32 limit_count;
            std::vector<const char*> get_patterns;
            bool is_desc;
            bool with_alpha;
            bool nosort;
            const char* store_dst;
            AggregateType aggregate;
            SortOptions() :
                    by(NULL), with_limit(false), limit_offset(0), limit_count(0), is_desc(false), with_alpha(false), nosort(
                            false), store_dst(
                    NULL), aggregate(AGGREGATE_EMPTY)
            {
            }
    };

    struct GeoAddOptions
    {
            uint8 coord_type;
            double x, y;
            Slice value;

            StringStringMap attrs;
            GeoAddOptions() :
                    coord_type(0), x(0), y(0)
            {
            }

            int Parse(const StringArray& args, std::string& err, uint32 off = 0);

    };

#define GEO_SEARCH_WITH_DISTANCES "WITHDISTANCES"
#define GEO_SEARCH_WITH_COORDINATES "WITHCOORDINATES"

    struct GeoSearchGetOption
    {
            bool get_coodinates;
            bool get_distances;
            bool get_attr;
            std::string get_pattern;
            GeoSearchGetOption() :
                    get_coodinates(false), get_distances(false), get_attr(false)
            {
            }
    };
    typedef std::deque<GeoSearchGetOption> GeoGetOptionDeque;

    struct GeoSearchOptions
    {
            uint8 coord_type;
            bool nosort;
            bool asc;   //sort by asc
            uint32 radius;  //range meters
            int32 offset;
            int32 limit;
            bool by_member;
            bool by_location;
            bool in_members;

            double x, y;
            std::string member;
            StringSet submembers;

            StringStringMap includes;
            StringStringMap excludes;

            GeoGetOptionDeque get_patterns;
            GeoSearchOptions() :
                    coord_type(0), nosort(true), asc(false), radius(0), offset(0), limit(0), by_member(false), by_location(
                            false), in_members(false), x(0), y(0)
            {
            }

            int Parse(const StringArray& args, std::string& err, uint32 off = 0);

    };

    struct GeoPoint
    {
            std::string value;
            double x;
            double y;

            StringStringMap attrs;

            /*
             * distance is a value stored while there is a
             */
            double distance;
            GeoPoint() :
                    x(0), y(0), distance(0)
            {
            }
        CODEC_DEFINE(x,y, attrs)
    };
    typedef std::deque<GeoPoint> GeoPointArray;

    struct LexRange
    {
            std::string min;
            std::string max;
            bool include_min;
            bool include_max;

            LexRange() :
                    include_min(false), include_max(false)
            {
            }
            bool Parse(const std::string& minstr, const std::string& maxstr);
    };

    struct ZRangeLexOptions
    {
            ZSetQueryOptions queryOption;
            LexRange range;
            bool reverse;

            int64 __cursor; //not used as para
            ZRangeLexOptions() :
                    reverse(false),__cursor(0)
            {
            }
    };

    /*
     * typedefs
     */
    typedef std::vector<ZSetMetaValue*> ZSetMetaValueArray;
    typedef std::vector<SetMetaValue*> SetMetaValueArray;
    typedef TreeMap<std::string, ValueData>::Type NameValueTable;

    typedef std::deque<ValueDataArray> ValueArrayArray;
    typedef TreeMap<uint64, BitSetElementValue>::Type BitSetElementValueMap;

    /*
     * return 0:continue -1:break
     */
    typedef int ValueVisitCallback(const ValueData& value, int cursor, void* cb);

    template<typename T>
    void delete_pointer_container(T& t)
    {
        typename T::iterator it = t.begin();
        while (it != t.end())
        {
            delete *it;
            it++;
        }
    }

    template<typename T, typename R>
    static void convert_vector_to_set(const T& array, R& set)
    {
        typename T::const_iterator it = array.begin();
        while (it != array.end())
        {
            set.insert(*it);
            it++;
        }
    }

    void encode_key(Buffer& buf, const KeyObject& key);
    KeyObject* decode_key(const Slice& key, KeyObject* expected);
    CommonMetaValue* decode_meta(const char* data, size_t size, bool only_head);
    void encode_meta(Buffer& buf, CommonMetaValue& meta);
    ValueObject* decode_value_obj(KeyType type, const char* data, size_t size);
    bool decode_value(Buffer& buf, ValueData& value, bool to_slice = false);
    bool decode_value_by_string(const std::string& str, ValueData& value);
    void encode_value(Buffer& buf, const ValueData& value);
    bool peek_dbkey_header(const Slice& key, DBID& db, KeyType& type);
    void next_key(const Slice& key, std::string& next);
    int ardb_compare_keys(const char* akbuf, size_t aksiz, const char* bkbuf, size_t bksiz);

}

#endif /* DATA_FORMAT_HPP_ */
