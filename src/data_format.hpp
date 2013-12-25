/*
 * data_format.hpp
 *
 *  Created on: 2013Äê12ÔÂ24ÈÕ
 *      Author: wqy
 */

#ifndef DATA_FORMAT_HPP_
#define DATA_FORMAT_HPP_
#include <msgpack.hpp>
#include <stdint.h>
#include <float.h>
#include <math.h>
#include <map>
#include <set>
#include <list>
#include <vector>
#include <deque>
#include <string>
#include "slice.hpp"

namespace ardb
{
    /*
     * 3 bytes, value from [0, 0xFFFFFF]
     */
    typedef uint32 DBID;

    typedef std::set<DBID> DBIDSet;

    enum KeyType
    {
        KEY_META = 0,
        STRING_META = 1,
        BITSET_META = 2,
        SET_META = 3,
        ZSET_META = 4,
        HASH_META = 5,
        LIST_META = 6,
        TABLE_META = 7,
        /*
         * Reserver 20 types
         */

        SET_ELEMENT = 20,

        ZSET_ELEMENT_SCORE = 30,
        ZSET_ELEMENT = 31,

        HASH_FIELD = 40,

        LIST_ELEMENT = 50,

        TABLE_INDEX = 60,
        TABLE_COL = 61,
        TABLE_SCHEMA = 62,

        BITSET_ELEMENT = 70,

        KEY_EXPIRATION_ELEMENT = 100,
        SCRIPT = 102,

        KEY_END = 255, /* max value for 1byte */
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

    enum ValueDataType
    {
        EMPTY_VALUE = 0, INTEGER_VALUE = 1, DOUBLE_VALUE = 2, BYTES_VALUE = 3
    };

    struct ValueObject
    {
            ValueDataType type;
            int64 integer_value;
            double double_value;
            std::string bytes_value;
            ValueObject(const Slice& slice) :
                    type(BYTES_VALUE), integer_value(0), double_value(0)
            {
                bytes_value.assign(slice.data(), slice.size());
            }
            void SetValue(const Slice& value)
            {
                type = BYTES_VALUE;
                bytes_value.assign(value.data(), value.size());
            }
            MSGPACK_DEFINE(type, integer_value, double_value, bytes_value);
            ValueObject() :
            type(EMPTY_VALUE), integer_value(0), double_value(0)
            {
            }
            void ToString(std::string& str)
            {

            }
        };

    struct ZSetKeyObject: public KeyObject
    {
            double score;
            ValueObject value;MSGPACK_DEFINE(score, value);
            ZSetKeyObject(const Slice& k, const Slice& v, double s, DBID id);
            ZSetKeyObject(const Slice& k, const ValueObject& v, double s,
                    DBID id);
        };
    struct ZSetScoreKeyObject: public KeyObject
    {
            ValueObject value;MSGPACK_DEFINE(value);
            ZSetScoreKeyObject(const Slice& k, const Slice& v, DBID id);
            ZSetScoreKeyObject(const Slice& k, const ValueObject& v, DBID id);
        };

    struct MetaValueHeader
    {
            KeyType type;
            uint64 expireat;
            MetaValueHeader() :
                    type(KEY_META), expireat(0)
            {
            }
            MSGPACK_DEFINE(type, expireat);
        };
    struct CommonMetaValue
    {
            MetaValueHeader header;
            virtual ~CommonMetaValue()
            {
            }
    };
    struct StringMetaValue: public CommonMetaValue
    {
            ValueObject value;MSGPACK_DEFINE(value);
            StringMetaValue()
            {
                header.type = STRING_META;
            }
        };

    struct ZSetMetaValue: public CommonMetaValue
    {
            uint32_t size;
            double min_score;
            double max_score;bool ziped;
                    MSGPACK_DEFINE(size, min_score, max_score, ziped);
            ZSetMetaValue() :
            size(0), min_score(0), max_score(0),ziped(false)
            {
            }
        };

    struct SetKeyObject: public KeyObject
    {
            ValueObject value;MSGPACK_DEFINE(value);
            SetKeyObject(const Slice& k, const Slice& v, DBID id);
            SetKeyObject(const Slice& k, const ValueObject& v, DBID id);
        };
    struct SetMetaValue
    {
            uint32_t size;bool ziped;
            ValueObject min;
            ValueObject max;MSGPACK_DEFINE(size,min, max );
            SetMetaValue() :
            size(0),ziped(false)
            {
            }
        };
    struct BitSetKeyObject: public KeyObject
    {
            uint64 index;MSGPACK_DEFINE(index);
            BitSetKeyObject(const Slice& k, uint64 v, DBID id) :
            KeyObject(k, BITSET_ELEMENT, id), index(v)
            {

            }
        };

    struct BitSetElementValue
    {
            uint32 bitcount;
            uint32 start;
            uint32 limit;
            std::string vals;MSGPACK_DEFINE(bitcount,start,limit,vals);
            BitSetElementValue() :
            bitcount(0), start(0), limit(0)
            {
            }
        };
    struct BitSetMetaValue
    {
            uint64 bitcount;
            uint64 min;
            uint64 max;MSGPACK_DEFINE(bitcount,min,max);
            BitSetMetaValue() :
            bitcount(0), min(0), max(0)
            {
            }
        };

    struct HashKeyObject: public KeyObject
    {
            ValueObject field;MSGPACK_DEFINE(field);
            HashKeyObject(const Slice& k, const Slice& f, DBID id) :
            KeyObject(k, HASH_FIELD, id), field(f)
            {
            }
        };
    struct ListKeyObject: public KeyObject
    {
            float score;MSGPACK_DEFINE(score);
            ListKeyObject(const Slice& k, double s, DBID id) :
            KeyObject(k, LIST_ELEMENT, id), score(s)
            {
            }
        };
    struct ListMetaValue
    {
            uint32_t size;bool ziped;
            float min_score;
            float max_score;MSGPACK_DEFINE(size,ziped,min_score,max_score);
            ListMetaValue() :
            size(0), ziped(false), min_score(0), max_score(0)
            {
            }
        };

    struct TableMetaValue
    {
            uint32 size;MSGPACK_DEFINE(size);
            TableMetaValue() :
            size(0)
            {
            }
        };
    struct TableSchemaValue
    {
            StringArray keynames;
            StringSet valnames;
            StringSet indexed;
            TableSchemaValue()
            {
            }
            int Index(const Slice& key);bool HasIndex(const Slice& name);
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

    void encode_meta(Buffer& buf, CommonMetaValue& key);

}

#endif /* DATA_FORMAT_HPP_ */
