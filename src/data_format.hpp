/*
 * data_format.hpp
 *
 *  Created on: 2013Äê12ÔÂ24ÈÕ
 *      Author: wqy
 */

#ifndef DATA_FORMAT_HPP_
#define DATA_FORMAT_HPP_
#include <stdint.h>
#include <float.h>
#include <math.h>
#include <map>
#include <set>
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

#define COMPARE_SLICE(a, b)  (a.size() == b.size()?(a.compare(b)):(a.size()>b.size()?1:-1))

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
		/*
		 * Reserver 20 types
		 */

		SET_ELEMENT = 20,

		ZSET_ELEMENT_SCORE = 30,
		ZSET_ELEMENT = 31,

		HASH_FIELD = 40,

		LIST_ELEMENT = 50,

		BITSET_ELEMENT = 70,

		KEY_EXPIRATION_ELEMENT = 100,
		SCRIPT = 102,

		KEY_END = 255, /* max value for 1byte */
	};

	struct DBItemKey
	{
			DBID db;
			Slice key;
			DBItemKey(const DBID& id = 0, const Slice& k = "") :
					db(id), key(k)
			{
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
		EMPTY_VALUE = 0, INTEGER_VALUE = 1, DOUBLE_VALUE = 2, BYTES_VALUE = 3
	};

	struct ValueData
	{
			uint8 type;
			int64 integer_value;
			double double_value;
			std::string bytes_value;

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
			void SetValue(int64 value)
			{
				type = INTEGER_VALUE;
				integer_value = value;
			}
			void SetValue(double value)
			{
				type = DOUBLE_VALUE;
				double_value = value;
			}
			ValueData() :
					type(EMPTY_VALUE), integer_value(0), double_value(0)
			{
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
			bool Decode(Buffer& buf);
//			template<typename Packer>
//			void msgpack_pack(Packer& pk) const
//			{
//				if (type == EMPTY_VALUE)
//				{
//					pk.pack_array(1);
//				} else
//				{
//					pk.pack_array(2);
//				}
//				pk.pack(type);
//				switch (type)
//				{
//					case DOUBLE_VALUE:
//					{
//						pk.pack(double_value);
//						break;
//					}
//					case INTEGER_VALUE:
//					{
//						pk.pack(integer_value);
//						break;
//					}
//					case BYTES_VALUE:
//					{
//						pk.pack(bytes_value);
//						break;
//					}
//					default:
//					{
//						break;
//					}
//				}
//
//			}
//			void msgpack_unpack(msgpack::object o)
//			{
//				if (o.type != msgpack::type::ARRAY)
//				{
//					throw msgpack::type_error();
//				}
//				const size_t size = o.via.array.size;
//				if (size <= 0)
//				{
//					return;
//				}
//				o.via.array.ptr[0].convert(&type);
//				if (size <= 1)
//				{
//					return;
//				}
//				switch (type)
//				{
//					case DOUBLE_VALUE:
//					{
//						o.via.array.ptr[1].convert(&double_value);
//						break;
//					}
//					case INTEGER_VALUE:
//					{
//						o.via.array.ptr[1].convert(&integer_value);
//						break;
//					}
//					case BYTES_VALUE:
//					{
//						o.via.array.ptr[1].convert(&bytes_value);
//						break;
//					}
//					default:
//					{
//						break;
//					}
//				}
//
//			}
//			void msgpack_object(msgpack::object* o, msgpack::zone* z) const
//			{
//				o->type = msgpack::type::ARRAY;
//				if (type == EMPTY_VALUE)
//				{
//					o->via.array.ptr = (msgpack::object*) z->malloc(
//					        sizeof(msgpack::object) * 1);
//					o->via.array.size = 1;
//				} else
//				{
//					o->via.array.ptr = (msgpack::object*) z->malloc(
//					        sizeof(msgpack::object) * 2);
//					o->via.array.size = 2;
//				}
//				o->via.array.ptr[0] = msgpack::object(type, z);
//				switch (type)
//				{
//					case DOUBLE_VALUE:
//					{
//						o->via.array.ptr[1] = msgpack::object(double_value, z);
//						break;
//					}
//					case INTEGER_VALUE:
//					{
//						o->via.array.ptr[1] = msgpack::object(integer_value, z);
//						break;
//					}
//					case BYTES_VALUE:
//					{
//						o->via.array.ptr[1] = msgpack::object(bytes_value, z);
//						break;
//					}
//					default:
//					{
//						break;
//					}
//				}
//			}
			std::string ToString(std::string& str) const;
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

	typedef std::vector<ValueData> ValueDataArray;
	typedef std::map<ValueData, double> ValueScoreMap;
	typedef std::map<ValueData, ValueData> HashFieldMap;
	typedef std::set<ValueData> ValueSet;
	typedef std::deque<ValueData> ValueArray;
	typedef std::vector<double> DoubleArray;
	typedef std::deque<Slice> SliceArray;
	typedef std::deque<std::string> StringArray;
	typedef std::vector<int64> Int64Array;
	typedef std::map<std::string, Slice> StringSliceMap;
	typedef std::set<std::string> StringSet;
	typedef std::set<Slice> SliceSet;
	typedef std::vector<uint32_t> WeightArray;
	typedef std::map<std::string, std::string> StringStringMap;
	typedef std::map<uint64, std::string> UInt64StringMap;

	struct ZSetElement
	{
			double score;
			ValueData value;
			ZSetElement():score(0){}
			CODEC_DEFINE(score, value)
	};
	typedef std::vector<ZSetElement> ZSetElementArray;

	struct ZSetKeyObject: public KeyObject
	{
			ZSetElement e;
			ZSetKeyObject(const Slice& k, const Slice& v, double s, DBID id);
			ZSetKeyObject(const Slice& k, const ValueData& v, double s,
			        DBID id);
	};
	struct ZSetScoreKeyObject: public KeyObject
	{
			ValueData value;
			ZSetScoreKeyObject(const Slice& k, const Slice& v, DBID id);
			ZSetScoreKeyObject(const Slice& k, const ValueData& v, DBID id);
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
			ValueData value;CODEC_DEFINE(value)
			;
			StringMetaValue()
			{
				header.type = STRING_META;
			}
	};

	struct ZSetMetaValue: public CommonMetaValue
	{
			uint32_t size;
			double min_score;
			double max_score;
			bool ziped;
			ZSetElementArray zipvs;
		CODEC_DEFINE(size, min_score, max_score, ziped, zipvs)
			;
			ZSetMetaValue() :
					size(0), min_score(0), max_score(0), ziped(false)
			{
				header.type = ZSET_META;
			}
	};
	struct SetMetaValue: public CommonMetaValue
	{
			uint32_t size;
			bool ziped;
			ValueData min;
			ValueData max;
			ValueSet zipvs;CODEC_DEFINE(size,ziped, min, max, zipvs)
			;
			SetMetaValue() :
					size(0), ziped(true)
			{
				header.type = SET_META;
			}
	};

	struct HashMetaValue: public CommonMetaValue
	{
			bool ziped;
			HashFieldMap values;CODEC_DEFINE(ziped, values)
			;
			HashMetaValue() :
					ziped(true)
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
			float min_score;
			float max_score;
			ValueDataArray zipvs;CODEC_DEFINE(size,ziped,min_score,max_score,zipvs)
			;
			ListMetaValue() :
					size(0), ziped(false), min_score(0), max_score(0)
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
			float score;
			ListKeyObject(const Slice& k, double s, DBID id) :
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

	enum AggregateType
	{
		AGGREGATE_EMPTY = 0,
		AGGREGATE_SUM = 1,
		AGGREGATE_MIN = 2,
		AGGREGATE_MAX = 3,
		AGGREGATE_COUNT = 4,
		AGGREGATE_AVG = 5,
	};

	struct QueryOptions
	{
			bool withscores;
			bool withlimit;
			int limit_offset;
			int limit_count;
			QueryOptions() :
					withscores(false), withlimit(false), limit_offset(0), limit_count(
					        0)
			{
			}
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
					by(NULL), with_limit(false), limit_offset(0), limit_count(
					        0), is_desc(false), with_alpha(false), nosort(
					        false), store_dst(NULL), aggregate(AGGREGATE_EMPTY)
			{
			}
	};

	/*
	 * typedefs
	 */
	typedef std::vector<ZSetMetaValue*> ZSetMetaValueArray;
	typedef std::vector<SetMetaValue*> SetMetaValueArray;
	typedef std::map<std::string, ValueData> NameValueTable;

	typedef std::deque<ValueArray> ValueArrayArray;
	typedef std::map<uint64, BitSetElementValue> BitSetElementValueMap;

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
	void encode_key(Buffer& buf, const KeyObject& key);
	KeyObject* decode_key(const Slice& key, KeyObject* expected);
	CommonMetaValue* decode_meta(const char* data, size_t size, bool only_head);
	void encode_meta(Buffer& buf, CommonMetaValue& meta);
	ValueObject* decode_value_obj(KeyType type, const char* data, size_t size);
	bool decode_value(Buffer& buf, ValueData& value);
	void encode_value(Buffer& buf, const ValueData& value);
	bool peek_dbkey_header(const Slice& key, DBID& db, KeyType& type);
	void next_key(const Slice& key, std::string& next);
	int ardb_compare_keys(const char* akbuf, size_t aksiz, const char* bkbuf,
	        size_t bksiz);

}

#endif /* DATA_FORMAT_HPP_ */
