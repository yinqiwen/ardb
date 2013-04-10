/*
 * ardb_data.hpp
 *
 *  Created on: 2013-4-3
 *      Author: wqy
 */

#ifndef ARDB_DATA_HPP_
#define ARDB_DATA_HPP_
#include <stdint.h>
#include <map>
#include <set>
#include <list>
#include <vector>
#include <deque>
#include <string>
#include <tr1/unordered_set>
#include <tr1/unordered_map>
#include "common.hpp"
#include "slice.hpp"
#include "util/buffer_helper.hpp"
#include "util/helpers.hpp"

#define COMPARE_NUMBER(a, b)  (a == b?0:(a>b?1:-1))

namespace ardb
{

	enum KeyType
	{
		KV = 0,
		SET_META = 1,
		SET_ELEMENT = 2,
		ZSET_META = 3,
		ZSET_ELEMENT_SCORE = 4,
		ZSET_ELEMENT = 5,
		HASH_META = 6,
		HASH_FIELD = 7,
		LIST_META = 8,
		LIST_ELEMENT = 9,
		KEY_END = 255,
	};

	enum ValueDataType
	{
		EMPTY = 0, INTEGER = 1, DOUBLE = 2, RAW = 3
	};

	struct KeyObject
	{
			KeyType type;
			Slice key;

			KeyObject(const Slice& k, KeyType T = KV) :
					type(T), key(k)
			{
			}
			virtual ~KeyObject()
			{
			}
	};

	struct ValueObject
	{
			uint8_t type;
			union
			{
					int64_t int_v;
					double double_v;
					Buffer* raw;
			} v;
			uint64_t expire;
			ValueObject() :
					type(EMPTY), expire(0)
			{
				v.int_v = 0;
			}
			ValueObject(int64_t iv) :
					type(INTEGER), expire(0)
			{
				v.int_v = iv;
			}
			ValueObject(double dv) :
					type(DOUBLE), expire(0)
			{
				v.double_v = dv;
			}
			ValueObject(const ValueObject& other)
			{
				Copy(other);
			}
			ValueObject & operator=(const ValueObject &rhs)
			{
				Copy(rhs);
				return *this;
			}
			inline bool operator<(const ValueObject& other) const
			{
				return Compare(other) < 0;
			}
			std::string ToString()const
			{
				switch (type)
				{
					case INTEGER:
					{
						Buffer tmp(64);
						tmp.Printf("%lld", v.int_v);
						return std::string(tmp.GetRawReadBuffer(),
						        tmp.ReadableBytes());
					}
					case DOUBLE:
					{
						Buffer tmp(64);
						tmp.Printf("%f", v.double_v);
						return std::string(tmp.GetRawReadBuffer(),
						        tmp.ReadableBytes());
					}
					default:
					{
						return std::string(v.raw->GetRawReadBuffer(),
						        v.raw->ReadableBytes());
					}
				}
			}
			void Clear()
			{
				if (type == RAW)
				{
					DELETE(v.raw);
				}
				v.int_v = 0;
				type = EMPTY;
			}
			void Copy(const ValueObject& other)
			{
				Clear();
				type = other.type;
				switch (type)
				{
					case EMPTY:
					{
						return;
					}
					case INTEGER:
					{
						v.int_v = other.v.int_v;
						return;
					}
					case DOUBLE:
					{
						v.double_v = other.v.double_v;
						return;
					}
					default:
					{
						v.raw = new Buffer(other.v.raw->ReadableBytes());
						v.raw->Write(other.v.raw->GetRawReadBuffer(),
						        other.v.raw->ReadableBytes());
						return;
					}
				}
			}
			int Compare(const ValueObject& other) const
			{
				if (type != other.type)
				{
					return COMPARE_NUMBER(type, other.type);
				}
				switch (type)
				{
					case EMPTY:
					{
						return 0;
					}
					case INTEGER:
					{
						return COMPARE_NUMBER(v.int_v, other.v.int_v);
					}
					case DOUBLE:
					{
						return COMPARE_NUMBER(v.double_v, other.v.double_v);
					}
					default:
					{
						Slice a(v.raw->GetRawReadBuffer(),
						        v.raw->ReadableBytes());
						Slice b(other.v.raw->GetRawReadBuffer(),
						        other.v.raw->ReadableBytes());
						return a.compare(b);
					}
				}
			}
			~ValueObject()
			{
				if (type == RAW)
				{
					DELETE(v.raw);
				}
			}
	};

	struct ZSetKeyObject: public KeyObject
	{
			//Slice value;
			ValueObject value;
			double score;
			ZSetKeyObject(const Slice& k, const Slice& v, double s);
			ZSetKeyObject(const Slice& k, const ValueObject& v, double s);
	};
	struct ZSetScoreKeyObject: public KeyObject
	{
			//Slice value;
			ValueObject value;
			ZSetScoreKeyObject(const Slice& k, const Slice& v);
			ZSetScoreKeyObject(const Slice& k, const ValueObject& v);
	};

	struct ZSetMetaValue
	{
			uint32_t size;
			double min_score;
			double max_score;
			ZSetMetaValue() :
					size(0), min_score(0), max_score(0)
			{
			}
	};

	struct SetKeyObject: public KeyObject
	{
			ValueObject value;
			SetKeyObject(const Slice& k, const Slice& v);
			SetKeyObject(const Slice& k, const ValueObject& v);
	};

	struct SetMetaValue
	{
			uint32_t size;
			ValueObject min;
			ValueObject max;
			SetMetaValue() :
					size(0)
			{
			}
	};

	struct HashKeyObject: public KeyObject
	{
			Slice field;
			HashKeyObject(const Slice& k, const Slice& f) :
					KeyObject(k, HASH_FIELD), field(f)
			{
			}
	};

	struct ListKeyObject: public KeyObject
	{
			double score;
			ListKeyObject(const Slice& k, double s) :
					KeyObject(k, LIST_ELEMENT), score(s)
			{
			}
	};

	struct ListMetaValue
	{
			uint32_t size;
			double min_score;
			double max_score;
			ListMetaValue() :
					size(0), min_score(0), max_score(0)
			{
			}
	};

	enum OperationType
	{
		NOOP = 0, ADD = 1
	};

	typedef uint64_t DBID;

	typedef std::map<ValueObject, double> ValueScoreMap;
	typedef std::vector<ZSetMetaValue> ZSetMetaValueArray;
	typedef std::vector<SetMetaValue> SetMetaValueArray;
	typedef std::set<ValueObject> ValueSet;
	typedef std::set<std::string> StringSet;
	typedef std::tr1::unordered_map<std::string, ValueSet> KeyValueSet;

	typedef std::deque<ValueObject> ValueArray;
	typedef std::deque<Slice> SliceArray;
	typedef std::deque<std::string> StringArray;
	typedef std::vector<uint32_t> WeightArray;

	void encode_key(Buffer& buf, const KeyObject& key);
	KeyObject* decode_key(const Slice& key);

	bool peek_key_type(const Slice& key, KeyType& type);

	void encode_value(Buffer& buf, const ValueObject& value);
	bool decode_value(Buffer& buf, ValueObject& value);
	void fill_raw_value(const Slice& value, ValueObject& valueobject);
	void smart_fill_value(const Slice& value, ValueObject& valueobject);
	int value_convert_to_raw(ValueObject& v);
	int value_convert_to_number(ValueObject& v);

}

#endif /* ARDB_DATA_HPP_ */
