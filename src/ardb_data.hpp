/*
 * ardb_data.hpp
 *
 *  Created on: 2013-4-3
 *      Author: wqy
 */

#ifndef ARDB_DATA_HPP_
#define ARDB_DATA_HPP_
#include <stdint.h>
#include <math.h>
#include <map>
#include <set>
#include <list>
#include <vector>
#include <deque>
#include <string>
#include <tr1/unordered_set>
#include <tr1/unordered_map>
#include <btree_map.h>
#include <btree_set.h>
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
		TABLE_META = 10,
		TABLE_INDEX = 11,
		TABLE_COL = 12,
		KEY_END = 255,
	};

	enum ValueDataType
	{
		EMPTY = 0, INTEGER = 1, DOUBLE = 2, RAW = 3, VALUE_END = 255
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
			//uint64_t expire;
			ValueObject() :
					type(EMPTY)
			{
				v.raw = NULL;
			}
			ValueObject(int64_t iv) :
					type(INTEGER)
			{
				v.int_v = iv;
			}
			ValueObject(double dv) :
					type(DOUBLE)
			{
				v.double_v = dv;
			}
			ValueObject(const ValueObject& other) :
					type(EMPTY)
			{
				v.int_v = 0;
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
			inline bool operator==(const ValueObject& other) const
			{
				return Compare(other) == 0;
			}
			const std::string& ToString(std::string& str) const
			{
				switch (type)
				{
					case EMPTY:
					{
						str = "";
						return str;
					}
					case INTEGER:
					{
						Buffer tmp(64);
						tmp.Printf("%lld", v.int_v);
						str.assign(tmp.GetRawReadBuffer(), tmp.ReadableBytes());
						return str;
					}
					case DOUBLE:
					{
						fast_dtoa(v.double_v, 10, str);
						return str;
					}
					default:
					{
						str.assign(v.raw->GetRawReadBuffer(),
								v.raw->ReadableBytes());
						return str;
					}
				}
			}
			double NumberValue()
			{
				switch (type)
				{
					case INTEGER:
					{
						return v.int_v;
					}
					case DOUBLE:
					{
						return v.double_v;
					}
					default:
					{
						return NAN;
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
				Clear();
			}
	};

	typedef std::string DBID;

	typedef std::map<ValueObject, double> ValueScoreMap;

	typedef std::set<ValueObject> ValueSet;
	typedef std::set<DBID> DBIDSet;

	typedef std::deque<ValueObject> ValueArray;
	typedef std::deque<Slice> SliceArray;
	typedef std::deque<std::string> StringArray;
	typedef std::vector<int64> Int64Array;
	typedef std::map<std::string, Slice> SliceMap;
	typedef std::set<std::string> StringSet;
	typedef std::vector<uint32_t> WeightArray;

	struct DBItemKey
	{
			DBID db;
			Slice key;
			DBItemKey(const DBID& id = "", const Slice& k = "") :
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
			bool Empty()
			{
				return db.empty();
			}
	};

	enum CompareOperator
	{
		CMP_EQAUL = 1,
		CMP_GREATE = 2,
		CMP_GREATE_EQ = 3,
		CMP_LESS = 4,
		CMP_LESS_EQ = 5,
		CMP_NOTEQ = 6
	};
	enum LogicalOperator
	{
		LOGIC_EMPTY = 0, LOGIC_AND = 1, LOGIC_OR = 2
	};

	struct Condition
	{
			std::string keyname;
			CompareOperator cmp;
			ValueObject keyvalue;
			LogicalOperator logicop;

			Condition(const std::string& name, CompareOperator compareop,
					const Slice& value, LogicalOperator logic = LOGIC_EMPTY);

			bool MatchValue(const ValueObject& v, int& cmpret)
			{
				int ret = v.Compare(keyvalue);
				cmpret = ret;
				switch (cmp)
				{
					case CMP_EQAUL:
						return ret == 0;
					case CMP_GREATE:
						return ret > 0;
					case CMP_GREATE_EQ:
						return ret >= 0;
					case CMP_LESS:
						return ret < 0;
					case CMP_LESS_EQ:
						return ret <= 0;
					case CMP_NOTEQ:
						return ret != 0;
					default:
						return false;
				}
			}

	};
	typedef std::vector<Condition> Conditions;

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
			float score;
			ListKeyObject(const Slice& k, double s) :
					KeyObject(k, LIST_ELEMENT), score(s)
			{
			}
	};

	struct ListMetaValue
	{
			uint32_t size;
			float min_score;
			float max_score;
			ListMetaValue() :
					size(0), min_score(0), max_score(0)
			{
			}
	};

	struct TableMetaValue
	{
			uint32_t size;
			StringArray keynames;
			StringSet valnames;
			TableMetaValue() :
					size(0)
			{
			}
			int Index(const Slice& key);
	};

	struct TableKeyIndex
	{
			ValueArray keyvals;
			bool operator<(const TableKeyIndex& other) const;
	};

	struct TableIndexKeyObject: public KeyObject
	{
			Slice kname;
			ValueObject keyvalue;
			TableKeyIndex index;
			TableIndexKeyObject(const Slice& tablename, const Slice& keyname,
					const ValueObject& v) :
					KeyObject(tablename, TABLE_INDEX), kname(keyname), keyvalue(
							v)
			{
			}
			TableIndexKeyObject(const Slice& tablename, const Slice& keyname,
					const Slice& v);
	};

	struct TableColKeyObject: public KeyObject
	{
			ValueArray keyvals;
			Slice col;
			TableColKeyObject(const Slice& tablename, const Slice& c) :
					KeyObject(tablename, TABLE_COL), col(c)
			{
			}
	};

	enum AggregateType
	{
		AGGREGATE_SUM = 0, AGGREGATE_MIN = 1, AGGREGATE_MAX = 2,
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
			SortOptions() :
					by(NULL), with_limit(false), limit_offset(0), limit_count(
							0), is_desc(false), with_alpha(false), nosort(
							false), store_dst(NULL)
			{
			}
	};

	struct TableQueryOptions
	{
			Conditions conds;
			SliceArray keys;
			SliceArray vals;
			static bool Parse(StringArray& args, uint32 offset,
					TableQueryOptions& options);
	};
	struct TableUpdateOptions
	{
			Conditions conds;
			SliceMap colnvs;
			static bool Parse(StringArray& args, uint32 offset,
					TableUpdateOptions& options);
	};
	struct TableDeleteOptions
	{
			Conditions conds;
			static bool Parse(StringArray& args, uint32 offset,
					TableDeleteOptions& options);
	};
	struct TableInsertOptions
	{
			SliceMap keynvs;
			SliceMap colnvs;
			static bool Parse(StringArray& args, uint32 offset,
					TableInsertOptions& options);
	};

	typedef std::vector<ZSetMetaValue> ZSetMetaValueArray;
	typedef std::vector<SetMetaValue> SetMetaValueArray;
	typedef std::deque<TableIndexKeyObject> TableRowKeyArray;
	typedef std::set<TableKeyIndex> TableKeyIndexSet;

	int compare_values(const ValueArray& a, const ValueArray& b);

	void encode_key(Buffer& buf, const KeyObject& key);
	KeyObject* decode_key(const Slice& key);

	void encode_value(Buffer& buf, const ValueObject& value);
	bool decode_value(Buffer& buf, ValueObject& value,
			bool copyRawValue = true);
	void fill_raw_value(const Slice& value, ValueObject& valueobject);
	void smart_fill_value(const Slice& value, ValueObject& valueobject);
	int value_convert_to_raw(ValueObject& v);
	int value_convert_to_number(ValueObject& v);

}

#endif /* ARDB_DATA_HPP_ */
