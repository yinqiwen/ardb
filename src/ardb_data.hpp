/*
 * ardb_data.hpp
 *
 *  Created on: 2013-4-3
 *      Author: wqy
 */

#ifndef ARDB_DATA_HPP_
#define ARDB_DATA_HPP_
#include <stdint.h>
#include <float.h>
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
	/*
	 * 3 bytes, value from [0, 0xFFFFFF]
	 */
	typedef uint32 DBID;

	typedef std::set<DBID> DBIDSet;

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
		TABLE_SCHEMA = 13,
		BITSET_META = 14,
		BITSET_ELEMENT = 15,
		KEY_END = 100,
	};

	enum ValueDataType
	{
		EMPTY = 0, INTEGER = 1, DOUBLE = 2, RAW = 3, VALUE_END = 255
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
			inline ValueObject& operator+=(const ValueObject& other)
			{
				if (type == EMPTY)
				{
					type = INTEGER;
					v.int_v = 0;
				}
				if (other.type == DOUBLE || type == DOUBLE)
				{
					double tmp = NumberValue();
					tmp += other.NumberValue();
					type = DOUBLE;
					v.double_v = tmp;
					return *this;
				}
				if ((other.type == INTEGER || other.type == EMPTY)
				        && type == INTEGER)
				{
					v.int_v += other.v.int_v;
					return *this;
				}
				return *this;
			}
			inline ValueObject& operator/=(uint32 i)
			{
				if (type == EMPTY)
				{
					type = INTEGER;
					v.int_v = 0;
				}
				if (type == RAW)
				{
					return *this;
				}
				double tmp = NumberValue();
				type = DOUBLE;
				if (i != 0)
				{
					v.double_v = tmp / i;
				}
				else
				{
					v.double_v = DBL_MAX;
				}
				return *this;
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
			double NumberValue() const
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

	typedef std::map<ValueObject, double> ValueScoreMap;

	typedef std::set<ValueObject> ValueSet;

	typedef std::deque<ValueObject> ValueArray;
	typedef std::vector<double> DoubleArray;
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
			ZSetKeyObject(const Slice& k, const Slice& v, double s, DBID id);
			ZSetKeyObject(const Slice& k, const ValueObject& v, double s,
			        DBID id);
	};
	struct ZSetScoreKeyObject: public KeyObject
	{
			//Slice value;
			ValueObject value;
			ZSetScoreKeyObject(const Slice& k, const Slice& v, DBID id);
			ZSetScoreKeyObject(const Slice& k, const ValueObject& v, DBID id);
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
			SetKeyObject(const Slice& k, const Slice& v, DBID id);
			SetKeyObject(const Slice& k, const ValueObject& v, DBID id);
	};

	struct BitSetKeyObject: public KeyObject
	{
			uint64 index;
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
			std::string vals;
			BitSetElementValue() :
					bitcount(0), start(0), limit(0)
			{
			}
	};

	struct BitSetMetaValue
	{
			uint64 bitcount;
			uint64 min;
			uint64 max;
			//uint64 limit;
			BitSetMetaValue() :
					bitcount(0), min(0), max(0)
			{
			}
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
			HashKeyObject(const Slice& k, const Slice& f, DBID id) :
					KeyObject(k, HASH_FIELD, id), field(f)
			{
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
			TableMetaValue() :
					size(0)
			{
			}
	};
	struct TableSchemaValue
	{
			StringArray keynames;
			StringSet valnames;
			TableSchemaValue()
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
			        const ValueObject& v, DBID id) :
					KeyObject(tablename, TABLE_INDEX, id), kname(keyname), keyvalue(
					        v)
			{
			}
			TableIndexKeyObject(const Slice& tablename, const Slice& keyname,
			        const Slice& v, DBID id);
	};

	struct TableColKeyObject: public KeyObject
	{
			ValueArray keyvals;
			Slice col;
			TableColKeyObject(const Slice& tablename, const Slice& c, DBID id) :
					KeyObject(tablename, TABLE_COL, id), col(c)
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
			SliceArray names;
			Slice orderby;
			bool with_limit;
			int32 limit_offset;
			int32 limit_count;
			bool with_desc_asc;
			bool is_desc;
			bool with_alpha;
			AggregateType aggregate;
			TableQueryOptions() :
					with_limit(false), limit_offset(0), limit_count(0), with_desc_asc(
					        false), is_desc(false), with_alpha(false), aggregate(
					        AGGREGATE_EMPTY)
			{

			}
			void Clear()
			{
				with_limit = false;
				limit_offset = 0;
				limit_count = 0;
				with_desc_asc = false;
				is_desc = false;
				with_alpha = false;
				aggregate = AGGREGATE_EMPTY;
				conds.clear();
				names.clear();
				orderby = Slice();
			}
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
			SliceMap nvs;
			static bool Parse(StringArray& args, uint32 offset,
			        TableInsertOptions& options);
			void Clear()
			{
				nvs.clear();
			}
	};

	typedef std::vector<ZSetMetaValue> ZSetMetaValueArray;
	typedef std::vector<SetMetaValue> SetMetaValueArray;
	typedef std::deque<TableIndexKeyObject> TableRowKeyArray;
	typedef btree::btree_set<TableKeyIndex> TableKeyIndexSet;
	typedef std::deque<ValueArray> ValueArrayArray;
	typedef btree::btree_map<std::string, std::string> StringStringMap;
	typedef std::map<uint64, std::string> StringMap;
	typedef std::map<uint64, BitSetElementValue> BitSetElementValueMap;

	int compare_values(const ValueArray& a, const ValueArray& b);

	void encode_key(Buffer& buf, const KeyObject& key);
	KeyObject* decode_key(const Slice& key, KeyObject* expected);
	bool peek_dbkey_header(const Slice& key, DBID& db, KeyType& type);

	void encode_value(Buffer& buf, const ValueObject& value);
	bool decode_value(Buffer& buf, ValueObject& value,
	        bool copyRawValue = true);
	void next_key(const Slice& key, std::string& next);
	void fill_raw_value(const Slice& value, ValueObject& valueobject);
	void smart_fill_value(const Slice& value, ValueObject& valueobject);
	int value_convert_to_raw(ValueObject& v);
	int value_convert_to_number(ValueObject& v);

	inline bool operator==(const ValueArray& x, const ValueArray& y)
	{
		if (x.size() != y.size())
		{
			return false;
		}
		for (uint32 i = 0; i < x.size(); i++)
		{
			if (x[i].Compare(y[i]) != 0)
			{
				return false;
			}
		}
		return true;
	}

	inline bool operator<(const ValueArray& x, const ValueArray& y)
	{
		return compare_values(x, y) < 0;
	}

}

#endif /* ARDB_DATA_HPP_ */
