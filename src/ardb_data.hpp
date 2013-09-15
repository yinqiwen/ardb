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

#define ARDB_GLOBAL_DB 0xFFFFFF

#define COMPARE_NUMBER(a, b)  (a == b?0:(a>b?1:-1))

#define COMPARE_SLICE(a, b)  (a.size() == b.size()?(a.compare(b)):(a.size()>b.size()?1:-1))

namespace ardb
{
	/*
	 * 3 bytes, value from [0, 0xFFFFFF]
	 */
	typedef uint32 DBID;

	typedef std::set<DBID> DBIDSet;

	enum KeyType
	{
		KV = 0, SET_META = 1, SET_ELEMENT = 2, ZSET_META = 3, ZSET_ELEMENT_SCORE = 4, ZSET_ELEMENT = 5, HASH_META = 6, HASH_FIELD = 7, LIST_META = 8, LIST_ELEMENT = 9, TABLE_META = 10, TABLE_INDEX = 11, TABLE_COL = 12, TABLE_SCHEMA = 13, BITSET_META = 14, BITSET_ELEMENT = 15, KEY_EXPIRATION_ELEMENT = 16, KEY_EXPIRATION_MAPPING = 17, SCRIPT = 18, KEY_END = 100,
	};

	enum ValueDataType
	{
		EMPTY = 0, INTEGER = 1, DOUBLE = 2, RAW = 3
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
			ValueObject(const std::string& str) :
					type(RAW)
			{
				v.raw = new Buffer(str.size());
				v.raw->Write(str.data(), str.size());
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
			inline bool operator>(const ValueObject& other) const
			{
				return Compare(other) > 0;
			}
			inline bool operator==(const ValueObject& other) const
			{
				return Compare(other) == 0;
			}
			inline bool operator>=(const ValueObject& other) const
			{
				return Compare(other) >= 0;
			}
			inline bool operator<=(const ValueObject& other) const
			{
				return Compare(other) <= 0;
			}
			inline bool operator!=(const ValueObject& other) const
			{
				return Compare(other) != 0;
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
				if ((other.type == INTEGER || other.type == EMPTY) && type == INTEGER)
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
				} else
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
						str.assign(v.raw->GetRawReadBuffer(), v.raw->ReadableBytes());
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
					case RAW:
					{
						v.raw = new Buffer(other.v.raw->ReadableBytes());
						v.raw->Write(other.v.raw->GetRawReadBuffer(), other.v.raw->ReadableBytes());
						return;
					}
					default:
					{
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
						Slice a(v.raw->GetRawReadBuffer(), v.raw->ReadableBytes());
						Slice b(other.v.raw->GetRawReadBuffer(), other.v.raw->ReadableBytes());
						return a.compare(b);
					}
				}
			}
			~ValueObject()
			{
				Clear();
			}
	};

	typedef btree::btree_map<ValueObject, double> ValueScoreMap;

	typedef btree::btree_set<ValueObject> ValueSet;

	typedef std::deque<ValueObject> ValueArray;
	typedef std::vector<double> DoubleArray;
	typedef std::deque<Slice> SliceArray;
	typedef std::deque<std::string> StringArray;
	typedef std::vector<int64> Int64Array;
	typedef btree::btree_map<std::string, Slice> StringSliceMap;
	typedef btree::btree_set<std::string> StringSet;
	typedef btree::btree_set<Slice> SliceSet;
	typedef std::vector<uint32_t> WeightArray;
	typedef btree::btree_map<std::string, std::string> StringStringMap;
	typedef btree::btree_map<uint64, std::string> UInt64StringMap;

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
		CMP_EQAUL = 1, CMP_GREATE = 2, CMP_GREATE_EQ = 3, CMP_LESS = 4, CMP_LESS_EQ = 5, CMP_NOTEQ = 6
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

			Condition(const std::string& name, CompareOperator compareop, const Slice& value, LogicalOperator logic = LOGIC_EMPTY);

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
			ZSetKeyObject(const Slice& k, const ValueObject& v, double s, DBID id);
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
			StringSet indexed;
			TableSchemaValue()
			{
			}
			int Index(const Slice& key);
			bool HasIndex(const Slice& name);
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

	typedef ValueArray TableKeyIndex;

	struct TableIndexKeyObject: public KeyObject
	{
			Slice colname;
			ValueObject colvalue;
			TableKeyIndex index;
			TableIndexKeyObject(const Slice& tablename, const Slice& keyname, const ValueObject& v, DBID id) :
					KeyObject(tablename, TABLE_INDEX, id), colname(keyname), colvalue(v)
			{
			}
			TableIndexKeyObject(const Slice& tablename, const Slice& keyname, const Slice& v, DBID id);
	};
	struct TableColKeyObject: public KeyObject
	{
			Slice colname;
			ValueArray index;
			TableColKeyObject(const Slice& tablename, const Slice& c, DBID id) :
					KeyObject(tablename, TABLE_COL, id), colname(c)
			{
			}
	};

	enum AggregateType
	{
		AGGREGATE_EMPTY = 0, AGGREGATE_SUM = 1, AGGREGATE_MIN = 2, AGGREGATE_MAX = 3, AGGREGATE_COUNT = 4, AGGREGATE_AVG = 5,
	};

	struct QueryOptions
	{
			bool withscores;
			bool withlimit;
			int limit_offset;
			int limit_count;
			QueryOptions() :
					withscores(false), withlimit(false), limit_offset(0), limit_count(0)
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
					by(NULL), with_limit(false), limit_offset(0), limit_count(0), is_desc(false), with_alpha(false), nosort(false), store_dst(NULL), aggregate(AGGREGATE_EMPTY)
			{
			}
	};

	struct TableQueryOptions
	{
			Conditions conds;
			StringArray names;
			Slice orderby;
			bool with_limit;
			int32 limit_offset;
			int32 limit_count;
			bool with_desc_asc;
			bool is_desc;
			bool with_alpha;
			AggregateType aggregate;
			TableQueryOptions() :
					with_limit(false), limit_offset(0), limit_count(-1), with_desc_asc(false), is_desc(false), with_alpha(false), aggregate(AGGREGATE_EMPTY)
			{

			}
			void Clear()
			{
				with_limit = false;
				limit_offset = 0;
				limit_count = -1;
				with_desc_asc = false;
				is_desc = false;
				with_alpha = false;
				aggregate = AGGREGATE_EMPTY;
				conds.clear();
				names.clear();
				orderby = Slice();
			}
			static bool Parse(StringArray& args, uint32 offset, TableQueryOptions& options);
	};
	struct TableUpdateOptions
	{
			Conditions conds;
			StringStringMap colnvs;
			static bool Parse(StringArray& args, uint32 offset, TableUpdateOptions& options);
	};
	struct TableDeleteOptions
	{
			Conditions conds;
			static bool Parse(StringArray& args, uint32 offset, TableDeleteOptions& options);
	};
	struct TableInsertOptions
	{
			StringStringMap nvs;
			static bool Parse(StringArray& args, uint32 offset, TableInsertOptions& options);
			void Clear()
			{
				nvs.clear();
			}
	};

	typedef std::vector<ZSetMetaValue> ZSetMetaValueArray;
	typedef std::vector<SetMetaValue> SetMetaValueArray;
	typedef std::deque<TableIndexKeyObject> TableRowKeyArray;

	/*
	 * Note: Crash on MacOSX if replaced by btree::btree_map
	 */
#ifdef __APPLE__
	typedef std::map<std::string, ValueObject> NameValueTable;
	typedef std::map<TableKeyIndex, NameValueTable> TableKeyIndexValueTable;
#else
	typedef btree::btree_map<std::string, ValueObject> NameValueTable;
	typedef btree::btree_map<TableKeyIndex, NameValueTable> TableKeyIndexValueTable;
#endif

	typedef std::deque<ValueArray> ValueArrayArray;
	typedef btree::btree_map<uint64, BitSetElementValue> BitSetElementValueMap;

	int compare_values(const ValueArray& a, const ValueArray& b);

	void encode_key(Buffer& buf, const KeyObject& key);
	KeyObject* decode_key(const Slice& key, KeyObject* expected);
	bool peek_dbkey_header(const Slice& key, DBID& db, KeyType& type);

	void encode_value(Buffer& buf, const ValueObject& value);
	bool decode_value(Buffer& buf, ValueObject& value, bool copyRawValue = true);
	void next_key(const Slice& key, std::string& next);
	void fill_raw_value(const Slice& value, ValueObject& valueobject);
	void smart_fill_value(const Slice& value, ValueObject& valueobject);
	int value_convert_to_raw(ValueObject& v);
	int value_convert_to_number(ValueObject& v);

	int type_to_string(KeyType type, std::string& str);
	int string_to_type(KeyType& type, const std::string& str);

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
