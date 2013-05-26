/*
 * table.cpp
 *
 *  Created on: 2013-4-11
 *      Author: wqy
 */
#include "ardb.hpp"

namespace ardb
{
	struct TableRow
	{
			ValueArray vs;
			int sort_item_idx;
			TableRow() :
					sort_item_idx(-1)
			{
			}
			int Compare(const TableRow& other) const
			{
				if (sort_item_idx >= 0 && vs.size() > sort_item_idx
				        && other.vs.size() > sort_item_idx)
				{
					return vs[sort_item_idx].Compare(other.vs[sort_item_idx]);
				}
				return 0;
			}
	};
	template<typename T>
	static bool greater_value(const T& v1, const T& v2)
	{
		return v1.Compare(v2) > 0;
	}
	template<typename T>
	static bool less_value(const T& v1, const T& v2)
	{
		return v1.Compare(v2) < 0;
	}

	static bool parse_nvs(StringArray& cmd, SliceMap& key_map, uint32& idx)
	{
		if (idx >= cmd.size())
		{
			return false;
		}
		uint32 i = idx;
		uint32 len = 0;
		if (!string_touint32(cmd[i], len))
		{
			return false;
		}
		i++;
		for (; i < cmd.size() && len > 0; i += 2, len--)
		{
			if (i + 1 < cmd.size())
			{
				key_map[cmd[i]] = cmd[i + 1];
			}
			else
			{
				return false;
			}
		}
		if (len != 0)
		{
			return false;
		}
		idx = i;
		return true;
	}
	static bool parse_strs(StringArray& cmd, SliceArray& strs, uint32& idx)
	{
		if (idx >= cmd.size())
		{
			return false;
		}
		uint32 i = idx;
		uint32 len = 0;
		if (!string_touint32(cmd[i], len))
		{
			return false;
		}
		i++;
		for (; i < cmd.size() && len > 0; i++, len--)
		{
			strs.push_back(cmd[i]);
		}
		if (len != 0)
		{
			return false;
		}
		idx = i;
		return true;
	}
	static const char* kGreaterEqual = ">=";
	static const char* kLessEqual = "<=";
	static const char* kGreater = ">";
	static const char* kLess = "<";
	static const char* kEqual = "=";
	static const char* kNotEqual = "!=";
	static bool parse_conds(StringArray& cmd, Conditions& conds, uint32& idx)
	{
		while (idx < cmd.size())
		{
			std::string& str = cmd[idx];
			std::string sep;
			CompareOperator cmp;
			size_t pos = std::string::npos;
			uint32 next = 0;
			if ((pos = str.find(kGreaterEqual, 0)) != std::string::npos)
			{
				cmp = CMP_GREATE_EQ;
				next = pos + 2;
			}
			else if ((pos = str.find(kLessEqual, 0)) != std::string::npos)
			{
				cmp = CMP_LESS_EQ;
				next = pos + 2;
			}
			else if ((pos = str.find(kGreater, 0)) != std::string::npos)
			{
				cmp = CMP_GREATE;
				next = pos + 1;
			}
			else if ((pos = str.find(kLess, 0)) != std::string::npos)
			{
				cmp = CMP_LESS;
				next = pos + 1;
			}
			else if ((pos = str.find(kNotEqual, 0)) != std::string::npos)
			{
				cmp = CMP_NOTEQ;
				next = pos + 2;
			}
			else if ((pos = str.find(kEqual, 0)) != std::string::npos)
			{
				cmp = CMP_EQAUL;
				next = pos + 1;
			}
			else
			{
				return false;
			}
			std::string keyname = str.substr(0, pos);
			std::string val;
			if (next < str.size())
			{
				val = str.substr(next);
			}
			if (val.empty() || keyname.empty())
			{
				return false;
			}
			LogicalOperator logicop = LOGIC_OR;
			if (idx + 1 < cmd.size())
			{
				if (!strcasecmp(cmd[idx + 1].c_str(), "and"))
				{
					logicop = LOGIC_AND;
				}
				else if (!strcasecmp(cmd[idx + 1].c_str(), "or"))
				{
					logicop = LOGIC_OR;
				}
				else
				{
					return false;
				}
				idx++;
			}
			Condition cond(keyname, cmp, val, logicop);
			conds.push_back(cond);
			idx++;
		}
		return true;
	}

	bool TableInsertOptions::Parse(StringArray& args, uint32 offset,
	        TableInsertOptions& options)
	{
		if (!parse_nvs(args, options.nvs, offset))
		{
			return false;
		}
		return offset == args.size();
	}

	bool TableDeleteOptions::Parse(StringArray& args, uint32 offset,
	        TableDeleteOptions& options)
	{
		if (offset >= args.size())
		{
			return true;
		}
		if (!strcasecmp(args[offset].c_str(), "where"))
		{
			offset = offset + 1;
			return parse_conds(args, options.conds, offset);
		}
		else
		{
			return false;
		}
	}

	bool TableUpdateOptions::Parse(StringArray& args, uint32 offset,
	        TableUpdateOptions& options)
	{
		if (!parse_nvs(args, options.colnvs, offset))
		{
			return false;
		}
		for (uint32 i = offset; i < args.size(); i++)
		{
			if (!strcasecmp(args[i].c_str(), "where"))
			{
				i = i + 1;
				return parse_conds(args, options.conds, i);
			}
			else
			{
				return false;
			}
		}
		return true;
	}

	bool TableQueryOptions::Parse(StringArray& args, uint32 offset,
	        TableQueryOptions& options)
	{
		if (!parse_strs(args, options.names, offset))
		{
			return false;
		}
		if (offset >= args.size())
		{
			return true;
		}

		for (uint32 i = offset; i < args.size(); i++)
		{
			if (!strcasecmp(args[i].c_str(), "asc"))
			{
				options.with_desc_asc = true;
				options.is_desc = false;
			}
			else if (!strcasecmp(args[i].c_str(), "desc"))
			{
				options.with_desc_asc = true;
				options.is_desc = true;
			}
			else if (!strcasecmp(args[i].c_str(), "alpha"))
			{
				options.with_alpha = true;
			}
			else if (!strcasecmp(args[i].c_str(), "limit")
			        && i < args.size() - 2)
			{
				options.with_limit = true;
				if (!string_toint32(args[i + 1], options.limit_offset)
				        || !string_toint32(args[i + 2], options.limit_count))
				{
					return -1;
				}
				i += 2;
			}
			else if (!strcasecmp(args[i].c_str(), "orderby")
			        && i < args.size() - 1)
			{
				options.orderby = args[i + 1].c_str();
				i++;
			}
			else if (!strcasecmp(args[i].c_str(), "aggregate")
			        && i < args.size() - 1)
			{
				if (!strcasecmp(args[i + 1].c_str(), "sum"))
				{
					options.aggregate = AGGREGATE_SUM;
				}
				else if (!strcasecmp(args[i + 1].c_str(), "max"))
				{
					options.aggregate = AGGREGATE_MAX;
				}
				else if (!strcasecmp(args[i + 1].c_str(), "min"))
				{
					options.aggregate = AGGREGATE_MIN;
				}
				else if (!strcasecmp(args[i + 1].c_str(), "count"))
				{
					options.aggregate = AGGREGATE_COUNT;
				}
				else if (!strcasecmp(args[i + 1].c_str(), "avg"))
				{
					options.aggregate = AGGREGATE_AVG;
				}
				else
				{
					return false;
				}
				i++;
			}
			else if (!strcasecmp(args[i].c_str(), "where"))
			{
				i = i + 1;
				return parse_conds(args, options.conds, i);
			}
			else
			{
				return false;
			}
		}
		return true;
	}

	static bool DecodeTableMetaData(ValueObject& v, TableMetaValue& meta)
	{
		if (v.type != RAW)
		{
			return false;
		}
		if (!BufferHelper::ReadVarUInt32(*(v.v.raw), meta.size))
		{
			return false;
		}
		return true;
	}
	static void EncodeTableMetaData(ValueObject& v, TableMetaValue& meta)
	{
		v.type = RAW;
		if (v.v.raw == NULL)
		{
			v.v.raw = new Buffer(16);
		}
		BufferHelper::WriteVarUInt32(*(v.v.raw), meta.size);
	}

	static bool DecodeTableSchemaData(ValueObject& v, TableSchemaValue& meta)
	{
		if (v.type != RAW)
		{
			return false;
		}
		uint32 len, collen;
		if (!BufferHelper::ReadVarUInt32(*(v.v.raw), len)
		        || !BufferHelper::ReadVarUInt32(*(v.v.raw), collen))
		{
			return false;
		}
		for (int i = 0; i < len; i++)
		{
			std::string tmp;
			if (!BufferHelper::ReadVarString(*(v.v.raw), tmp))
			{
				return false;
			}
			meta.keynames.push_back(tmp);
		}
		for (int i = 0; i < collen; i++)
		{
			std::string tmp;
			if (!BufferHelper::ReadVarString(*(v.v.raw), tmp))
			{
				return false;
			}
			meta.valnames.insert(tmp);
		}
		return true;
	}
	static void EncodeTableSchemaData(ValueObject& v, TableSchemaValue& meta)
	{
		v.type = RAW;
		if (v.v.raw == NULL)
		{
			v.v.raw = new Buffer(16);
		}
		BufferHelper::WriteVarUInt32(*(v.v.raw), meta.keynames.size());
		BufferHelper::WriteVarUInt32(*(v.v.raw), meta.valnames.size());
		for (int i = 0; i < meta.keynames.size(); i++)
		{
			BufferHelper::WriteVarString(*(v.v.raw), meta.keynames[i]);
		}
		StringSet::iterator it = meta.valnames.begin();
		while (it != meta.valnames.end())
		{
			BufferHelper::WriteVarString(*(v.v.raw), *it);
			it++;
		}
	}

	int TableSchemaValue::Index(const Slice& key)
	{
		for (uint32 i = 0; i < keynames.size(); i++)
		{
			if (!strcasecmp(keynames[i].c_str(), key.data()))
			{
				return i;
			}
		}
		return -1;
	}

	int Ardb::GetTableMetaValue(const DBID& db, const Slice& tableName,
	        TableMetaValue& meta)
	{
		KeyObject k(tableName, TABLE_META, db);
		ValueObject v;
		if (0 == GetValue(k, &v))
		{
			if (!DecodeTableMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
			return 0;
		}
		return ERR_NOT_EXIST;
	}
	void Ardb::SetTableMetaValue(const DBID& db, const Slice& tableName,
	        TableMetaValue& meta)
	{
		KeyObject k(tableName, TABLE_META, db);
		ValueObject v;
		EncodeTableMetaData(v, meta);
		SetValue(k, v);
	}

	int Ardb::GetTableSchemaValue(const DBID& db, const Slice& key,
	        TableSchemaValue& meta)
	{
		KeyObject k(key, TABLE_SCHEMA, db);
		ValueObject v;
		if (0 == GetValue(k, &v))
		{
			if (!DecodeTableSchemaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
			return 0;
		}
		return ERR_NOT_EXIST;
	}
	void Ardb::SetTableSchemaValue(const DBID& db, const Slice& key,
	        TableSchemaValue& meta)
	{
		KeyObject k(key, TABLE_SCHEMA, db);
		ValueObject v;
		EncodeTableSchemaData(v, meta);
		SetValue(k, v);
	}

	int Ardb::TCreate(const DBID& db, const Slice& tableName,
	        SliceArray& keynames)
	{
		TableSchemaValue schema;
		GetTableSchemaValue(db, tableName, schema);
		SliceArray::iterator it = keynames.begin();
		while (it != keynames.end())
		{
			std::string name(it->data(), it->size());
			bool found = false;
			StringArray::iterator sit = schema.keynames.begin();
			while (sit != schema.keynames.end())
			{
				if (!strcasecmp(sit->c_str(), it->data()))
				{
					found = true;
					break;
				}
				sit++;
			}
			if (!found)
			{
				schema.keynames.push_back(name);
			}
			it++;
		}
		SetTableSchemaValue(db, tableName, schema);
		TableMetaValue meta;
		if (0 != GetTableMetaValue(db, tableName, meta))
		{
			SetTableMetaValue(db, tableName, meta);
		}
		return 0;
	}

	int Ardb::TUnionRowKeys(const DBID& db, const Slice& tableName,
	        Condition& cond, TableKeyIndexSet& results)
	{
		TableIndexKeyObject index(tableName, cond.keyname, cond.keyvalue, db);
		results.clear();
		bool reverse = cond.cmp == CMP_LESS || cond.cmp == CMP_LESS_EQ;
		if (cond.cmp == CMP_NOTEQ)
		{
			index.keyvalue.Clear();
		}
		struct TUnionIndexWalk: public WalkHandler
		{
				Condition& icond;
				TableKeyIndexSet& tres;
				bool rwalk;
				TUnionIndexWalk(Condition& c, TableKeyIndexSet& i) :
						icond(c), tres(i), rwalk(false)
				{
				}
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					TableIndexKeyObject* sek = (TableIndexKeyObject*) k;
					if (sek->kname.compare(icond.keyname) != 0)
					{
						if (!rwalk)
						{
							return -1;
						}
						else
						{
							return cursor == 0 ? 0 : -1;
						}
					}
					int cmp = 0;
					if (!icond.MatchValue(sek->keyvalue, cmp))
					{
						std::string str1, str2;
						switch (icond.cmp)
						{
							case CMP_NOTEQ:
								return 0;
							case CMP_GREATE:
							case CMP_LESS:
							{
								return cursor == 0 && cmp == 0 ? 0 : -1;
							}
							default:
								return -1;
						}
					}
					tres.insert(sek->index);
					return 0;
				}
		} walk(cond, results);
		walk.rwalk = reverse;
		Walk(index, reverse, &walk);
		return 0;
	}

	int Ardb::TInterRowKeys(const DBID& db, const Slice& tableName,
	        Condition& cond, TableKeyIndexSet& interset,
	        TableKeyIndexSet& results)
	{
		TableIndexKeyObject index(tableName, cond.keyname, cond.keyvalue, db);
		results.clear();
		bool reverse = cond.cmp == CMP_LESS || cond.cmp == CMP_LESS_EQ;
		if (cond.cmp == CMP_NOTEQ)
		{
			index.keyvalue.Clear();
		}
		struct TInterIndexWalk: public WalkHandler
		{
				Condition& icond;
				TableKeyIndexSet& tinter;
				TableKeyIndexSet& tres;
				bool isReverse;
				TInterIndexWalk(Condition& c, TableKeyIndexSet& i,
				        TableKeyIndexSet& r, bool isRev) :
						icond(c), tinter(i), tres(r), isReverse(isRev)
				{
				}
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					TableIndexKeyObject* sek = (TableIndexKeyObject*) k;
					if (sek->kname.compare(icond.keyname) != 0)
					{
						if (!isReverse)
						{
							return -1;
						}
						else
						{
							return cursor == 0 ? 0 : -1;
						}
					}
					int cmp = 0;
					if (!icond.MatchValue(sek->keyvalue, cmp))
					{
						switch (icond.cmp)
						{
							case CMP_NOTEQ:
								return 0;
							case CMP_GREATE:
							case CMP_LESS:
							{
								return cursor == 0 && cmp == 0 ? 0 : -1;
							}
							default:
								return -1;
						}
					}
					if (tinter.count(sek->index) > 0)
					{
						tres.insert(sek->index);
					}

					if (isReverse)
					{
						if (sek->index < *(tinter.begin()))
						{
							/*
							 * abort iter since next element would be less than set's begin
							 */
							return -1;
						}
					}
					else
					{
						if (*(tinter.rbegin()) < sek->index)
						{
							/*
							 * abort iter since next element would be greater than set's end
							 */
							return -1;
						}
					}
					return 0;
				}
		} walk(cond, interset, results, reverse);
		Walk(index, reverse, &walk);
		return 0;
	}

	int Ardb::TGetIndexs(const DBID& db, const Slice& tableName,
	        Conditions& conds, TableKeyIndexSet*& indexs,
	        TableKeyIndexSet*& temp)
	{
		if (conds.empty())
		{
			struct TAllIndexWalk: public WalkHandler
			{
					TableKeyIndexSet& tres;
					TAllIndexWalk(TableKeyIndexSet& i) :
							tres(i)
					{
					}
					int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
					{
						TableIndexKeyObject* sek = (TableIndexKeyObject*) k;
						tres.insert(sek->index);
						return 0;
					}
			} walk(*indexs);
			TableIndexKeyObject start(tableName, Slice(), ValueObject(), db);
			Walk(start, false, &walk);
			return 0;
		}

		TableKeyIndexSet* current = indexs;
		TableKeyIndexSet* next = temp;
		for (int i = 0; i < conds.size(); i++)
		{
			Condition& cond = conds[i];
			if (i == 0)
			{
				TUnionRowKeys(db, tableName, cond, *current);
			}
			else
			{
				if (conds[i - 1].logicop == LOGIC_OR)
				{
					TUnionRowKeys(db, tableName, cond, *current);
				}
				else //"and"
				{
					TInterRowKeys(db, tableName, cond, *current, *next);
					TableKeyIndexSet* tmp = current;
					current = next;
					next = tmp;
				}
			}
		}
		indexs = current;
		return 0;
	}

	int Ardb::TGetAll(const DBID& db, const Slice& tableName,
	        ValueArray& values)
	{
		TableQueryOptions options;
		TableSchemaValue schema;
		GetTableSchemaValue(db, tableName, schema);
		if (schema.keynames.empty() && schema.valnames.empty())
		{
			return 0;
		}
		StringArray::iterator it = schema.keynames.begin();
		while (it != schema.keynames.end())
		{
			options.names.push_back(*it);
			it++;
		}
		StringSet::iterator sit = schema.valnames.begin();
		while (sit != schema.valnames.end())
		{
			options.names.push_back(*sit);
			sit++;
		}
		std::string err;
		TGet(db, tableName, options, values, err);
		return 0;
	}

	int Ardb::TGet(const DBID& db, const Slice& tableName,
	        TableQueryOptions& options, ValueArray& values, std::string& err)
	{
		TableMetaValue meta;
		GetTableMetaValue(db, tableName, meta);
		TableSchemaValue schema;
		GetTableSchemaValue(db, tableName, schema);
		if (meta.size == 0)
		{
			return 0;
		}
		typedef std::pair<int, Slice> NameHolder;
		std::vector<NameHolder> idx_array;
		SliceArray::const_iterator kit = options.names.begin();
		int sort_idx = -1;
		while (kit != options.names.end())
		{
			NameHolder holder;
			holder.first = schema.Index(*kit);
			holder.second = *kit;
			idx_array.push_back(holder);
			if (!strcasecmp(kit->data(), options.orderby.data()))
			{
				sort_idx = idx_array.size() - 1;
			}
			kit++;
		}

		/*
		 * Default sort by the first element
		 */
		if (options.with_desc_asc && !options.names.empty()
		        && options.orderby.empty())
		{
			sort_idx = 0;
		}
		if (sort_idx == -1 && options.orderby.size() > 0)
		{
			err = "Invalid orderby param";
			return -1;
		}
		if (options.names.size() > 1
		        && (options.aggregate == AGGREGATE_COUNT
		                || options.aggregate == AGGREGATE_MIN
		                || options.aggregate == AGGREGATE_MAX))
		{
			err = "Invalid aggregate param for more than 1 TGet name";
			return -1;
		}

		TableKeyIndexSet set1, set2;
		TableKeyIndexSet* index = &set1;
		TableKeyIndexSet* tmp = &set2;
		TGetIndexs(db, tableName, options.conds, index, tmp);
		if (options.aggregate == AGGREGATE_COUNT)
		{
			ValueObject countobj((int64) index->size());
			values.push_back(countobj);
			return 0;
		}

		TableKeyIndexSet::iterator iit = index->begin();
		std::deque<TableRow> rows;
		while (iit != index->end())
		{
			TableRow row;
			row.sort_item_idx = sort_idx;
			std::vector<NameHolder>::iterator kkit = idx_array.begin();
			while (kkit != idx_array.end())
			{
				NameHolder& holder = *kkit;
				if (holder.first < 0)
				{
					TableColKeyObject k(tableName, holder.second, db);
					k.keyvals = iit->keyvals;
					ValueObject v;
					GetValue(k, &v);
					if (options.with_alpha)
					{
						value_convert_to_raw(v);
					}
					else
					{
						value_convert_to_number(v);
					}
					row.vs.push_back(v);
				}
				else
				{
					if (holder.first < iit->keyvals.size())
					{
						ValueObject v = iit->keyvals[holder.first];
						if (options.with_alpha)
						{
							value_convert_to_raw(v);
						}
						else
						{
							value_convert_to_number(v);
						}
						row.vs.push_back(v);
					}
					else
					{
						row.vs.push_back(ValueObject());
					}
				}
				kkit++;
			}
			rows.push_back(row);
			iit++;
		}
		if (sort_idx >= 0)
		{
			if (!options.is_desc)
			{
				std::sort(rows.begin(), rows.end(), less_value<TableRow>);
			}
			else
			{
				std::sort(rows.begin(), rows.end(), greater_value<TableRow>);
			}
		}
		if (!options.with_limit)
		{
			options.limit_offset = 0;
			options.limit_count = rows.size();
		}

		switch (options.aggregate)
		{
			case AGGREGATE_SUM:
			{
				TableRow row;
				uint32 colsize = options.names.size();
				for (uint32 i = 0; i < colsize; i++)
				{
					ValueObject v;
					for (uint32 j = 0; j < rows.size(); j++)
					{
						v += rows[j].vs[i];
					}
					row.vs.push_back(v);
				}
				rows.clear();
				rows.push_back(row);
				break;
			}
			case AGGREGATE_AVG:
			{
				TableRow row;
				uint32 colsize = options.names.size();
				for (uint32 i = 0; i < colsize; i++)
				{
					ValueObject v;
					for (uint32 j = 0; j < rows.size(); j++)
					{
						v += rows[j].vs[i];
					}
					v /= rows.size();
					row.vs.push_back(v);
				}
				rows.clear();
				rows.push_back(row);
				break;
			}
			case AGGREGATE_MAX:
			case AGGREGATE_MIN:
			{
				TableRow row;
				uint32 colsize = options.names.size();
				for (uint32 i = 0; i < colsize; i++)
				{
					for (uint32 j = 0; j < rows.size(); j++)
					{
						ValueObject& v = rows[j].vs[i];
						if (row.vs.empty())
						{
							row.vs.push_back(v);
						}
						else
						{
							bool max = (AGGREGATE_MAX == options.aggregate);
							if (v.type != EMPTY)
							{
								if ((max && row.vs[0].Compare(v) < 0)
								        || (!max && row.vs[0].Compare(v) > 0))
								{
									row.vs.pop_front();
									row.vs.push_back(rows[j].vs[i]);
								}
							}
						}
					}
				}
				rows.clear();
				rows.push_back(row);
				break;
			}
			default:
			{
				break;
			}
		}
		uint32 count = 0;
		for (uint32 i = options.limit_offset;
		        i < rows.size() && count < options.limit_count; i++)
		{
			ValueArray::iterator it = rows[i].vs.begin();
			while (it != rows[i].vs.end())
			{
				values.push_back(*it);
				it++;
			}
			count++;
		}
		return 0;
	}

	int Ardb::TUpdate(const DBID& db, const Slice& tableName,
	        TableUpdateOptions& options)
	{
		TableKeyIndexSet set1, set2;
		TableKeyIndexSet* index = &set1;
		TableKeyIndexSet* tmp = &set2;
		TGetIndexs(db, tableName, options.conds, index, tmp);
		if (index->empty())
		{
			return -1;
		}
		TableSchemaValue schema;
		GetTableSchemaValue(db, tableName, schema);
		uint32 colsize = schema.valnames.size();
		DEBUG_LOG("###Found %d rows for update", index->size());
		KeyLockerGuard keyguard(m_key_locker, db, tableName);
		SliceMap::const_iterator it = options.colnvs.begin();
		BatchWriteGuard guard(GetEngine());
		while (!index->empty() && it != options.colnvs.end())
		{
			TableKeyIndexSet::iterator iit = index->begin();
			ValueObject v;
			smart_fill_value(it->second, v);
			while (iit != index->end())
			{
				TableColKeyObject k(tableName, it->first, db);
				k.keyvals = iit->keyvals;
				SetValue( k, v);
				schema.valnames.insert(it->first);
				iit++;
			}
			it++;
		}
		if (schema.valnames.size() != colsize)
		{
			SetTableSchemaValue(db, tableName, schema);
		}
		return index->size();
	}

	bool Ardb::TRowExists(const DBID& db, const Slice& tableName,
	        ValueArray& rowkey)
	{
		TableColKeyObject cursor(tableName, Slice(), db);
		cursor.keyvals = rowkey;
		Iterator* iter = FindValue(cursor, true);
		bool exist = false;
		if (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey);
			if (NULL == kk || kk->type != TABLE_COL
			        || kk->key.compare(tableName) != 0)
			{
				//nothing
			}
			else
			{
				TableColKeyObject* tk = (TableColKeyObject*) kk;
				if (tk->keyvals == rowkey)
				{
					exist = true;
				}
			}
			DELETE(kk);
		}
		DELETE(iter);
		return exist;
	}
	int Ardb::TInsert(const DBID& db, const Slice& tableName,
	        TableInsertOptions& options, bool replace, std::string& err)
	{
		KeyLockerGuard keyguard(m_key_locker, db, tableName);
		TableSchemaValue schema;
		GetTableSchemaValue(db, tableName, schema);
		TableMetaValue meta;
		GetTableMetaValue(db, tableName, meta);
		if (schema.keynames.empty())
		{
			char err_cause[tableName.size() + 256];
			sprintf(err_cause, "Table %s is not created.", tableName.data());
			ERROR_LOG("%s", err_cause);
			err = err_cause;
			return -1;
		}
		SliceMap keynvs, colnvs;
		SliceMap::iterator sit = options.nvs.begin();
		while (sit != options.nvs.end())
		{
			if (schema.Index(sit->first) != -1)
			{
				keynvs[sit->first] = sit->second;
			}
			else
			{
				colnvs[sit->first] = sit->second;
			}
			sit++;
		}

		if (schema.keynames.size() != keynvs.size())
		{
			char err_cause[tableName.size() + 256];
			sprintf(err_cause,
			        "Table %s has %d keys defined, while only %d keys found in insert conditions.",
			        tableName.data(), schema.keynames.size(), keynvs.size());
			ERROR_LOG( "%s", err_cause);
			err = err_cause;
			return -1;
		}
		//write index
		TableKeyIndex index;
		for (uint32 i = 0; i < schema.keynames.size(); i++)
		{
			ValueObject v;
			smart_fill_value(keynvs[schema.keynames[i]], v);
			index.keyvals.push_back(v);
		}
		uint32 colsize = schema.valnames.size();
		bool hasrecord = TRowExists(db, tableName, index.keyvals);
		BatchWriteGuard guard(GetEngine());

		//write value
		SliceMap::const_iterator it = colnvs.begin();
		while (it != colnvs.end())
		{
			TableColKeyObject k(tableName, it->first, db);
			k.keyvals = index.keyvals;
			if (!replace)
			{
				if (0 == GetValue(k, NULL))
				{
					err = "Failed to insert since row exists.";
					return -1;
				}
			}
			ValueObject v;
			smart_fill_value(it->second, v);
			SetValue(k, v);
			schema.valnames.insert(it->first);
			it++;
		}

		//write index
		SliceMap::const_iterator kit = keynvs.begin();
		while (kit != keynvs.end())
		{
			TableIndexKeyObject tik(tableName, kit->first, kit->second, db);
			tik.index = index;
			ValueObject empty;
			SetValue(tik, empty);
			kit++;
		}

		if (!hasrecord)
		{
			meta.size++;
			SetTableMetaValue(db, tableName, meta);
		}
		if (schema.valnames.size() != colsize)
		{
			SetTableSchemaValue(db, tableName, schema);
		}
		return 0;
	}

	int Ardb::TDel(const DBID& db, const Slice& tableName,
	        TableDeleteOptions& options, std::string& err)
	{
		if (options.conds.empty())
		{
			return TClear(db, tableName);
		}
		KeyLockerGuard keyguard(m_key_locker, db, tableName);
		TableMetaValue meta;
		GetTableMetaValue(db, tableName, meta);
		if (meta.size == 0)
		{
			return 0;
		}
		TableSchemaValue schema;
		GetTableSchemaValue(db, tableName, schema);
		Conditions::iterator cit = options.conds.begin();
		while (cit != options.conds.end())
		{
			if (schema.Index(cit->keyname) == -1)
			{
				err = "where condition MUST use table key";
				return -1;
			}
			cit++;
		}
		TableKeyIndexSet set1, set2;
		TableKeyIndexSet* index = &set1;
		TableKeyIndexSet* tmp = &set2;
		TGetIndexs(db, tableName, options.conds, index, tmp);
		if (index->empty())
		{
			return 0;
		}

		BatchWriteGuard guard(GetEngine());
		TableKeyIndexSet::iterator iit = index->begin();
		while (iit != index->end())
		{
			for (uint32 i = 0; i < schema.keynames.size(); i++)
			{
				TableIndexKeyObject tik(tableName, schema.keynames[i],
				        iit->keyvals[i], db);
				tik.index.keyvals = iit->keyvals;
				DelValue(tik);
			}
			StringSet::iterator colit = schema.valnames.begin();
			while (colit != schema.valnames.end())
			{
				TableColKeyObject k(tableName, *colit, db);
				k.keyvals = iit->keyvals;
				DelValue(k);
				colit++;
			}
			iit++;
		}
		meta.size -= index->size();
		SetTableMetaValue(db, tableName, meta);
		return index->size();
	}
	int Ardb::TClear(const DBID& db, const Slice& tableName)
	{
		struct TClearWalk: public WalkHandler
		{
				Ardb* tdb;
				TClearWalk(Ardb* d) :
						tdb(d)
				{
				}
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					if (k->type == TABLE_COL)
					{
						TableColKeyObject* tk = (TableColKeyObject*) k;
						tdb->DelValue(*tk);
					}
					else if (k->type == TABLE_INDEX)
					{
						TableIndexKeyObject* tk = (TableIndexKeyObject*) k;
						tdb->DelValue(*tk);
					}
					return 0;
				}
		};
		int count = TCount(db, tableName);
		KeyLockerGuard keyguard(m_key_locker, db, tableName);
		BatchWriteGuard guard(GetEngine());
		TClearWalk walk(this);
		Slice empty;
		TableIndexKeyObject istart(tableName, empty, empty, db);
		TableColKeyObject cstart(tableName, empty, db);
		Walk( istart, false, &walk);
		Walk( cstart, false, &walk);
		KeyObject k(tableName, TABLE_META, db);
		KeyObject sck(tableName, TABLE_SCHEMA, db);
		DelValue(k);
		DelValue(sck);
		return count;
	}
	int Ardb::TCount(const DBID& db, const Slice& tableName)
	{
		TableMetaValue meta;
		GetTableMetaValue(db, tableName, meta);
		return meta.size;
	}

	int Ardb::TDesc(const DBID& db, const Slice& tableName, std::string& str)
	{
		TableSchemaValue schema;
		if (0 == GetTableSchemaValue(db, tableName, schema))
		{
			str.append("Keys: ");
			for (int i = 0; i < schema.keynames.size(); i++)
			{
				str.append(schema.keynames[i]).append(" ");
			}
			str.append("Cols: ");
			StringSet::iterator it = schema.valnames.begin();
			while (it != schema.valnames.end())
			{
				str.append(*it).append(" ");
				it++;
			}
			return 0;
		}
		return -1;
	}
}
