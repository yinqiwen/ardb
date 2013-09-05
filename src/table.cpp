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
				if (sort_item_idx >= 0 && vs.size() > (uint32) sort_item_idx
						&& other.vs.size() > (uint32) sort_item_idx)
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

	static bool parse_nvs(StringArray& cmd, StringStringMap& key_map, uint32& idx)
	{
		if (idx >= cmd.size())
		{
			return false;
		}
		uint32 i = idx;
		uint32 len = 0;
		if (!string_touint32(cmd[i], len))
		{
			//try parse as 'key=value[,key=value...]'
			std::vector<std::string> ss = split_string(cmd[i], ",");
			for (uint32 i = 0; i < ss.size(); i++)
			{
				if (ss[i].find("=") == std::string::npos)
				{
					return false;
				}
				std::vector<std::string> kv = split_string(ss[i], "=");
				if (kv.size() != 2)
				{
					return false;
				}
				key_map[trim_string(kv[0])] = trim_string(kv[1]);
			}
			idx++;
			return true;
		}
		//parse as 'numkvs key value [key value ��]'
		i++;
		for (; i < cmd.size() && len > 0; i += 2, len--)
		{
			if (i + 1 < cmd.size())
			{
				key_map[cmd[i]] = cmd[i + 1];
			} else
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
	static bool parse_strs(StringArray& cmd, StringArray& strs, uint32& idx)
	{
		if (idx >= cmd.size())
		{
			return false;
		}
		uint32 i = idx;
		uint32 len = 0;
		if (!string_touint32(cmd[i], len))
		{
			//try parse as 'key=value[,key=value...]'
			std::vector<std::string> ss = split_string(cmd[i], ",");
			for (uint32 i = 0; i < ss.size(); i++)
			{
				strs.push_back(trim_string(ss[i]));
			}
			idx++;
			return true;
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
			} else if ((pos = str.find(kLessEqual, 0)) != std::string::npos)
			{
				cmp = CMP_LESS_EQ;
				next = pos + 2;
			} else if ((pos = str.find(kGreater, 0)) != std::string::npos)
			{
				cmp = CMP_GREATE;
				next = pos + 1;
			} else if ((pos = str.find(kLess, 0)) != std::string::npos)
			{
				cmp = CMP_LESS;
				next = pos + 1;
			} else if ((pos = str.find(kNotEqual, 0)) != std::string::npos)
			{
				cmp = CMP_NOTEQ;
				next = pos + 2;
			} else if ((pos = str.find(kEqual, 0)) != std::string::npos)
			{
				cmp = CMP_EQAUL;
				next = pos + 1;
			} else
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
				} else if (!strcasecmp(cmd[idx + 1].c_str(), "or"))
				{
					logicop = LOGIC_OR;
				} else
				{
					return false;
				}
				idx++;
			}
			Condition cond(trim_string(keyname), cmp, trim_string(val),
					logicop);
			conds.push_back(cond);
			idx++;
		}
		return true;
	}

	bool TableInsertOptions::Parse(StringArray& args, uint32 offset,
			TableInsertOptions& options)
	{
		options.Clear();
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
		} else
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
			} else
			{
				return false;
			}
		}
		return true;
	}

	bool TableQueryOptions::Parse(StringArray& args, uint32 offset,
			TableQueryOptions& options)
	{
		options.Clear();
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
			} else if (!strcasecmp(args[i].c_str(), "desc"))
			{
				options.with_desc_asc = true;
				options.is_desc = true;
			} else if (!strcasecmp(args[i].c_str(), "alpha"))
			{
				options.with_alpha = true;
			} else if (!strcasecmp(args[i].c_str(), "limit")
					&& i < args.size() - 2)
			{
				options.with_limit = true;
				if (!string_toint32(args[i + 1], options.limit_offset)
						|| !string_toint32(args[i + 2], options.limit_count))
				{
					return -1;
				}
				i += 2;
			} else if (!strcasecmp(args[i].c_str(), "orderby")
					&& i < args.size() - 1)
			{
				options.orderby = args[i + 1].c_str();
				i++;
			} else if (!strcasecmp(args[i].c_str(), "aggregate")
					&& i < args.size() - 1)
			{
				if (!strcasecmp(args[i + 1].c_str(), "sum"))
				{
					options.aggregate = AGGREGATE_SUM;
				} else if (!strcasecmp(args[i + 1].c_str(), "max"))
				{
					options.aggregate = AGGREGATE_MAX;
				} else if (!strcasecmp(args[i + 1].c_str(), "min"))
				{
					options.aggregate = AGGREGATE_MIN;
				} else if (!strcasecmp(args[i + 1].c_str(), "count"))
				{
					options.aggregate = AGGREGATE_COUNT;
				} else if (!strcasecmp(args[i + 1].c_str(), "avg"))
				{
					options.aggregate = AGGREGATE_AVG;
				} else
				{
					return false;
				}
				i++;
			} else if (!strcasecmp(args[i].c_str(), "where"))
			{
				i = i + 1;
				return parse_conds(args, options.conds, i);
			} else
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
		uint32 len, collen, indexedlen;
		if (!BufferHelper::ReadVarUInt32(*(v.v.raw), len)
				|| !BufferHelper::ReadVarUInt32(*(v.v.raw), collen)
				|| !BufferHelper::ReadVarUInt32(*(v.v.raw), indexedlen))
		{
			return false;
		}
		for (uint32 i = 0; i < len; i++)
		{
			std::string tmp;
			if (!BufferHelper::ReadVarString(*(v.v.raw), tmp))
			{
				return false;
			}
			meta.keynames.push_back(tmp);
		}
		for (uint32 i = 0; i < collen; i++)
		{
			std::string tmp;
			if (!BufferHelper::ReadVarString(*(v.v.raw), tmp))
			{
				return false;
			}
			meta.valnames.insert(tmp);
		}
		for (uint32 i = 0; i < indexedlen; i++)
		{
			std::string tmp;
			if (!BufferHelper::ReadVarString(*(v.v.raw), tmp))
			{
				return false;
			}
			meta.indexed.insert(tmp);
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
		BufferHelper::WriteVarUInt32(*(v.v.raw), meta.indexed.size());
		for (uint32 i = 0; i < meta.keynames.size(); i++)
		{
			BufferHelper::WriteVarString(*(v.v.raw), meta.keynames[i]);
		}
		StringSet::iterator it = meta.valnames.begin();
		while (it != meta.valnames.end())
		{
			BufferHelper::WriteVarString(*(v.v.raw), *it);
			it++;
		}
		it = meta.indexed.begin();
		while (it != meta.indexed.end())
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
		if (valnames.count(std::string(key.data(), key.size())) > 0)
		{
			//Common column name
			return -1;
		}
		//invalid name
		return ERR_NOT_EXIST;
	}
	bool TableSchemaValue::HasIndex(const Slice& name)
	{
		std::string str(name.data(), name.size());
		return indexed.count(str) > 0 || Index(name) >= 0;
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
			TableSchemaValue& schema, Condition& cond,
			SliceSet& prefetch_keyset, TableKeyIndexValueTable& results)
	{
		results.clear();
		TableIndexKeyObject index(tableName, cond.keyname, cond.keyvalue, db);
		TableColKeyObject colstart(tableName, cond.keyname, db);
		bool reverse = cond.cmp == CMP_LESS || cond.cmp == CMP_LESS_EQ;
		if (cond.cmp == CMP_NOTEQ)
		{
			index.colvalue.Clear();
		}
		struct TUnionIndexWalk: public WalkHandler
		{
				TableSchemaValue& sc;
				SliceSet& prefech_kset;
				Condition& icond;
				TableKeyIndexValueTable& tres;
				bool rwalk;
				TUnionIndexWalk(TableSchemaValue& schema,
						SliceSet& prefetch_keyset, Condition& c,
						TableKeyIndexValueTable& i) :
						sc(schema), prefech_kset(prefetch_keyset), icond(c), tres(
								i), rwalk(false)
				{
				}
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					Slice colname;
					ValueObject* colvalue = NULL;
					ValueArray* index = NULL;
					if (k->type == TABLE_INDEX)
					{
						TableIndexKeyObject* sek = (TableIndexKeyObject*) k;
						colname = sek->colname;
						colvalue = &(sek->colvalue);
						index = &(sek->index);
					} else
					{
						TableColKeyObject* sek = (TableColKeyObject*) k;
						colname = sek->colname;
						colvalue = v;
						index = &(sek->index);
					}

					if (colname.compare(icond.keyname) != 0)
					{
						if (!rwalk)
						{
							return -1;
						} else
						{
							return cursor == 0 ? 0 : -1;
						}
					}
					int cmp = 0;
					if (!icond.MatchValue(*colvalue, cmp))
					{
						std::string str1, str2;
						switch (icond.cmp)
						{
							case CMP_NOTEQ:
								return 0;
							case CMP_GREATE:
							case CMP_LESS:
							{
								return cursor == 0 && cmp == 0 ?
										0 : (k->type == TABLE_INDEX ? -1 : 0);
							}
							default:
							{
								return k->type == TABLE_INDEX ? -1 : 0;
							}
						}
					}
					tres[*index][icond.keyname] = *colvalue;
					if (!prefech_kset.empty())
					{
						SliceSet::iterator kit = prefech_kset.begin();
						while (kit != prefech_kset.end())
						{
							int idx = sc.Index(*kit);
							if (idx >= 0 && idx < (int) (sc.keynames.size()))
							{
								std::string kname(kit->data(), kit->size());
								tres[*index][kname] = (*index)[idx];
							}
							kit++;
						}
					}
					return 0;
				}
		} walk(schema, prefetch_keyset, cond, results);
		walk.rwalk = reverse;
		if (schema.HasIndex(cond.keyname))
		{
			Walk(index, reverse, &walk);
		} else
		{
			Walk(colstart, reverse, &walk);
		}
		return 0;
	}

	int Ardb::TInterRowKeys(const DBID& db, const Slice& tableName,
			TableSchemaValue& schema, Condition& cond,
			SliceSet& prefetch_keyset, TableKeyIndexValueTable& interset,
			TableKeyIndexValueTable& results)
	{
		TableIndexKeyObject index(tableName, cond.keyname, cond.keyvalue, db);
		TableColKeyObject colstart(tableName, cond.keyname, db);
		results.clear();
		bool reverse = cond.cmp == CMP_LESS || cond.cmp == CMP_LESS_EQ;
		if (cond.cmp == CMP_NOTEQ)
		{
			index.colvalue.Clear();
		}
		struct TInterIndexWalk: public WalkHandler
		{
				TableSchemaValue& sc;
				SliceSet& prefech_kset;
				Condition& icond;
				TableKeyIndexValueTable& tinter;
				TableKeyIndexValueTable& tres;
				bool isReverse;
				TInterIndexWalk(TableSchemaValue& schema,
						SliceSet& prefetch_keyset, Condition& c,
						TableKeyIndexValueTable& i, TableKeyIndexValueTable& r,
						bool isRev) :
						sc(schema), prefech_kset(prefetch_keyset), icond(c), tinter(
								i), tres(r), isReverse(isRev)
				{
				}
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					Slice colname;
					ValueObject* colvalue = NULL;
					ValueArray* index = NULL;
					if (k->type == TABLE_INDEX)
					{
						TableIndexKeyObject* sek = (TableIndexKeyObject*) k;
						colname = sek->colname;
						colvalue = &(sek->colvalue);
						index = &(sek->index);
					} else
					{
						TableColKeyObject* sek = (TableColKeyObject*) k;
						colname = sek->colname;
						colvalue = v;
						index = &(sek->index);
					}
					if (colname.compare(icond.keyname) != 0)
					{
						if (!isReverse)
						{
							return -1;
						} else
						{
							return cursor == 0 ? 0 : -1;
						}
					}
					int cmp = 0;
					if (!icond.MatchValue(*colvalue, cmp))
					{
						switch (icond.cmp)
						{
							case CMP_NOTEQ:
								return 0;
							case CMP_GREATE:
							case CMP_LESS:
							{
								return cursor == 0 && cmp == 0 ?
										0 : (k->type == TABLE_INDEX ? -1 : 0);
							}
							default:
							{
								return k->type == TABLE_INDEX ? -1 : 0;
							}
						}
					}
					if (tinter.count(*index) > 0)
					{
						tres[*index][icond.keyname] = *colvalue;
						if (!prefech_kset.empty())
						{
							SliceSet::iterator kit = prefech_kset.begin();
							while (kit != prefech_kset.end())
							{
								int idx = sc.Index(*kit);
								if (idx >= 0
										&& idx < (int) (sc.keynames.size()))
								{
									std::string kname(kit->data(), kit->size());
									tres[*index][kname] = (*index)[idx];
								}
								kit++;
							}
						}
					}

					if (isReverse)
					{
						if ((*index) < tinter.begin()->first)
						{
							/*
							 * abort iter since next element would be less than set's begin
							 */
							return -1;
						}
					} else
					{
						if (tinter.rbegin()->first < (*index))
						{
							/*
							 * abort iter since next element would be greater than set's end
							 */
							return -1;
						}
					}
					return 0;
				}
		} walk(schema, prefetch_keyset, cond, interset, results, reverse);
		if (schema.HasIndex(cond.keyname))
		{
			Walk(index, reverse, &walk);
		} else
		{
			Walk(colstart, reverse, &walk);
		}
		return 0;
	}

	int Ardb::TGetIndexs(const DBID& db, const Slice& tableName,
			TableSchemaValue& schema, Conditions& conds,
			SliceSet& prefetch_keyset, TableKeyIndexValueTable*& indexs,
			TableKeyIndexValueTable*& temp)
	{
		if (conds.empty())
		{
			struct TAllIndexWalk: public WalkHandler
			{
					Slice firstKeyName;
					TableKeyIndexValueTable& tres;
					TAllIndexWalk(const Slice& s, TableKeyIndexValueTable& i) :
							firstKeyName(s), tres(i)
					{
					}
					int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
					{
						TableIndexKeyObject* sek = (TableIndexKeyObject*) k;
						if (sek->colname != firstKeyName)
						{
							return -1;
						}
						std::string colname(sek->colname.data(),
								sek->colname.size());
						tres[sek->index][colname] = sek->colvalue;
						return 0;
					}
			} walk(schema.keynames[0], *indexs);
			TableIndexKeyObject start(tableName, schema.keynames[0],
					ValueObject(), db);
			Walk(start, false, &walk);
			return 0;
		}

		TableKeyIndexValueTable* current = indexs;
		TableKeyIndexValueTable* next = temp;
		for (uint32 i = 0; i < conds.size(); i++)
		{
			Condition& cond = conds[i];
			if (i == 0)
			{
				TUnionRowKeys(db, tableName, schema, cond, prefetch_keyset,
						*current);
			} else
			{
				if (conds[i - 1].logicop == LOGIC_OR)
				{
					TUnionRowKeys(db, tableName, schema, cond, prefetch_keyset,
							*current);
				} else //"and"
				{
					TInterRowKeys(db, tableName, schema, cond, prefetch_keyset,
							*current, *next);
					TableKeyIndexValueTable* tmp = current;
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
		values.clear();
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

	int Ardb::TDelCol(const DBID& db, const Slice& tableName, const Slice& col)
	{
		TableSchemaValue schema;
		if (0 != GetTableSchemaValue(db, tableName, schema))
		{
			return -1;
		}
		int idx = schema.Index(col);
		if (idx == ERR_NOT_EXIST)
		{
			return 0;
		}
		struct TIndexWalk: public WalkHandler
		{
				Ardb* tdb;
				Slice name;
				TIndexWalk(Ardb* db, const Slice& n) :
						tdb(db), name(n)
				{
				}
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					if (k->type == TABLE_INDEX)
					{
						TableIndexKeyObject* sek = (TableIndexKeyObject*) k;
						if (sek->colname != name)
						{
							return -1;
						}
						tdb->DelValue(*sek);
					} else if (k->type == TABLE_COL)
					{
						TableColKeyObject* sek = (TableColKeyObject*) k;
						if (sek->colname != name)
						{
							return -1;
						}
						tdb->DelValue(*sek);
					}
					return 0;
				}
		} walk(this, col);
		KeyLockerGuard keyguard(m_key_locker, db, tableName);
		BatchWriteGuard guard(GetEngine());
		TableIndexKeyObject start(tableName, col, ValueObject(), db);
		TableColKeyObject col_start(tableName, col, db);
		if (idx >= 0 || schema.HasIndex(col))
		{
			Walk(start, false, &walk);
		}
		if (idx < 0)
		{
			Walk(col_start, false, &walk);
		}
		return 0;
	}

	int Ardb::TCreateIndex(const DBID& db, const Slice& tableName,
			const Slice& col)
	{
		TableSchemaValue schema;
		if (0 != GetTableSchemaValue(db, tableName, schema))
		{
			return -1;
		}
		if (schema.HasIndex(col))
		{
			return 0;
		}
		struct TColWalk: public WalkHandler
		{
				Ardb* tdb;
				Slice name;
				TColWalk(Ardb* db, const Slice& n) :
						tdb(db), name(n)
				{
				}
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					TableColKeyObject* sek = (TableColKeyObject*) k;
					if (sek->colname != name)
					{
						return -1;
					}
					TableIndexKeyObject index(sek->key, name, *v, sek->db);
					index.index = sek->index;
					ValueObject empty;
					tdb->SetValue(index, empty);
					return 0;
				}
		} walk(this, col);
		KeyLockerGuard keyguard(m_key_locker, db, tableName);
		BatchWriteGuard guard(GetEngine());
		schema.indexed.insert(std::string(col.data(), col.size()));
		TableColKeyObject start(tableName, col, db);
		Walk(start, false, &walk);
		SetTableSchemaValue(db, tableName, schema);
		return 0;
	}

	int Ardb::TCol(const DBID& db, const Slice& tableName,
			TableSchemaValue& schema, const Slice& col,
			TableKeyIndexValueTable& rs)
	{
		int idx = schema.Index(col);
		if (idx == ERR_NOT_EXIST)
		{
			return 0;
		}
		struct TColWalk: public WalkHandler
		{
				Slice name;
				int colidx;
				TableSchemaValue& sc;
				TableKeyIndexValueTable& vvs;
				TColWalk(const Slice& n, int idx, TableSchemaValue& scm,
						TableKeyIndexValueTable& vs) :
						name(n), colidx(idx), sc(scm), vvs(vs)
				{
				}
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					if (colidx >= 0)
					{
						TableIndexKeyObject* sek = (TableIndexKeyObject*) k;
						if (sek->colname != name)
						{
							return -1;
						}
						std::string colname(name.data(), name.size());
						vvs[sek->index][colname] = sek->colvalue;
						//prefetch key value
						for (uint32 i = 0; i < sc.keynames.size(); i++)
						{
							if (i != (uint32) colidx)
							{
								vvs[sek->index][sc.keynames[colidx]] =
										sek->index[colidx];
							}
						}
					} else
					{
						TableColKeyObject* sek = (TableColKeyObject*) k;
						if (sek->colname != name)
						{
							return -1;
						}
						std::string colname(name.data(), name.size());
						vvs[sek->index][colname] = *v;
					}
					return 0;
				}
		} walk(col, idx, schema, rs);
		if (idx >= 0)
		{
			TableIndexKeyObject start(tableName, col, ValueObject(), db);
			Walk(start, false, &walk);
		} else
		{
			TableColKeyObject start(tableName, col, db);
			Walk(start, false, &walk);
		}
		return 0;
	}

	int Ardb::TGet(const DBID& db, const Slice& tableName,
			TableQueryOptions& options, ValueArray& values, std::string& err)
	{
		values.clear();
		TableMetaValue meta;
		GetTableMetaValue(db, tableName, meta);
		TableSchemaValue schema;
		GetTableSchemaValue(db, tableName, schema);
		if (meta.size == 0)
		{
			return 0;
		}
		SliceSet prefetch_keyset;
		StringArray::iterator kit = options.names.begin();
		int sort_idx = -1;
		for (uint32 i = 0; i < options.names.size(); i++)
		{
			int idx = schema.Index(options.names[i]);
			if (idx == ERR_NOT_EXIST)
			{
				err = "Invalid tget param";
				DEBUG_LOG("ERROR:%s", err.c_str());
				return -1;
			} else if (idx >= 0)
			{
				prefetch_keyset.insert(*kit);
			}
			if (!strcasecmp(kit->data(), options.orderby.data()))
			{
				sort_idx = i;
			}
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
			DEBUG_LOG("ERROR:%s", err.c_str());
			return -1;
		}
		if (options.names.size() > 1
				&& (options.aggregate == AGGREGATE_COUNT
						|| options.aggregate == AGGREGATE_MIN
						|| options.aggregate == AGGREGATE_MAX))
		{
			err = "Invalid aggregate param for more than 1 TGet name";
			DEBUG_LOG("ERROR:%s", err.c_str());
			return -1;
		}

		std::deque<TableRow> rows;
		if (options.conds.empty())
		{
			TableKeyIndexValueTable rs;
			StringArray::const_iterator nit = options.names.begin();
			bool all_key_filled = false;
			while (nit != options.names.end())
			{
				//Read records sequentially
				const Slice& colname = *nit;
				if (schema.Index(colname) >= 0 && !all_key_filled)
				{
					if (!all_key_filled)
					{
						all_key_filled = true;
					} else
					{
						nit++;
						continue;
					}
				}
				TCol(db, tableName, schema, colname, rs);
				nit++;
			}

			if (options.aggregate == AGGREGATE_COUNT)
			{
				ValueObject countobj((int64) rs.size());
				values.push_back(countobj);
				return 0;
			}
			TableKeyIndexValueTable::iterator tit = rs.begin();
			while (tit != rs.end())
			{
				TableRow row;
				row.sort_item_idx = sort_idx;
				StringArray::const_iterator nit = options.names.begin();
				while (nit != options.names.end())
				{
					std::string colname(nit->data(), nit->size());
					ValueObject& vv = tit->second[colname];
					NameValueTable::iterator found = tit->second.find(colname);
					if (options.with_alpha)
					{
						value_convert_to_raw(vv);
					} else
					{
						value_convert_to_number(vv);
					}
					row.vs.push_back(vv);
					nit++;
				}
				rows.push_back(row);
				tit++;
			}
			rs.clear();
		} else //!options.conds.empty()
		{
			//check colname in conditions
			Conditions::iterator cit = options.conds.begin();
			while (cit != options.conds.end())
			{
				if (schema.Index(cit->keyname) == ERR_NOT_EXIST)
				{
					err = "Invalid where condition";
					DEBUG_LOG("ERROR:%s", err.c_str());
					return -1;
				}
				cit++;
			}

			TableKeyIndexValueTable set1, set2;
			TableKeyIndexValueTable* index = &set1;
			TableKeyIndexValueTable* tmp = &set2;
			TGetIndexs(db, tableName, schema, options.conds, prefetch_keyset,
					index, tmp);
			if (options.aggregate == AGGREGATE_COUNT)
			{
				ValueObject countobj((int64) index->size());
				values.push_back(countobj);
				return 0;
			}
			TableKeyIndexValueTable::iterator iit = index->begin();
			while (iit != index->end())
			{
				TableRow row;
				row.sort_item_idx = sort_idx;
				StringArray::const_iterator nit = options.names.begin();
				while (nit != options.names.end())
				{
					std::string colname(nit->data(), nit->size());
					ValueObject vv;
					NameValueTable::iterator found = iit->second.find(colname);
					std::string str;
					if (found == iit->second.end())
					{
						TableColKeyObject search_key(tableName, colname, db);
						search_key.index = iit->first;
						GetValue(search_key, &vv);
					} else
					{
						vv = found->second;
					}
					if (options.with_alpha)
					{
						value_convert_to_raw(vv);
					} else
					{
						value_convert_to_number(vv);
					}
					row.vs.push_back(vv);
					nit++;
				}
				rows.push_back(row);
				iit++;
			}
		}

		if (sort_idx >= 0)
		{
			if (!options.is_desc)
			{
				std::sort(rows.begin(), rows.end(), less_value<TableRow>);
			} else
			{
				std::sort(rows.begin(), rows.end(), greater_value<TableRow>);
			}
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
					std::string str;
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
						} else
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
		if (!options.with_limit)
		{
			options.limit_offset = 0;
			options.limit_count = rows.size();
		}
		if (options.limit_count <= 0)
		{
			options.limit_count = rows.size();
		}
		uint32 count = 0;
		for (uint32 i = options.limit_offset;
				i < rows.size() && count < (uint32) options.limit_count; i++)
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
		TableSchemaValue schema;
		if (0 != GetTableSchemaValue(db, tableName, schema))
		{
			return -1;
		}
		TableKeyIndexValueTable set1, set2;
		TableKeyIndexValueTable* index = &set1;
		TableKeyIndexValueTable* tmp = &set2;
		SliceSet empty;
		TGetIndexs(db, tableName, schema, options.conds, empty, index, tmp);
		if (index->empty())
		{
			return -1;
		}
		uint32 colsize = schema.valnames.size();
		DEBUG_LOG("###Found %d rows for update", index->size());
		KeyLockerGuard keyguard(m_key_locker, db, tableName);
		StringStringMap::const_iterator it = options.colnvs.begin();
		BatchWriteGuard guard(GetEngine());
		while (!index->empty() && it != options.colnvs.end())
		{
			TableKeyIndexValueTable::iterator iit = index->begin();
			ValueObject v;
			smart_fill_value(it->second, v);
			Slice colname = it->first;
			int idx = schema.Index(colname);
			while (iit != index->end())
			{
				if (idx >= 0)
				{
					//delete old index
					TableIndexKeyObject old(tableName, it->first,
							iit->first[idx], db);
					old.index = iit->first;
					DelValue(old);
					TableIndexKeyObject newkey(tableName, it->first, v, db);
					newkey.index = iit->first;
					newkey.index[idx] = v;
					ValueObject empty;
					SetValue(newkey, empty);
				} else
				{
					TableColKeyObject k(tableName, it->first, db);
					k.index = iit->first;
					if (schema.HasIndex(colname))
					{
						//delete old index
						ValueObject oldvalue;
						if (0 == GetValue(k, &oldvalue))
						{
							TableIndexKeyObject old(tableName, it->first,
									oldvalue, db);
							old.index = iit->first;
							DelValue(old);
						}
					}
					SetValue(k, v);
				}
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
			TableSchemaValue& schema, ValueArray& rowkey)
	{
		TableIndexKeyObject cursor(tableName, schema.keynames[0], rowkey[0],
				db);
		cursor.index = rowkey;
		return GetValue(cursor, NULL) == 0;
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
		StringSliceMap keynvs, colnvs;
		StringStringMap::iterator sit = options.nvs.begin();
		while (sit != options.nvs.end())
		{
			if (schema.Index(sit->first) >= 0)
			{
				keynvs[sit->first] = sit->second;
			} else
			{
				colnvs[sit->first] = sit->second;
			}
			sit++;
		}

		if (schema.keynames.size() != keynvs.size())
		{
			char err_cause[tableName.size() + 256];
			sprintf(err_cause,
					"Table %s has %zu keys defined, while only %zu keys found in insert conditions.",
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
			index.push_back(v);
		}
		uint32 colsize = schema.valnames.size();
		bool hasrecord = TRowExists(db, tableName, schema, index);
		BatchWriteGuard guard(GetEngine());

		//write value
		StringSliceMap::const_iterator it = colnvs.begin();
		while (it != colnvs.end())
		{
			ValueObject v;
			smart_fill_value(it->second, v);
			TableColKeyObject k(tableName, it->first, db);
			k.index = index;
			if (!replace)
			{
				ValueObject oldv;
				if (0 == GetValue(k, &oldv))
				{
					if (!replace)
					{
						err = "Failed to insert since row exists.";
						return -1;
					}
					if (oldv.Compare(v) == 0)
					{
						it++;
						continue;
					}
				}
			}
			SetValue(k, v);
			//write index
			if (schema.HasIndex(it->first))
			{
				TableIndexKeyObject tik(tableName, it->first, v, db);
				tik.index = index;
				ValueObject empty;
				SetValue(tik, empty);
			}
			schema.valnames.insert(it->first);
			it++;
		}
		//write index
		StringSliceMap::const_iterator kit = keynvs.begin();
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
			if (schema.Index(cit->keyname) < 0)
			{
				err = "where condition MUST use table key";
				return -1;
			}
			cit++;
		}
		TableKeyIndexValueTable set1, set2;
		TableKeyIndexValueTable* index = &set1;
		TableKeyIndexValueTable* tmp = &set2;
		SliceSet empty;
		TGetIndexs(db, tableName, schema, options.conds, empty, index, tmp);
		if (index->empty())
		{
			return 0;
		}
		KeyLockerGuard keyguard(m_key_locker, db, tableName);
		BatchWriteGuard guard(GetEngine());
		TableKeyIndexValueTable::iterator iit = index->begin();
		while (iit != index->end())
		{
			for (uint32 i = 0; i < schema.keynames.size(); i++)
			{
				TableIndexKeyObject tik(tableName, schema.keynames[i],
						iit->first[i], db);
				tik.index = iit->first;
				DelValue(tik);
			}
			StringSet::iterator colit = schema.valnames.begin();
			while (colit != schema.valnames.end())
			{
				TableColKeyObject k(tableName, *colit, db);
				k.index = iit->first;
				if (schema.HasIndex(*colit))
				{
					ValueObject colv;
					if (0 == GetValue(k, &colv))
					{
						TableIndexKeyObject tik(tableName, *colit, colv, db);
						tik.index = iit->first;
						DelValue(tik);
					}
				}
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
					if (k->type == TABLE_INDEX)
					{
						TableIndexKeyObject* tk = (TableIndexKeyObject*) k;
						tdb->DelValue(*tk);
					} else
					{
						TableColKeyObject* tk = (TableColKeyObject*) k;
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
		Walk(istart, false, &walk);
		Walk(cstart, false, &walk);
		KeyObject k(tableName, TABLE_META, db);
		KeyObject sck(tableName, TABLE_SCHEMA, db);
		DelValue(k);
		DelValue(sck);
		SetExpiration(db, tableName, 0, false);
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
			for (uint32 i = 0; i < schema.keynames.size(); i++)
			{
				str.append(schema.keynames[i]).append(" ");
			}
			str.append("Cols: ");
			StringSet::iterator it = schema.valnames.begin();
			while (it != schema.valnames.end())
			{
				str.append(*it);
				if (schema.HasIndex(*it))
				{
					str.append("(I)");
				}
				str.append(" ");
				it++;
			}
			return 0;
		}
		return -1;
	}
}
