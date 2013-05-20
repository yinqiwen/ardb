/*
 * table.cpp
 *
 *  Created on: 2013-4-11
 *      Author: wqy
 */
#include "ardb.hpp"

namespace ardb
{
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
		if (!parse_nvs(args, options.keynvs, offset)
		        || !parse_nvs(args, options.colnvs, offset))
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
				options.is_desc = false;
			}
			else if (!strcasecmp(args[i].c_str(), "desc"))
			{
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
			else if (!strcasecmp(args[offset].c_str(), "where"))
			{
				offset = offset + 1;
				return parse_conds(args, options.conds, offset);
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
		uint32 len, collen;
		if (!BufferHelper::ReadVarUInt32(*(v.v.raw), meta.size)
		        || !BufferHelper::ReadVarUInt32(*(v.v.raw), len)
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
	static void EncodeTableMetaData(ValueObject& v, TableMetaValue& meta)
	{
		v.type = RAW;
		if (v.v.raw == NULL)
		{
			v.v.raw = new Buffer(16);
		}
		BufferHelper::WriteVarUInt32(*(v.v.raw), meta.size);
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

	int TableMetaValue::Index(const Slice& key)
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
		KeyObject k(tableName, TABLE_META);
		ValueObject v;
		if (0 == GetValue(db, k, &v))
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
		KeyObject k(tableName, TABLE_META);
		ValueObject v;
		EncodeTableMetaData(v, meta);
		SetValue(db, k, v);
	}

	int Ardb::TCreate(const DBID& db, const Slice& tableName,
	        SliceArray& keynames)
	{
		TableMetaValue meta;
		GetTableMetaValue(db, tableName, meta);
		SliceArray::iterator it = keynames.begin();
		while (it != keynames.end())
		{
			std::string name(it->data(), it->size());
			bool found = false;
			StringArray::iterator sit = meta.keynames.begin();
			while (sit != meta.keynames.end())
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
				meta.keynames.push_back(name);
			}
			it++;
		}
		SetTableMetaValue(db, tableName, meta);
		return 0;
	}

	int Ardb::TUnionRowKeys(const DBID& db, const Slice& tableName,
	        Condition& cond, TableKeyIndexSet& results)
	{
		TableIndexKeyObject index(tableName, cond.keyname, cond.keyvalue);
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
		Walk(db, index, reverse, &walk);
		return 0;
	}

	int Ardb::TInterRowKeys(const DBID& db, const Slice& tableName,
	        Condition& cond, TableKeyIndexSet& interset,
	        TableKeyIndexSet& results)
	{
		TableIndexKeyObject index(tableName, cond.keyname, cond.keyvalue);
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
							return -1;
						}
					}
					else
					{
						if (*(tinter.begin()) < sek->index)
						{
							return -1;
						}
					}
					return 0;
				}
		} walk(cond, interset, results, reverse);
		Walk(db, index, reverse, &walk);
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
			TableIndexKeyObject start(tableName, Slice(), ValueObject());
			Walk(db, start, false, &walk);
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

	int Ardb::TGet(const DBID& db, const Slice& tableName,
	        const SliceArray& names, Conditions& conds, ValueArray& values)
	{
		TableMetaValue meta;
		GetTableMetaValue(db, tableName, meta);
		if (meta.size == 0)
		{
			return 0;
		}
		typedef std::pair<int, Slice> NameHolder;

		std::vector<NameHolder> idx_array;
		SliceArray::const_iterator kit = names.begin();
		while (kit != names.end())
		{
			NameHolder holder;
			holder.first = meta.Index(*kit);
			holder.second = *kit;
			idx_array.push_back(holder);
			kit++;
		}

		TableKeyIndexSet set1, set2;
		TableKeyIndexSet* index = &set1;
		TableKeyIndexSet* tmp = &set2;
		TGetIndexs(db, tableName, conds, index, tmp);
		DEBUG_LOG("###Found %d index", index->size());

		TableKeyIndexSet::iterator iit = index->begin();
		while (iit != index->end())
		{
			std::vector<NameHolder>::iterator kkit = idx_array.begin();
			while (kkit != idx_array.end())
			{
				NameHolder& holder = *kkit;
				TableKeyIndexSet::iterator iit = index->begin();
				if (holder.first < 0)
				{
					TableColKeyObject k(tableName, holder.second);
					k.keyvals = iit->keyvals;
					ValueObject v;
					GetValue(db, k, &v);
					values.push_back(v);
				}
				else
				{
					if (holder.first < iit->keyvals.size())
					{
						values.push_back(iit->keyvals[holder.first]);
					}
					else
					{
						values.push_back(ValueObject());
					}
				}
				kkit++;
			}
			iit++;
		}
		return 0;
	}
	int Ardb::TUpdate(const DBID& db, const Slice& tableName,
	        const SliceMap& colvals, Conditions& conds)
	{
		TableKeyIndexSet set1, set2;
		TableKeyIndexSet* index = &set1;
		TableKeyIndexSet* tmp = &set2;
		TGetIndexs(db, tableName, conds, index, tmp);
		if (index->empty())
		{
			return -1;
		}
		KeyLockerGuard keyguard(m_key_locker, db, tableName);
		SliceMap::const_iterator it = colvals.begin();
		BatchWriteGuard guard(GetDB(db));
		while (!index->empty() && it != colvals.end())
		{
			TableKeyIndexSet::iterator iit = index->begin();
			ValueObject v;
			smart_fill_value(it->second, v);
			while (iit != index->end())
			{
				TableColKeyObject k(tableName, it->first);
				k.keyvals = iit->keyvals;
				SetValue(db, k, v);
				iit++;
			}
			it++;
		}
		return 0;
	}

	bool Ardb::TRowExists(const DBID& db, const Slice& tableName,
	        ValueArray& rowkey)
	{
		TableColKeyObject cursor(tableName, Slice());
		cursor.keyvals = rowkey;
		Iterator* iter = FindValue(db, cursor);
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
	        const SliceMap& keyvals, const SliceMap& colvals, bool replace,
	        std::string& err)
	{
		KeyLockerGuard keyguard(m_key_locker, db, tableName);
		TableMetaValue meta;
		GetTableMetaValue(db, tableName, meta);
		if (meta.keynames.empty())
		{
			char err_cause[tableName.size() + 256];
			sprintf(err_cause, "Table %s is not created.", tableName.data());
			ERROR_LOG("%s", err_cause);
			err = err_cause;
			return -1;
		}
		if (meta.keynames.size() != keyvals.size())
		{
			char err_cause[tableName.size() + 256];
			sprintf(err_cause, "Table %s has different %d keys defined.",
			        tableName.data(), meta.keynames.size());
			ERROR_LOG( "%s", err_cause);
			err = err_cause;
			return -1;
		}
		//write index
		TableKeyIndex index;
		for (uint32 i = 0; i < meta.keynames.size(); i++)
		{
			ValueObject v;
			SliceMap::const_iterator found = keyvals.find(meta.keynames[i]);
			if (found == keyvals.end())
			{
				char err_cause[tableName.size() + 256];
				sprintf(err_cause, "No table %s defined key %s found in input.",
				        tableName.data(), meta.keynames[i].c_str());
				ERROR_LOG( "%s", err_cause);
				err = err_cause;
				return -1;
			}
			smart_fill_value(found->second, v);
			index.keyvals.push_back(v);
		}
		uint32 colsize = meta.valnames.size();
		bool hasrecord = TRowExists(db, tableName, index.keyvals);
		BatchWriteGuard guard(GetDB(db));

		//write value
		SliceMap::const_iterator it = colvals.begin();
		while (it != colvals.end())
		{
			TableColKeyObject k(tableName, it->first);
			k.keyvals = index.keyvals;
			if (!replace)
			{
				if (0 == GetValue(db, k, NULL))
				{
					err = "Failed to insert since row exists.";
					return -1;
				}
			}
			ValueObject v;
			smart_fill_value(it->second, v);
			SetValue(db, k, v);
			meta.valnames.insert(it->first);
			it++;
		}

		//write index
		SliceMap::const_iterator kit = keyvals.begin();
		while (kit != keyvals.end())
		{
			TableIndexKeyObject tik(tableName, kit->first, kit->second);
			tik.index = index;
			ValueObject empty;
			SetValue(db, tik, empty);
			kit++;
		}

		if (!hasrecord)
		{
			meta.size++;
		}
		if (!hasrecord || meta.valnames.size() != colsize)
		{
			SetTableMetaValue(db, tableName, meta);
		}
		return 0;
	}

	int Ardb::TDel(const DBID& db, const Slice& tableName, Conditions& conds)
	{
		if (conds.empty())
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
		TableKeyIndexSet set1, set2;
		TableKeyIndexSet* index = &set1;
		TableKeyIndexSet* tmp = &set2;
		TGetIndexs(db, tableName, conds, index, tmp);
		if (index->empty())
		{
			return 0;
		}

		struct TDelWalk: public WalkHandler
		{
				Ardb* tdb;
				const DBID& tdbid;
				const ValueArray& cmp;
				TDelWalk(Ardb* d, const DBID& id, const ValueArray& c) :
						tdb(d), tdbid(id), cmp(c)
				{
				}
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					TableColKeyObject* ck = (TableColKeyObject*) k;
					if (compare_values(ck->keyvals, cmp) == 0)
					{
						tdb->DelValue(tdbid, *ck);
					}
					return -1;
				}
		};

		BatchWriteGuard guard(GetDB(db));
		TableKeyIndexSet::iterator iit = index->begin();
		while (iit != index->end())
		{
			for (uint32 i = 0; i < meta.keynames.size(); i++)
			{
				TableIndexKeyObject tik(tableName, meta.keynames[i],
				        iit->keyvals[i]);
				tik.index.keyvals = iit->keyvals;
				DelValue(db, tik);
			}
			StringSet::iterator colit = meta.valnames.begin();
			while (colit != meta.valnames.end())
			{
				TableColKeyObject k(tableName, *colit);
				k.keyvals = iit->keyvals;
				DelValue(db, k);
				colit++;
			}
			iit++;
		}
		meta.size -= index->size();
		SetTableMetaValue(db, tableName, meta);
		return 0;
	}
	int Ardb::TClear(const DBID& db, const Slice& tableName)
	{
		struct TClearWalk: public WalkHandler
		{
				Ardb* tdb;
				const DBID& tdbid;
				TClearWalk(Ardb* d, const DBID& id) :
						tdb(d), tdbid(id)
				{
				}
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					if (k->type == TABLE_COL)
					{
						TableColKeyObject* tk = (TableColKeyObject*) k;
						tdb->DelValue(tdbid, *tk);
					}
					else if (k->type == TABLE_INDEX)
					{
						TableIndexKeyObject* tk = (TableIndexKeyObject*) k;
						tdb->DelValue(tdbid, *tk);
					}
					return 0;
				}
		};
		KeyLockerGuard keyguard(m_key_locker, db, tableName);
		BatchWriteGuard guard(GetDB(db));
		TClearWalk walk(this, db);
		Slice empty;
		TableIndexKeyObject istart(tableName, empty, empty);
		TableColKeyObject cstart(tableName, empty);
		Walk(db, istart, false, &walk);
		Walk(db, cstart, false, &walk);
		KeyObject k(tableName, TABLE_META);
		DelValue(db, k);
		return 0;
	}
	int Ardb::TCount(const DBID& db, const Slice& tableName)
	{
		TableMetaValue meta;
		GetTableMetaValue(db, tableName, meta);
		return meta.size;
	}

	int Ardb::TDesc(const DBID& db, const Slice& tableName, std::string& str)
	{
		TableMetaValue meta;
		if (0 == GetTableMetaValue(db, tableName, meta))
		{
			str.append("Keys: ");
			for (int i = 0; i < meta.keynames.size(); i++)
			{
				str.append(meta.keynames[i]).append(" ");
			}
			str.append("Cols: ");
			StringSet::iterator it = meta.valnames.begin();
			while (it != meta.valnames.end())
			{
				str.append(*it).append(" ");
				it++;
			}
			return 0;
		}
		return -1;
	}
}

