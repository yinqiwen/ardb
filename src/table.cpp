/*
 * table.cpp
 *
 *  Created on: 2013-4-11
 *      Author: wqy
 */
#include "ardb.hpp"

namespace ardb
{
	static bool DecodeTableMetaData(ValueObject& v, TableMetaValue& meta)
	{
		if (v.type != RAW)
		{
			return false;
		}
		uint32 len;
		if (!BufferHelper::ReadVarUInt32(*(v.v.raw), meta.size)
		        || !BufferHelper::ReadVarUInt32(*(v.v.raw), len))
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
		for (int i = 0; i < meta.keynames.size(); i++)
		{
			BufferHelper::WriteVarString(*(v.v.raw), meta.keynames[i]);
		}
	}

	typedef std::map<std::string, TableMetaValue> TableMetaValueCache;
	static TableMetaValueCache kMetaCache;
	int Ardb::GetTableMetaValue(const DBID& db, const Slice& tableName,
	        TableMetaValue& meta)
	{
		TableMetaValueCache::iterator found = kMetaCache.find(
		        std::string(tableName.data(), tableName.size()));
		if (found != kMetaCache.end())
		{
			meta = found->second;
			return 0;
		}
		KeyObject k(tableName, TABLE_META);
		ValueObject v;
		if (0 == GetValue(db, k, &v))
		{
			if (!DecodeTableMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
			kMetaCache[std::string(tableName.data(), tableName.size())] = meta;
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
		if (0 == GetTableMetaValue(db, tableName, meta))
		{
			return ERR_KEY_EXIST;
		}
		SliceArray::iterator it = keynames.begin();
		while (it != keynames.end())
		{
			std::string name(it->data(), it->size());
			meta.keynames.push_back(name);
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
				TUnionIndexWalk(Condition& c, TableKeyIndexSet& i) :
						icond(c), tres(i)
				{
				}
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					TableIndexKeyObject* sek = (TableIndexKeyObject*) k;
					if (sek->kname.compare(icond.keyname) != 0)
					{
						return -1;
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
					tres.insert(sek->index);
					return 0;
				}
		} walk(cond, results);
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
						return -1;
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
	        const SliceArray& cols, Conditions& conds, StringArray& values)
	{
		TableKeyIndexSet set1, set2;
		TableKeyIndexSet* index = &set1;
		TableKeyIndexSet* tmp = &set2;
		TGetIndexs(db, tableName, conds, index, tmp);
		DEBUG_LOG("###Found %d index", index->size());
		SliceArray::const_iterator it = cols.begin();
		while (it != cols.end())
		{
			TableKeyIndexSet::iterator iit = index->begin();
			while (iit != index->end())
			{
				TableColKeyObject k(tableName, *it);
				k.keyvals = iit->keyvals;
				ValueObject v;
				GetValue(db, k, &v);
				values.push_back(v.ToString());
				iit++;
			}
			it++;
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
	int Ardb::TReplace(const DBID& db, const Slice& tableName,
	        const SliceMap& keyvals, const SliceMap& colvals)
	{
		TableMetaValue meta;
		GetTableMetaValue(db, tableName, meta);
		if (meta.keynames.empty())
		{
			ERROR_LOG("Table %s is not created.", tableName.data());
			return -1;
		}
		if (meta.keynames.size() != keyvals.size())
		{
			ERROR_LOG(
			        "Table %s has different %d keys defined.", meta.keynames.size());
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
				ERROR_LOG(
				        "No key %s found in input.", meta.keynames[i].c_str());
				return -1;
			}
			smart_fill_value(found->second, v);
			index.keyvals.push_back(v);
		}
		BatchWriteGuard guard(GetDB(db));
		SliceMap::const_iterator kit = keyvals.begin();
		while (kit != keyvals.end())
		{
			TableIndexKeyObject tik(tableName, kit->first, kit->second);
			tik.index = index;
			ValueObject empty;
			SetValue(db, tik, empty);
			kit++;
		}
		//write value
		bool hasrecord = false;
		SliceMap::const_iterator it = colvals.begin();
		while (it != colvals.end())
		{
			TableColKeyObject k(tableName, it->first);
			k.keyvals = index.keyvals;
			if (!hasrecord && 0 == GetValue(db, k, NULL))
			{
				hasrecord = true;
			}
			ValueObject v;
			smart_fill_value(it->second, v);
			SetValue(db, k, v);
			it++;
		}
		if (hasrecord)
		{
			meta.size++;
			SetTableMetaValue(db, tableName, meta);
		}
		return 0;
	}

	int Ardb::TDelCol(const DBID& db, const Slice& tableName, Conditions& conds,
	        const Slice& col)
	{
		TableMetaValue meta;
		GetTableMetaValue(db, tableName, meta);
		if (meta.size == 0)
		{
			return 0;
		}
		std::string colstr(col.data(), col.size());
		if (meta.valnames.erase(colstr) == 0)
		{
			return -1;
		}
		TableKeyIndexSet set1, set2;
		TableKeyIndexSet* index = &set1;
		TableKeyIndexSet* tmp = &set2;
		TGetIndexs(db, tableName, conds, index, tmp);

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
			TableColKeyObject k(tableName, col);
			k.keyvals = iit->keyvals;
			for (uint32 i = 0; i < meta.keynames.size(); i++)
			{
				TableIndexKeyObject tik(tableName, meta.keynames[i],
				        k.keyvals[i]);
				tik.index.keyvals = k.keyvals;
				DelValue(db, tik);
			}
			TDelWalk walk(this, db, k.keyvals);
			Walk(db, k, false, &walk);
			iit++;
		}
		SetTableMetaValue(db, tableName, meta);
		return index->size();
	}

	int Ardb::TDel(const DBID& db, const Slice& tableName, Conditions& conds)
	{
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
		StringSet::iterator colit = meta.valnames.begin();
		while (colit != meta.valnames.end())
		{
			TableKeyIndexSet::iterator iit = index->begin();
			while (iit != index->end())
			{
				TableColKeyObject k(tableName, *colit);
				k.keyvals = iit->keyvals;
				for (uint32 i = 0; i < meta.keynames.size(); i++)
				{
					TableIndexKeyObject tik(tableName, meta.keynames[i],
					        k.keyvals[i]);
					tik.index.keyvals = k.keyvals;
					DelValue(db, tik);
				}
				TDelWalk walk(this, db, k.keyvals);
				Walk(db, k, false, &walk);
				iit++;
			}
			colit++;
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
		//return meta.size;
		return meta.size;
	}
}

