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
					if (sek->key.compare(icond.keyname) != 0)
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
					if (sek->key.compare(icond.keyname) != 0)
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
						if (sek->index < *(tres.begin()))
						{
							return -1;
						}
					} else
					{
						if (*(tres.begin()) < sek->index)
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
			} else
			{
				if (conds[i - 1].logicop == LOGIC_OR)
				{
					TUnionRowKeys(db, tableName, cond, *current);
				} else  //"and"
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
		//TODO write index

		ValueArray kvals;
		SliceMap::const_iterator it = colvals.begin();
		while (it != colvals.end())
		{
			TableColKeyObject k(tableName, it->first);
			k.keyvals = kvals;
			if (0 != GetValue(db, k, NULL))
			{
				meta.size++;
				SetTableMetaValue(db, tableName, meta);
			}
			ValueObject v;
			smart_fill_value(it->second, v);
			SetValue(db, k, v);
			it++;
		}
		return 0;
	}
	int Ardb::TDel(const DBID& db, const Slice& tableName,
			const SliceArray& fields, Conditions& conds)
	{
		TableKeyIndexSet set1, set2;
		TableKeyIndexSet* index = &set1;
		TableKeyIndexSet* tmp = &set2;
		TGetIndexs(db, tableName, conds, index, tmp);

		BatchWriteGuard guard(GetDB(db));
		TableKeyIndexSet::iterator iit = index->begin();
		while (iit != index->end())
		{
			Slice empty;
			TableColKeyObject k(tableName, empty);
			k.keyvals = iit->keyvals;


			iit++;
		}
		return 0;
	}
	int Ardb::TClear(const DBID& db, const Slice& tableName)
	{
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

