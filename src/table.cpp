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

	int Ardb::TGetRowKeys(const DBID& db, const Slice& tableName,
			Condition& cond, TableMetaValue& meta, TableKeyIndexSet*& results,
			TableKeyIndexSet*& current, Condition* lastcond)
	{
		TableIndexKeyObject index(tableName, cond.keyname, cond.keyvalue);
		bool interop = false;
		if (NULL != lastcond && lastcond->logicop == "and" && current != results)
		{
			interop = true;
		}
		bool reverse = (cond.cmp == "<" || cond.cmp == "<=");
		struct TIndexWalk: public WalkHandler
		{
				Condition& icond;
				ValueObject condStart;
				ValueObject consEnd;
				TableKeyIndexSet*& t_res;
				TableKeyIndexSet*& t_cmp;
				int OnKeyValue(KeyObject* k, ValueObject* v)
				{
					TableIndexKeyObject* sek = (TableIndexKeyObject*) k;
					if (sek->kname.compare(icond.keyname) != 0)
					{
						return -1;
					}

					if (interop)
					{

					}
					if (icond.cmp == "==")
					{
						if (sek->keyvalue.Compare(condVal) == 0)
						{
							ks.push_back(*sek);
						}
						return -1;
					} else if (icond.cmp == ">" || icond.cmp == "<")
					{
						if (sek->keyvalue.Compare(condVal) == 0)
						{
							return 0;
						}
					}
					//t_res.push_back(sek->keyvalue);
					return 0;
				}
				TIndexWalk(TableKeyIndexSet*& ks, TableKeyIndexSet*& cmp, Condition& c)
						: icond(c), t_res(ks), t_cmp(cmp)
				{
					smart_fill_value(icond.keyvalue, condVal);
				}
		} walk(results, current, cond);
		Walk(db, index, reverse, &walk);
		return 0;
	}

	int Ardb::TGetCol(const DBID& db, const Slice& tableName, const Slice& col,
			Conditions& conds, TableMetaValue& meta, StringArray& values)
	{
		TableKeyIndexSet kss;
		for (int i = 0; i < conds.size(); i++)
		{
			Condition& cond = conds[i];
			Condition* lastcond = NULL;
			if (i > 0)
			{
				lastcond = conds[i - 1];
			}
			TGetRowKeys(db, tableName, cond, meta, kss, lastcond);
		}

		TableRowKeyArray::iterator vit = kss[0].begin();
		while (vit != kss[kss.size() - 1].end())
		{
			TableIndexKeyObject& row = *vit;
			TableColKeyObject k(tableName, col);
			k.keyvals = row.keys;
			ValueObject v;
			GetValue(db, k, &v);
			values.push_back(v.ToString());
			vit++;
		}
		return 0;
	}

	int Ardb::TGet(const DBID& db, const Slice& tableName,
			const SliceArray& cols, Conditions& conds, StringArray& values)
	{

		return 0;
	}
	int Ardb::TSet(const DBID& db, const Slice& tableName,
			const SliceArray& fields, KeyCondition& conds,
			const StringArray& values)
	{
		return 0;
	}
	int Ardb::TDel(const DBID& db, const Slice& tableName,
			const SliceArray& fields, KeyCondition& conds,
			const StringArray& values)
	{
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

