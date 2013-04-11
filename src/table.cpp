/*
 * table.cpp
 *
 *  Created on: 2013-4-11
 *      Author: wqy
 */
#include "ardb.hpp"

namespace ardb
{
	struct TableMetaKeyObject: public KeyObject
	{
			SliceArray primary_keys;
			uint32_t size;
			bool hasIndex;
			TableMetaKeyObject(const Slice& k, const SliceArray& pks) :
					KeyObject(k, TABLE_META), size(0), hasIndex(false)
			{
			}
	};

	struct TableIndexKeyObject: public KeyObject
	{
			Slice tkeyName;
			ValueObject tkeyValue;
			ValueArray  tks;
			SliceArray primary_keys;
			uint32_t size;
			bool hasIndex;
			TableIndexKeyObject(const Slice& k, const SliceArray& pks) :
					KeyObject(k, TABLE_META), size(0), hasIndex(false)
			{
			}
	};

	int Ardb::TCreate(DBID db, const Slice& tableName, SliceArray& keys)
	{
		return 0;
	}

	int Ardb::TGet(DBID db, const Slice& tableName, const SliceArray& fields,
			KeyCondition& conds, StringArray& values)
	{
		return 0;
	}
	int Ardb::TSet(DBID db, const Slice& tableName, const SliceArray& fields,
			KeyCondition& conds, const StringArray& values)
	{
		return 0;
	}
	int Ardb::TDel(DBID db, const Slice& tableName, const SliceArray& fields,
			KeyCondition& conds, const StringArray& values)
	{
		return 0;
	}
	int Ardb::TClear(DBID db, const Slice& tableName)
	{
		return 0;
	}
	int Ardb::TCount(DBID db, const Slice& tableName)
	{
		//TableMetaKeyObject meta;
		//return meta.size;
		return 0;
	}
}

