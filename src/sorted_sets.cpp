/*
 * sorted_sets.cpp
 *
 *  Created on: 2013-4-2
 *      Author: wqy
 */

#include "rddb.hpp"

namespace rddb
{
	static bool DecodeZSetMetaData(ValueObject& v, ZSetMetaValue& meta)
	{
		if (v.type != RAW)
		{
			return false;
		}
		return BufferHelper::ReadVarUInt32(*(v.v.raw), meta.size);
	}
	static void EncodeZSetMetaData(ValueObject& v, ZSetMetaValue& meta)
	{
		v.type = RAW;
		if (v.v.raw == NULL)
		{
			v.v.raw = new Buffer(16);
		}
		BufferHelper::WriteVarUInt32(*(v.v.raw), meta.size);
	}

	int RDDB::ZAdd(DBID db, const Slice& key, int64_t score, const Slice& value)
	{
		KeyObject k(key, ZSET_META);
		ValueObject v;
		ZSetMetaValue meta;
		if (0 == GetValue(db, k, v))
		{
			if (!DecodeZSetMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
		}
		ZSetKeyBarrierObject zk(key, value);
		ValueObject zv;
		if (0 != GetValue(db, zk, zv) )
		{
			meta.size++;
			zv.type = EMPTY;
			BatchWriteGuard guard(GetDB(db));
			SetValue(db, zk, zv);
			EncodeZSetMetaData(v, meta);
			return SetValue(db, k, v) == 0 ? 1 : -1;
		}
		return 0;
	}

	int RDDB::ZCard(DBID db, const Slice& key)
	{
		KeyObject k(key, ZSET_META);
		ValueObject v;
		ZSetMetaValue meta;
		if (0 == GetValue(db, k, v))
		{
			if (!DecodeZSetMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
			return meta.size;
		}
		return ERR_NOT_EXIST;
	}

	int RDDB::ZCount(DBID db, const Slice& key, int64_t min, int64_t max)
	{

	}

}

