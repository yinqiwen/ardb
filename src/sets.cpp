/*
 * sets.cpp
 *
 *  Created on: 2013-4-2
 *      Author: wqy
 */

#include "rddb.hpp"

namespace rddb
{
	static bool DecodeSetMetaData(ValueObject& v, SetMetaValue& meta)
	{
		if (v.type != RAW)
		{
			return false;
		}
		return BufferHelper::ReadVarUInt32(*(v.v.raw), meta.size);
	}
	static void EncodeSetMetaData(ValueObject& v, SetMetaValue& meta)
	{
		v.type = RAW;
		if (v.v.raw == NULL)
		{
			v.v.raw = new Buffer(16);
		}
		BufferHelper::WriteVarUInt32(*(v.v.raw), meta.size);
	}

	int RDDB::SAdd(DBID db, const Slice& key, const Slice& value)
	{
		KeyObject k(key, SET_META);
		ValueObject v;
		SetMetaValue meta;
		if (0 == GetValue(db, k, v))
		{
			if (!DecodeSetMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
		}
		SetKeyObject sk(key, value);
		ValueObject sv;
		if (0 != GetValue(db, sk, sv))
		{
			meta.size++;
			sv.type = EMPTY;
			BatchWriteGuard guard(GetDB(db));
			SetValue(db, sk, sv);
			EncodeSetMetaData(v, meta);
			return SetValue(db, k, v) == 0 ? 1 : -1;
		}
		return 0;
	}

	int RDDB::SCard(DBID db, const Slice& key)
	{
		KeyObject k(key, SET_META);
		ValueObject v;
		SetMetaValue meta;
		if (0 == GetValue(db, k, v))
		{
			if (!DecodeSetMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
			return meta.size;
		}
		return ERR_NOT_EXIST;
	}

	int RDDB::SIsMember(DBID db, const Slice& key, const Slice& value)
	{
		SetKeyObject sk(key, value);
		ValueObject sv;
		if (0 != GetValue(db, sk, sv))
		{
			return ERR_NOT_EXIST;
		}
		return 1;
	}

	int RDDB::SRem(DBID db, const Slice& key, const Slice& value)
	{
		KeyObject k(key, SET_META);
		ValueObject v;
		SetMetaValue meta;
		if (0 == GetValue(db, k, v))
		{
			if (!DecodeSetMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
		}
		SetKeyObject sk(key, value);
		ValueObject sv;
		if (0 != GetValue(db, sk, sv))
		{
			meta.size--;
			sv.type = EMPTY;
			BatchWriteGuard guard(GetDB(db));
			DelValue(db, sk);
			EncodeSetMetaData(v, meta);
			return SetValue(db, k, v) == 0 ? 1 : -1;
		}
		return 0;
	}
}

