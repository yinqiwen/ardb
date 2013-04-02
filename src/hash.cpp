/*
 * hash.cpp
 *
 *  Created on: 2013-3-31
 *      Author: wqy
 */
#include "rddb.hpp"

namespace rddb
{
	int RDDB::SetHashValue(DBID db, const Slice& key, const Slice& field,
			ValueObject& value)
	{
		HashKeyObject k(key, field);
		return SetValue(db, k, value);
	}
	int RDDB::HSet(DBID db, const Slice& key, const Slice& field,
			const Slice& value)
	{
		ValueObject valueobject;
		FillValueObject(value, valueobject);
		return SetHashValue(db, key, field, valueobject);
	}

	int RDDB::HSetNX(DBID db, const Slice& key, const Slice& field,
			const Slice& value)
	{
		if (HExists(db, key, field))
		{
			return ERR_KEY_EXIST;
		}
		return HSet(db, key, field, value);
	}

	int RDDB::HDel(DBID db, const Slice& key, const Slice& field)
	{
		HashKeyObject k(key, field);
		return DelValue(db, k);
	}

	int RDDB::HGet(DBID db, const Slice& key, const Slice& field,
			ValueObject& value)
	{
		HashKeyObject k(key, field);
		if (0 == GetValue(db, k, value))
		{
			return 0;
		}
		return ERR_NOT_EXIST;
	}

	bool RDDB::HExists(DBID db, const Slice& key, const Slice& field)
	{
		ValueObject v;
		return HGet(db, key, field, v) == 0;
	}

	int RDDB::HIncrby(DBID db, const Slice& key, const Slice& field,
			int64_t increment, int64_t& value)
	{
		ValueObject v;
		v.type = INTEGER;
		HGet(db, key, field, v);
		if (v.type != INTEGER)
		{
			return ERR_INVALID_TYPE;
		}
		v.v.int_v += increment;
		value = v.v.int_v;
		return SetHashValue(db, key, field, v);
	}

	int RDDB::HIncrbyFloat(DBID db, const Slice& key, const Slice& field,
			double increment, double& value)
	{
		ValueObject v;
		v.type = DOUBLE;
		HGet(db, key, field, v);
		if (v.type != DOUBLE)
		{
			return ERR_INVALID_TYPE;
		}
		v.v.double_v += increment;
		value = v.v.int_v;
		return SetHashValue(db, key, field, v);
	}
}

