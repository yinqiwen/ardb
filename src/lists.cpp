/*
 * lists.cpp
 *
 *  Created on: 2013-4-1
 *      Author: wqy
 */
#include "rddb.hpp"

namespace rddb
{
	static bool DecodeListMetaData(ValueObject& v, ListMetaValue& meta)
	{
		if (v.type != RAW)
		{
			return false;
		}
		return BufferHelper::ReadVarUInt32(*(v.v.raw), meta.size)
				&& BufferHelper::ReadVarInt32(*(v.v.raw), meta.min_score)
				&& BufferHelper::ReadVarInt32(*(v.v.raw), meta.max_score);
	}
	static void EncodeListMetaData(ValueObject& v, ListMetaValue& meta)
	{
		v.type = RAW;
		if (v.v.raw == NULL)
		{
			v.v.raw = new Buffer(16);
		}
		BufferHelper::WriteVarUInt32(*(v.v.raw), meta.size);
		BufferHelper::WriteVarInt32(*(v.v.raw), meta.min_score);
		BufferHelper::WriteVarInt32(*(v.v.raw), meta.max_score);
	}

	int RDDB::ListPush(DBID db, const Slice& key, const Slice& value,
			bool athead, bool onlyexist)
	{
		KeyObject k(key, LIST_META);
		ValueObject v;
		ListMetaValue meta;
		int32_t score;
		if (0 == GetValue(db, k, v))
		{
			if (!DecodeListMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
			meta.size++;
			if (athead)
			{
				meta.min_score--;
				score = meta.min_score;
			} else
			{
				meta.max_score++;
				score = meta.max_score;
			}
		} else
		{
			if (onlyexist)
			{
				return ERR_NOT_EXIST;
			}
		}
		BatchWriteGuard guard(GetDB(db));
		ListKeyObject lk(key, score);
		ValueObject lv;
		fill_value(value, lv);
		if (0 == SetValue(db, lk, lv))
		{
			EncodeListMetaData(v, meta);
			return SetValue(db, k, v);
		}
		return -1;
	}
	int RDDB::RPush(DBID db, const Slice& key, const Slice& value)
	{
		return ListPush(db, key, value, false, false);
	}

	int RDDB::RPushx(DBID db, const Slice& key, const Slice& value)
	{
		return ListPush(db, key, value, false, true);
	}

	int RDDB::LPush(DBID db, const Slice& key, const Slice& value)
	{
		return ListPush(db, key, value, true, false);
	}

	int RDDB::LPushx(DBID db, const Slice& key, const Slice& value)
	{
		return ListPush(db, key, value, true, true);
	}

	int RDDB::ListPop(DBID db, const Slice& key, bool athead,
			ValueObject& value)
	{
		KeyObject k(key, LIST_META);
		ValueObject v;
		ListMetaValue meta;
		int32_t score;
		if (0 == GetValue(db, k, v))
		{
			if (!DecodeListMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
			meta.size--;
			if (athead)
			{
				score = meta.min_score;
				meta.min_score++;
			} else
			{
				score = meta.max_score;
				meta.max_score--;
			}
			BatchWriteGuard guard(GetDB(db));
			ListKeyObject lk(key, score);
			if (GetValue(db, lk, value) == 0)
			{
				DelValue(db, lk);
				return SetValue(db, k, v);
			}
			return -1;
		} else
		{
			return ERR_NOT_EXIST;
		}
	}

	int RDDB::LPop(DBID db, const Slice& key, ValueObject& value)
	{
		return ListPop(db, key, true, value);
	}
	int RDDB::RPop(DBID db, const Slice& key, ValueObject& v)
	{
		return ListPop(db, key, false, v);
	}

	int RDDB::LLen(DBID db, const Slice& key)
	{
		KeyObject k(key, LIST_META);
		ValueObject v;
		ListMetaValue meta;
		if (0 == GetValue(db, k, v))
		{
			if (!DecodeListMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
			return meta.size;
		} else
		{
			return ERR_NOT_EXIST;
		}
	}
}

