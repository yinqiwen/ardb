/*
 * sorted_sets.cpp
 *
 *  Created on: 2013-4-2
 *      Author: wqy
 */

#include "rddb.hpp"
#include <float.h>

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

	int RDDB::ZAdd(DBID db, const Slice& key, double score, const Slice& value)
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

		ZSetScoreKeyObject zk(key, value);
		ValueObject zv;
		if (0 != GetValue(db, zk, zv))
		{
			meta.size++;
			zv.type = DOUBLE;
			zv.v.double_v = score;
			BatchWriteGuard guard(GetDB(db));
			SetValue(db, zk, zv);
			ZSetKeyObject zsk(key, value, score);
			ValueObject zsv;
			zsv.type = EMPTY;
			SetValue(db, zsk, zsv);
			EncodeZSetMetaData(v, meta);
			return SetValue(db, k, v) == 0 ? 1 : -1;
		} else
		{
			if (zv.v.double_v != score)
			{
				BatchWriteGuard guard(GetDB(db));
				ZSetKeyObject zsk(key, value, zv.v.double_v);
				DelValue(db, zsk);
				zsk.score = score;
				ValueObject zsv;
				zsv.type = EMPTY;
				SetValue(db, zsk, zsv);
				zv.type = DOUBLE;
				zv.v.double_v = score;
				return SetValue(db, zk, zv);
			}
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

	int RDDB::ZScore(DBID db, const Slice& key, const Slice& value,
			double& score)
	{
		ZSetScoreKeyObject zk(key, value);
		ValueObject zv;
		if (0 != GetValue(db, zk, zv))
		{
			return ERR_NOT_EXIST;
		}
		score = zv.v.double_v;
		return 0;
	}

	int RDDB::ZIncrby(DBID db, const Slice& key, double increment,
			const Slice& value, double& score)
	{
		ZSetScoreKeyObject zk(key, value);
		ValueObject zv;
		if (0 == GetValue(db, zk, zv))
		{
			BatchWriteGuard guard(GetDB(db));
			SetValue(db, zk, zv);
			ZSetKeyObject zsk(key, value, zv.v.double_v);
			DelValue(db, zsk);
			zsk.score += increment;
			ValueObject zsv;
			zsv.type = EMPTY;
			SetValue(db, zsk, zsv);
			score = zsk.score;
			return 0;
		}
		return ERR_NOT_EXIST;
	}

	int RDDB::ZRem(DBID db, const Slice& key, const Slice& value)
	{
		ZSetScoreKeyObject zk(key, value);
		ValueObject zv;
		if (0 == GetValue(db, zk, zv))
		{
			BatchWriteGuard guard(GetDB(db));
			DelValue(db, zk);
			ZSetKeyObject zsk(key, value, zv.v.double_v);
			DelValue(db, zsk);
			KeyObject k(key, ZSET_META);
			ValueObject v;
			ZSetMetaValue meta;
			if (0 == GetValue(db, k, v))
			{
				if (!DecodeZSetMetaData(v, meta))
				{
					return ERR_INVALID_TYPE;
				}
				if (meta.size > 1)
				{
					meta.size--;
					SetValue(db, k, v);
				}
			}
			return 0;
		}
		return ERR_NOT_EXIST;
	}

	int RDDB::ZCount(DBID db, const Slice& key, const std::string& min,
			const std::string& max)
	{
		bool containmin = true;
		bool containmax = true;
		double min_score, max_score;
		const char* min_str = min.c_str();
		const char* max_str = max.c_str();
		if (min.at(0) == '(')
		{
			containmin = false;
			min_str++;
		}
		if (max.at(0) == '(')
		{
			containmax = false;
			max_str++;
		}
		if (strcasecmp(min_str, "-inf") == 0)
		{
			min_score = -DBL_MAX;
		} else
		{
			if (!str_todouble(min_str, min_score))
			{
				return ERR_INVALID_STR;
			}
		}

		if (strcasecmp(max_str, "+inf") == 0)
		{
			max_score = DBL_MAX;
		} else
		{
			if (!str_todouble(max_str, max_score))
			{
				return ERR_INVALID_STR;
			}
		}

		int count = 0;
		Slice empty;
		ZSetKeyObject start(key, empty, min_score);
		Iterator* iter = FindValue(db, start);
		if (NULL != iter)
		{
			while (iter->Valid())
			{
				Slice k = iter->Key();
				KeyObject* ko = decode_key(k);
				if (NULL == ko || ko->type != ZSET_ELEMENT)
				{
					break;
				}
				ZSetKeyObject* zko = (ZSetKeyObject*) ko;
				if (zko->score > min_score && zko->score < max_score)
				{
					count++;
				}
				if (containmin && zko->score == min_score)
				{
					count++;
				}
				if (containmax && zko->score == max_score)
				{
					count++;
				}
				DELETE(ko);
				iter->Next();
			}
			DELETE(iter);
		}
		return count;
	}
}

