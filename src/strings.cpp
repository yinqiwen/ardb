/*
 * strings.cpp
 *
 *  Created on: 2013-3-29
 *      Author: wqy
 */
#include "rddb.hpp"

namespace rddb
{
	int RDDB::Append(DBID db, const void* key, int keysize, const char* value,
	        int valuesize)
	{
		KeyObject k(KV, key, keysize);
		ValueObject v;
		if (GetValue(db, k, v) < 0)
		{
			v.type = RAW;
			v.v.raw = new Buffer(const_cast<char*>(value), 0, valuesize);
		}
		else
		{
			ValueObject2RawBuffer(v);
			v.v.raw->Write(value, valuesize);
		}

		uint32_t size = v.v.raw->ReadableBytes();
		int ret = SetValue(db, k, v);
		if (0 == ret)
		{
			return size;
		}
		return ret;
	}

	int RDDB::Incr(DBID db, const void* key, int keysize, int64_t& value)
	{
		return Incrby(db, key, keysize, 1, value);
	}
	int RDDB::Incrby(DBID db, const void* key, int keysize, int64_t increment,
	        int64_t& value)
	{
		KeyObject k(KV, key, keysize);
		ValueObject v;
		if (GetValue(db, k, v) < 0)
		{
			return -1;
		}
		if (v.type == INTEGER)
		{
			v.v.int_v += increment;
			SetValue(db, k, v);
			value = v.v.int_v;
			return 0;
		}
		else
		{
			return -1;
		}
	}

	int RDDB::Decr(DBID db, const void* key, int keysize, int64_t& value)
	{
		return Decrby(db, key, keysize, 1, value);
	}

	int RDDB::Decrby(DBID db, const void* key, int keysize, int64_t decrement,
	        int64_t& value)
	{
		return Incrby(db, key, keysize, 0 - decrement, value);
	}

	int RDDB::IncrbyFloat(DBID db, const void* key, int keysize,
	        double increment, double& value)
	{
		KeyObject k(KV, key, keysize);
		ValueObject v;
		if (GetValue(db, k, v) < 0)
		{
			return -1;
		}
		if (v.type == INTEGER)
		{
			v.type = DOUBLE;
			double dv = v.v.int_v;
			v.v.double_v = dv;
		}
		if (v.type == DOUBLE)
		{
			v.v.double_v += increment;
			SetValue(db, k, v);
			value = v.v.double_v;
			return 0;
		}
		else
		{
			return -1;
		}
	}

	int RDDB::GetRange(DBID db, const void* key, int keysize, int start,
	        int end, ValueObject& v)
	{
		KeyObject k(KV, key, keysize);
		if (GetValue(db, k, v) < 0)
		{
			return ERR_NOT_EXIST;
		}
		if (v.type != RAW)
		{
			ValueObject2RawBuffer(v);
		}
		start = RealPosition(v.v.raw, start);
		end = RealPosition(v.v.raw, end);
		if (start > end)
		{
			return ERR_OUTOFRANGE;
		}
		v.v.raw->SetReadIndex(start);
		v.v.raw->SetWriteIndex(end + 1);
		return RDDB_OK;
	}

	int RDDB::SetRange(DBID db, const void* key, int keysize, int start,
	        const void* value, int valuesize)
	{
		KeyObject k(KV, key, keysize);
		ValueObject v;
		if (GetValue(db, k, v) < 0)
		{
			return ERR_NOT_EXIST;
		}
		if (v.type != RAW)
		{
			ValueObject2RawBuffer(v);
		}
		start = RealPosition(v.v.raw, start);
		v.v.raw->SetWriteIndex(start);
		v.v.raw->Write(value, valuesize);
		return SetValue(db, k, v);
	}

	int RDDB::GetSet(DBID db, const void* key, int keysize, const char* value,
	        int valuesize, ValueObject& v)
	{
		KeyObject k(KV, key, keysize);
		if (GetValue(db, k, v) < 0)
		{
			return ERR_NOT_EXIST;
		}
		return Set(db, key, keysize, value, valuesize);
	}
}

