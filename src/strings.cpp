/*
 * strings.cpp
 *
 *  Created on: 2013-3-29
 *      Author: wqy
 */
#include "rddb.hpp"

namespace rddb
{
	//static uint32_t kXIncrSeed = 0;
	int RDDB::Append(DBID db, const Slice& key, const Slice& value)
	{
		KeyObject k(key);
		ValueObject v;
		if (GetValue(db, k, v) < 0)
		{
			v.type = RAW;
			v.v.raw = new Buffer(const_cast<char*>(value.data()), 0,
					value.size());
		} else
		{
			value_convert_to_raw(v);
			v.v.raw->Write(value.data(), value.size());
		}

		uint32_t size = v.v.raw->ReadableBytes();
		int ret = SetValue(db, k, v);
		if (0 == ret)
		{
			return size;
		}
		return ret;
	}

	int RDDB::XIncrby(DBID db, const Slice& key, int64_t increment)
	{
		KeyObject k(key);
		return 0;
	}

	int RDDB::Incr(DBID db, const Slice& key, int64_t& value)
	{
		return Incrby(db, key, 1, value);
	}
	int RDDB::Incrby(DBID db, const Slice& key, int64_t increment,
			int64_t& value)
	{
		KeyObject k(key);
		ValueObject v;
		if (GetValue(db, k, v) < 0)
		{
			return -1;
		}
		value_convert_to_number(v);
		if (v.type == INTEGER)
		{
			v.v.int_v += increment;
			SetValue(db, k, v);
			value = v.v.int_v;
			return 0;
		} else
		{
			return -1;
		}
	}

	int RDDB::Decr(DBID db, const Slice& key, int64_t& value)
	{
		return Decrby(db, key, 1, value);
	}

	int RDDB::Decrby(DBID db, const Slice& key, int64_t decrement,
			int64_t& value)
	{
		return Incrby(db, key, 0 - decrement, value);
	}

	int RDDB::IncrbyFloat(DBID db, const Slice& key, double increment,
			double& value)
	{
		KeyObject k(key);
		ValueObject v;
		if (GetValue(db, k, v) < 0)
		{
			return -1;
		}
		value_convert_to_number(v);
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
		} else
		{
			return -1;
		}
	}

	int RDDB::GetRange(DBID db, const Slice& key, int start, int end,
			ValueObject& v)
	{
		KeyObject k(key);
		if (GetValue(db, k, v) < 0)
		{
			return ERR_NOT_EXIST;
		}
		if (v.type != RAW)
		{
			value_convert_to_raw(v);
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

	int RDDB::SetRange(DBID db, const Slice& key, int start, const Slice& value)
	{
		KeyObject k(key);
		ValueObject v;
		if (GetValue(db, k, v) < 0)
		{
			return ERR_NOT_EXIST;
		}
		if (v.type != RAW)
		{
			value_convert_to_raw(v);
		}
		start = RealPosition(v.v.raw, start);
		v.v.raw->SetWriteIndex(start);
		v.v.raw->Write(value.data(), value.size());
		return SetValue(db, k, v);
	}

	int RDDB::GetSet(DBID db, const Slice& key, const Slice& value,
			ValueObject& v)
	{
		KeyObject k(key);
		if (GetValue(db, k, v) < 0)
		{
			return ERR_NOT_EXIST;
		}
		return Set(db, key, value);
	}
}

