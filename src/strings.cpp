 /*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 * 
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 * 
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 * 
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS 
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
 *THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "ardb.hpp"

namespace ardb
{
	int Ardb::Append(const DBID& db, const Slice& key, const Slice& value)
	{
		KeyObject k(key, KV, db);
		ValueObject v;
		if (GetValue(k, &v) < 0)
		{
			v.type = RAW;
			v.v.raw = new Buffer(const_cast<char*>(value.data()), 0,
			        value.size());
		}
		else
		{
			value_convert_to_raw(v);
			v.v.raw->Write(value.data(), value.size());
		}

		uint32_t size = v.v.raw->ReadableBytes();
		int ret = SetValue(k, v);
		if (0 == ret)
		{
			return size;
		}
		return ret;
	}

	int Ardb::Incr(const DBID& db, const Slice& key, int64_t& value)
	{
		return Incrby(db, key, 1, value);
	}
	int Ardb::Incrby(const DBID& db, const Slice& key, int64_t increment,
	        int64_t& value)
	{
		KeyObject k(key, KV, db);
		ValueObject v;
		if (GetValue(k, &v) < 0)
		{
			v.type = INTEGER;
			v.v.int_v = 0;
		}
		value_convert_to_number(v);
		if (v.type == INTEGER)
		{
			v.v.int_v += increment;
			SetValue(k, v);
			value = v.v.int_v;
			return 0;
		}
		else
		{
			return -1;
		}
	}

	int Ardb::Decr(const DBID& db, const Slice& key, int64_t& value)
	{
		return Decrby(db, key, 1, value);
	}

	int Ardb::Decrby(const DBID& db, const Slice& key, int64_t decrement,
	        int64_t& value)
	{
		return Incrby(db, key, 0 - decrement, value);
	}

	int Ardb::IncrbyFloat(const DBID& db, const Slice& key, double increment,
	        double& value)
	{
		KeyObject k(key, KV, db);
		ValueObject v;
		if (GetValue(k, &v) < 0)
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
			SetValue(k, v);
			value = v.v.double_v;
			return 0;
		}
		else
		{
			return -1;
		}
	}

	int Ardb::GetRange(const DBID& db, const Slice& key, int start, int end,
	        std::string& v)
	{
		KeyObject k(key, KV, db);
		ValueObject vo;
		if (GetValue(k, &vo) < 0)
		{
			return ERR_NOT_EXIST;
		}
		if (vo.type != RAW)
		{
			value_convert_to_raw(vo);
		}
		start = RealPosition(vo.v.raw, start);
		end = RealPosition(vo.v.raw, end);
		if (start > end)
		{
			return ERR_OUTOFRANGE;
		}
		vo.v.raw->SetReadIndex(start);
		vo.v.raw->SetWriteIndex(end + 1);
		vo.ToString(v);
		return ARDB_OK;
	}

	int Ardb::SetRange(const DBID& db, const Slice& key, int start,
	        const Slice& value)
	{
		KeyObject k(key, KV, db);
		ValueObject v;
		if (GetValue(k, &v) < 0)
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
		int len = v.v.raw->ReadableBytes();
		return SetValue(k, v) == 0 ? len : 0;
	}

	int Ardb::GetSet(const DBID& db, const Slice& key, const Slice& value,
	        std::string& v)
	{
		if (Get(db, key, &v) < 0)
		{
			Set(db, key, value);
			return ERR_NOT_EXIST;
		}
		return Set(db, key, value);
	}

}

