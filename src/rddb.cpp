/*
 * rddb.cpp
 *
 *  Created on: 2013-3-27
 *      Author: wqy
 */
#include "rddb.hpp"
#include <string.h>

namespace rddb
{

	Buffer* RDDB::ValueObject2RawBuffer(ValueObject& v)
	{
		if (v.type != RAW)
		{
			int64_t iv = v.v.int_v;
			double dv = v.v.double_v;
			v.type = RAW;
			v.v.raw = new Buffer(16);
			if (v.type == INTEGER)
			{
				v.v.raw->Printf("%lld", iv);
			}
			else if (v.type == DOUBLE)
			{
				v.v.raw->Printf("%f", dv);
			}
		}
		return v.v.raw;
	}

	size_t RDDB::RealPosition(Buffer* buf, int pos)
	{
		if (pos < 0)
		{
			pos = buf->ReadableBytes() + pos;
		}
		if (pos >= buf->ReadableBytes())
		{
			pos = buf->ReadableBytes() - 1;
		}
		if (pos < 0)
		{
			pos = 0;
		}
		return pos;
	}

	RDDB::RDDB(KeyValueEngine* engine) :
			m_engine(engine)
	{
	}

	int RDDB::HSet(DBID db, const void* key, int keysize, const void* field,
	        int fieldsize, const void* value, int valuesize)
	{
		Buffer tmp(keysize + fieldsize + 16);
		BufferHelper::WriteFixUInt8(tmp, HASH_FIELD);
		BufferHelper::WriteVarUInt32(tmp, keysize);
		tmp.Write(key, keysize);
		//padding '0'
		tmp.WriteByte('0');
		m_engine->Put(db, tmp.GetRawBuffer(), tmp.ReadableBytes(), "", 0);
		tmp.AdvanceWriteIndex(-1);
		tmp.WriteByte('1');
		BufferHelper::WriteVarUInt32(tmp, fieldsize);
		tmp.Write(field, fieldsize);
		return m_engine->Put(db, tmp.GetRawBuffer(), tmp.ReadableBytes(), value,
		        valuesize);
	}

	int RDDB::ZAdd(DBID db, const void* key, int keysize, int64_t score,
	        const void* value, int valuesize)
	{
		Buffer tmp(keysize + 16);
		BufferHelper::WriteFixUInt8(tmp, ZSET_ELEMENT);
		BufferHelper::WriteVarUInt32(tmp, keysize);
		tmp.Write(key, keysize);
		BufferHelper::WriteVarUInt64(tmp, score);
		tmp.Write(value, valuesize);
		std::string getvalue;
		if (m_engine->Get(db, tmp.GetRawBuffer(), tmp.ReadableBytes(),
		        &getvalue) == 0)
		{
			return 0;
		}
		return m_engine->Put(db, tmp.GetRawBuffer(), tmp.ReadableBytes(), "", 0);
	}

	int RDDB::SAdd(DBID db, const void* key, int keysize, const void* value,
	        int valuesize)
	{
		Buffer tmp(keysize + 16);
		BufferHelper::WriteFixUInt8(tmp, SET_ELEMENT);
		BufferHelper::WriteVarUInt32(tmp, keysize);
		tmp.Write(key, keysize);
		BufferHelper::WriteVarUInt32(tmp, valuesize);
		tmp.Write(value, valuesize);
		std::string getvalue;
		if (m_engine->Get(db, tmp.GetRawBuffer(), tmp.ReadableBytes(),
		        &getvalue) == 0)
		{
			return 0;
		}
		return m_engine->Put(db, tmp.GetRawBuffer(), tmp.ReadableBytes(), "", 0);
	}

	int RDDB::RPush(DBID db, const void* key, int keysize, const void* value,
	        int valuesize)
	{
		Buffer tmp(keysize + 16);
		BufferHelper::WriteFixUInt8(tmp, LIST_ELEMENT);
		BufferHelper::WriteVarUInt32(tmp, keysize);
		tmp.Write(key, keysize);
		return 0;
	}
}

