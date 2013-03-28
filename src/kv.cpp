/*
 * kv.cpp
 *
 *  Created on: 2013-3-28
 *      Author: wqy
 */
#include "rddb.hpp"

namespace rddb
{
	void RDDB::EncodeKVKey(Buffer& buf, const void* key, int keysize)
	{
		BufferHelper::WriteFixUInt8(buf, KV);
		BufferHelper::WriteVarUInt32(buf, keysize);
		buf.Write(key, keysize);
	}
	void RDDB::EncodeKVValue(Buffer& buf, const void* value, int valuesize)
	{
		BufferHelper::WriteVarUInt32(buf, valuesize);
		buf.Write(value, valuesize);
	}

	int RDDB::Set(DBID db, const void* key, int keysize, const void* value,
			int valuesize)
	{
		Buffer tmp(keysize + 16);
		EncodeKVKey(tmp, key, keysize);
		return m_engine->Put(db, tmp.GetRawBuffer(), tmp.ReadableBytes(), value,
				valuesize);
	}

	int RDDB::Get(DBID db, const void* key, int keysize, std::string* value)
	{
		Buffer tmp(keysize + 16);
		EncodeKVKey(tmp, key, keysize);
		return m_engine->Get(db, tmp.GetRawBuffer(), tmp.ReadableBytes(), value);
	}

	int RDDB::Del(DBID db, const void* key, int keysize)
	{
		Buffer tmp(keysize + 16);
		EncodeKVKey(tmp, key, keysize);
		return m_engine->Del(db, tmp.GetRawBuffer(), tmp.ReadableBytes());
	}

	bool RDDB::Exists(DBID db, const void* key, int keysize)
	{
		std::string value;
		return Get(db, key, keysize, &value) == 0;
	}

	int RDDB::Expire(DBID db, const void* key, int keysize, uint32_t secs)
	{
		Buffer tmp(keysize + 16);
		EncodeKVKey(tmp, key, keysize);
		uint64_t now = get_current_epoch_nanos();
		return -1;
	}
}

