/*
 * kv.cpp
 *
 *  Created on: 2013-3-28
 *      Author: wqy
 */
#include "rddb.hpp"

namespace rddb
{
	void RDDB::EncodeKey(Buffer& buf, KeyObject& key)
	{
		BufferHelper::WriteFixUInt8(buf, key.type);
		BufferHelper::WriteVarUInt32(buf, key.len);
		buf.Write(key.raw, key.len);
	}
	void RDDB::EncodeValue(Buffer& buf, ValueObject& value)
	{
		BufferHelper::WriteFixUInt8(buf, value.type);
		switch (value.type)
		{
			case INTEGER:
			{
				BufferHelper::WriteVarInt64(buf, value.v.int_v);
				break;
			}
			case DOUBLE:
			{
				BufferHelper::WriteVarDouble(buf, value.v.double_v);
				break;
			}
			default:
			{
				BufferHelper::WriteVarInt64(buf, value.len);
				buf.Write(value.v.raw, value.len);
				break;
			}
		}
		if (value.expire > 0)
		{
			BufferHelper::WriteVarInt64(buf, value.expire);
		}
	}

	int RDDB::GetValueExpiration(std::string* value, uint64_t& expiration)
	{
		expiration = 0;
		Buffer readbuf(const_cast<char*>(value->c_str()), 0, value->size());
		uint32_t valuesize;
		if (!BufferHelper::ReadVarUInt32(readbuf, valuesize))
		{
			return -1;
		}
		size_t value_start_pos = readbuf.GetReadIndex();
		readbuf.SkipBytes(valuesize);
		if (readbuf.Readable())
		{
			BufferHelper::ReadVarUInt64(readbuf, expiration);
		}
		*value = value->substr(value_start_pos, valuesize);
		return 0;
	}

	int RDDB::SetValue(DBID db, KeyObject& key, ValueObject& value)
	{
		Buffer tmp(key.len + 16);
		EncodeKVKey(tmp, key, keysize);

	}

	int RDDB::Set(DBID db, const void* key, int keysize, const void* value,
			int valuesize)
	{
		Buffer tmp(keysize + 16);
		EncodeKVKey(tmp, key, keysize);
		Buffer vbuf(valuesize + 16);
		EncodeKVValue(vbuf, value, valuesize);
		return m_engine->Put(db, tmp.GetRawBuffer(), tmp.ReadableBytes(),
				vbuf.GetRawBuffer(), vbuf.ReadableBytes());
	}

	int RDDB::Get(DBID db, const void* key, int keysize, std::string* value)
	{
		if (!value)
		{
			return -1;
		}
		Buffer tmp(keysize + 16);
		EncodeKVKey(tmp, key, keysize);
		int ret = m_engine->Get(db, tmp.GetRawBuffer(), tmp.ReadableBytes(),
				value);
		if (ret == 0)
		{
			uint64_t expiration;
			GetValueExpiration(value, expiration);
			if (expiration > 0)
			{
				uint64_t now = get_current_epoch_nanos();
				if (now >= expiration)
				{
					Del(db, key, keysize);
					return -1;
				}
			}
		}
		return ret;
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

	int RDDB::SetExpiration(DBID db, const void* key, int keysize,
			uint64_t expire)
	{
		std::string value;
		if (0 == Get(db, key, keysize, &value))
		{
			Buffer tmp(keysize + 16);
			EncodeKVKey(tmp, key, keysize);
			Buffer vbuf(value.size() + 16);
			EncodeKVValue(vbuf, value.c_str(), value.size());
			BufferHelper::WriteVarUInt64(vbuf, expire);
			return m_engine->Put(db, tmp.GetRawBuffer(), tmp.ReadableBytes(),
					vbuf.GetRawBuffer(), vbuf.ReadableBytes());
		}
		return -1;
	}
	int RDDB::Expire(DBID db, const void* key, int keysize, uint32_t secs)
	{
		uint64_t now = get_current_epoch_nanos();
		uint64_t expire = now + (uint64_t) secs * 1000000000L;
		return SetExpiration(db, key, keysize, expire);
	}

	int RDDB::Expireat(DBID db, const void* key, int keysize, uint32_t ts)
	{
		Buffer tmp(keysize + 16);
		EncodeKVKey(tmp, key, keysize);
		uint64_t expire = (uint64_t) ts * 1000000000L;
		return SetExpiration(db, key, keysize, expire);
	}

	int Move(DBID srcdb, const void* key, int keysize, DBID dstdb)
	{
		//std::string value;
		return -1;
	}
}

