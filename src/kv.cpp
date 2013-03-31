/*
 * kv.cpp
 *
 *  Created on: 2013-3-28
 *      Author: wqy
 */
#include "rddb.hpp"

namespace rddb
{
	void RDDB::EncodeKey(Buffer& buf, const KeyObject& key)
	{
		BufferHelper::WriteFixUInt8(buf, key.type);
		BufferHelper::WriteVarUInt32(buf, key.len);
		buf.Write(key.raw, key.len);
	}
	void RDDB::EncodeValue(Buffer& buf, const ValueObject& value)
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
				if (NULL != value.v.raw)
				{
					BufferHelper::WriteVarInt64(buf,
					        value.v.raw->ReadableBytes());
					buf.Write(value.v.raw, value.v.raw->ReadableBytes());
				}
				else
				{
					BufferHelper::WriteVarInt64(buf, 0);
				}
				break;
			}
		}
		BufferHelper::WriteVarInt64(buf, value.expire);
	}

	bool RDDB::DecodeValue(Buffer& buf, ValueObject& value)
	{
		if (!BufferHelper::ReadFixUInt8(buf, value.type))
		{
			return -1;
		}
		switch (value.type)
		{
			case INTEGER:
			{
				if (!BufferHelper::ReadVarInt64(buf, value.v.int_v))
				{
					return -1;
				}
				break;
			}
			case DOUBLE:
			{
				if (!BufferHelper::ReadVarDouble(buf, value.v.double_v))
				{
					return -1;
				}
				break;
			}
			default:
			{
				uint32_t len;
				if (!BufferHelper::ReadVarUInt32(buf, len)
				        || buf.ReadableBytes() < len)
				{
					return -1;
				}
				value.v.raw = new Buffer(len);
				buf.Read(value.v.raw, len);
				break;
			}
		}
		BufferHelper::ReadVarUInt64(buf, value.expire);
		return 0;
	}

	void RDDB::FillValueObject(const void* value, int valuesize,
	        ValueObject& valueobject)
	{
		valueobject.type = RAW;
		int64_t intv;
		double doublev;
		if (raw_toint64(value, valuesize, intv))
		{
			valueobject.type = INTEGER;
			valueobject.v.int_v = intv;
		}
		else if (raw_todouble(value, valuesize, doublev))
		{
			valueobject.type = DOUBLE;
			valueobject.v.double_v = doublev;
		}
		else
		{
			char* v = (char*) value;
			valueobject.v.raw = new Buffer(v, 0, valuesize);
		}
	}

	int RDDB::GetValue(DBID db, const KeyObject& key, ValueObject& v)
	{
		Buffer keybuf(key.len + 16);
		EncodeKey(keybuf, key);
		std::string value;
		int ret = m_engine->Get(db, keybuf.GetRawBuffer(),
		        keybuf.ReadableBytes(), &value);
		if (ret == 0)
		{
			Buffer readbuf(const_cast<char*>(value.c_str()), 0, value.size());
			if (0 == DecodeValue(readbuf, v))
			{
				if (v.expire > 0)
				{
					uint64_t now = get_current_epoch_nanos();
					if (now >= v.expire)
					{
						m_engine->Del(db, keybuf.GetRawBuffer(),
						        keybuf.ReadableBytes());
						return ERR_NOT_EXIST;
					}
				}
			}
		}
		return ERR_NOT_EXIST;
	}

	int RDDB::SetValue(DBID db, KeyObject& key, ValueObject& value)
	{
		Buffer keybuf(key.len + 16);
		EncodeKey(keybuf, key);
		Buffer valuebuf(64);
		EncodeValue(valuebuf, value);
		return m_engine->Put(db, keybuf.GetRawBuffer(), keybuf.ReadableBytes(),
		        valuebuf.GetRawBuffer(), valuebuf.ReadableBytes());

	}

	int RDDB::Set(DBID db, const void* key, int keysize, const void* value,
	        int valuesize)
	{
		return SetEx(db, key, keysize, value, valuesize, 0);
	}

	int RDDB::SetNX(DBID db, const void* key, int keysize, const void* value,
	        int valuesize)
	{
		if (!Exists(db, key, keysize))
		{
			KeyObject keyobject(KV, key, keysize);
			ValueObject valueobject;
			FillValueObject(value, valuesize, valueobject);
			return SetValue(db, keyobject, valueobject);
		}
		return 0;
	}

	int RDDB::SetEx(DBID db, const void* key, int keysize, const void* value,
	        int valuesize, uint32_t secs)
	{
		return PSetEx(db, key, keysize, value, valuesize, secs * 1000);
	}
	int RDDB::PSetEx(DBID db, const void* key, int keysize, const void* value,
	        int valuesize, uint32_t ms)
	{
		KeyObject keyobject(KV, key, keysize);
		ValueObject valueobject;
		FillValueObject(value, valuesize, valueobject);
		uint64_t now = get_current_epoch_nanos();
		uint64_t expire = now + (uint64_t) ms * 1000000L;
		valueobject.expire = expire;
		return SetValue(db, keyobject, valueobject);
	}

	int RDDB::Get(DBID db, const void* key, int keysize, ValueObject& value)
	{
		KeyObject keyobject(KV, key, keysize);
		return GetValue(db, keyobject, value);
	}

	int RDDB::Del(DBID db, const void* key, int keysize)
	{
		KeyObject keyobject(KV, key, keysize);
		Buffer keybuf(keysize + 16);
		EncodeKey(keybuf, keyobject);
		return m_engine->Del(db, keybuf.GetRawBuffer(), keybuf.ReadableBytes());
	}

	bool RDDB::Exists(DBID db, const void* key, int keysize)
	{
		ValueObject value;
		return Get(db, key, keysize, value) == 0;
	}

	int RDDB::SetExpiration(DBID db, const void* key, int keysize,
	        uint64_t expire)
	{
		KeyObject keyobject(KV, key, keysize);
		ValueObject value;
		if (0 == GetValue(db, keyobject, value))
		{
			value.expire = expire;
			return SetValue(db, keyobject, value);
		}
		return ERR_NOT_EXIST;
	}

	int RDDB::Strlen(DBID db, const void* key, int keysize)
	{
		ValueObject v;
		if (0 == Get(db, key, keysize, v))
		{
			ValueObject2RawBuffer(v);
			return v.v.raw->ReadableBytes();
		}
		return ERR_NOT_EXIST;
	}
	int RDDB::Expire(DBID db, const void* key, int keysize, uint32_t secs)
	{
		uint64_t now = get_current_epoch_nanos();
		uint64_t expire = now + (uint64_t) secs * 1000000000L;
		return SetExpiration(db, key, keysize, expire);
	}

	int RDDB::Expireat(DBID db, const void* key, int keysize, uint32_t ts)
	{
		uint64_t expire = (uint64_t) ts * 1000000000L;
		return SetExpiration(db, key, keysize, expire);
	}

	int RDDB::Persist(DBID db, const void* key, int keysize)
	{
		return SetExpiration(db, key, keysize, 0);
	}

	int RDDB::Pexpire(DBID db, const void* key, int keysize, uint32_t ms)
	{
		uint64_t now = get_current_epoch_nanos();
		uint64_t expire = now + (uint64_t) ms * 1000000L;
		return SetExpiration(db, key, keysize, expire);
	}

	int Move(DBID srcdb, const void* key, int keysize, DBID dstdb)
	{
		//std::string value;
		return -1;
	}
}

