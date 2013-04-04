/*
 * kv.cpp
 *
 *  Created on: 2013-3-28
 *      Author: wqy
 */
#include "rddb.hpp"

namespace rddb
{
	void RDDB::EncodeValue(Buffer& buf, const ValueObject& value)
	{
		BufferHelper::WriteFixUInt8(buf, value.type);
		switch (value.type)
		{
			case EMPTY:
			{
				break;
			}
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
					BufferHelper::WriteVarUInt32(buf,
							value.v.raw->ReadableBytes());
					buf.Write(value.v.raw, value.v.raw->ReadableBytes());
				} else
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
			case EMPTY:
			{
				break;
			}
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
					printf("######%d  %d\n", len, buf.ReadableBytes());
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

//Need consider "0001" situation
	void RDDB::FillValueObject(const Slice& value, ValueObject& valueobject)
	{
		valueobject.type = RAW;
		int64_t intv;
		double doublev;
		if (raw_toint64(value.data(), value.size(), intv))
		{
			valueobject.type = INTEGER;
			valueobject.v.int_v = intv;
		} else if (raw_todouble(value.data(), value.size(), doublev))
		{
			valueobject.type = DOUBLE;
			valueobject.v.double_v = doublev;
		} else
		{
			char* v = const_cast<char*>(value.data());
			valueobject.v.raw = new Buffer(v, 0, value.size());
		}
	}

	int RDDB::GetValue(DBID db, const KeyObject& key, ValueObject& v)
	{
		Buffer keybuf(key.key.size() + 16);
		encode_key(keybuf, key);
		Slice k(keybuf.GetRawReadBuffer(), keybuf.ReadableBytes());
		std::string value;
		int ret = GetDB(db)->Get(k, &value);
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
						GetDB(db)->Del(k);
						return ERR_NOT_EXIST;
					}
				} else
				{
					return RDDB_OK;
				}
			}
		}
		return ERR_NOT_EXIST;
	}

	Iterator* RDDB::FindValue(DBID db, KeyObject& key)
	{
		Buffer keybuf(key.key.size() + 16);
		encode_key(keybuf, key);
		Slice k(keybuf.GetRawReadBuffer(), keybuf.ReadableBytes());
		std::string str;
		Iterator* iter = GetDB(db)->Find(k);
		if (NULL != iter)
		{
			if (!iter->Valid())
			{
				delete iter;
				return NULL;
			}
		}
		return iter;
	}

	int RDDB::SetValue(DBID db, KeyObject& key, ValueObject& value)
	{
		Buffer keybuf(key.key.size() + 16);
		encode_key(keybuf, key);
		Buffer valuebuf(64);
		EncodeValue(valuebuf, value);
		Slice k(keybuf.GetRawReadBuffer(), keybuf.ReadableBytes());
		Slice v(valuebuf.GetRawReadBuffer(), valuebuf.ReadableBytes());
		return GetDB(db)->Put(k, v);
	}

	int RDDB::DelValue(DBID db, KeyObject& key)
	{
		Buffer keybuf(key.key.size() + 16);
		encode_key(keybuf, key);
		Slice k(keybuf.GetRawReadBuffer(), keybuf.ReadableBytes());
		return GetDB(db)->Del(k);
	}

	int RDDB::Set(DBID db, const Slice& key, const Slice& value)
	{
		return SetEx(db, key, value, 0);
	}

	int RDDB::SetNX(DBID db, const Slice& key, const Slice& value)
	{
		if (!Exists(db, key))
		{
			KeyObject keyobject(key);
			ValueObject valueobject;
			FillValueObject(value, valueobject);
			return SetValue(db, keyobject, valueobject);
		}
		return 0;
	}

	int RDDB::SetEx(DBID db, const Slice& key, const Slice& value,
			uint32_t secs)
	{
		return PSetEx(db, key, value, secs * 1000);
	}
	int RDDB::PSetEx(DBID db, const Slice& key, const Slice& value, uint32_t ms)
	{
		KeyObject keyobject(key);
		ValueObject valueobject;
		FillValueObject(value, valueobject);
		uint64_t expire = 0;
		if (ms > 0)
		{
			uint64_t now = get_current_epoch_nanos();
			expire = now + (uint64_t) ms * 1000000L;
		}
		valueobject.expire = expire;
		return SetValue(db, keyobject, valueobject);
	}

	int RDDB::Get(DBID db, const Slice& key, ValueObject& value)
	{
		KeyObject keyobject(key);
		return GetValue(db, keyobject, value);
	}

	int RDDB::Del(DBID db, const Slice& key)
	{
		KeyObject k(key);
		return DelValue(db, k);
	}

	bool RDDB::Exists(DBID db, const Slice& key)
	{
		ValueObject value;
		return Get(db, key, value) == 0;
	}

	int RDDB::SetExpiration(DBID db, const Slice& key, uint64_t expire)
	{
		KeyObject keyobject(key);
		ValueObject value;
		if (0 == GetValue(db, keyobject, value))
		{
			value.expire = expire;
			return SetValue(db, keyobject, value);
		}
		return ERR_NOT_EXIST;
	}

	int RDDB::Strlen(DBID db, const Slice& key)
	{
		ValueObject v;
		if (0 == Get(db, key, v))
		{
			ValueObject2RawBuffer(v);
			return v.v.raw->ReadableBytes();
		}
		return ERR_NOT_EXIST;
	}
	int RDDB::Expire(DBID db, const Slice& key, uint32_t secs)
	{
		uint64_t now = get_current_epoch_nanos();
		uint64_t expire = now + (uint64_t) secs * 1000000000L;
		return SetExpiration(db, key, expire);
	}

	int RDDB::Expireat(DBID db, const Slice& key, uint32_t ts)
	{
		uint64_t expire = (uint64_t) ts * 1000000000L;
		return SetExpiration(db, key, expire);
	}

	int RDDB::Persist(DBID db, const Slice& key)
	{
		return SetExpiration(db, key, 0);
	}

	int RDDB::Pexpire(DBID db, const Slice& key, uint32_t ms)
	{
		uint64_t now = get_current_epoch_nanos();
		uint64_t expire = now + (uint64_t) ms * 1000000L;
		return SetExpiration(db, key, expire);
	}

	int RDDB::Move(DBID srcdb, const Slice& key, DBID dstdb)
	{
		ValueObject v;
		if (0 == Get(srcdb, key, v))
		{
			Del(srcdb, key);
			KeyObject k(key);
			return SetValue(dstdb, k, v);
		}
		return ERR_NOT_EXIST;
	}
}

