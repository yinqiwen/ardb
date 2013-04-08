/*
 * kv.cpp
 *
 *  Created on: 2013-3-28
 *      Author: wqy
 */
#include "rddb.hpp"
#include <bitset>

namespace rddb
{

	int RDDB::GetValue(DBID db, const KeyObject& key, ValueObject* v)
	{
		Buffer keybuf(key.key.size() + 16);
		encode_key(keybuf, key);
		Slice k(keybuf.GetRawReadBuffer(), keybuf.ReadableBytes());
		std::string value;
		int ret = GetDB(db)->Get(k, &value);
		if (ret == 0)
		{
			if (NULL == v)
			{
				return 0;
			}
			Buffer readbuf(const_cast<char*>(value.c_str()), 0, value.size());
			if (decode_value(readbuf, *v))
			{
				if (v->expire > 0 && get_current_epoch_nanos() >= v->expire)
				{
					GetDB(db)->Del(k);
					return ERR_NOT_EXIST;
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
		return iter;
	}

	int RDDB::SetValue(DBID db, KeyObject& key, ValueObject& value)
	{
		Buffer keybuf(key.key.size() + 16);
		encode_key(keybuf, key);
		Buffer valuebuf(64);
		encode_value(valuebuf, value);
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

	int RDDB::MSet(DBID db, SliceArray& keys, SliceArray& values)
	{
		if (keys.size() != values.size())
		{
			return ERR_INVALID_ARGS;
		}
		SliceArray::iterator kit = keys.begin();
		SliceArray::iterator vit = values.begin();
		BatchWriteGuard guard(GetDB(db));
		while (kit != keys.end())
		{
			KeyObject keyobject(*kit);
			ValueObject valueobject;
			fill_value(*vit, valueobject);
			SetValue(db, keyobject, valueobject);
			kit++;
			vit++;
		}
		return 0;
	}

	int RDDB::MSetNX(DBID db, SliceArray& keys, SliceArray& values)
	{
		if (keys.size() != values.size())
		{
			return ERR_INVALID_ARGS;
		}
		SliceArray::iterator kit = keys.begin();
		SliceArray::iterator vit = values.begin();
		BatchWriteGuard guard(GetDB(db));
		while (kit != keys.end())
		{
			KeyObject keyobject(*kit);
			ValueObject valueobject;
			if (0 != GetValue(db, keyobject, &valueobject))
			{
				fill_value(*vit, valueobject);
				SetValue(db, keyobject, valueobject);
			}
			kit++;
			vit++;
		}
		return 0;
	}

	int RDDB::Set(DBID db, const Slice& key, const Slice& value, int ex, int px,
			int nxx)
	{
		KeyObject k(key);
		if (-1 == nxx)
		{
			if (0 == GetValue(db, k, NULL))
			{
				return ERR_KEY_EXIST;
			}
		} else if (1 == nxx)
		{
			if (0 != GetValue(db, k, NULL))
			{
				return ERR_NOT_EXIST;
			}
		}

		if (px > 0)
		{
			PSetEx(db, key, value, px);
		}
		SetEx(db, key, value, ex);
		return 0;
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
			fill_value(value, valueobject);
			int ret = SetValue(db, keyobject, valueobject);
			return ret == 0 ? 1 : ret;
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
		fill_value(value, valueobject);
		uint64_t expire = 0;
		if (ms > 0)
		{
			uint64_t now = get_current_epoch_nanos();
			expire = now + (uint64_t) ms * 1000000L;
		}
		valueobject.expire = expire;
		return SetValue(db, keyobject, valueobject);
	}

	int RDDB::Get(DBID db, const Slice& key, std::string* value)
	{
		KeyObject keyobject(key);
		ValueObject v;
		int ret = GetValue(db, keyobject, &v);
		if (0 == ret)
		{
			if (NULL != value)
			{
				value->assign(v.ToString());
			}
		}
		return ret;
	}

	int RDDB::MGet(DBID db, SliceArray& keys, StringArray& value)
	{
		SliceArray::iterator it = keys.begin();
		while (it != keys.end())
		{
			std::string v;
			Get(db, *it, &v);
			value.push_back(v);
			it++;
		}
		return 0;
	}

	int RDDB::Del(DBID db, const Slice& key)
	{
		KeyObject k(key);
		return DelValue(db, k);
	}

	bool RDDB::Exists(DBID db, const Slice& key)
	{
		return Get(db, key, NULL) == 0;
	}

	int RDDB::SetExpiration(DBID db, const Slice& key, uint64_t expire)
	{
		KeyObject keyobject(key);
		ValueObject value;
		if (0 == GetValue(db, keyobject, &value))
		{
			value.expire = expire;
			return SetValue(db, keyobject, value);
		}
		return ERR_NOT_EXIST;
	}

	int RDDB::Strlen(DBID db, const Slice& key)
	{
		std::string v;
		if (0 == Get(db, key, &v))
		{
			return v.size();
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

	int RDDB::PTTL(DBID db, const Slice& key)
	{
		ValueObject v;
		KeyObject k(key);
		if (0 == GetValue(db, k, &v))
		{
			int ttl = 0;
			if (v.expire > 0)
			{
				uint64_t now = get_current_epoch_nanos();
				uint64_t ttlsns = v.expire - now;
				ttl = ttlsns / 1000000L;
				if (ttlsns % 1000000000L >= 500000L)
				{
					ttl++;
				}
			}
			return ttl;
		}
		return ERR_NOT_EXIST;
	}

	int RDDB::TTL(DBID db, const Slice& key)
	{
		int ttl = PTTL(db, key);
		if (ttl > 0)
		{
			ttl = ttl / 1000;
			if (ttl % 1000L >= 500)
			{
				ttl++;
			}
		}
		return ttl;
	}

	int RDDB::Rename(DBID db, const Slice& key1, const Slice& key2)
	{
		ValueObject v;
		KeyObject k1(key1);
		if (0 == GetValue(db, k1, &v))
		{
			BatchWriteGuard guard(GetDB(db));
			Del(db, key1);
			KeyObject k2(key2);
			return SetValue(db, k2, v);
		}
		return ERR_NOT_EXIST;
	}

	int RDDB::RenameNX(DBID db, const Slice& key1, const Slice& key2)
	{
		std::string v1;
		if (0 == Get(db, key1, &v1))
		{
			if (0 == Get(db, key2, NULL))
			{
				return 0;
			}
			BatchWriteGuard guard(GetDB(db));
			Del(db, key1);
			return Set(db, key2, v1) != 0 ? -1 : 1;
		}
		return ERR_NOT_EXIST;
	}

	int RDDB::Move(DBID srcdb, const Slice& key, DBID dstdb)
	{
		std::string v;
		if (0 == Get(srcdb, key, &v))
		{
			Del(srcdb, key);
			KeyObject k(key);
			return Set(dstdb, key, v);
		}
		return ERR_NOT_EXIST;
	}

}

