/*
 * kv.cpp
 *
 *  Created on: 2013-3-28
 *      Author: wqy
 */
#include "ardb.hpp"
#include <bitset>
#include <fnmatch.h>

namespace ardb
{

	int Ardb::GetValue(const DBID& db, const KeyObject& key, ValueObject* v,
			uint64* expire)
	{
//		if(NULL != v)
//		{
//			v->type = INTEGER;
//			v->v.int_v = 123;
//		}
//		return ARDB_OK;
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
			Buffer readbuf(const_cast<char*>(value.data()), 0, value.size());
			if (decode_value(readbuf, *v))
			{
				uint64 tmp = 0;
				BufferHelper::ReadVarUInt64(readbuf, tmp);
				if (NULL != expire)
				{
					*expire = tmp;
				}
				if (tmp > 0 && get_current_epoch_micros() >= tmp)
				{
					GetDB(db)->Del(k);
					return ERR_NOT_EXIST;
				} else
				{
					return ARDB_OK;
				}
			}
		}
		return ERR_NOT_EXIST;
	}

	Iterator* Ardb::FindValue(const DBID& db, KeyObject& key)
	{
		Buffer keybuf(key.key.size() + 16);
		encode_key(keybuf, key);
		Slice k(keybuf.GetRawReadBuffer(), keybuf.ReadableBytes());
		Iterator* iter = GetDB(db)->Find(k);
		return iter;
	}

	int Ardb::SetValue(const DBID& db, KeyObject& key, ValueObject& value,
			uint64 expire)
	{
		static Buffer keybuf;
		keybuf.Clear();
		keybuf.EnsureWritableBytes(key.key.size() + 16);
		encode_key(keybuf, key);
		static Buffer valuebuf;
		valuebuf.Clear();
		valuebuf.EnsureWritableBytes(64);
		encode_value(valuebuf, value);
		if (expire > 0)
		{
			BufferHelper::WriteVarUInt64(valuebuf, expire);
		}
		Slice k(keybuf.GetRawReadBuffer(), keybuf.ReadableBytes());
		Slice v(valuebuf.GetRawReadBuffer(), valuebuf.ReadableBytes());
		return GetDB(db)->Put(k, v);
	}

	int Ardb::DelValue(const DBID& db, KeyObject& key)
	{
		Buffer keybuf(key.key.size() + 16);
		encode_key(keybuf, key);
		Slice k(keybuf.GetRawReadBuffer(), keybuf.ReadableBytes());
		return GetDB(db)->Del(k);
	}

	int Ardb::MSet(const DBID& db, SliceArray& keys, SliceArray& values)
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
			smart_fill_value(*vit, valueobject);
			SetValue(db, keyobject, valueobject);
			kit++;
			vit++;
		}
		return 0;
	}

	int Ardb::MSetNX(const DBID& db, SliceArray& keys, SliceArray& values)
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
				smart_fill_value(*vit, valueobject);
				SetValue(db, keyobject, valueobject);
			} else
			{
				guard.MarkFailed();
				return -1;
			}
			kit++;
			vit++;
		}
		return keys.size();
	}

	int Ardb::Set(const DBID& db, const Slice& key, const Slice& value, int ex,
			int px, int nxx)
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

	int Ardb::Set(const DBID& db, const Slice& key, const Slice& value)
	{
		return SetEx(db, key, value, 0);
	}

	int Ardb::SetNX(const DBID& db, const Slice& key, const Slice& value)
	{
		if (!Exists(db, key))
		{
			KeyObject keyobject(key);
			ValueObject valueobject;
			smart_fill_value(value, valueobject);
			int ret = SetValue(db, keyobject, valueobject);
			return ret == 0 ? 1 : ret;
		}
		return 0;
	}

	int Ardb::SetEx(const DBID& db, const Slice& key, const Slice& value,
			uint32_t secs)
	{
		return PSetEx(db, key, value, secs * 1000);
	}
	int Ardb::PSetEx(const DBID& db, const Slice& key, const Slice& value,
			uint32_t ms)
	{
		KeyObject keyobject(key);
		ValueObject valueobject;
		smart_fill_value(value, valueobject);
		uint64_t expire = 0;
		if (ms > 0)
		{
			uint64_t now = get_current_epoch_micros();
			expire = now + (uint64_t) ms * 1000L;
		}
		return SetValue(db, keyobject, valueobject, expire);
		//return 0;
	}

	int Ardb::GetValue(const DBID& db, const Slice& key, ValueObject* value)
	{
		KeyObject keyobject(key);
		int ret = GetValue(db, keyobject, value, NULL);
		return ret;
	}

	int Ardb::Get(const DBID& db, const Slice& key, std::string* value)
	{
		ValueObject v;
		int ret = GetValue(db, key, &v);
		if (0 == ret)
		{
			if (NULL != value)
			{
				value->assign(v.ToString());
				//value->assign("121321");
			}
		}
		return ret;
	}

	int Ardb::MGet(const DBID& db, SliceArray& keys, ValueArray& value)
	{
		SliceArray::iterator it = keys.begin();
		int i = 0;
		while (it != keys.end())
		{
			ValueObject v;
			value.push_back(v);
			GetValue(db, *it, &value[i]);
			it++;
			i++;
		}
		return 0;
	}

	int Ardb::Del(const DBID& db, const Slice& key)
	{
		KeyObject k(key);
		DelValue(db, k);
//		HClear(db, key);
//		LClear(db, key);
//		ZClear(db, key);
//		SClear(db, key);
//		TClear(db, key);
		return 0;
	}

	int Ardb::Del(const DBID& db, const SliceArray& keys)
	{
		BatchWriteGuard guard(GetDB(db));
		SliceArray::const_iterator it = keys.begin();
		while (it != keys.end())
		{
			Del(db, *it);
			it++;
		}
		return 0;
	}

	bool Ardb::Exists(const DBID& db, const Slice& key)
	{
		return Get(db, key, NULL) == 0;
	}

	int Ardb::SetExpiration(const DBID& db, const Slice& key, uint64_t expire)
	{
		KeyObject keyobject(key);
		ValueObject value;
		if (0 == GetValue(db, keyobject, &value))
		{
			return SetValue(db, keyobject, value, expire);
		}
		return ERR_NOT_EXIST;
	}

	int Ardb::Strlen(const DBID& db, const Slice& key)
	{
		std::string v;
		if (0 == Get(db, key, &v))
		{
			return v.size();
		}
		return 0;
	}
	int Ardb::Expire(const DBID& db, const Slice& key, uint32_t secs)
	{
		uint64_t now = get_current_epoch_micros();
		uint64_t expire = now + (uint64_t) secs * 1000000L;
		return SetExpiration(db, key, expire);
	}

	int Ardb::Expireat(const DBID& db, const Slice& key, uint32_t ts)
	{
		uint64_t expire = (uint64_t) ts * 1000000L;
		return SetExpiration(db, key, expire);
	}

	int Ardb::Persist(const DBID& db, const Slice& key)
	{
		return SetExpiration(db, key, 0);
	}

	int Ardb::Pexpire(const DBID& db, const Slice& key, uint32_t ms)
	{
		uint64_t now = get_current_epoch_micros();
		uint64_t expire = now + (uint64_t) ms * 1000;
		return SetExpiration(db, key, expire);
	}

	int Ardb::PTTL(const DBID& db, const Slice& key)
	{
		ValueObject v;
		KeyObject k(key);
		uint64 expire = 0;
		if (0 == GetValue(db, k, &v, &expire))
		{
			int ttl = 0;
			if (expire > 0)
			{
				uint64_t now = get_current_epoch_micros();
				uint64_t ttlsus = expire - now;
				ttl = ttlsus / 1000;
				if (ttlsus % 1000 >= 500)
				{
					ttl++;
				}
			}
			return ttl;
		}
		return ERR_NOT_EXIST;
	}

	int Ardb::TTL(const DBID& db, const Slice& key)
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

	int Ardb::Rename(const DBID& db, const Slice& key1, const Slice& key2)
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

	int Ardb::RenameNX(const DBID& db, const Slice& key1, const Slice& key2)
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

	int Ardb::Move(DBID srcdb, const Slice& key, DBID dstdb)
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

	int Ardb::Keys(const DBID& db, const std::string& pattern, StringSet& ret)
	{
		Slice empty;
		KeyObject start(empty);
		Iterator* iter = FindValue(db, start);
		while (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey);
			if (NULL != kk)
			{
				KeyType type = kk->type;
				std::string key(kk->key.data(), kk->key.size());
				if (fnmatch(pattern.c_str(), key.c_str(), 0) == 0)
				{
					ret.insert(key);
				}
				DELETE(kk);
			}
			iter->Next();
		}
		DELETE(iter);
		return 0;
	}
}

