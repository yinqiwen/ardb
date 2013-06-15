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
#include <bitset>
#include <fnmatch.h>

namespace ardb
{
	int Ardb::GetValue(const KeyObject& key, ValueObject* v, uint64* expire)
	{
		Buffer keybuf(key.key.size() + 16);
		encode_key(keybuf, key);
		Slice k(keybuf.GetRawReadBuffer(), keybuf.ReadableBytes());
		std::string value;
		int ret = GetEngine()->Get(k, &value);
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
					GetEngine()->Del(k);
					return ERR_NOT_EXIST;
				} else
				{
					return ARDB_OK;
				}
			}
		}
		return ERR_NOT_EXIST;
	}

	Iterator* Ardb::FindValue(KeyObject& key, bool cache)
	{
		Buffer keybuf(key.key.size() + 16);
		encode_key(keybuf, key);
		Slice k(keybuf.GetRawReadBuffer(), keybuf.ReadableBytes());
		Iterator* iter = GetEngine()->Find(k, cache);
		return iter;
	}

	int Ardb::SetValue(KeyObject& key, ValueObject& value, uint64 expire)
	{
		if (NULL != m_key_watcher)
		{
			m_key_watcher->OnKeyUpdated(key.db, key.key);
		}
		Buffer keybuf;
		keybuf.EnsureWritableBytes(key.key.size() + 16);
		encode_key(keybuf, key);
		Buffer valuebuf;
		valuebuf.EnsureWritableBytes(64);
		encode_value(valuebuf, value);
		if (expire > 0)
		{
			BufferHelper::WriteVarUInt64(valuebuf, expire);
		}
		Slice k(keybuf.GetRawReadBuffer(), keybuf.ReadableBytes());
		Slice v(valuebuf.GetRawReadBuffer(), valuebuf.ReadableBytes());
		return RawSet(k, v);
	}

	int Ardb::DelValue(KeyObject& key)
	{
		if (NULL != m_key_watcher)
		{
			m_key_watcher->OnKeyUpdated(key.db, key.key);
		}
		Buffer keybuf(key.key.size() + 16);
		encode_key(keybuf, key);
		Slice k(keybuf.GetRawReadBuffer(), keybuf.ReadableBytes());
		return RawDel(k);
	}

	int Ardb::MSet(const DBID& db, SliceArray& keys, SliceArray& values)
	{
		if (keys.size() != values.size())
		{
			return ERR_INVALID_ARGS;
		}
		SliceArray::iterator kit = keys.begin();
		SliceArray::iterator vit = values.begin();
		BatchWriteGuard guard(GetEngine());
		while (kit != keys.end())
		{
			KeyObject keyobject(*kit, KV, db);
			ValueObject valueobject;
			smart_fill_value(*vit, valueobject);
			SetValue(keyobject, valueobject);
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
		BatchWriteGuard guard(GetEngine());
		while (kit != keys.end())
		{
			KeyObject keyobject(*kit, KV, db);
			ValueObject valueobject;
			if (0 != GetValue(keyobject, &valueobject))
			{
				smart_fill_value(*vit, valueobject);
				SetValue(keyobject, valueobject);
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
		KeyObject k(key, KV, db);
		if (-1 == nxx)
		{
			if (0 == GetValue(k, NULL))
			{
				return ERR_KEY_EXIST;
			}
		} else if (1 == nxx)
		{
			if (0 != GetValue(k, NULL))
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
			KeyObject keyobject(key, KV, db);
			ValueObject valueobject;
			smart_fill_value(value, valueobject);
			int ret = SetValue(keyobject, valueobject);
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
		KeyObject keyobject(key, KV, db);
		ValueObject valueobject;
		smart_fill_value(value, valueobject);
		uint64_t expire = 0;
		if (ms > 0)
		{
			uint64_t now = get_current_epoch_micros();
			expire = now + (uint64_t) ms * 1000L;
		}
		return SetValue(keyobject, valueobject, expire);
		//return 0;
	}

	int Ardb::GetValue(const DBID& db, const Slice& key, ValueObject* value)
	{
		KeyObject keyobject(key, KV, db);
		int ret = GetValue(keyobject, value, NULL);
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
				v.ToString(*value);
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
		int type = Type(db, key);
		switch (type)
		{
			case KV:
			{
				KeyObject k(key, KV, db);
				DelValue(k);
				break;
			}
			case HASH_FIELD:
			{
				HClear(db, key);
				break;
			}
			case ZSET_ELEMENT_SCORE:
			{
				ZClear(db, key);
				break;
			}
			case LIST_META:
			{
				LClear(db, key);
				break;
			}
			case TABLE_META:
			{
				TClear(db, key);
				break;
			}
			case SET_ELEMENT:
			{
				SClear(db, key);
				break;
			}
			case BITSET_META:
			{
				BitClear(db, key);
				break;
			}
			default:
			{
				return -1;
			}
		}
		return 0;
	}

	int Ardb::Del(const DBID& db, const SliceArray& keys)
	{
		BatchWriteGuard guard(GetEngine());
		int size = 0;
		SliceArray::const_iterator it = keys.begin();
		while (it != keys.end())
		{
			int ret = Del(db, *it);
			it++;
			if (ret >= 0)
			{
				size++;
			}
		}
		return size;
	}

	bool Ardb::Exists(const DBID& db, const Slice& key)
	{
		return Get(db, key, NULL) == 0;
	}

	int Ardb::SetExpiration(const DBID& db, const Slice& key, uint64_t expire)
	{
		KeyObject keyobject(key, KV, db);
		ValueObject value;
		if (0 == GetValue(keyobject, &value))
		{
			return SetValue(keyobject, value, expire);
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

	int Ardb::Pexpireat(const DBID& db, const Slice& key, uint64_t ms)
	{
		return SetExpiration(db, key, ms);
	}

	int64 Ardb::PTTL(const DBID& db, const Slice& key)
	{
		ValueObject v;
		KeyObject k(key, KV, db);
		uint64 expire = 0;
		if (0 == GetValue(k, &v, &expire))
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

	int64 Ardb::TTL(const DBID& db, const Slice& key)
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
		KeyObject k1(key1, KV, db);
		if (0 == GetValue(k1, &v))
		{
			BatchWriteGuard guard(GetEngine());
			Del(db, key1);
			KeyObject k2(key2, KV, db);
			return SetValue(k2, v);
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
			BatchWriteGuard guard(GetEngine());
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
			return Set(dstdb, key, v);
		}
		return ERR_NOT_EXIST;
	}

//	int Ardb::Keys(const DBID& db, const std::string& pattern, StringSet& ret)
//	{
//		Slice empty;
//		KeyObject start(empty, KV, db);
//		Iterator* iter = FindValue(start);
//		while (NULL != iter && iter->Valid())
//		{
//			Slice tmpkey = iter->Key();
//			KeyObject* kk = decode_key(tmpkey);
//			if (NULL != kk && kk->db == db)
//			{
//				std::string key(kk->key.data(), kk->key.size());
//				if (fnmatch(pattern.c_str(), key.c_str(), 0) == 0)
//				{
//					if(ret.insert(key).second)
//					{
//						INFO_LOG("Key type :%d  %d", kk->type, kk->db);
//					}
//				}
//				DELETE(kk);
//			}
//			iter->Next();
//		}
//		DELETE(iter);
//		return 0;
//	}

	int Ardb::Keys(const DBID& db, const std::string& pattern, StringSet& ret)
	{
		std::string lastkey;
		KeyType keytype = KV;
		while (keytype <= TABLE_META)
		{
			KeyObject start(lastkey, keytype, db);
			HashKeyObject hstart(lastkey, Slice(), db);
			Iterator* iter = FindValue(keytype == HASH_FIELD ? hstart : start);
			if (!iter->Valid())
			{
				DELETE(iter);
				break;
			}
			while (NULL != iter && iter->Valid())
			{
				Slice tmpkey = iter->Key();
				KeyObject* kk = decode_key(tmpkey, NULL);
				if (NULL != kk)
				{
					if (kk->db != db)
					{
						DELETE(kk);
						DELETE(iter);
						return 0;
					}
					if (kk->type != keytype)
					{
						lastkey = "";
						switch (keytype)
						{
							case KV:
							{
								keytype = SET_META;
								break;
							}
							case SET_META:
							{
								keytype = ZSET_META;
								break;
							}
							case ZSET_META:
							{
								keytype = HASH_FIELD;
								break;
							}
							case HASH_FIELD:
							{
								keytype = LIST_META;
								break;
							}
							case LIST_META:
							{
								keytype = TABLE_META;
								break;
							}
							case TABLE_META:
							default:
							{
								DELETE(kk);
								DELETE(iter);
								return 0;
							}
						}
						break;
					}
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
		}
		return 0;
	}
}

