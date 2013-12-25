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
    int Ardb::GetValue(const KeyObject& key, ValueObject* v)
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
                return ARDB_OK;
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

    int Ardb::SetValue(KeyObject& key, ValueObject& value)
    {
        DBWatcher& watcher = m_watcher.GetValue();
        watcher.data_changed = true;
        if (watcher.on_key_update != NULL)
        {
            watcher.on_key_update(key.db, key.key, watcher.on_key_update_data);
        }
        Buffer keybuf;
        keybuf.EnsureWritableBytes(key.key.size() + 16);
        encode_key(keybuf, key);
        Buffer valuebuf;
        valuebuf.EnsureWritableBytes(64);
        encode_value(valuebuf, value);
        Slice k(keybuf.GetRawReadBuffer(), keybuf.ReadableBytes());
        Slice v(valuebuf.GetRawReadBuffer(), valuebuf.ReadableBytes());
        return RawSet(k, v);
    }

    int Ardb::DelValue(KeyObject& key)
    {
        DBWatcher& watcher = m_watcher.GetValue();
        if (watcher.on_key_update != NULL)
        {
            watcher.on_key_update(key.db, key.key, watcher.on_key_update_data);
        }
        watcher.data_changed = true;
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
            KeyObject keyobject(*kit, KEY_META, db);
            StringMetaValue meta;
            meta.value.SetValue(*vit);
            SetMeta(keyobject, meta);
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
            if (!Exists(db, *kit))
            {
                KeyObject keyobject(*kit, KEY_META, db);
                StringMetaValue meta;
                meta.value.SetValue(*vit);
                SetMeta(keyobject, meta);
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
            KeyObject keyobject(key, KEY_META, db);
            StringMetaValue meta;
            meta.value.SetValue(value);
            int ret = SetMeta(keyobject, meta);
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
        KeyObject keyobject(key, KEY_META, db);
        StringMetaValue meta;
        meta.value.SetValue(value);
        uint64_t expire = 0;
        if (ms > 0)
        {
            uint64_t now = get_current_epoch_micros();
            expire = now + (uint64_t) ms * 1000L;
            meta.header.expireat = expire;
        }
        return SetMeta(keyobject, meta);
    }

    int Ardb::GetValue(const DBID& db, const Slice& key, ValueObject* value)
    {
        KeyObject keyobject(key, KV, db);
        int ret = GetValue(keyobject, value);
        return ret;
    }

    int Ardb::Get(const DBID& db, const Slice& key, std::string& value)
    {
        StringMetaValue meta;
        int ret = GetMeta(db, key, meta, false);
        if (0 == ret)
        {
            meta.value.ToString(value);
        }
        return ret;
    }

    int Ardb::MGet(const DBID& db, SliceArray& keys, StringArray& values)
    {
        SliceArray::iterator it = keys.begin();
        while (it != keys.end())
        {
            std::string v;
            Get(db, *it, v);
            values.push_back(v);
            it++;
        }
        return 0;
    }

    int Ardb::Del(const DBID& db, const Slice& key)
    {
        CommonMetaValue* meta = DelMeta(db, key);
        if(NULL == meta)
        {
            return 0;
        }
        int type = Type(db, key);
        switch (meta->header.type)
        {
            case STRING_META:
            {
                //do nothing
                break;
            }
            case HASH_META:
            {
                HClear(db, key);
                break;
            }
            case ZSET_META:
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
            case SET_META:
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
        DELETE(meta);
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
        int type = Type(db, key);
        if (type == -1)
        {
            return false;
        }
        uint64 expire = 0;
        if (0 == GetExpiration(db, key, expire))
        {
            if (expire <= get_current_epoch_micros())
            {
                Del(db, key);
                return false;
            }
        }
        return true;
    }

    int Ardb::GetExpiration(const DBID& db, const Slice& key, uint64& expire)
    {
        CommonMetaValue meta;
        CommonMetaValue* p = &meta;
        if (0 == GetMeta(db, key, p, true))
        {
            expire = meta.header.expireat;
            return 0;
        }
        return -1;
    }

    int Ardb::SetExpiration(const DBID& db, const Slice& key, uint64 expire,
            bool check_exists)
    {
        if (check_exists && expire > 0 && !Exists(db, key))
        {
            return -1;
        }
        BatchWriteGuard guard(GetEngine());
        uint64 old_expire;
        if (0 == GetExpiration(db, key, old_expire))
        {
            if (old_expire == expire)
            {
                return 0;
            }
            ExpireKeyObject expire_key(key, old_expire, db);
            DelValue(expire_key);
        }
        if (expire > 0)
        {
            ExpireKeyObject keyobject(key, expire, db);
            ValueObject empty;
            return SetValue(keyobject, empty);
        }
        return 0;
    }

    void Ardb::CheckExpireKey(const DBID& db, uint64 maxexec,
            uint32 maxcheckitems)
<<<<<<< HEAD
    {
        uint32 checked = 0;
        uint64 start = get_current_epoch_micros();
        Slice empty;
        ExpireKeyObject keyobject(empty, 0, db);
        Iterator* iter = FindValue(keyobject, true);
        BatchWriteGuard guard(GetEngine());
        while (NULL != iter && iter->Valid())
        {
            uint64 now = get_current_epoch_micros();
            if (now - start >= maxexec * 1000)
            {
                break;
            }
            if (checked >= maxcheckitems)
            {
                break;
            }
            checked++;
            Slice tmpkey = iter->Key();
            KeyObject* kk = decode_key(tmpkey, NULL);
            if (NULL == kk || kk->type != KEY_EXPIRATION_ELEMENT)
            {
                DELETE(kk);
                break;
            } else
            {
                ExpireKeyObject* ek = (ExpireKeyObject*) kk;
                KeyObject mek(ek->key, KEY_EXPIRATION_MAPPING, ek->db);
                if (ek->expireat <= now)
                {
                    DelValue(mek);
                    DelValue(*ek);
                    Del(db, ek->key);
                    DELETE(kk);
                } else
                {
                    DELETE(kk);
                    break;
                }
            }
            iter->Next();
        }
        DELETE(iter);
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
        return SetExpiration(db, key, expire, true);
    }

    int Ardb::Expireat(const DBID& db, const Slice& key, uint32_t ts)
    {
        uint64_t expire = (uint64_t) ts * 1000000L;
        return SetExpiration(db, key, expire, true);
    }

    int Ardb::Persist(const DBID& db, const Slice& key)
    {
        return SetExpiration(db, key, 0, true);
    }

    int Ardb::Pexpire(const DBID& db, const Slice& key, uint32_t ms)
    {
        uint64_t now = get_current_epoch_micros();
        uint64_t expire = now + (uint64_t) ms * 1000;
        return SetExpiration(db, key, expire, true);
    }

    int Ardb::Pexpireat(const DBID& db, const Slice& key, uint64_t ms)
    {
        return SetExpiration(db, key, ms, true);
    }

    int64 Ardb::PTTL(const DBID& db, const Slice& key)
    {
        uint64 expire = 0;
        if (0 == GetExpiration(db, key, expire))
        {
            int ttl = 0;
            if (expire > 0)
            {
                uint64_t now = get_current_epoch_micros();
                if (expire < now)
                {
                    Del(db, key);
                    return ERR_NOT_EXIST;
                }
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

    int Ardb::KeysCount(const DBID& db, const std::string& pattern,
            int64& count)
    {
        count = 0;
        KeyType types[] = { KV, SET_META, ZSET_META, LIST_META, TABLE_META,
                BITSET_META };
        for (uint32 i = 0; i < arraysize(types); i++)
        {
            std::string startkey;
            KeyObject start(startkey, types[i], db);
            Iterator* iter = FindValue(start);
            while (NULL != iter && iter->Valid())
            {
                Slice tmpkey = iter->Key();
                KeyObject* kk = decode_key(tmpkey, NULL);
                if (kk->type != types[i])
                {
                    DELETE(kk);
                    break;
                }
                count++;
                DELETE(kk);
                iter->Next();
            }
            DELETE(iter);
        }

        // hash keys
        std::string startkey;
        while (true)
        {
            KeyObject start(startkey, HASH_FIELD, db);
            Iterator* iter = FindValue(start);
            std::string current(startkey.data(), startkey.size());
            if (NULL != iter && iter->Valid())
            {
                Slice tmpkey = iter->Key();
                KeyObject* kk = decode_key(tmpkey, NULL);
                if (kk->type != HASH_FIELD)
                {
                    DELETE(kk);
                    DELETE(iter);
                    return 0;
                }
                current.assign(kk->key.data(), kk->key.size());
                count++;
                DELETE(kk);
            }
            DELETE(iter);
            next_key(current, startkey);
        }
        return 0;
    }

    int Ardb::Keys(const DBID& db, const std::string& pattern,
            const KeyObject& from, uint32 limit, StringArray& ret,
            KeyType& last_keytype)
    {
        if (from.type != HASH_FIELD || from.type == KEY_END)
        {
            KeyType srctype = from.type;
            if (srctype == KEY_END)
            {
                srctype = KV;
            }

            KeyType types[] = { KV, SET_META, ZSET_META, LIST_META, TABLE_META,
                    BITSET_META };
            for (uint32 i = 0; i < arraysize(types); i++)
            {
                if (types[i] < srctype)
                {
                    continue;
                }
                std::string startkey;
                if (from.type == types[i])
                {
                    next_key(from.key, startkey);
                }
                KeyObject start(startkey, types[i], db);
                Iterator* iter = FindValue(start);
                while (NULL != iter && iter->Valid())
                {
                    Slice tmpkey = iter->Key();
                    KeyObject* kk = decode_key(tmpkey, NULL);
                    if (kk->type != types[i] || kk->db != db)
                    {
                        DELETE(kk);
                        break;
                    }
                    ret.push_back(std::string(kk->key.data(), kk->key.size()));
                    DELETE(kk);
                    if (ret.size() == limit)
                    {
                        last_keytype = types[i];
                        DELETE(iter);
                        return 0;
                    }
                    iter->Next();
                }
                DELETE(iter);
            }
        }

        // hash keys
        std::string startkey;
        if (from.type == HASH_FIELD)
        {
            next_key(from.key, startkey);
        }
        while (true)
        {
            KeyObject start(startkey, HASH_FIELD, db);
            Iterator* iter = FindValue(start);
            std::string current(startkey.data(), startkey.size());
            if (NULL != iter && iter->Valid())
            {
                Slice tmpkey = iter->Key();
                KeyObject* kk = decode_key(tmpkey, NULL);
                if (kk->type != HASH_FIELD || kk->db != db)
                {
                    DELETE(kk);
                    DELETE(iter);
                    return 0;
                }
                current.assign(kk->key.data(), kk->key.size());
                ret.push_back(current);
                DELETE(kk);
                if (ret.size() == limit)
                {
                    last_keytype = HASH_FIELD;
                    DELETE(iter);
                    return 0;
                }
            }
            DELETE(iter);
            next_key(current, startkey);
        }
        return 0;
    }
=======
	{
	    uint32 checked = 0;
	    uint64 start = get_current_epoch_micros();
		Slice empty;
		ExpireKeyObject keyobject(empty, 0, db);
		Iterator* iter = FindValue(keyobject, true);
		BatchWriteGuard guard(GetEngine());
		while (NULL != iter && iter->Valid())
		{
		    uint64 now = get_current_epoch_micros();
		    if(now - start >= maxexec*1000)
		    {
		        break;
		    }
		    if(checked >= maxcheckitems)
		    {
		        break;
		    }
		    checked++;
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey, NULL);
			if (NULL == kk || kk->type != KEY_EXPIRATION_ELEMENT)
			{
				DELETE(kk);
				break;
			} else
			{
				ExpireKeyObject* ek = (ExpireKeyObject*) kk;
				KeyObject mek(ek->key, KEY_EXPIRATION_MAPPING, ek->db);
				if (ek->expireat <= now)
				{
				    DelValue(mek);
				    DelValue(*ek);
					Del(db, ek->key);
					DELETE(kk);
				} else
				{
					DELETE(kk);
					break;
				}
			}
			iter->Next();
		}
		DELETE(iter);
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
		return SetExpiration(db, key, expire, true);
	}

	int Ardb::Expireat(const DBID& db, const Slice& key, uint32_t ts)
	{
		uint64_t expire = (uint64_t) ts * 1000000L;
		return SetExpiration(db, key, expire, true);
	}

	int Ardb::Persist(const DBID& db, const Slice& key)
	{
		return SetExpiration(db, key, 0, true);
	}

	int Ardb::Pexpire(const DBID& db, const Slice& key, uint32_t ms)
	{
		uint64_t now = get_current_epoch_micros();
		uint64_t expire = now + (uint64_t) ms * 1000;
		return SetExpiration(db, key, expire, true);
	}

	int Ardb::Pexpireat(const DBID& db, const Slice& key, uint64_t ms)
	{
		return SetExpiration(db, key, ms, true);
	}

	int64 Ardb::PTTL(const DBID& db, const Slice& key)
	{
		uint64 expire = 0;
		if (0 == GetExpiration(db, key, expire))
		{
			int ttl = 0;
			if (expire > 0)
			{
				uint64_t now = get_current_epoch_micros();
				if (expire < now)
				{
					Del(db, key);
					return ERR_NOT_EXIST;
				}
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

	int Ardb::KeysCount(const DBID& db, const std::string& pattern, int64& count)
	{
		count = 0;
		KeyType types[] =
		{ KV, SET_META, ZSET_META, LIST_META, TABLE_META, BITSET_META };
		for (uint32 i = 0; i < arraysize(types); i++)
		{
			std::string startkey;
			KeyObject start(startkey, types[i], db);
			Iterator* iter = FindValue(start);
			while (NULL != iter && iter->Valid())
			{
				Slice tmpkey = iter->Key();
				KeyObject* kk = decode_key(tmpkey, NULL);
				if (kk->type != types[i])
				{
					DELETE(kk);
					break;
				}
				std::string key(kk->key.data(), kk->key.size());
				if (fnmatch(pattern.c_str(), key.c_str(), 0) == 0)
				{
				    count++;
				}
				DELETE(kk);
				iter->Next();
			}
			DELETE(iter);
		}

		// hash keys
		std::string startkey;
		while (true)
		{
			KeyObject start(startkey, HASH_FIELD, db);
			Iterator* iter = FindValue(start);
			std::string current(startkey.data(), startkey.size());
			if (NULL != iter && iter->Valid())
			{
				Slice tmpkey = iter->Key();
				KeyObject* kk = decode_key(tmpkey, NULL);
				if (kk->type != HASH_FIELD)
				{
					DELETE(kk);
					DELETE(iter);
					return 0;
				}
				current.assign(kk->key.data(), kk->key.size());
				if (fnmatch(pattern.c_str(), current.c_str(), 0) == 0)
				{
					count++;
				}
				DELETE(kk);
			}
			DELETE(iter);
			next_key(current, startkey);
		}
		return 0;
	}

	int Ardb::Keys(const DBID& db, const std::string& pattern, const KeyObject& from, uint32 limit, StringArray& ret, KeyType& last_keytype)
	{
		if (from.type != HASH_FIELD || from.type == KEY_END)
		{
			KeyType srctype = from.type;
			if (srctype == KEY_END)
			{
				srctype = KV;
			}

			KeyType types[] =
			{ KV, SET_META, ZSET_META, LIST_META, TABLE_META, BITSET_META };
			for (uint32 i = 0; i < arraysize(types); i++)
			{
				if (types[i] < srctype)
				{
					continue;
				}
				std::string startkey;
				if (from.type == types[i])
				{
					next_key(from.key, startkey);
				}
				KeyObject start(startkey, types[i], db);
				Iterator* iter = FindValue(start);
				while (NULL != iter && iter->Valid())
				{
					Slice tmpkey = iter->Key();
					KeyObject* kk = decode_key(tmpkey, NULL);
					if (kk->type != types[i] || kk->db != db)
					{
						DELETE(kk);
						break;
					}
					std::string key(kk->key.data(), kk->key.size());
					if (fnmatch(pattern.c_str(), key.c_str(), 0) == 0)
					{
					    ret.push_back(key);
					}

					DELETE(kk);
					if (ret.size() == limit)
					{
						last_keytype = types[i];
						DELETE(iter);
						return 0;
					}
					iter->Next();
				}
				DELETE(iter);
			}
		}

		// hash keys
		std::string startkey;
		if (from.type == HASH_FIELD)
		{
			next_key(from.key, startkey);
		}
		while (true)
		{
			KeyObject start(startkey, HASH_FIELD, db);
			Iterator* iter = FindValue(start);
			std::string current = startkey;
			if (NULL != iter && iter->Valid())
			{
				Slice tmpkey = iter->Key();
				KeyObject* kk = decode_key(tmpkey, NULL);
				if (kk->type != HASH_FIELD|| kk->db != db)
				{
					DELETE(kk);
					DELETE(iter);
					return 0;
				}
				current.assign(kk->key.data(), kk->key.size());
				if (fnmatch(pattern.c_str(), current.c_str(), 0) == 0)
				{
				    ret.push_back(current);
				}
				DELETE(kk);
				if (ret.size() == limit)
				{
					last_keytype = HASH_FIELD;
					DELETE(iter);
					return 0;
				}
			}
			DELETE(iter);
			next_key(current, startkey);
		}
		return 0;
	}
>>>>>>> 180fb4fcd4d68a7d5632c028f9c2958835cce814
}

