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
            meta.value.SetValue(*vit, false);
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
                meta.value.SetValue(*vit, false);
                SetMeta(keyobject, meta);
            }
            else
            {
                guard.MarkFailed();
                return -1;
            }
            kit++;
            vit++;
        }
        return keys.size();
    }

    int Ardb::Set(const DBID& db, const Slice& key, const Slice& value, int ex, int px, int nxx)
    {
        if (-1 == nxx || 1 == nxx || m_config.check_type_before_set_string)
        {
            CommonMetaValue* meta = GetMeta(db, key, true);
            if (NULL != meta && meta->header.type != STRING_META)
            {
                DELETE(meta);
                return ERR_INVALID_TYPE;
            }
            if (-1 == nxx && NULL != meta)
            {
                DELETE(meta);
                return ERR_KEY_EXIST;
            }
            if (1 == nxx && NULL == meta)
            {
                DELETE(meta);
                return ERR_NOT_EXIST;
            }
            DELETE(meta);
        }
        uint64 expireat = 0;
        if (px > 0 || ex > 0)
        {
            uint64_t now = get_current_epoch_micros();
            if (px > 0)
            {
                expireat = now + (uint64_t) px * 1000L;
            }
            if (ex > 0)
            {
                expireat = now + (uint64_t) ex * 1000L * 1000L;
            }
        }
        StringMetaValue smeta;
        smeta.value.SetValue(value, false);
        smeta.header.expireat = expireat;
        return SetMeta(db, key, smeta);
    }

    int Ardb::Set(const DBID& db, const Slice& key, const Slice& value)
    {
        return Set(db, key, value, 0, 0, 0);
    }

    int Ardb::SetNX(const DBID& db, const Slice& key, const Slice& value)
    {
        return Set(db, key, value, 0, 0, -1);
    }

    int Ardb::SetEx(const DBID& db, const Slice& key, const Slice& value, uint32_t secs)
    {
        return Set(db, key, value, secs, 0, 0);
    }
    int Ardb::PSetEx(const DBID& db, const Slice& key, const Slice& value, uint32_t ms)
    {
        return Set(db, key, value, 0, ms, 0);
    }

    int Ardb::Get(const DBID& db, const Slice& key, std::string& value)
    {
        CommonMetaValue* meta = GetMeta(db, key, false);
        if (NULL != meta)
        {
            if (meta->header.type != STRING_META)
            {
                DELETE(meta);
                return ERR_INVALID_TYPE;
            }
            StringMetaValue* smeta = (StringMetaValue*) meta;
            smeta->value.ToString(value);
            DELETE(meta);
            return 0;
        }
        return ERR_NOT_EXIST;
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
        CommonMetaValue* meta = GetMeta(db, key, true);
        if (NULL == meta)
        {
            return -1;
        }

        switch (meta->header.type)
        {
            case STRING_META:
            {
                DelMeta(db, key, meta);
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
                DELETE(meta);
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
        CommonMetaValue* meta = GetMeta(db, key, true);
        if (NULL != meta)
        {
            expire = meta->header.expireat;
            DELETE(meta);
            return expire > 0 ? 0 : -1;
        }
        return -1;
    }

    int Ardb::SetExpiration(const DBID& db, const Slice& key, uint64 expire, bool check_exists)
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
            EmptyValueObject empty;
            return SetKeyValueObject(keyobject, empty);
        }
        return 0;
    }

    void Ardb::CheckExpireKey(const DBID& db, uint64 maxexec, uint32 maxcheckitems)
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
            }
            else
            {
                ExpireKeyObject* ek = (ExpireKeyObject*) kk;
                if (ek->expireat <= now)
                {
                    DelValue(*ek);
                    Del(db, ek->key);
                    DELETE(kk);
                }
                else
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
        if (0 == Get(db, key, v))
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
        CommonMetaValue* meta = GetMeta(db, key1, false);
        if (NULL == meta)
        {
            return ERR_NOT_EXIST;
        }
        Del(db, key2);
        int err = 0;
        switch (meta->header.type)
        {
            case STRING_META:
            {
                BatchWriteGuard guard(GetEngine());
                StringMetaValue* cmeta = (StringMetaValue*) meta;
                KeyObject k1(key1, KEY_META, db);
                DelValue(k1);
                SetMeta(db, key2, *cmeta);
                break;
            }
            case LIST_META:
            {
                ListMetaValue* cmeta = (ListMetaValue*) meta;
                RenameList(db, key1, db, key2, cmeta);
                break;
            }
            case HASH_META:
            {
                HashMetaValue* cmeta = (HashMetaValue*) meta;
                RenameHash(db, key1, db, key2, cmeta);
                break;
            }
            case SET_META:
            {
                SetMetaValue* cmeta = (SetMetaValue*) meta;
                RenameSet(db, key1, db, key2, cmeta);
                break;
            }
            case ZSET_META:
            {
                ZSetMetaValue* cmeta = (ZSetMetaValue*) meta;
                RenameZSet(db, key1, db, key2, cmeta);
                break;
            }
            default:
            {
                err = ERR_INVALID_TYPE;
                break;
            }
        }
        DELETE(meta);
        return err;
    }

    int Ardb::RenameNX(const DBID& db, const Slice& key1, const Slice& key2)
    {
        if (Exists(db, key2))
        {
            return ERR_KEY_EXIST;
        }
        return Rename(db, key1, key2);
    }

    int Ardb::Move(DBID srcdb, const Slice& key, DBID dstdb)
    {
        CommonMetaValue* meta = GetMeta(srcdb, key, false);
        if (NULL == meta)
        {
            return ERR_NOT_EXIST;
        }
        Del(dstdb, key);
        int err = 0;
        switch (meta->header.type)
        {
            case STRING_META:
            {
                BatchWriteGuard guard(GetEngine());
                StringMetaValue* cmeta = (StringMetaValue*) meta;
                KeyObject k1(key, KEY_META, srcdb);
                DelValue(k1);
                SetMeta(dstdb, key, *cmeta);
                break;
            }
            case LIST_META:
            {
                ListMetaValue* cmeta = (ListMetaValue*) meta;
                RenameList(srcdb, key, dstdb, key, cmeta);
                break;
            }
            case HASH_META:
            {
                HashMetaValue* cmeta = (HashMetaValue*) meta;
                RenameHash(srcdb, key, dstdb, key, cmeta);
                break;
            }
            case SET_META:
            {
                SetMetaValue* cmeta = (SetMetaValue*) meta;
                RenameSet(srcdb, key, dstdb, key, cmeta);
                break;
            }
            case ZSET_META:
            {
                ZSetMetaValue* cmeta = (ZSetMetaValue*) meta;
                RenameZSet(srcdb, key, dstdb, key, cmeta);
                break;
            }
            default:
            {
                err = ERR_INVALID_TYPE;
                break;
            }
        }
        DELETE(meta);
        return err;
    }

    int Ardb::KeysCount(const DBID& db, const std::string& pattern, int64& count)
    {
        std::string from;
        KeyObject start(from, KEY_META, db);
        std::string::size_type found = pattern.find('*');
        if (found != std::string::npos)
        {
            from = pattern.substr(0, found + 1);
            start.key = from;
        }
        Iterator* iter = FindValue(start);
        while (NULL != iter && iter->Valid())
        {
            Slice tmpkey = iter->Key();
            KeyObject* kk = decode_key(tmpkey, NULL);
            if (kk->type != KEY_META)
            {
                DELETE(kk);
                DELETE(iter);
                return 0;
            }
            std::string str(kk->key.data(), kk->key.size());
            if (fnmatch(pattern.c_str(), str.c_str(), 0) == 0)
            {
                count++;
            }
            DELETE(kk);
            iter->Next();
        }
        DELETE(iter);
        return 0;
    }

    int Ardb::Scan(const DBID& db, const std::string& cursor, const std::string& pattern, uint32 limit,
            StringArray& ret, std::string& newcursor)
    {
        std::string from;
        if (!cursor.empty())
        {
            from.assign(cursor.data(), cursor.size());
        }
        if (cursor == "0")
        {
            from = "";
        }
        Keys(db, pattern, from, limit, ret);
        if (ret.empty())
        {
            newcursor = "0";
        }
        else
        {
            newcursor = (*ret.rbegin());
        }
        return 0;
    }

    int Ardb::Keys(const DBID& db, const std::string& pattern, const std::string& from, uint32 limit, StringArray& ret)
    {
        std::string fromstr = from;
        KeyObject start(fromstr, KEY_META, db);

        if (from.empty() && pattern.size() > 1)
        {
            std::string::size_type found = pattern.find('*');
            if (found != std::string::npos)
            {
                fromstr = pattern.substr(0, found + 1);
                start.key = fromstr;
            }
        }

        Iterator* iter = FindValue(start);
        while (NULL != iter && iter->Valid())
        {
            Slice tmpkey = iter->Key();
            KeyObject* kk = decode_key(tmpkey, NULL);
            if (kk->type != KEY_META)
            {
                DELETE(kk);
                DELETE(iter);
                return 0;
            }
            std::string str(kk->key.data(), kk->key.size());
            if ((pattern.empty() || fnmatch(pattern.c_str(), str.c_str(), 0) == 0) && str != from)
            {
                ret.push_back(str);
                if (ret.size() >= limit)
                {
                    DELETE(kk);
                    DELETE(iter);
                    return 0;
                }
            }
            DELETE(kk);
            iter->Next();
        }
        DELETE(iter);
        return 0;
    }

    int Ardb::Randomkey(const DBID& db, std::string& key)
    {
        std::string empty, min, max;
        if (0 == NextKey(db, empty, min) && 0 == LastKey(db, max))
        {
            if (min == max)
            {
                key = min;
                return 0;
            }
            std::string random_key = random_between_string(min, max);
            NextKey(db, random_key, key);
            return 0;
        }
        return -1;
    }

    int Ardb::CacheLoad(const DBID& db, const Slice& key)
    {
        if (NULL != m_level1_cahce)
        {
            return m_level1_cahce->Load(db, key);
        }
        return -1;
    }
    int Ardb::CacheEvict(const DBID& db, const Slice& key)
    {
        if (NULL != m_level1_cahce)
        {
            return m_level1_cahce->Evict(db, key);
        }
        return -1;
    }

    int Ardb::CacheStatus(const DBID& db, const Slice& key, std::string& status)
    {
        if (NULL != m_level1_cahce)
        {
            uint8 st = 0;
            int ret = m_level1_cahce->PeekCacheStatus(db, key, st);
            if (ret != 0)
            {
                return ret;
            }
            switch (st)
            {
                case L1_CACHE_LOADING:
                {
                    status = "loading";
                    break;
                }
                case L1_CACHE_LOADED:
                {
                    status = "loaded";
                    break;
                }
                default:
                {
                    status = "unknown";
                    break;
                }
            }
            return 0;
        }
        return -1;
    }
}

