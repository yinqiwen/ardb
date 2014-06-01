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

#include "db.hpp"
#include <string.h>
#include <sstream>
#include "util/thread/thread.hpp"
#include "helper/db_helpers.hpp"

#define MAX_STRING_LENGTH 1024

namespace ardb
{
    size_t Ardb::RealPosition(std::string& buf, int pos)
    {
        if (pos < 0)
        {
            pos = buf.size() + pos;
        }
        if (pos > 0 && (uint32) pos >= buf.size())
        {
            pos = buf.size() - 1;
        }
        if (pos < 0)
        {
            pos = 0;
        }
        return pos;
    }

    const std::string& Ardb::MaxString()
    {
        static std::string value;
        if (value.empty())
        {
            value.resize(MAX_STRING_LENGTH);
            char* v = &(value[0]);
            memset(v, 127, MAX_STRING_LENGTH);
        }

        return value;
    }

    Ardb::Ardb(KeyValueEngineFactory* engine, uint32 multi_thread_num) :
            m_engine_factory(engine), m_engine(NULL), m_key_locker(multi_thread_num), m_db_helper(
            NULL), m_level1_cahce(NULL)
    {
    }

    bool Ardb::Init(const ArdbConfig& cfg)
    {
        m_config = cfg;
        if (NULL == m_engine)
        {
            INFO_LOG("Start init storage engine.");
            m_engine = m_engine_factory->CreateDB(m_engine_factory->GetName().c_str());
            if (NULL == m_engine)
            {
                return false;
            }
            if (NULL != m_engine)
            {
                /**
                 * Init db helper
                 */
                NEW(m_db_helper, DBHelper(this));
                if (m_config.L1_cache_memory_limit > 0)
                {
                    NEW(m_level1_cahce, L1Cache(this));
                }
                m_db_helper->Start();
                INFO_LOG("Init storage engine success.");
            }
        }
        return m_engine != NULL;
    }

    Ardb::~Ardb()
    {
        if (NULL != m_db_helper)
        {
            m_db_helper->StopSelf();
            m_db_helper->Join();
            DELETE(m_db_helper);
        }
        DELETE(m_level1_cahce);
        if (NULL != m_engine)
        {
            m_engine_factory->CloseDB(m_engine);
        }
    }

    void Ardb::Walk(KeyObject& key, bool reverse, bool decodeValue, WalkHandler* handler)
    {
        bool checkType = true;
        if (key.type == KEY_END)
        {
            key.type = KEY_META;
            key.key = Slice();
            checkType = false;
        }
        bool isFirstElement = true;
        Iterator* iter = FindValue(key);
        if (NULL != iter && !iter->Valid() && reverse)
        {
            iter->SeekToLast();
            isFirstElement = false;
        }
        uint32 cursor = 0;
        while (NULL != iter && iter->Valid())
        {
            Slice tmpkey = iter->Key();
            KeyObject* kk = decode_key(tmpkey, checkType ? &key : NULL);
            if (NULL == kk)
            {
                DELETE(kk);
                if (reverse && isFirstElement)
                {
                    iter->Prev();
                    isFirstElement = false;
                    continue;
                }
                break;
            }
            ValueObject* v = decodeValue ? decode_value_obj(kk->type, iter->Value().data(), iter->Value().size()) :
            NULL;
            int ret = handler->OnKeyValue(kk, v, cursor++);
            DELETE(kk);
            DELETE(v);
            if (ret < 0)
            {
                break;
            }
            if (reverse)
            {
                iter->Prev();
            }
            else
            {
                iter->Next();
            }
        }
        DELETE(iter);
    }

    KeyValueEngine* Ardb::GetEngine()
    {
        return m_engine;
    }

    int Ardb::RawSet(const Slice& key, const Slice& value)
    {
        int ret = GetEngine()->Put(key, value);
        return ret;
    }
    int Ardb::RawDel(const Slice& key)
    {
        int ret = GetEngine()->Del(key);
        return ret;
    }

    int Ardb::RawGet(const Slice& key, std::string* value)
    {
        return GetEngine()->Get(key, value, false);
    }

    int Ardb::Type(const DBID& db, const Slice& key)
    {
        KeyType type;
        if (0 == GetType(db, key, type))
        {
            return type;
        }
        return -1;
    }

    void Ardb::VisitDB(const DBID& db, RawValueVisitor* visitor, Iterator* iter)
    {
        Iterator* current_iter = iter;
        if (NULL == current_iter)
        {
            current_iter = NewIterator(db);
        }
        while (NULL != current_iter && current_iter->Valid())
        {
            DBID tmpdb;
            KeyType t;
            if (!peek_dbkey_header(current_iter->Key(), tmpdb, t) || tmpdb != db)
            {
                break;
            }
            int ret = 0;
            if (t != KEY_END)
            {
                ret = visitor->OnRawKeyValue(current_iter->Key(), current_iter->Value());
            }
            current_iter->Next();
            if (ret == -1)
            {
                break;
            }
        }
        if (NULL == iter)
        {
            DELETE(current_iter);
        }
    }
    void Ardb::VisitAllDB(RawValueVisitor* visitor, Iterator* iter)
    {
        Iterator* current_iter = iter;
        if (NULL == current_iter)
        {
            current_iter = NewIterator();
        }
        while (NULL != current_iter && current_iter->Valid())
        {
            int ret = visitor->OnRawKeyValue(current_iter->Key(), current_iter->Value());
            current_iter->Next();
            if (ret == -1)
            {
                break;
            }
        }
        if (NULL == iter)
        {
            DELETE(current_iter);
        }
    }

    Iterator* Ardb::NewIterator()
    {
        Slice empty;
        return GetEngine()->Find(empty, false);
    }
    Iterator* Ardb::NewIterator(const DBID& db)
    {
        KeyObject key(Slice(), KEY_META, db);
        return FindValue(key);
    }

    void Ardb::PrintDB(const DBID& db)
    {
        Slice empty;
        KeyObject start(empty, KEY_META, db);
        Iterator* iter = FindValue(start);
        while (NULL != iter && iter->Valid())
        {
            Slice tmpkey = iter->Key();
            KeyObject* kk = decode_key(tmpkey, NULL);
            if (kk->db != db)
            {
                DELETE(kk);
                break;
            }
            ValueData v;
            Buffer readbuf(const_cast<char*>(iter->Value().data()), 0, iter->Value().size());
            decode_value(readbuf, v);
            if (NULL != kk)
            {
                std::string str;
                std::string keystr(kk->key.data(), kk->key.size());
                switch (kk->type)
                {
                    case HASH_FIELD:
                    {
                        HashKeyObject* hk = (HashKeyObject*) kk;
                        std::string f;
                        hk->field.ToString(f);
                        DEBUG_LOG("[Hash]Key=%s, Field=%s, Value=%s", keystr.c_str(), f.c_str(),
                                v.ToString(str).c_str());
                        break;
                    }
                    case LIST_ELEMENT:
                    {
                        //ListKeyObject* hk = (ListKeyObject*) kk;
                        DEBUG_LOG("[List]Key=%s, Value=%s", keystr.c_str(), v.ToString(str).c_str());
                        break;
                    }
                    case SET_ELEMENT:
                    {
                        SetKeyObject* hk = (SetKeyObject*) kk;
                        std::string f;
                        hk->value.ToString(f);
                        DEBUG_LOG("[Set]Key=%s, Member=%s", keystr.c_str(), f.c_str());
                        break;
                    }
                    default:
                    {
                        DEBUG_LOG("[%d]Key=%s, Value=%s", kk->type, kk->key.data(), v.ToString(str).c_str());
                        break;
                    }
                }

            }
            DELETE(kk);
            iter->Next();
        }
        DELETE(iter);
    }

    int Ardb::CompactAll()
    {
        struct CompactTask: public Thread
        {
                Ardb* adb;
                CompactTask(Ardb* db) :
                        adb(db)
                {
                }
                void Run()
                {
                    adb->GetEngine()->CompactRange(Slice(), Slice());
                    delete this;
                }
        };
        /*
         * Start a background thread to compact kvs
         */
        Thread* t = new CompactTask(this);
        t->Start();
        return 0;
        return 0;
    }

    void Ardb::GetAllDBIDSet(DBIDSet& dbs)
    {
        DBID current = 0;
        DBID next = 0;
        while (true)
        {
            next = 0;
            if (DBExist(current, next))
            {
                dbs.insert(current);
                current++;
            }
            else
            {
                if (next == ARDB_GLOBAL_DB)
                {
                    break;
                }
                dbs.insert(next);
                current = next + 1;
            }
        }
    }

    bool Ardb::DBExist(const DBID& db, DBID& nextdb)
    {
        KeyObject start(Slice(), KEY_META, db);
        Iterator* iter = FindValue(start, false);
        bool found = false;
        nextdb = db;
        if (NULL != iter && iter->Valid())
        {
            Slice tmpkey = iter->Key();
            KeyObject* kk = decode_key(tmpkey, NULL);
            if (NULL != kk)
            {
                if (kk->db == db)
                {
                    found = true;
                }
                else
                {
                    nextdb = kk->db;
                }
            }
            DELETE(kk);
        }
        DELETE(iter);
        return found;
    }

    int Ardb::CompactDB(const DBID& db)
    {
        struct CompactTask: public Thread
        {
                Ardb* adb;
                DBID dbid;
                CompactTask(Ardb* db, DBID id) :
                        adb(db), dbid(id)
                {
                }
                void Run()
                {
                    KeyObject start(Slice(), KEY_META, dbid);
                    KeyObject end(Slice(), KEY_META, dbid + 1);
                    Buffer sbuf, ebuf;
                    encode_key(sbuf, start);
                    encode_key(ebuf, end);
                    adb->GetEngine()->CompactRange(sbuf.AsString(), ebuf.AsString());
                    delete this;
                }
        };
        /*
         * Start a background thread to compact kvs
         */
        Thread* t = new CompactTask(this, db);
        t->Start();
        return 0;
    }

    int Ardb::FlushDB(const DBID& db)
    {
        struct VisitorTask: public RawValueVisitor, public Thread
        {
                Ardb* adb;
                DBID dbid;
                VisitorTask(Ardb* db, DBID id) :
                        adb(db), dbid(id)
                {
                }
                int OnRawKeyValue(const Slice& key, const Slice& value)
                {
                    adb->RawDel(key);
                    return 0;
                }
                void Run()
                {
                    adb->GetEngine()->BeginBatchWrite();
                    adb->VisitDB(dbid, this);
                    adb->GetEngine()->CommitBatchWrite();
                    KeyObject start(Slice(), KEY_META, dbid);
                    KeyObject end(Slice(), KEY_META, dbid + 1);
                    Buffer sbuf, ebuf;
                    encode_key(sbuf, start);
                    encode_key(ebuf, end);
                    adb->GetEngine()->CompactRange(sbuf.AsString(), ebuf.AsString());
                    delete this;
                }
        };
        /*
         * Start a background thread to delete kvs
         */
        Thread* t = new VisitorTask(this, db);
        t->Start();
        return 0;
    }

    int Ardb::FlushAll()
    {
        struct VisitorTask: public RawValueVisitor, public Thread
        {
                Ardb* db;
                VisitorTask(Ardb* adb) :
                        db(adb)
                {
                }
                int OnRawKeyValue(const Slice& key, const Slice& value)
                {
                    db->RawDel(key);
                    return 0;
                }
                void Run()
                {
                    db->GetEngine()->BeginBatchWrite();
                    db->VisitAllDB(this);
                    db->GetEngine()->CommitBatchWrite();
                    db->GetEngine()->CompactRange(Slice(), Slice());
                    delete this;
                }
        };
        /*
         * Start a background thread to delete kvs
         */
        Thread* t = new VisitorTask(this);
        t->Start();
        return 0;
    }

    Iterator* Ardb::FindValue(KeyObject& key, bool cache)
    {
        Buffer keybuf(key.key.size() + 16);
        encode_key(keybuf, key);
        Slice k(keybuf.GetRawReadBuffer(), keybuf.ReadableBytes());
        Iterator* iter = GetEngine()->Find(k, cache);
        return iter;
    }

    void Ardb::FindValue(Iterator* iter, KeyObject& key)
    {
        Buffer keybuf(key.key.size() + 16);
        encode_key(keybuf, key);
        Slice k(keybuf.GetRawReadBuffer(), keybuf.ReadableBytes());
        iter->Seek(k);
    }

    int Ardb::GetRawValue(const KeyObject& key, std::string& value)
    {
        Buffer keybuf(key.key.size() + 16);
        encode_key(keybuf, key);
        Slice k(keybuf.GetRawReadBuffer(), keybuf.ReadableBytes());
        bool fill_cache = true;
        int ret = GetEngine()->Get(k, &value, fill_cache);
        if (ret == 0)
        {
            return ARDB_OK;
        }
        return ERR_NOT_EXIST;
    }

    int Ardb::SetRawValue(KeyObject& key, Buffer& value)
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
        Slice k(keybuf.GetRawReadBuffer(), keybuf.ReadableBytes());
        Slice v(value.GetRawReadBuffer(), value.ReadableBytes());
        return RawSet(k, v);
    }

    int Ardb::GetType(const DBID& db, const Slice& key, KeyType& type)
    {
        CommonMetaValue* meta = GetMeta(db, key, true);
        if (NULL != meta)
        {
            type = meta->header.type;
            DELETE(meta);
            return 0;
        }
        return ERR_NOT_EXIST;
    }
    int Ardb::SetMeta(const DBID& db, const Slice& key, CommonMetaValue& meta)
    {
        KeyObject metakey(key, KEY_META, db);
        return SetMeta(metakey, meta);
    }
    int Ardb::SetMeta(KeyObject& key, CommonMetaValue& meta)
    {
        DBWatcher& watcher = m_watcher.GetValue();
        watcher.data_changed = true;
        if (watcher.on_key_update != NULL)
        {
            watcher.on_key_update(key.db, key.key, watcher.on_key_update_data);
        }
        if (meta.header.expireat > 0)
        {
            SetExpiration(key.db, key.key, meta.header.expireat, false);
        }
        Buffer keybuf;
        keybuf.EnsureWritableBytes(key.key.size() + 16);
        encode_key(keybuf, key);
        Buffer valuebuf;
        valuebuf.EnsureWritableBytes(64);
        encode_meta(valuebuf, meta);
        Slice k(keybuf.GetRawReadBuffer(), keybuf.ReadableBytes());
        Slice v(valuebuf.GetRawReadBuffer(), valuebuf.ReadableBytes());
        return RawSet(k, v);
    }

    int Ardb::DelMeta(const DBID& db, const Slice& key, CommonMetaValue* meta)
    {
        if (NULL == meta)
        {
            return -1;
        }
        if (meta->header.expireat > 0)
        {
            BatchWriteGuard guard(GetEngine());
            KeyObject k(key, KEY_META, db);
            DelValue(k);
            ExpireKeyObject ek(key, meta->header.expireat, db);
            DelValue(ek);
        }
        else
        {
            KeyObject k(key, KEY_META, db);
            DelValue(k);
        }
        return 0;
    }

    CommonMetaValue* Ardb::GetMeta(const DBID& db, const Slice& key, bool onlyHead)
    {
        KeyObject verkey(key, KEY_META, db);
        std::string v;
        if (0 == GetRawValue(verkey, v) && v.size() > 1)
        {
            return decode_meta(v.data(), v.size(), onlyHead);
        }
        else
        {
            return NULL;
        }
    }

    int Ardb::GetScript(const std::string& funacname, std::string& funcbody)
    {
        KeyObject verkey(funacname, SCRIPT, ARDB_GLOBAL_DB);
        CommonValueObject v;
        int ret = GetKeyValueObject(verkey, v);
        if (0 == ret)
        {
            funcbody = v.data.bytes_value;
            return 0;
        }
        return ret;
    }
    int Ardb::SaveScript(const std::string& funacname, const std::string& funcbody)
    {
        KeyObject key(funacname, SCRIPT, ARDB_GLOBAL_DB);
        CommonValueObject v;
        v.data.SetValue(funcbody, false);
        return SetKeyValueObject(key, v);
    }

    int Ardb::FlushScripts()
    {
        KeyObject start(Slice(), SCRIPT, ARDB_GLOBAL_DB);
        Iterator* iter = FindValue(start, false);
        BatchWriteGuard guard(GetEngine());
        while (NULL != iter && iter->Valid())
        {
            Slice tmpkey = iter->Key();
            KeyObject* kk = decode_key(tmpkey, NULL);
            if (NULL != kk)
            {
                if (kk->type == SCRIPT)
                {
                    DelValue(*kk);
                }
                else
                {
                    break;
                }
            }
            DELETE(kk);
            iter->Next();
        }
        DELETE(iter);
        return 0;
    }

    int Ardb::NextKey(const DBID& db, const std::string& key, std::string& nextkey)
    {
        KeyObject start(key, KEY_META, db);
        Iterator* iter = FindValue(start);
        int err = ERR_NOT_EXIST;
        if (NULL != iter && iter->Valid())
        {
            Slice tmpkey = iter->Key();
            KeyObject* kk = decode_key(tmpkey, NULL);
            if (NULL != kk && kk->type == KEY_META && kk->db == db)
            {
                nextkey.assign(kk->key.data(), kk->key.size());
                err = 0;
            }
            DELETE(kk);
        }
        DELETE(iter);
        return err;
    }
    int Ardb::LastKey(const DBID& db, std::string& lastkey)
    {
        KeyObject start(Slice(), STRING_META, db);
        Iterator* iter = FindValue(start);
        if (NULL == iter || !iter->Valid())
        {
            DELETE(iter);
            iter = NewIterator();
            iter->SeekToLast();
        }
        else
        {
            iter->Prev();
        }
        int err = ERR_NOT_EXIST;
        if (iter->Valid())
        {
            Slice tmpkey = iter->Key();
            KeyObject* kk = decode_key(tmpkey, NULL);
            if (NULL != kk && kk->type == KEY_META && kk->db == db)
            {
                lastkey.assign(kk->key.data(), kk->key.size());
                err = 0;
            }
            DELETE(kk);
        }
        DELETE(iter);
        return err;
    }

    CacheItem* Ardb::GetLoadedCache(const DBID& db, const Slice& key, KeyType type, bool evict_non_loaded,
            bool create_if_not_exist)
    {
        if (NULL != m_level1_cahce)
        {
            CacheItem* cache = m_level1_cahce->Get(db, key, type, create_if_not_exist);
            bool valid_cache = true;
            if (NULL != cache)
            {
                if (cache->IsLoaded())
                {
                    return cache;
                }
                else
                {
                    if (evict_non_loaded)
                    {
                        valid_cache = false;
                    }
                }
            }
            m_level1_cahce->Recycle(cache);
            if (!valid_cache)
            {
                m_level1_cahce->Evict(db, key);
            }
        }
        return NULL;
    }

}

