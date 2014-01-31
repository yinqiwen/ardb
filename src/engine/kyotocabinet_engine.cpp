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

#include "kyotocabinet_engine.hpp"
#include "ardb.hpp"
#include "data_format.hpp"
#include "util/helpers.hpp"
#include <string.h>

namespace ardb
{
    int32_t KCDBComparator::compare(const char* akbuf, size_t aksiz,
            const char* bkbuf, size_t bksiz)
    {
        return ardb_compare_keys(akbuf, aksiz, bkbuf, bksiz);
    }

    KCDBEngineFactory::KCDBEngineFactory(const Properties& props)
    {
        ParseConfig(props, m_cfg);
    }

    void KCDBEngineFactory::ParseConfig(const Properties& props,
            KCDBConfig& cfg)
    {
        cfg.path = ".";
        conf_get_string(props, "data-dir", cfg.path);
    }
    KeyValueEngine* KCDBEngineFactory::CreateDB(const std::string& db)
    {
        KCDBEngine* engine = new KCDBEngine();
        char tmp[m_cfg.path.size() + db.size() + 10];
        sprintf(tmp, "%s/%s", m_cfg.path.c_str(), db.c_str());
        KCDBConfig cfg = m_cfg;
        cfg.path = tmp;
        if (engine->Init(cfg) != 0)
        {
            DELETE(engine);
            return NULL;
        }
        DEBUG_LOG("Create DB:%s at path:%s success", db.c_str(), tmp);
        return engine;
    }

    void KCDBEngineFactory::CloseDB(KeyValueEngine* engine)
    {
        DELETE(engine);
    }

    void KCDBEngineFactory::DestroyDB(KeyValueEngine* engine)
    {
        KCDBEngine* kcdb = (KCDBEngine*) engine;
        if (NULL != kcdb)
        {
            kcdb->Clear();
        }
        DELETE(engine);
    }

    KCDBEngine::KCDBEngine() :
            m_db(NULL)
    {

    }

    KCDBEngine::~KCDBEngine()
    {
        Close();
        DELETE(m_db);
    }

    void KCDBEngine::Clear()
    {
        if (NULL != m_db)
        {
            m_db->clear();
        }
    }
    void KCDBEngine::Close()
    {
        if (NULL != m_db)
        {
            m_db->close();
        }
    }

    int KCDBEngine::Init(const KCDBConfig& cfg)
    {
        make_file(cfg.path);
        m_db = new kyotocabinet::TreeDB;
        int tune_options = kyotocabinet::TreeDB::TSMALL
                | kyotocabinet::TreeDB::TLINEAR;
        //tune_options |= kyotocabinet::TreeDB::TCOMPRESS;
        m_db->tune_options(tune_options);
        m_db->tune_page_cache(4194304);
        m_db->tune_page(1024);
        m_db->tune_map(256LL << 20);
        m_db->tune_comparator(&m_comparator);
        if (!m_db->open(cfg.path.c_str(),
                kyotocabinet::TreeDB::OWRITER | kyotocabinet::TreeDB::OCREATE))
        {
            ERROR_LOG("Unable to open DB:%s", cfg.path.c_str());
            DELETE(m_db);
            return -1;
        }
        return 0;
    }

    int KCDBEngine::BeginBatchWrite()
    {
        m_batch_local.GetValue().AddRef();
        return 0;
    }
    int KCDBEngine::CommitBatchWrite()
    {
        BatchHolder& holder = m_batch_local.GetValue();
        holder.ReleaseRef();
        if (holder.EmptyRef())
        {
            return FlushWriteBatch(holder);
        }
        return 0;
    }
    int KCDBEngine::DiscardBatchWrite()
    {
        BatchHolder& holder = m_batch_local.GetValue();
        holder.ReleaseRef();
        holder.Clear();
        return 0;
    }

    int KCDBEngine::FlushWriteBatch(BatchHolder& holder)
    {
        StringSet::iterator it = holder.batch_del.begin();
        while(it != holder.batch_del.end())
        {
            m_db->remove(*it);
            it++;
        }
        StringStringMap::iterator sit = holder.batch_put.begin();
        while(sit != holder.batch_put.end())
        {
            m_db->set(sit->first, sit->second);
            sit++;
        }
        holder.Clear();
        return 0;
    }

    int KCDBEngine::Put(const Slice& key, const Slice& value)
    {
        bool success = true;
        BatchHolder& holder = m_batch_local.GetValue();
        if (!holder.EmptyRef())
        {
            holder.Put(key, value);
        } else
        {
            success = m_db->set(key.data(), key.size(), value.data(),
                    value.size());
        }

        return success ? 0 : -1;
    }
    int KCDBEngine::Get(const Slice& key, std::string* value)
    {
        size_t len;
        bool success = false;
        char* data = m_db->get(key.data(), key.size(), &len);
        success = data != NULL;
        if (success && NULL != value)
        {
            value->assign(data, len);
        }
        DELETE_A(data);
        return success ? 0 : -1;
    }
    int KCDBEngine::Del(const Slice& key)
    {
        BatchHolder& holder = m_batch_local.GetValue();
        if (!holder.EmptyRef())
        {
            holder.Del(key);
            return 0;
        } else
        {
            return m_db->remove(key.data(), key.size()) ? 0 : -1;
        }
    }

    Iterator* KCDBEngine::Find(const Slice& findkey, bool cache)
    {
        kyotocabinet::DB::Cursor* cursor = m_db->cursor();
        bool ret = cursor->jump(findkey.data(), findkey.size());
        if (!ret)
        {
            ret = cursor->jump_back();
        }
        return new KCDBIterator(cursor, ret);
    }

    void KCDBIterator::SeekToFirst()
    {
        m_valid = m_cursor->jump();
    }
    void KCDBIterator::SeekToLast()
    {
        m_valid = m_cursor->jump_back();
    }

    void KCDBIterator::Next()
    {
        m_valid = m_cursor->step();
    }
    void KCDBIterator::Prev()
    {
        m_valid = m_cursor->step_back();
    }
    Slice KCDBIterator::Key() const
    {
        Buffer& tmp = const_cast<Buffer&>(m_key_buffer);
        tmp.Clear();
        size_t len;
        char* key = m_cursor->get_key(&len, false);
        if (NULL != key)
        {
            tmp.Write(key, len);
        }
        DELETE_A(key);
        return Slice(m_key_buffer.GetRawReadBuffer(), len);
    }
    Slice KCDBIterator::Value() const
    {
        Buffer& tmp = const_cast<Buffer&>(m_value_buffer);
        tmp.Clear();
        size_t len;
        char* value = m_cursor->get_value(&len, false);
        if (NULL != value)
        {
            tmp.Write(value, len);
        }
        DELETE_A(value);
        return Slice(m_value_buffer.GetRawReadBuffer(), len);
    }
    bool KCDBIterator::Valid()
    {
        return m_valid;
    }
}

