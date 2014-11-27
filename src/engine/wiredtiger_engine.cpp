/*
 *Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
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

#include "wiredtiger_engine.hpp"
#include "codec.hpp"
#include "util/file_helper.hpp"
#include <string.h>
#include <stdlib.h>

#define LEVELDB_SLICE(slice) leveldb::Slice(slice.data(), slice.size())
#define ARDB_SLICE(slice) Slice(slice.data(), slice.size())

#define CHECK_WT_RETURN(ret)         do{\
                                       if (0 != ret) \
                                        {             \
                                           ERROR_LOG("WiredTiger Error: %s at %s:%d", wiredtiger_strerror(ret), __FILE__, __LINE__);\
                                            ret = -1;  \
                                         }\
                                       }while(0)

#define ARDB_TABLE "table:ardb"

namespace ardb
{
    static WT_CURSOR* create_wiredtiger_cursor(WT_SESSION* session)
    {
        WT_CURSOR* cursor = NULL;
        int ret = session->open_cursor(session,
        ARDB_TABLE, NULL, "raw", &cursor);
        if (0 != ret)
        {
            ERROR_LOG("Error create cursor for reason: %s", wiredtiger_strerror(ret));
            return NULL;
        }
        return cursor;
    }

    WiredTigerEngineFactory::WiredTigerEngineFactory(const Properties& props)
    {
        ParseConfig(props, m_cfg);
    }

    void WiredTigerEngineFactory::ParseConfig(const Properties& props, WiredTigerConfig& cfg)
    {
        cfg.path = ".";
        conf_get_string(props, "data-dir", cfg.path);
        conf_get_int64(props, "wiredtiger.batch_commit_watermark", cfg.batch_commit_watermark);
        conf_get_string(props, "wiredtiger.init_options", cfg.init_options);
        conf_get_string(props, "wiredtiger.init_table_options", cfg.init_table_options);
        //conf_get_bool(props, "leveldb.logenable", cfg.logenable);
    }

    KeyValueEngine* WiredTigerEngineFactory::CreateDB(const std::string& name)
    {
        WiredTigerEngine* engine = new WiredTigerEngine();
        WiredTigerConfig cfg = m_cfg;
        char tmp[cfg.path.size() + name.size() + 10];
        sprintf(tmp, "%s/%s", cfg.path.c_str(), name.c_str());
        cfg.path = tmp;
        if (engine->Init(cfg) != 0)
        {
            DELETE(engine);
            ERROR_LOG("Failed to create DB:%s", name.c_str());
            return NULL;
        }
        return engine;
    }
    void WiredTigerEngineFactory::DestroyDB(KeyValueEngine* engine)
    {
        DELETE(engine);
    }
    void WiredTigerEngineFactory::CloseDB(KeyValueEngine* engine)
    {
        DELETE(engine);
    }
    void WiredTigerIterator::SeekToFirst()
    {
        m_iter_ret = m_iter->reset(m_iter);
    }
    void WiredTigerIterator::SeekToLast()
    {
        m_iter->reset(m_iter);
        m_iter_ret = m_iter->prev(m_iter);
    }
    void WiredTigerIterator::Seek(const Slice& target)
    {
        WT_ITEM key_item;
        key_item.data = target.data();
        key_item.size = target.size();
        m_iter->set_key(m_iter, &key_item);
        int exact;
        if ((m_iter_ret = m_iter->search_near(m_iter, &exact)) == 0)
        {
            if (exact < 0)
            {
                Next();
            }
        }
    }
    void WiredTigerIterator::Next()
    {
        m_iter_ret = m_iter->next(m_iter);
    }
    void WiredTigerIterator::Prev()
    {
        m_iter_ret = m_iter->prev(m_iter);
    }
    Slice WiredTigerIterator::Key() const
    {
        return Slice((const char*) m_key_item.data, m_key_item.size);
    }
    Slice WiredTigerIterator::Value() const
    {
        if (0 == m_iter->get_value(m_iter, &m_value_item))
        {
            return Slice((const char*) m_value_item.data, m_value_item.size);
        }
        return Slice();
    }
    bool WiredTigerIterator::Valid()
    {
        if (0 == m_iter_ret)
        {
            m_iter_ret = m_iter->get_key(m_iter, &m_key_item);
        }
        return 0 == m_iter_ret;
    }
    WiredTigerIterator::~WiredTigerIterator()
    {
        if (NULL != m_iter)
        {
            m_iter->close(m_iter);
            m_engine->GetContextHolder().ReleaseTranscRef();
        }
    }

    WiredTigerEngine::WiredTigerEngine() :
            m_db(NULL)
    {

    }
    WiredTigerEngine::~WiredTigerEngine()
    {
        Close();
    }

    void WiredTigerEngine::Close()
    {
//        m_running = false;
//        DELETE(m_background);
        if (NULL != m_db)
        {
            m_db->close(m_db, NULL);
            DELETE(m_db);
        }
    }

//    void WiredTigerEngine::Run()
//    {
//        while (m_running)
//        {
//            ContextHolder& holder = GetContextHolder();
//            WT_SESSION* session = holder.session;
//            WT_CURSOR *cursor = session != NULL ? create_wiredtiger_cursor(session) : NULL;
//            if (NULL == cursor)
//            {
//                Thread::Sleep(10, MILLIS);
//                continue;
//            }
//            int rc = session->begin_transaction(session, "isolation=snapshot");
//            CHECK_WT_RETURN(rc);
//            bool trasnc_active = rc == 0;
//            WriteOperation* op = NULL;
//            int count = 0;
//            while (m_write_queue.Pop(op))
//            {
//                switch (op->type)
//                {
//                    case PUT_OP:
//                    {
//                        PutOperation* pop = (PutOperation*) op;
//                        WT_ITEM k, v;
//                        k.data = const_cast<char*>(pop->key.data());
//                        k.size = pop->key.size();
//                        v.data = const_cast<char*>(pop->value.data());
//                        v.size = pop->value.size();
//                        cursor->set_key(cursor, &k);
//                        cursor->set_value(cursor, &v);
//                        int rc = cursor->insert(cursor);
//                        CHECK_WT_RETURN(rc);
//                        DELETE(op);
//                        break;
//                    }
//                    case DEL_OP:
//                    {
//                        DelOperation* pop = (DelOperation*) op;
//                        WT_ITEM k;
//                        k.data = const_cast<char*>(pop->key.data());
//                        k.size = pop->key.size();
//                        cursor->set_key(cursor, &k);
//                        int rc = cursor->remove(cursor);
//                        CHECK_WT_RETURN(rc);
//                        DELETE(op);
//                        break;
//                    }
//                    case CKP_OP:
//                    {
//                        if (trasnc_active)
//                        {
//                            session->commit_transaction(session, NULL);
//                            trasnc_active = false;
//                        }
//                        //mdb_txn_commit (txn);
//                        //txn = NULL;
//                        CheckPointOperation* cop = (CheckPointOperation*) op;
//                        cop->Notify();
//                        break;
//                    }
//                    default:
//                    {
//                        break;
//                    }
//                }
//                count++;
//                if (count == m_cfg.batch_commit_watermark || !trasnc_active)
//                {
//                    break;
//                }
//            }
//            if (trasnc_active)
//            {
//                rc = session->commit_transaction(session, NULL);
//                CHECK_WT_RETURN(rc);
//            }
//            if (count == 0)
//            {
//                m_queue_cond.Lock();
//                m_queue_cond.Wait(5);
//                m_queue_cond.Unlock();
//            }
//        }
//    }

    static int __ardb_compare_keys(WT_COLLATOR *collator, WT_SESSION *session, const WT_ITEM *v1, const WT_ITEM *v2,
            int *cmp)
    {
        const char *s1 = (const char *) v1->data;
        const char *s2 = (const char *) v2->data;

        (void) session; /* unused */
        (void) collator; /* unused */

        *cmp = CommonComparator::Compare(s1, v1->size, s2, v2->size);
        return (0);
    }
    static WT_COLLATOR ardb_comparator =
        { __ardb_compare_keys, NULL, NULL };
    int WiredTigerEngine::Init(const WiredTigerConfig& cfg)
    {
        m_cfg = cfg;
        make_dir(cfg.path);
        int ret = 0;
        if (m_cfg.init_options.empty())
        {
            m_cfg.init_options = "create,cache_size=500M,statistics=(fast)";
        }
        if ((ret = wiredtiger_open(m_cfg.path.c_str(), NULL, m_cfg.init_options.c_str(), &m_db)) != 0)
        {
            ERROR_LOG("Error connecting to %s: %s", m_cfg.path.c_str(), wiredtiger_strerror(ret));
            return -1;
        }
        ret = m_db->add_collator(m_db, "ardb_comparator", &ardb_comparator, NULL);
        CHECK_WT_RETURN(ret);
//        m_running = true;
//        m_background = new Thread(this);
//        m_background->Start();
        return 0;
    }

    WiredTigerEngine::ContextHolder& WiredTigerEngine::GetContextHolder()
    {
        ContextHolder& holder = m_context.GetValue();
        if (NULL == holder.session)
        {
            int ret = 0;
            if ((ret = m_db->open_session(m_db, NULL, NULL, &holder.session)) != 0)
            {
                ERROR_LOG("Error opening a session on %s: %s", m_cfg.path.c_str(), wiredtiger_strerror(ret));
            }
            else
            {
                if (m_cfg.init_table_options.empty())
                {
                    m_cfg.init_table_options =
                            "key_format=u,value_format=u,prefix_compression=true,collator=ardb_comparator";
                }
                else
                {
                    if (m_cfg.init_table_options.find("collator=ardb_comparator") == std::string::npos)
                    {
                        m_cfg.init_table_options.append(",collator=ardb_comparator");
                    }
                }
                ret = holder.session->create(holder.session, ARDB_TABLE, m_cfg.init_table_options.c_str());
                CHECK_WT_RETURN(ret);
            }
            holder.engine = this;
        }
        return holder;
    }

    int WiredTigerEngine::BeginBatchWrite()
    {
        ContextHolder& holder = GetContextHolder();
        WT_SESSION* session = holder.session;
        if (NULL == session)
        {
            return -1;
        }
        holder.AddBatchRef();
        return 0;
    }
    int WiredTigerEngine::CommitBatchWrite()
    {
        ContextHolder& holder = GetContextHolder();
        holder.ReleaseBatchRef();
//        if (holder.batch_ref == 0)
//        {
//            CheckPointOperation* ck = new CheckPointOperation(holder.cond);
//            m_write_queue.Push(ck);
//            NotifyBackgroundThread();
//            ck->Wait();
//            DELETE(ck);
//        }
        return 0;
    }
    int WiredTigerEngine::DiscardBatchWrite()
    {
        ContextHolder& holder = GetContextHolder();
        holder.ReleaseBatchRef();
//        if (holder.batch_ref == 0)
//        {
//            CheckPointOperation* ck = new CheckPointOperation(holder.cond);
//            m_write_queue.Push(ck);
//            NotifyBackgroundThread();
//            ck->Wait();
//            DELETE(ck);
//        }
        return 0;
    }
//    void WiredTigerEngine::NotifyBackgroundThread()
//    {
//        m_queue_cond.Lock();
//        m_queue_cond.Notify();
//        m_queue_cond.Unlock();
//    }
//
//    void WiredTigerEngine::WaitWriteComplete(ContextHolder& holder)
//    {
//        CheckPointOperation* ck = new CheckPointOperation(holder.cond);
//        m_write_queue.Push(ck);
//        NotifyBackgroundThread();
//        ck->Wait();
//        DELETE(ck);
//    }

    void WiredTigerEngine::CompactRange(const Slice& begin, const Slice& end)
    {
        ContextHolder& holder = GetContextHolder();
        WT_SESSION* session = holder.session;
        if (NULL == session)
        {
            return;
        }
        session->compact(session, ARDB_TABLE, NULL);
    }

    void WiredTigerEngine::ContextHolder::AddTranscRef()
    {
        trasc_ref++;
        if (trasc_ref == 1)
        {
            int ret = session->begin_transaction(session, "isolation=snapshot");
            CHECK_WT_RETURN(ret);
            readonly_transc = true;
        }
    }
    void WiredTigerEngine::ContextHolder::ReleaseTranscRef()
    {
        if (trasc_ref > 0)
        {
            trasc_ref--;
            if (0 == trasc_ref)
            {
                int ret = session->commit_transaction(session, NULL);
                CHECK_WT_RETURN(ret);
                if (!readonly_transc)
                {
                    //engine->WaitWriteComplete(*this);
                }
            }
        }
    }

    void WiredTigerEngine::ContextHolder::AddBatchRef()
    {
        batch_ref++;
        if (batch_ref == 1 && NULL == batch)
        {
            batch = create_wiredtiger_cursor(session);
        }
    }
    void WiredTigerEngine::ContextHolder::ReleaseBatchRef()
    {
        batch_ref--;
        if (0 == batch_ref && NULL != batch)
        {
            batch->close(batch);
            batch = NULL;
        }
    }
    void WiredTigerEngine::ContextHolder::IncBatchWriteCount()
    {
        batch_write_count++;
    }
    void WiredTigerEngine::ContextHolder::RestartBatchWrite()
    {
        batch_write_count = 0;
        if (NULL != batch)
        {
            batch->close(batch);
        }
        batch = create_wiredtiger_cursor(session);
    }

    int WiredTigerEngine::Put(const Slice& key, const Slice& value, const Options& options)
    {
        ContextHolder& holder = GetContextHolder();
        WT_SESSION* session = holder.session;
        if (NULL == session)
        {
            return -1;
        }
//        if (holder.trasc_ref > 0)
//        {
//            PutOperation* op = new PutOperation;
//            op->key.assign((const char*) key.data(), key.size());
//            op->value.assign((const char*) value.data(), value.size());
//            m_write_queue.Push(op);
//            holder.readonly_transc = false;
//            return 0;
//        }
        int ret = 0;
        WT_CURSOR *cursor = holder.batch == NULL ? create_wiredtiger_cursor(session) : holder.batch;
        if (NULL == cursor)
        {
            return -1;
        }
        WT_ITEM key_item, value_item;
        key_item.data = key.data();
        key_item.size = key.size();
        value_item.data = value.data();
        value_item.size = value.size();
        cursor->set_key(cursor, &key_item);
        cursor->set_value(cursor, &value_item);
        ret = cursor->insert(cursor);
        if (0 != ret)
        {
            ERROR_LOG("Error write data for reason: %s", wiredtiger_strerror(ret));
            ret = -1;
        }
        if (holder.batch == NULL)
        {
            ret = cursor->close(cursor);
            CHECK_WT_RETURN(ret);
        }
        else
        {
            holder.IncBatchWriteCount();
            if (holder.batch_write_count >= m_cfg.batch_commit_watermark)
            {
                holder.RestartBatchWrite();
            }
        }
        return ret;
    }
    int WiredTigerEngine::Get(const Slice& key, std::string* value, const Options& options)
    {
        WT_SESSION* session = GetContextHolder().session;
        if (NULL == session)
        {
            return -1;
        }
        WT_CURSOR *cursor = create_wiredtiger_cursor(session);
        if (NULL == cursor)
        {
            return -1;
        }
        WT_ITEM key_item, value_item;
        key_item.data = key.data();
        key_item.size = key.size();
        cursor->set_key(cursor, &key_item);
        int ret = 0;
        if ((ret = cursor->search(cursor)) == 0)
        {
            ret = cursor->get_value(cursor, &value_item);
            if (0 == ret)
            {
                if (NULL != value)
                {
                    value->assign((const char*) value_item.data, value_item.size);
                }
            }
            else
            {
                CHECK_WT_RETURN(ret);
            }
        }
        cursor->close(cursor);
        return ret;
    }
    int WiredTigerEngine::Del(const Slice& key, const Options& options)
    {
        ContextHolder& holder = GetContextHolder();
        WT_SESSION* session = holder.session;
        if (NULL == session)
        {
            return -1;
        }
//        if (holder.trasc_ref > 0)
//        {
//            DelOperation* op = new DelOperation;
//            op->key.assign((const char*) key.data(), key.size());
//            m_write_queue.Push(op);
//            holder.readonly_transc = false;
//            return 0;
//        }
        int ret = 0;
        WT_CURSOR *cursor = holder.batch == NULL ? create_wiredtiger_cursor(session) : holder.batch;
        if (NULL == cursor)
        {
            return -1;
        }
        WT_ITEM key_item;
        key_item.data = key.data();
        key_item.size = key.size();
        cursor->set_key(cursor, &key_item);
        ret = cursor->remove(cursor);
        CHECK_WT_RETURN(ret);
        if (holder.batch == NULL)
        {
            cursor->close(cursor);
            CHECK_WT_RETURN(ret);
        }
        else
        {
            holder.IncBatchWriteCount();
            if (holder.batch_write_count >= m_cfg.batch_commit_watermark)
            {
                holder.RestartBatchWrite();
            }
        }
        return 0;
    }

    Iterator* WiredTigerEngine::Find(const Slice& findkey, const Options& options)
    {
        ContextHolder& holder = GetContextHolder();
        WT_SESSION* session = holder.session;
        if (NULL == session)
        {
            return NULL;
        }
        int ret, exact;
        holder.AddTranscRef();
        WT_CURSOR *cursor = create_wiredtiger_cursor(session);
        if (NULL == cursor)
        {
            holder.ReleaseTranscRef();
            return NULL;
        }
        WT_ITEM key_item;
        key_item.data = findkey.data();
        key_item.size = findkey.size();
        cursor->set_key(cursor, &key_item);

        if ((ret = cursor->search_near(cursor, &exact)) == 0)
        {
            if (exact < 0)
            {
                ret = cursor->next(cursor);
            }
            if (0 == ret)
            {
                return new WiredTigerIterator(this, cursor);
            }
        }
        DEBUG_LOG("Error find data for reason: %s", wiredtiger_strerror(ret));
        cursor->close(cursor);
        holder.ReleaseTranscRef();
        return NULL;
    }

    static int print_cursor(WT_CURSOR *cursor, std::string& str)
    {
        WT_ITEM desc, pvalue;
        uint64_t value;
        int ret;

        while ((ret = cursor->next(cursor)) == 0 && (ret = cursor->get_value(cursor, &desc, &pvalue, &value)) == 0)
        {
            if (value != 0)
            {
                str.append((const char*) desc.data, desc.size).append(": ").append((const char*) pvalue.data,
                        pvalue.size).append("\r\n");
            }
        }
        return (ret == WT_NOTFOUND ? 0 : ret);
    }

    const std::string WiredTigerEngine::Stats()
    {
        std::string info;
        int major_v, minor_v, patch;
        (void) wiredtiger_version(&major_v, &minor_v, &patch);
        info.append("WiredTiger version:").append(stringfromll(major_v)).append(".").append(stringfromll(minor_v)).append(
                ".").append(stringfromll(patch)).append("\r\n");
        info.append("WiredTiger Init Options:").append(m_cfg.init_options).append("\r\n");
        info.append("WiredTiger Table Init Options:").append(m_cfg.init_table_options).append("\r\n");

        ContextHolder& holder = m_context.GetValue();
        WT_SESSION* session = holder.session;
        if (NULL == session)
        {
            return info;
        }
        WT_CURSOR *cursor;
        int ret;

        /*! [statistics database function] */
        if ((ret = session->open_cursor(session, "statistics:", NULL, NULL, &cursor)) == 0)
        {
            print_cursor(cursor, info);
            cursor->close(cursor);
        }
        if ((ret = session->open_cursor(session, "statistics:table:ardb", NULL, NULL, &cursor)) == 0)
        {
            print_cursor(cursor, info);
            cursor->close(cursor);
        }
        return info;

    }

    int WiredTigerEngine::MaxOpenFiles()
    {
        //return m_options.max_open_files;
        return 100;
    }

}

