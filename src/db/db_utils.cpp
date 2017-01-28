/*
 *Copyright (c) 2013-2016, yinqiwen <yinqiwen@gmail.com>
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
#include "thread/spin_mutex_lock.hpp"
#include "thread/lock_guard.hpp"
#include "db_utils.hpp"
#include "util/file_helper.hpp"
#include "thread/event_condition.hpp"
#include "db.hpp"

#define DEFAULT_LOCAL_ENCODE_BUFFER_SIZE 8192

#define ARDB_PUT_OP     1
#define ARDB_PUT_RAW_OP 2
#define ARDB_CMD_OP     3
#define ARDB_CKP_OP     4

OP_NAMESPACE_BEGIN

    Slice DBLocalContext::GetSlice(const KeyObject& key)
    {
        Buffer& key_encode_buffer = GetEncodeBufferCache();
        return key.Encode(key_encode_buffer, false, g_engine->GetFeatureSet().support_namespace ? false : true);
    }
    void DBLocalContext::GetSlices(const KeyObject& key, const ValueObject& val, Slice ss[2])
    {
        Buffer& encode_buffer = GetEncodeBufferCache();
        key.Encode(encode_buffer, false, g_engine->GetFeatureSet().support_namespace ? false : true);
        size_t key_len = encode_buffer.ReadableBytes();
        val.Encode(encode_buffer);
        size_t value_len = encode_buffer.ReadableBytes() - key_len;
        ss[0] = Slice(encode_buffer.GetRawBuffer(), key_len);
        ss[1] = Slice(encode_buffer.GetRawBuffer() + key_len, value_len);
    }

    Buffer& DBLocalContext::GetEncodeBufferCache()
    {
        encode_buffer_cache.Clear();
        encode_buffer_cache.Compact(DEFAULT_LOCAL_ENCODE_BUFFER_SIZE);
        return encode_buffer_cache;
    }

    struct DBWriterWorker: public Thread
    {
            Context worker_ctx;
            CallFlags flags;
            DBWriter* writer;
            bool running;
            volatile bool writing;
            DBWriterWorker(DBWriter* w) :
                    writer(w), running(true),writing(false)
            {
            }
            void Call(RedisCommandFrame& cmd)
            {
                writing = true;
                worker_ctx.ClearFlags();
                worker_ctx.flags = flags;
                g_db->Call(worker_ctx, cmd);
                RedisReply& r = worker_ctx.GetReply();
                if(r.IsErr())
                {
                    WARN_LOG("Slave sync error:%s", r.Error().c_str());
                }
                r.Clear();
                writing = false;
            }
            void Run()
            {
                while (running)
                {
                    RedisCommandFrame* cmd = writer->Dequeue(1);
                    if(NULL != cmd)
                    {
                        Call(*cmd);
                        DELETE(cmd);
                    }
                }
            }
            void AdviceStop()
            {
                running = false;
            }
    };

    DBWriter::DBWriter()
    {

    }
    void DBWriter::Init(int workers)
    {
        if (workers > 1)
        {
            for (size_t i = 0; i < workers; i++)
            {
                DBWriterWorker* worker = NULL;
                NEW(worker, DBWriterWorker(this));
                worker->Start();
                m_workers.push_back(worker);
            }
        }
    }

    int DBWriter::Put(Context& ctx, const Data& ns, const Slice& key, const Slice& value)
    {
        /*
         * multi thread only work faster for commands
         */
        return g_engine->PutRaw(ctx, ns, key, value);
    }
    int DBWriter::Put(Context& ctx, const KeyObject& k, const ValueObject& value)
    {
        /*
         * multi thread only work faster for commands
         */
        return g_engine->Put(ctx, k, value);
    }

    void DBWriter::Enqueue(RedisCommandFrame& cmd)
    {

        if (!strncasecmp(cmd.GetCommand().c_str(), "select", 6))
        {
            /*
             * wait all commands in queue executed, because 'select' affect all commands later
             */
            LockGuard<ThreadMutexLock> guard(m_queue_lock);
            while(!m_queue.empty())
            {
                m_queue_lock.Wait(1);
            }
            for(size_t i = 0 ; i < m_workers.size(); i++)
            {
                while(m_workers[i]->writing)
                {
                    m_queue_lock.Wait(1);
                }
                m_workers[i]->Call(cmd);
            }
            return;
        }
        LockGuard<ThreadMutexLock> guard(m_queue_lock);
        while(m_queue.size() >= g_db->GetConf().max_slave_worker_queue)
        {
            m_queue_lock.Wait(1);
        }
        RedisCommandFrame* new_cmd;
        NEW(new_cmd, RedisCommandFrame);
        *new_cmd = cmd;
        m_queue.push_back(new_cmd);
        m_queue_lock.Notify();
    }
    RedisCommandFrame* DBWriter::Dequeue(int timeout)
    {
        LockGuard<ThreadMutexLock> guard(m_queue_lock);
        if(m_queue.empty())
        {
            m_queue_lock.Wait(timeout);
        }
        RedisCommandFrame* cmd = NULL;
        if(!m_queue.empty())
        {
            cmd =  m_queue.front();
            m_queue.pop_front();
        }
        return cmd;
    }
    int64 DBWriter::QueueSize()
    {
        LockGuard<ThreadMutexLock> guard(m_queue_lock);
        return m_queue.size();
    }
    void DBWriter::SetNamespace(Context& ctx, const std::string& ns)
    {
        ctx.ns.SetString(ns, false);
        for (size_t i = 0; i < m_workers.size(); i++)
        {
            m_workers[i]->worker_ctx.ns.SetString(ns, false);
        }
    }

    void DBWriter::SetMasterClient(Context& ctx)
    {
        for (size_t i = 0; i < m_workers.size(); i++)
        {
            m_workers[i]->worker_ctx.client = ctx.client;
        }
    }

    void DBWriter::Clear()
    {
        for (size_t i = 0; i < m_workers.size(); i++)
        {
            m_workers[i]->worker_ctx.client = NULL;
        }
    }
    void DBWriter::SetDefaulFlags(CallFlags flags)
    {
        for (size_t i = 0; i < m_workers.size(); i++)
        {
            m_workers[i]->flags = flags;
        }
    }
    int DBWriter::Put(Context& ctx,RedisCommandFrame& cmd)
    {
        if(m_workers.empty())
        {
            g_db->Call(ctx, cmd);
            return 0;
        }
        Enqueue(cmd);
        return 0;
    }

    void DBWriter::Stop()
    {
        for (size_t i = 0; i < m_workers.size(); i++)
        {
            m_workers[i]->AdviceStop();
        }
        for (size_t i = 0; i < m_workers.size(); i++)
        {
            m_workers[i]->Join();
            DELETE(m_workers[i]);
        }
        m_workers.clear();
    }

    DBWriter::~DBWriter()
    {
        Stop();
    }

OP_NAMESPACE_END

