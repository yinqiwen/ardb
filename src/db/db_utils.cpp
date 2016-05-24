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

    struct DBWriteOperation
    {
            RedisCommandFrame cmd;
            uint8 type;
    };
    struct DBWriterWorker: public Thread
    {
            Context worker_ctx;
            SPSCQueue<DBWriteOperation*> write_queue;
            EventCondition event_cond;
            volatile uint32 queue_size;
            bool running;
            DBWriterWorker() :queue_size(0), running(true)
            {
                worker_ctx.flags.create_if_notexist = 1;
                worker_ctx.flags.bulk_loading = 1;
            }
            void Run()
            {
                while (running)
                {
                    DBWriteOperation* op = NULL;
                    int count = 0;
                    while (write_queue.Pop(op))
                    {
                        count++;
                        switch (op->type)
                        {
                            case ARDB_CMD_OP:
                            {
                                g_db->Call(worker_ctx, op->cmd);
                                break;
                            }
                            case ARDB_CKP_OP:
                            {
                                //g_engine->CommitWriteBatch(worker_ctx);
                                event_cond.Notify();
                                break;
                            }
                            default:
                            {
                                ERROR_LOG("Invalid operation:%d", op->type);
                                break;
                            }
                        }
                        DELETE(op);
                        atomic_sub_uint32(&queue_size, 1);
                    }
                    if (count == 0)
                    {
                        Thread::Sleep(1, MILLIS);
                    }
                }
            }
            void AdviceStop()
            {
                WaitBatchWrite();
                running = false;
            }
            void WaitBatchWrite()
            {
                while (queue_size > 0)
                {
                    DBWriteOperation* end = NULL;
                    NEW(end, DBWriteOperation);
                    end->type = ARDB_CKP_OP;
                    atomic_add_uint32(&queue_size, 1);
                    write_queue.Push(end);
                    event_cond.Wait();
                }
            }
            void Offer(DBWriteOperation* op)
            {
                atomic_add_uint32(&queue_size, 1);
                write_queue.Push(op);
                if (queue_size >= 10000)
                {
                    WaitBatchWrite();
                }
            }
            void Offer(RedisCommandFrame& cmd)
            {
                DBWriteOperation* op = NULL;
                NEW(op, DBWriteOperation);
                op->cmd = cmd;
                op->type = ARDB_CMD_OP;
                Offer(op);
            }
    };

    DBWriter::DBWriter(int workers) :
            m_cursor(0)
    {
        if (workers > 1)
        {
            for (size_t i = 0; i < workers; i++)
            {
                DBWriterWorker* worker = NULL;
                NEW(worker, DBWriterWorker);
                worker->Start();
                m_workers.push_back(worker);
            }
        }
    }

    DBWriterWorker* DBWriter::GetWorker()
    {
        if (m_cursor >= m_workers.size())
        {
            m_cursor = 0;
        }
        DBWriterWorker* worker = m_workers[m_cursor];
        m_cursor++;
        return worker;
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

