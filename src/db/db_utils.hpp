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

#ifndef DB_UTILS_HPP_
#define DB_UTILS_HPP_

#include "common/common.hpp"
#include "codec.hpp"
#include "context.hpp"
#include "thread/thread_local.hpp"
#include "thread/thread_mutex_lock.hpp"
#include "thread/spin_rwlock.hpp"
#include "thread/thread_mutex_lock.hpp"
#include "thread/event_condition.hpp"
#include "util/concurrent_queue.hpp"

#define ENGINE_ERR(err)  (kEngineNotFound == err ? ERR_ENTRY_NOT_EXIST:(0 == err ? 0 :(err + STORAGE_ENGINE_ERR_OFFSET)))
#define ENGINE_NERR(err)  (kEngineNotFound == err ? 0:(0 == err ? 0 :(err + STORAGE_ENGINE_ERR_OFFSET)))

OP_NAMESPACE_BEGIN

    struct DBLocalContext
    {
            Buffer encode_buffer_cache;
            std::string string_cache;
            Buffer& GetEncodeBufferCache();
            Slice GetSlice(const KeyObject& key);
            void GetSlices(const KeyObject& key, const ValueObject& val, Slice ss[2]);
            std::string& GetStringCache()
            {
                string_cache.clear();
                return string_cache;
            }
            virtual ~DBLocalContext()
            {
            }
    };

    /*
     *  A multi thread db writer, which could do db write operations by several threads to increase
     *  write performance.
     *  It's used in loading snapshot.
     */
    class DBWriterWorker;
    class DBWriter
    {
        private:
            std::vector<DBWriterWorker*> m_workers;
            ThreadMutexLock m_queue_lock;
            std::deque<RedisCommandFrame*> m_queue;
            void Enqueue(RedisCommandFrame& cmd);
            RedisCommandFrame* Dequeue(int timeout);
            friend class DBWriterWorker;
        public:
            DBWriter();
            void Init(int workers);
            int Put(Context& ctx, const Data& ns, const Slice& key, const Slice& value);
            int Put(Context& ctx, const KeyObject& k, const ValueObject& value);
            int Put(Context& ctx,RedisCommandFrame& cmd);
            void SetNamespace(Context& ctx, const std::string& ns);
            void SetDefaulFlags(CallFlags flags);
            void SetMasterClient(Context& ctx);
            int64 QueueSize();
            void Stop();
            void Clear();
            ~DBWriter();
    };

OP_NAMESPACE_END

#endif /* SRC_DB_DB_UTILS_HPP_ */
