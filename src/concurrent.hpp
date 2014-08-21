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

#ifndef CONCURRENT_HPP_
#define CONCURRENT_HPP_
#include "common/common.hpp"
#include "thread/thread_mutex.hpp"
#include "thread/thread_mutex_lock.hpp"
#include "thread/thread_local.hpp"
#include "thread/lock_guard.hpp"
#include "thread/spin_rwlock.hpp"
#include "codec.hpp"
#include <stack>

OP_NAMESPACE_BEGIN
    struct Barrier: public ThreadMutexLock
    {
            volatile uint32 in_lock;
            volatile uint32 wait_num;
            volatile pthread_t pid;
            Barrier() :
                    in_lock(0), wait_num(0), pid(0)
            {
            }
            uint32 AddLockRef()
            {
                return atomic_add_uint32(&in_lock, 1);
            }
            uint32 DecLockRef()
            {
                return atomic_sub_uint32(&in_lock, 1);
            }
    };

    struct DBItemStackKey
    {
            DBID db;
            Slice key;
            DBItemStackKey(const DBID& id = 0, const Slice& k = "") :
                    db(id), key(k)
            {
            }
            bool operator<(const DBItemStackKey& other) const
            {
                if (db < other.db)
                {
                    return true;
                }
                if (db == other.db)
                {
                    return key.compare(other.key) < 0;
                }
                return false;
            }
    };
    struct DBItemKey
    {
            DBID db;
            std::string key;
            DBItemKey(const DBID& id = 0, const std::string& k = "") :
                    db(id), key(k)
            {
            }
            bool operator<(const DBItemKey& other) const
            {
                if (db < other.db)
                {
                    return true;
                }
                if (db == other.db)
                {
                    return key.compare(other.key) < 0;
                }
                return false;
            }
    };

    struct KeyLocker
    {
            typedef TreeMap<DBItemStackKey, Barrier*>::Type ThreadMutexLockTable;
            typedef std::stack<Barrier*> ThreadMutexLockStack;
            SpinRWLock m_barrier_lock;
            typedef ReadLockGuard<SpinRWLock> SpinReadLockGuard;
            typedef WriteLockGuard<SpinRWLock> SpinWriteLockGuard;
            ThreadMutexLockStack m_barrier_pool;
            ThreadMutexLockTable m_barrier_table;
            uint32 m_pool_size;
            KeyLocker() :
                    m_pool_size(0)
            {
            }
            void Init(uint32 thread_num)
            {
                m_pool_size = thread_num;
                for (uint32 i = 0; i < thread_num; i++)
                {
                    Barrier* lock = new Barrier;
                    m_barrier_pool.push(lock);
                }
            }
            ~KeyLocker()
            {
                while (!m_barrier_pool.empty())
                {
                    Barrier* p = m_barrier_pool.top();
                    delete p;
                    m_barrier_pool.pop();
                }
            }
            Barrier* AddLockKey(const DBID& db, const Slice& key)
            {
                if (m_pool_size <= 1)
                {
                    return NULL;
                }
                while (true)
                {
                    Barrier* barrier = NULL;
                    {
                        SpinWriteLockGuard guard(m_barrier_lock);
                        /*
                         * Merge find/insert operations into one 'insert' invocation
                         */
                        std::pair<ThreadMutexLockTable::iterator, bool> insert = m_barrier_table.insert(
                                std::make_pair(DBItemStackKey(db, key), (Barrier*) NULL));
                        if (!insert.second && NULL != insert.first->second)
                        {
                            barrier = insert.first->second;
                            if (barrier->pid == pthread_self())
                            {
                                barrier->AddLockRef();
                                return barrier;
                            }
                        }
                        else
                        {
                            if (!m_barrier_pool.empty())
                            {
                                barrier = m_barrier_pool.top();
                                m_barrier_pool.pop();
                            }
                            else
                            {
                                barrier = new Barrier;
                            }
                            barrier->AddLockRef();
                            barrier->pid = pthread_self();
                            insert.first->second = barrier;
                            return barrier;
                        }
                    }
                    if (NULL != barrier)
                    {
                        LockGuard<ThreadMutexLock> guard(*barrier);
                        if (barrier->in_lock > 0)
                        {
                            barrier->wait_num++;
                            barrier->Wait();
                            barrier->wait_num--;
                        }
                    }
                }
                return NULL;
            }
            void ClearLockKey(const DBID& db, const Slice& key, Barrier* barrier)
            {
                if (m_pool_size <= 1)
                {
                    return;
                }
                if (NULL != barrier)
                {
                    bool recycle = (barrier->DecLockRef() == 0);
                    if (recycle)
                    {
                        {
                            SpinWriteLockGuard guard(m_barrier_lock);
                            barrier->pid = 0;
                            m_barrier_table.erase(DBItemStackKey(db, key));
                        }
                        {
                            LockGuard<ThreadMutexLock> guard(*barrier);
                            if (barrier->wait_num > 0)
                            {
                                barrier->NotifyAll();
                            }
                        }
                        {
                            SpinWriteLockGuard guard(m_barrier_lock);
                            m_barrier_pool.push(barrier);
                        }

                    }
                }
            }
    };

    struct KeyLockerGuard
    {
            KeyLocker& locker;
            DBID db;
            Slice key;
            Barrier* barrier;
            KeyLockerGuard(KeyLocker& loc, const DBID& id, const Slice& k) :
                    locker(loc), db(id), key(k), barrier(NULL)
            {
                barrier = locker.AddLockKey(db, key);
            }
            ~KeyLockerGuard()
            {
                locker.ClearLockKey(db, key, barrier);
            }
    };
OP_NAMESPACE_END
#endif /* CONCURRENT_HPP_ */
