/*
 *Copyright (c) 2013-2018, yinqiwen <yinqiwen@gmail.com>
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
#include "db/db.hpp"

OP_NAMESPACE_BEGIN

    struct ContexWithReply
    {
            Context* ctx;
            RedisReply* reply;
            ContexWithReply(Context* c, RedisReply* r)
                    : ctx(c), reply(r)
            {
            }
            ~ContexWithReply()
            {
                DELETE(reply);
            }
    };

    void Ardb::AsyncUnblockKeysCallback(Channel* ch, void * data)
    {
        ContexWithReply* c = (ContexWithReply*) data;
        if (NULL == ch || ch->IsClosed())
        {
            DELETE(c);
            return;
        }
        g_db->UnblockKeys(*(c->ctx), true, c->reply);
        DELETE(c);
    }

    int Ardb::UnblockKeys(Context& ctx, bool sync, RedisReply* reply)
    {
        if (ctx.keyslocked)
        {
            FATAL_LOG("Can not modify block dataset when key locked.");
        }
        if (!sync)
        {
            ctx.client->client->GetService().AsyncIO(ctx.client->client->GetID(), AsyncUnblockKeysCallback,
                    new ContexWithReply(&ctx, reply));
            return 0;
        }
        if (NULL != reply)
        {
            Channel* ch = ctx.client->client;
            ch->Write(*reply);
        }
        LockGuard<SpinMutexLock> guard(m_block_keys_lock, true);
        if (ctx.bpop != NULL && !m_blocked_ctxs.empty())
        {
            BlockingState::BlockKeyTable::iterator it = ctx.GetBPop().keys.begin();
            while (it != ctx.GetBPop().keys.end())
            {
                const KeyPrefix& prefix = it->first;
                BlockedContextTable::iterator blocked_found = m_blocked_ctxs.find(prefix);
                if (blocked_found != m_blocked_ctxs.end())
                {
                    ContextSet& blocked_set = blocked_found->second;
                    blocked_set.erase(&ctx);
                    if (blocked_set.empty())
                    {
                        m_blocked_ctxs.erase(blocked_found);
                    }
                }
                it++;
            }
            ctx.ClearBPop();
        }
        ctx.client->client->UnblockRead();
        return 0;
    }

    int Ardb::BlockForKeys(Context& ctx, const StringArray& keys, const AnyArray& vals, KeyType ktype, uint32 mstimeout)
    {
        if (ctx.keyslocked)
        {
            FATAL_LOG("Can not modify block dataset when key locked.");
        }
        if (mstimeout > 0)
        {
            ctx.GetBPop().timeout = (uint64) mstimeout * 1000 + get_current_epoch_micros();
        }
        ctx.GetBPop().block_keytype = ktype;
        ctx.client->client->BlockRead();
        LockGuard<SpinMutexLock> guard(m_block_keys_lock);
        for (size_t i = 0; i < keys.size(); i++)
        {
            KeyPrefix prefix;
            prefix.ns = ctx.ns;
            prefix.key.SetString(keys[i], false);
            if(vals.size() != keys.size())
            {
                ctx.GetBPop().keys.insert(BlockingState::BlockKeyTable::value_type(prefix, NULL));
            }else
            {
                ctx.GetBPop().keys.insert(BlockingState::BlockKeyTable::value_type(prefix, vals[i]));
            }
            m_blocked_ctxs[prefix].insert(&ctx);
        }
        return 0;
    }

    int Ardb::SignalKeyAsReady(Context& ctx, const std::string& key)
    {
        if (ctx.keyslocked)
        {
            FATAL_LOG("Can not modify block dataset when key locked.");
        }
        LockGuard<SpinMutexLock> guard(m_block_keys_lock, !ctx.flags.block_keys_locked);
        KeyPrefix prefix;
        prefix.ns = ctx.ns;
        prefix.key.SetString(key, false);
        if (m_blocked_ctxs.find(prefix) == m_blocked_ctxs.end())
        {
            return -1;
        }
        if (NULL == m_ready_keys)
        {
            NEW(m_ready_keys, ReadyKeySet);
        }
        m_ready_keys->insert(prefix);
        return 0;
    }
    int Ardb::WakeClientsBlockingOnKeys(Context& ctx)
    {
        if (NULL == m_ready_keys)
        {
            return 0;
        }
        ReadyKeySet ready_keys;
        {
            LockGuard<SpinMutexLock> guard(m_block_keys_lock);
            if (NULL == m_ready_keys)
            {
                return 0;
            }
            if (m_ready_keys->empty())
            {
                return 0;
            }
            ready_keys = *m_ready_keys;
            DELETE(m_ready_keys);
        }
        ReadyKeySet::iterator sit = ready_keys.begin();
        while (sit != ready_keys.end())
        {
            const KeyPrefix& ready_key = *sit;
            {
                LockGuard<SpinMutexLock> block_guard(m_block_keys_lock);
                ctx.flags.block_keys_locked = 1;
                BlockedContextTable::iterator fit = m_blocked_ctxs.find(ready_key);
                if (fit != m_blocked_ctxs.end())
                {
                    ContextSet& wait_set = fit->second;
                    ContextSet::iterator cit = wait_set.begin();
                    while (cit != wait_set.end())
                    {
                        Context* unblock_client = *cit;
                        if (NULL != unblock_client->bpop)
                        {
                            int err = 0;
                            switch(unblock_client->bpop->block_keytype)
                            {
                                case KEY_LIST:
                                {
                                    err = WakeClientsBlockingOnList(ctx, ready_key, *unblock_client);
                                    break;
                                }
                                case KEY_ZSET:
                                {
                                    err = WakeClientsBlockingOnZSet(ctx, ready_key, *unblock_client);
                                    break;
                                }
                                case KEY_STREAM:
                                {
                                    err = WakeClientsBlockingOnStream(ctx, ready_key, *unblock_client);
                                    break;
                                }
                                default:
                                {
                                    ERROR_LOG("Not support block on keytype:%u", unblock_client->bpop->block_keytype);
                                    break;
                                }
                            }
                            if (0 == err)
                            {
                                cit = wait_set.erase(cit);
                                //ServeClientBlockedOnList(*unblock_client, ready_key, pop_value);
                            }
                            else
                            {
                                cit++;
                            }
                        }
                    }
                }
            }
            sit++;
        }
        return 0;
    }
OP_NAMESPACE_END

