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

#ifndef CONTEXT_HPP_
#define CONTEXT_HPP_
#include "common/common.hpp"
#include "thread/thread_local.hpp"
#include "channel/all_includes.hpp"
#include "types.hpp"

#define ARDB_MAX_NAMESPACE_SIZE  512

using namespace ardb::codec;
OP_NAMESPACE_BEGIN
    struct CallFlags
    {
            unsigned no_wal :1;
            unsigned no_fill_reply :1;
            unsigned create_if_notexist :1;
            unsigned fuzzy_check :1;
            unsigned redis_compatible :1;
            unsigned iterate_multi_keys :1;
            unsigned iterate_no_upperbound :1;
            unsigned iterate_total_order :1; //if iterator need invoke JumpToLast/Prev later, this flag should be set to 1

            /*
             * caller type
             */
            unsigned slave :1;
            unsigned lua :1;
            unsigned pubsub :1;
            unsigned bulk_loading :1;
            unsigned reply_off :1;
            unsigned reply_skip :1;
            unsigned block_keys_locked :1;
            CallFlags() :
                    no_wal(0), no_fill_reply(0), create_if_notexist(0), fuzzy_check(0), redis_compatible(0), iterate_multi_keys(
                            0), iterate_no_upperbound(0), iterate_total_order(0), slave(0), lua(0), pubsub(0), bulk_loading(
                            0), reply_off(0), reply_skip(0), block_keys_locked(0)
            {
            }
    };

    struct KeyPrefix
    {
            Data ns;
            Data key;
            bool operator<(const KeyPrefix& other) const
            {
                int cmp = ns.Compare(other.ns);
                if (cmp < 0)
                {
                    return true;
                }
                if (cmp > 0)
                {
                    return false;
                }
                return key < other.key;
            }
            void Clear()
            {
                ns.Clear();
                key.Clear();
            }
            bool IsNil() const
            {
                return ns.IsNil() && key.IsNil();
            }
    };
    typedef TreeSet<KeyPrefix>::Type KeyPrefixSet;

    class Context;
    struct ClientId
    {
            uint32 id;
            Context* ctx;
            ClientId() :
                    id(0), ctx(NULL)
            {
            }
            bool operator<(const ClientId& other) const
            {
                int64 cmp = (int64) id - (int64) other.id;
                if (cmp < 0)
                {
                    return true;
                }
                if (cmp > 0)
                {
                    return false;
                }
                return ctx < other.ctx;
            }
    };

    struct ClientContext
    {
            bool processing;
            std::string name;
            Channel* client;
            ClientId clientid;
            int64 uptime;
            int64 last_interaction_ustime;
            int64 resume_ustime;
            ClientContext() :
                    processing(false), client(NULL), uptime(0), last_interaction_ustime(0), resume_ustime(-1)
            {
            }
    };

    struct TransactionContext
    {
            bool started;
            bool abort;
            bool cas;
            RedisCommandFrameArray cached_cmds;
            typedef TreeSet<KeyPrefix>::Type WatchKeySet;
            WatchKeySet watched_keys;
            TransactionContext() :
                    started(false), abort(false), cas(false)
            {
            }
    };
    struct PubSubContext
    {
            StringTreeSet pubsub_channels;
            StringTreeSet pubsub_patterns;
    };

    struct BlockingState
    {
            typedef TreeSet<KeyPrefix>::Type BlockKeySet;
            BlockKeySet keys;
            KeyPrefix target;
            uint64 timeout;
            BlockingState() :
                    timeout(0)
            {
            }
    };

    class Context
    {
        private:
            RedisReply *reply;
        public:
            Data ns;
            ClientContext* client;
            TransactionContext* transc;
            PubSubContext* pubsub;
            BlockingState* bpop;
            RedisCommandFrame* current_cmd;

            int dirty;
            uint32 last_cmdtype;
            int transc_err;
            CallFlags flags;
            bool authenticated;
            bool keyslocked;

            const void* engine_snapshot;

            Context() :
                    reply(NULL), client(NULL), transc(NULL), pubsub(
                    NULL), bpop(NULL), current_cmd(NULL), dirty(0), last_cmdtype(REDIS_CMD_INVALID), transc_err(0), authenticated(
                            true), keyslocked(false), engine_snapshot(NULL)
            {
                ns.SetString("0", false);
            }
            void RewriteClientCommand(const RedisCommandFrame& cmd)
            {
                if (NULL != current_cmd)
                {
                    *current_cmd = cmd;
                }
            }
            void SetReply(RedisReply* r)
            {
                if (NULL != reply && !reply->IsPooled())
                {
                    DELETE(reply);
                }
                reply = r;
            }
            RedisReply& GetReply()
            {
                if (NULL == reply)
                {
                    NEW(reply, RedisReply);
                }
                return *reply;
            }
            void ClearFlags()
            {
                /*
                 * The first 8bit are caller flags which need to be clear
                 */
                ((char*) (&flags))[0] = 0;
            }
            void ClearState()
            {
                SetReply(NULL);
                current_cmd = NULL;
                ClearFlags();
                dirty = 0;
            }
            void ClearTransaction()
            {
                DELETE(transc);
            }
            void ClearPubsub()
            {
                DELETE(pubsub);
            }
            void ClearBPop()
            {
                DELETE(bpop);
            }
            bool InTransaction()
            {
                return transc != NULL && transc->started;
            }
            bool IsSubscribed()
            {
                return pubsub != NULL;
            }
            bool IsBlocking()
            {
                return NULL != bpop && !bpop->keys.empty();
            }
            BlockingState& GetBPop()
            {
                if (NULL == bpop)
                {
                    NEW(bpop, BlockingState);
                }
                return *bpop;
            }
            TransactionContext& GetTransaction()
            {
                if (NULL == transc)
                {
                    NEW(transc, TransactionContext);
                }
                return *transc;
            }
            bool AbortTransaction()
            {
                if (InTransaction())
                {
                    GetTransaction().abort = true;
                    return true;
                }
                return false;
            }
            PubSubContext& GetPubsub()
            {
                if (NULL == pubsub)
                {
                    NEW(pubsub, PubSubContext);
                }
                return *pubsub;
            }
            int64_t SubscriptionsCount()
            {
                if (NULL == pubsub)
                {
                    return 0;
                }
                return pubsub->pubsub_channels.size() + pubsub->pubsub_patterns.size();
            }
            ~Context()
            {
                SetReply(NULL);
            }
    };
    typedef TreeSet<Context*>::Type ContextSet;
    typedef TreeSet<ClientId>::Type ClientIdSet;

OP_NAMESPACE_END

#endif /* CONTEXT_HPP_ */
