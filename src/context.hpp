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
#include "rocksdb_engine.hpp"
#include "thread/thread_local.hpp"
#include "channel/all_includes.hpp"
#include "concurrent.hpp"
#include "codec.hpp"

using namespace ardb::codec;
OP_NAMESPACE_BEGIN
    struct CallFlags
    {
            unsigned no_wal :1;
            unsigned no_fill_reply :1;
            unsigned create_if_notexist :1;
            unsigned fuzzy_check :1;
            CallFlags() :
                    no_wal(0), no_fill_reply(0), create_if_notexist(0), fuzzy_check(0)
            {
            }
    };

    struct ClientContext
    {
            bool processing;
            std::string name;
            Channel* client;
            int64 uptime;
            int64 last_interaction_ustime;
            ClientContext() :
                    processing(false), client(NULL), uptime(0), last_interaction_ustime(0)
            {
            }
    };

    struct TransactionContext
    {
            bool abort;
            RedisCommandFrameArray cached_cmds;

            TransactionContext() :
                    abort(false)
            {
            }
    };
    struct PubSubContext
    {
            StringSet pubsub_channels;
            StringSet pubsub_patterns;
    };

    class Context
    {
        private:
            RedisReply *reply;
        public:
            bool authenticated;
            CallFlags flags;
            Data ns;

            RedisCommandFrame* current_cmd;
            ClientContext* client;
            TransactionContext* transc;
            PubSubContext* pubsub;
            int dirty;

            int transc_err;
            int64 sequence;  //recv command sequence in the server, start from 1
            Context() :
                    authenticated(true), reply(NULL), current_cmd(NULL), client(NULL), transc(NULL), pubsub(NULL), dirty(0), transc_err(0), sequence(0)
            {
                ns.SetInt64(0);
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
            void ClearState()
            {
                SetReply(NULL);
                current_cmd = NULL;
            }
            void ClearTransaction()
            {
                DELETE(transc);
            }
            void ClearPubsub()
            {
                DELETE(pubsub);
            }
            bool InTransaction()
            {
                return transc != NULL;
            }
            bool IsSubscribed()
            {
                return pubsub != NULL;
            }
            TransactionContext& GetTransaction()
            {
                if (NULL == transc)
                {
                    NEW(transc, TransactionContext);
                }
                return *transc;
            }
            PubSubContext& GetPubsub()
            {
                if (NULL == pubsub)
                {
                    NEW(pubsub, PubSubContext);
                }
                return *pubsub;
            }
            ~Context()
            {
                SetReply(NULL);
            }
    };

OP_NAMESPACE_END

#endif /* CONTEXT_HPP_ */
