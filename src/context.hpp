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
    };

    struct ClientContext
    {
            std::string name;
            Channel* client;
            int64 uptime;
            int64 last_interaction_ustime;
            bool authenticated;
            bool processing;
    };

    struct TransactionContext
    {

    };

    struct Context
    {
            CallFlags flags;
            Data ns;
            RedisReply reply;

            ClientContext* client;
            TransactionContext* transc;
            int dirty;

            int transc_err;
            int64 sequence;  //recv command sequence in the server, start from 1
            Context() :
                    client(NULL), transc(NULL), dirty(0), transc_err(0), sequence(0)
            {
                ns.SetInt64(0);
            }
            void RewriteClientCommand(const RedisCommandFrame& cmd)
            {

            }
            RedisReply& GetReply();
            ~Context()
            {
                //DELETE(client);
            }
    };

OP_NAMESPACE_END

#endif /* CONTEXT_HPP_ */
