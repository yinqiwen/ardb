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
#include "ardb.hpp"

OP_NAMESPACE_BEGIN

    int Ardb::Get(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& key = cmd.GetArguments()[0];
        KeyObject k(ctx.currentDB, V_STRING, key);
        ctx.reply.type = REDIS_REPLY_STRING;
        int err = m_engine.Get(k, ctx.reply.str);
        if (err == ERR_ENTRY_NOT_EXIST)
        {
            ctx.reply.type = REDIS_REPLY_NIL;
        }
        else if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Ardb::MGet(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.reply.type = REDIS_REPLY_ARRAY;
        KeyObjectArray ks;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            KeyObject k(ctx.currentDB, V_STRING, cmd.GetArguments()[i]);
            ks.push_back(k);
        }

        return 0;
    }

    int Ardb::Append(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& append = cmd.GetArguments()[1];
        KeyObject key(ctx.currentDB, V_STRING, cmd.GetArguments()[0]);
        MergeOp op;
        op.op = cmd.GetType();
        op.val = append;
        int err = m_engine.Merge(key, op);
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        else
        {

        }
        return 0;
    }

    int Ardb::GenericSet(Context& ctx, const Slice& key, const Slice& value, GenericSetOptions& options)
    {
        return 0;
    }

    int Ardb::PSetEX(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    int Ardb::MSet(Context& ctx, RedisCommandFrame& cmd)
    {
        if (cmd.GetArguments().size() % 2 != 0)
        {
            fill_error_reply(ctx.reply, "wrong number of arguments for MSET");
            return 0;
        }
        GenericSetOptions options;
        if (cmd.GetType() == REDIS_CMD_MSETNX)
        {
            options.with_nx = true;
        }
        BatchWriteGuard guard(ctx);
        for (uint32 i = 0; i < cmd.GetArguments().size(); i += 2)
        {
            int err = GenericSet(ctx, cmd.GetArguments()[i], cmd.GetArguments()[i + 1], options);
            if (0 != err || ctx.reply.type == REDIS_REPLY_NIL)
            {
                guard.MarkFailed();
                break;
            }
        }
        if (cmd.GetType() == REDIS_CMD_MSETNX)
        {
            fill_int_reply(ctx.reply, guard.Success() ? 1 : 0);
        }
        else
        {
            fill_status_reply(ctx.reply, "OK");
        }
        return 0;
    }

    int Ardb::MSetNX(Context& ctx, RedisCommandFrame& cmd)
    {
        return MSet(ctx, cmd);
    }

    int Ardb::IncrbyFloat(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::IncrDecrCommand(Context& ctx, const Slice& key, int64 incr)
    {
        return 0;
    }

    int Ardb::Incrby(Context& ctx, RedisCommandFrame& cmd)
    {
    }

    int Ardb::Incr(Context& ctx, RedisCommandFrame& cmd)
    {
    }

    int Ardb::Decrby(Context& ctx, RedisCommandFrame& cmd)
    {
    }

    int Ardb::Decr(Context& ctx, RedisCommandFrame& cmd)
    {

    }

    int Ardb::GetSet(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::GetRange(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::Set(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& key = cmd.GetArguments()[0];
        const std::string& value = cmd.GetArguments()[1];
    }

    int Ardb::SetEX(Context& ctx, RedisCommandFrame& cmd)
    {

    }
    int Ardb::SetNX(Context& ctx, RedisCommandFrame& cmd)
    {
    }
    int Ardb::SetRange(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }
    int Ardb::Strlen(Context& ctx, RedisCommandFrame& cmd)
    {
    }

OP_NAMESPACE_END

