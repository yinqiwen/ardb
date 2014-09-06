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
    int Ardb::Quit(Context& ctx, RedisCommandFrame& cmd)
    {
        fill_status_reply(ctx.reply, "OK");
        return -1;
    }
    int Ardb::Ping(Context& ctx, RedisCommandFrame& cmd)
    {
        fill_status_reply(ctx.reply, "PONG");
        return 0;
    }
    int Ardb::Echo(Context& ctx, RedisCommandFrame& cmd)
    {
        fill_str_reply(ctx.reply, cmd.GetArguments()[0]);
        return 0;
    }
    int Ardb::Select(Context& ctx, RedisCommandFrame& cmd)
    {
        uint32 newdb = 0;
        if (!string_touint32(cmd.GetArguments()[0], newdb) || newdb > 0xFFFFFF)
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        ctx.currentDB = newdb;
        fill_status_reply(ctx.reply, "OK");
        LockGuard<SpinMutexLock> guard(m_cached_dbids_lock);
        m_cached_dbids.insert(newdb);
        return 0;
    }

    int Ardb::Auth(Context& ctx, RedisCommandFrame& cmd)
    {
        if (m_cfg.requirepass.empty())
        {
            fill_error_reply(ctx.reply, "Client sent AUTH, but no password is set");
        }
        else if (m_cfg.requirepass != cmd.GetArguments()[0])
        {
            ctx.authenticated = false;
            fill_error_reply(ctx.reply, "invalid password");
        }
        else
        {
            ctx.authenticated = true;
            fill_status_reply(ctx.reply, "OK");
        }
        return 0;
    }

OP_NAMESPACE_END

