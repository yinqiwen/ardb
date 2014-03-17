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

#include "ardb_server.hpp"

namespace ardb
{
    int ArdbServer::Quit(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        fill_status_reply(ctx.reply, "OK");
        return -1;
    }
    int ArdbServer::Ping(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        fill_status_reply(ctx.reply, "PONG");
        return 0;
    }
    int ArdbServer::Echo(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        fill_str_reply(ctx.reply, cmd.GetArguments()[0]);
        return 0;
    }
    int ArdbServer::Select(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        if (!string_touint32(cmd.GetArguments()[0], ctx.currentDB) || ctx.currentDB > 0xFFFFFF)
        {
            fill_error_reply(ctx.reply, "ERR value is not an integer or out of range");
            return 0;
        }
        m_clients_holder.ChangeCurrentDB(ctx.conn, ctx.currentDB);
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }
}

