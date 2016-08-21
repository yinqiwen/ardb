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
#include "db/db.hpp"

OP_NAMESPACE_BEGIN
    int Ardb::Quit(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.GetReply().SetStatusCode(STATUS_OK);
        //-1 means close connection
        return -1;
    }
    int Ardb::Ping(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.GetReply().SetStatusCode(STATUS_PONG);
        return 0;
    }
    int Ardb::Echo(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.GetReply().SetString(cmd.GetArguments()[0]);
        return 0;
    }
    int Ardb::Select(Context& ctx, RedisCommandFrame& cmd)
    {
        if (cmd.GetArguments()[0].size() > ARDB_MAX_NAMESPACE_SIZE)
        {
            ctx.GetReply().SetErrorReason("too large namespace string length.");
            return 0;
        }
        if(!strcmp(TTL_DB_NSMAESPACE, cmd.GetArguments()[0].c_str()))
        {
            ctx.GetReply().SetErrorReason("Can NOT select TTL DB.");
            return 0;
        }
        ctx.ns.SetString(cmd.GetArguments()[0], false);
        ctx.GetReply().SetStatusCode(STATUS_OK);
        return 0;
    }

    int Ardb::Auth(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        if(GetConf().requirepass.empty())
        {
        	reply.SetErrorReason("Client sent AUTH, but no password is set");
        	return 0;
        }
        if (GetConf().requirepass != cmd.GetArguments()[0])
        {
            ctx.authenticated = false;
            reply.SetErrCode(ERR_AUTH_FAILED);
        }
        else
        {
            ctx.authenticated = true;
            reply.SetStatusCode(STATUS_OK);
        }
        return 0;
    }

OP_NAMESPACE_END

