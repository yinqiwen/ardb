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

#include "../repl/snapshot.hpp"
#include "db/db.hpp"
#include "coro/coro_channel.hpp"

OP_NAMESPACE_BEGIN

    bool Ardb::MarkRestoring(Context& ctx, bool enable)
    {
        RedisReply& reply = ctx.GetReply();
        LockGuard<SpinMutexLock> guard(m_restoring_lock);
        if (enable)
        {
            if (NULL == m_restoring_nss)
            {
                NEW(m_restoring_nss, DataSet);
            }
            if (m_restoring_nss->count(ctx.ns) > 0)
            {
                reply.SetErrorReason("current db already restoring.");
                return false;
            }
            m_restoring_nss->insert(ctx.ns);
        }
        else
        {
            if (NULL == m_restoring_nss || m_restoring_nss->erase(ctx.ns) == 0)
            {
                reply.SetErrorReason("current db is not restoring");
                return false;
            }
            if (m_restoring_nss->empty())
            {
                DELETE(m_restoring_nss);
            }
        }
        reply.SetStatusCode(STATUS_OK);
        return true;
    }
    bool Ardb::IsRestoring(Context& ctx, const Data& ns)
    {
        if (NULL == m_restoring_nss)
        {
            return false;
        }
        LockGuard<SpinMutexLock> guard(m_restoring_lock);
        if (NULL == m_restoring_nss)
        {
            return false;
        }
        return m_restoring_nss->count(ns) > 0;
    }

    int Ardb::RestoreChunk(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.flags.create_if_notexist = 1;
        ObjectBuffer obuffer(cmd.GetArguments()[0]);
        if (obuffer.ArdbLoad(ctx))
        {
            ctx.dirty++;
            ctx.GetReply().SetStatusCode(STATUS_OK);
        }
        else
        {
            ctx.GetReply().SetErrorReason("Bad chunk format");
        }
        return 0;
    }

    /*
     * RESTOREDB  START|END|ABORT
     */
    int Ardb::RestoreDB(Context& ctx, RedisCommandFrame& cmd)
    {
        if (!strcasecmp(cmd.GetArguments()[0].c_str(), "start"))
        {
            if (!MarkRestoring(ctx, true))
            {
                return 0;
            }
            FlushDB(ctx, ctx.ns);
            RedisCommandFrame flushdb("flushdb");
            flushdb.AddArg(ctx.ns.AsString());
            ctx.RewriteClientCommand(flushdb);
            INFO_LOG("RestoreDB %s started, flush it first.", ctx.ns.AsString().c_str());
        }
        else if (!strcasecmp(cmd.GetArguments()[0].c_str(), "end"))
        {
            if (!MarkRestoring(ctx, false))
            {
                return 0;
            }
            INFO_LOG("RestoreDB %s completed.", ctx.ns.AsString().c_str());
        }
        else if (!strcasecmp(cmd.GetArguments()[0].c_str(), "abort"))
        {
            if (!MarkRestoring(ctx, false))
            {
                return 0;
            }
            FlushDB(ctx, ctx.ns);
            RedisCommandFrame flushdb("flushdb");
            flushdb.AddArg(ctx.ns.AsString());
            ctx.RewriteClientCommand(flushdb);
            INFO_LOG("RestoreDB %s aborted, flush broken content.", ctx.ns.AsString().c_str());
        }
        else
        {
            ctx.GetReply().SetErrCode(ERR_INVALID_SYNTAX);
        }
        return 0;
    }

    struct MigrateDBContext
    {
            ChannelService* io_serv;
            uint32 clientid;
            std::string host;
            uint16 port;
            uint32 timeout;
            Data src_db;
            Data dst_db;
            bool copy;

            MigrateDBContext() :
                    io_serv(NULL), clientid(0), port(0), timeout(1000), copy(false)
            {
            }
    };

    void Ardb::MigrateDBCoroTask(void* data)
    {
        Context migtare_dbctx;
        MigrateDBContext* ctx = (MigrateDBContext*) data;
        RedisReply r;
        Channel* src_client = NULL;
        RedisReply* restore_reply = NULL;
        RedisReplyArray* migrate_reply = NULL;
        migtare_dbctx.ns = ctx->src_db;
        bool migrate_success = false;
        RedisCommandFrameArray commands;
        RedisCommandFrame rawset;
        Buffer buffer;
        ObjectBuffer obuffer;
        KeyObject startkey(ctx->src_db, KEY_META, "");
        Iterator* iter = NULL;
        CoroRedisClient redis_client(ctx->io_serv->NewClientSocketChannel());
        redis_client.Init();
        commands.resize(2);
        SocketHostAddress remote_address(ctx->host, ctx->port);
        if (!redis_client.SyncConnect(&remote_address, ctx->timeout))
        {
            goto _coro_exit;
        }

        //1. select & restordb & flushdb first
        commands[0].SetFullCommand("select %s", ctx->dst_db.AsString().c_str());
        commands[1].SetFullCommand("restoredb start");
        migrate_reply = redis_client.SyncMultiCall(commands, ctx->timeout);
        if (NULL == migrate_reply || migrate_reply->size() != commands.size())
        {
            goto _coro_exit;
        }
        for (size_t i = 0; i < migrate_reply->size(); i++)
        {
            if (NULL == migrate_reply->at(i) || migrate_reply->at(i)->IsErr())
            {
                if (NULL != migrate_reply->at(i))
                {
                    WARN_LOG("Migrate chunk failed with reason:%s", restore_reply->Error().c_str());
                    r.SetErrorReason(migrate_reply->at(i)->Error());
                }
                goto _coro_exit;
            }
        }

        //2. iterate all kv and send them to remote
        migtare_dbctx.flags.iterate_multi_keys = 1;
        migtare_dbctx.flags.iterate_no_upperbound = 1;
        migtare_dbctx.flags.iterate_total_order = 1;
        iter = g_db->GetEngine()->Find(migtare_dbctx, startkey);

        migrate_success = true;
        while (iter->Valid())
        {
            KeyObject& k = iter->Key();
            int64 ttl = 0;
            if (iter->Key().GetType() == KEY_META)
            {
                ttl = iter->Value().GetTTL();
            }
            obuffer.ArdbSaveRawKeyValue(iter->RawKey(), iter->RawValue(), buffer, ttl);
            iter->Next();
            /*
             * if uncompressed chunk size greater than 512KB or last uncompressed chunk, generate 'RestoreChunk <chunk>' to target host
             */
            if (buffer.ReadableBytes() >= 512 * 1024 || !iter->Valid())
            {
                obuffer.ArdbFlushWriteBuffer(buffer);
                rawset.Clear();
                rawset.SetCommand("RestoreChunk");
                rawset.ReserveArgs(1);
                rawset.GetMutableArgument(0)->assign(obuffer.GetInternalBuffer().GetRawReadBuffer(), obuffer.GetInternalBuffer().ReadableBytes());
                obuffer.Reset();
                restore_reply = redis_client.SyncCall(rawset, ctx->timeout);
                if (NULL == restore_reply || restore_reply->IsErr())
                {
                    migrate_success = false;
                    if (NULL != restore_reply)
                    {
                        r.SetErrorReason(restore_reply->Error());
                    }
                    break;
                }
            }
        }
        DELETE(iter);
        _coro_exit: commands[1].SetFullCommand("restoredb %s", migrate_success ? "end" : "abort");
        redis_client.SyncCall(commands[1], ctx->timeout);
        redis_client.Close();
        if (migrate_success)
        {
            r.SetStatusCode(STATUS_OK);
        }
        else if (r.IsNil())
        {
            r.SetErrorReason("migrate db failed.");
        }
        src_client = ctx->io_serv->GetChannel(ctx->clientid);
        if (NULL != src_client)
        {
            src_client->UnblockRead();
            src_client->Write(r);
        }
        DELETE(ctx);
    }

    /*
     * this command only works with ardb instances. it would copy current db's data and send to remote instance.
     * syntax:  MigrateDB host port destination-db timeout
     *
     */
    int Ardb::MigrateDB(Context& ctx, RedisCommandFrame& cmd)
    {
        MigrateDBContext* migrate_ctx = NULL;
        RedisReply& reply = ctx.GetReply();
        const std::string& host = cmd.GetArguments()[0];
        uint32 port;
        int copy = 0, replace = 0;
        int64 timeout;
        if (!string_toint64(cmd.GetArguments()[3], timeout) || !string_touint32(cmd.GetArguments()[1], port))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }
        if (timeout < 0)
        {
            timeout = 0;
        }
        NEW(migrate_ctx, MigrateDBContext);
        migrate_ctx->io_serv = &(ctx.client->client->GetService());
        migrate_ctx->clientid = ctx.client->client->GetID();
        migrate_ctx->host = host;
        migrate_ctx->port = port;
        migrate_ctx->copy = copy;
        migrate_ctx->src_db = ctx.ns;
        migrate_ctx->dst_db.SetString(cmd.GetArguments()[2], false);
        migrate_ctx->timeout = timeout;
        ctx.client->client->BlockRead(); //do not read any data from client until the migrate task finish
        Scheduler::CurrentScheduler().StartCoro(0, MigrateDBCoroTask, migrate_ctx);
        reply.type = 0; //let coroutine task to reply client
        return 0;
    }

    struct MigrateContext
    {
            ChannelService* io_serv;
            uint32 clientid;
            std::string host;
            uint16 port;
            uint32 timeout;
            KeyObjectArray keys;
            Data src_db;
            Data destination_db;
            bool copy;
            bool replace;

            MigrateContext() :
                    io_serv(NULL), clientid(0), port(0), timeout(1000), copy(false), replace(false)
            {
            }
    };

    void Ardb::MigrateCoroTask(void* data)
    {
        Context migtare_dbctx;
        RedisCommandFrameArray commands;
        RedisReply r;
        Channel* src_client = NULL;
        KeyObjectArray lock_keys;
        RedisReplyArray* migrate_reply = NULL;
        MigrateContext* ctx = (MigrateContext*) data;
        migtare_dbctx.ns = ctx->src_db;
        CoroRedisClient redis_client(ctx->io_serv->NewClientSocketChannel());
        redis_client.Init();
        SocketHostAddress remote_address(ctx->host, ctx->port);

        KeysLockGuard guard(migtare_dbctx, ctx->keys);
        bool migrate_success = false;
        //printf("####connecting %s %d\n", ctx->host.c_str(),ctx->port);
        if (!redis_client.SyncConnect(&remote_address, ctx->timeout))
        {
            goto _coro_exit;
        }

        commands.resize(1);
        commands[0].SetCommand("select");
        commands[0].AddArg(ctx->destination_db.AsString());
        migrate_success = true;
        for (size_t i = 0; i < ctx->keys.size(); i++)
        {
            ObjectBuffer buffer;
            std::string content;
            uint64 ttl;
            if (!buffer.RedisSave(migtare_dbctx, ctx->keys[i].GetKey().AsString(), content, &ttl))
            {
                continue;
            }
            uint64 now = get_current_epoch_millis();
            if (ttl > 0 && ttl > now)
            {
                ttl -= now;
            }
            else
            {
                ttl = 0;
            }
            commands.resize(commands.size() + 1);
            RedisCommandFrame& cmd = commands[commands.size() - 1];
            cmd.SetCommand("restore");
            cmd.AddArg(ctx->keys[i].GetKey().AsString());
            cmd.AddArg(stringfromll(ttl));
            cmd.AddArg(content);
            if (ctx->replace)
            {
                cmd.AddArg("replace");
            }
        }
        if (commands.size() == 1)
        {
            r.SetStatusCode(STATUS_NOKEY);
            migrate_success = false;
        }
        if (migrate_success)
        {
            migrate_reply = redis_client.SyncMultiCall(commands, ctx->timeout);
            if (NULL != migrate_reply && migrate_reply->size() == commands.size())
            {
                for (size_t i = 0; i < commands.size(); i++)
                {
                    if (NULL == migrate_reply->at(i) || migrate_reply->at(i)->IsErr())
                    {
                        if (!r.IsErr())
                        {
                            std::string err = "Target instance replied with error:";
                            if (NULL != migrate_reply->at(i))
                            {
                                const std::string& target_err = migrate_reply->at(i)->Error();
                                if (!target_err.empty() && target_err[0] == '-')
                                {
                                    err.append(target_err.c_str() + 1);
                                }
                                else
                                {
                                    err.append(target_err);
                                }
                            }
                            r.SetErrorReason(err);
                        }
                        migrate_success = false;
                    }
                    else
                    {
                        if (!ctx->copy && i > 0)
                        {
                            g_db->DelKey(migtare_dbctx, commands[i].GetArguments()[0]);
                        }
                    }
                }
            }
            else
            {
                migrate_success = false;
                if (redis_client.IsTimeout())
                {
                    r.SetErrorReason("migrate keys timeout.");
                }
            }
        }
        _coro_exit: redis_client.Close();
        if (migrate_success)
        {
            r.SetStatusCode(STATUS_OK);
        }
        else if (r.IsNil())
        {
            r.SetErrorReason("migrate keys failed.");
        }
        src_client = ctx->io_serv->GetChannel(ctx->clientid);
        if (NULL != src_client)
        {
            src_client->Write(r);
            src_client->UnblockRead();
        }
        DELETE(ctx);
    }

    int Ardb::Migrate(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        const std::string& host = cmd.GetArguments()[0];
        uint32 port;
        int copy = 0, replace = 0;
        int64 timeout;
        if (!string_toint64(cmd.GetArguments()[4], timeout) || !string_touint32(cmd.GetArguments()[1], port))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }
        if (timeout <= 0)
        {
            timeout = 1000;
        }
        /* To support the KEYS option we need the following additional state. */
        int first_key = 2; /* Argument index of the first key. */
        int num_keys = 1; /* By default only migrate the 'key' argument. */
        for (size_t j = 5; j < cmd.GetArguments().size(); j++)
        {
            if (!strcasecmp(cmd.GetArguments()[j].c_str(), "copy"))
            {
                copy = 1;
            }
            else if (!strcasecmp(cmd.GetArguments()[j].c_str(), "replace"))
            {
                replace = 1;
            }
            else if (!strcasecmp(cmd.GetArguments()[j].c_str(), "keys"))
            {
                if (cmd.GetArguments()[2].size() != 0)
                {
                    reply.SetErrorReason("When using MIGRATE KEYS option, the key argument"
                            " must be set to the empty string");
                    return 0;
                }
                first_key = j + 1;
                num_keys = cmd.GetArguments().size() - j - 1;
                break; /* All the remaining args are keys. */
            }
            else
            {
                reply.SetErrCode(ERR_INVALID_SYNTAX);
                return 0;
            }
        }
        MigrateContext* migrate_ctx = NULL;
        NEW(migrate_ctx, MigrateContext);
        for (size_t i = first_key; i < cmd.GetArguments().size() && migrate_ctx->keys.size() < num_keys; i++)
        {
            KeyObject k(ctx.ns, KEY_META, cmd.GetArguments()[i]);
            migrate_ctx->keys.push_back(k);
        }

        migrate_ctx->io_serv = &(ctx.client->client->GetService());
        migrate_ctx->clientid = ctx.client->client->GetID();
        migrate_ctx->host = host;
        migrate_ctx->port = port;
        migrate_ctx->copy = copy;
        migrate_ctx->replace = replace;
        migrate_ctx->destination_db.SetString(cmd.GetArguments()[3], false);
        migrate_ctx->src_db = ctx.ns;
        ctx.client->client->BlockRead(); //do not read any data from client until the migrate task finish
        Scheduler::CurrentScheduler().StartCoro(0, MigrateCoroTask, migrate_ctx);
        reply.type = 0; //let coroutine task to reply client
        return 0;
    }

OP_NAMESPACE_END

