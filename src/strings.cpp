/*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
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

#include "db.hpp"
#include "ardb_server.hpp"

namespace ardb
{
    int ArdbServer::Append(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        const std::string& key = cmd.GetArguments()[0];
        const std::string& value = cmd.GetArguments()[1];
        int ret = m_db->Append(ctx.currentDB, key, value);
        if (ret > 0)
        {
            fill_int_reply(ctx.reply, ret);
        }
        else
        {
            fill_error_reply(ctx.reply, "failed to append key:%s", key.c_str());
        }
        return 0;
    }

    int ArdbServer::PSetEX(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        uint32 mills;
        if (!string_touint32(cmd.GetArguments()[1], mills))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        m_db->PSetEx(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[2], mills);
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }

    int ArdbServer::MSetNX(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        if (cmd.GetArguments().size() % 2 != 0)
        {
            fill_error_reply(ctx.reply, "wrong number of arguments for MSETNX");
            return 0;
        }
        SliceArray keys;
        SliceArray vals;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i += 2)
        {
            keys.push_back(cmd.GetArguments()[i]);
            vals.push_back(cmd.GetArguments()[i + 1]);
        }
        int count = m_db->MSetNX(ctx.currentDB, keys, vals);
        fill_int_reply(ctx.reply, count);
        return 0;
    }

    int ArdbServer::MSet(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        if (cmd.GetArguments().size() % 2 != 0)
        {
            fill_error_reply(ctx.reply, "wrong number of arguments for MSET");
            return 0;
        }
        SliceArray keys;
        SliceArray vals;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i += 2)
        {
            keys.push_back(cmd.GetArguments()[i]);
            vals.push_back(cmd.GetArguments()[i + 1]);
        }
        m_db->MSet(ctx.currentDB, keys, vals);
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }

    int ArdbServer::MGet(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        SliceArray keys;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            keys.push_back(cmd.GetArguments()[i]);
        }
        StringArray res;
        m_db->MGet(ctx.currentDB, keys, res);
        fill_str_array_reply(ctx.reply, res);
        return 0;
    }

    int ArdbServer::IncrbyFloat(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        double increment, val;
        if (!string_todouble(cmd.GetArguments()[1], increment))
        {
            fill_error_reply(ctx.reply, "value is not a float or out of range");
            return 0;
        }
        int ret = m_db->IncrbyFloat(ctx.currentDB, cmd.GetArguments()[0], increment, val);
        if (ret == 0)
        {
            fill_double_reply(ctx.reply, val);
        }
        else
        {
            fill_error_reply(ctx.reply, "value is not a float or out of range");
        }
        return 0;
    }

    int ArdbServer::Incrby(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int64 increment, val;
        if (!string_toint64(cmd.GetArguments()[1], increment))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        int ret = m_db->Incrby(ctx.currentDB, cmd.GetArguments()[0], increment, val);
        if (ret == 0)
        {
            fill_int_reply(ctx.reply, val);
        }
        else
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
        }
        return 0;
    }

    int ArdbServer::Incr(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int64_t val;
        int ret = m_db->Incr(ctx.currentDB, cmd.GetArguments()[0], val);
        if (ret == 0)
        {
            fill_int_reply(ctx.reply, val);
        }
        else
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
        }
        return 0;
    }

    int ArdbServer::GetSet(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        std::string v;
        int ret = m_db->GetSet(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], v);
        if (ret < 0)
        {
            ctx.reply.type = REDIS_REPLY_NIL;
        }
        else
        {
            fill_str_reply(ctx.reply, v);
        }
        return 0;
    }

    int ArdbServer::GetRange(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int32 start, end;
        if (!string_toint32(cmd.GetArguments()[1], start) || !string_toint32(cmd.GetArguments()[2], end))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        std::string v;
        m_db->GetRange(ctx.currentDB, cmd.GetArguments()[0], start, end, v);
        fill_str_reply(ctx.reply, v);
        return 0;
    }

    int ArdbServer::Set(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        const std::string& key = cmd.GetArguments()[0];
        const std::string& value = cmd.GetArguments()[1];
        int ret = 0;
        if (cmd.GetArguments().size() == 2)
        {
            ret = m_db->Set(ctx.currentDB, key, value);
        }
        else
        {
            uint32 i = 0;
            uint64 px = 0, ex = 0;
            for (i = 2; i < cmd.GetArguments().size(); i++)
            {
                std::string tmp = string_tolower(cmd.GetArguments()[i]);
                if (tmp == "ex" || tmp == "px")
                {
                    int64 iv;
                    if (!raw_toint64(cmd.GetArguments()[i + 1].c_str(), cmd.GetArguments()[i + 1].size(), iv) || iv < 0)
                    {
                        fill_error_reply(ctx.reply, "value is not an integer or out of range");
                        return 0;
                    }
                    if (tmp == "px")
                    {
                        px = iv;
                    }
                    else
                    {
                        ex = iv;
                    }
                    i++;
                }
                else
                {
                    break;
                }
            }
            bool hasnx = false, hasxx = false;
            bool syntaxerror = false;
            if (i < cmd.GetArguments().size() - 1)
            {
                syntaxerror = true;
            }
            if (i == cmd.GetArguments().size() - 1)
            {
                std::string cmp = string_tolower(cmd.GetArguments()[i]);
                if (cmp != "nx" && cmp != "xx")
                {
                    syntaxerror = true;
                }
                else
                {
                    hasnx = cmp == "nx";
                    hasxx = cmp == "xx";
                }
            }
            if (syntaxerror)
            {
                fill_error_reply(ctx.reply, "syntax error");
                return 0;
            }
            int nxx = 0;
            if (hasnx)
            {
                nxx = -1;
            }
            if (hasxx)
            {
                nxx = 1;
            }
            ret = m_db->Set(ctx.currentDB, key, value, ex, px, nxx);
        }
        if (0 == ret)
        {
            fill_status_reply(ctx.reply, "OK");
        }
        else
        {
            switch (ret)
            {
                case ERR_INVALID_TYPE:
                {
                    fill_error_reply(ctx.reply, "invalid type");
                    break;
                }
                case ERR_KEY_EXIST:
                case ERR_NOT_EXIST:
                {
                    ctx.reply.type = REDIS_REPLY_NIL;
                    break;
                }
                default:
                {
                    fill_error_reply(ctx.reply, "set failed");
                    break;
                }
            }
        }
        return 0;
    }

    int ArdbServer::Get(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        const std::string& key = cmd.GetArguments()[0];
        std::string value;
        int ret = m_db->Get(ctx.currentDB, key, value);
        if (0 == ret)
        {
            fill_str_reply(ctx.reply, value);
            //ctx.reply.type = REDIS_REPLY_NIL;
        }
        else
        {
            if(ERR_INVALID_TYPE == ret)
            {
                fill_error_reply(ctx.reply, "Operation against a key holding the wrong kind of value");
            }
            else
            {
                ctx.reply.type = REDIS_REPLY_NIL;
            }

        }
        return 0;
    }

    int ArdbServer::Decrby(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int64 decrement, val;
        if (!string_toint64(cmd.GetArguments()[1], decrement))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        int ret = m_db->Decrby(ctx.currentDB, cmd.GetArguments()[0], decrement, val);
        if (ret == 0)
        {
            fill_int_reply(ctx.reply, val);
        }
        else
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
        }
        return 0;
    }

    int ArdbServer::Decr(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int64_t val;
        int ret = m_db->Decr(ctx.currentDB, cmd.GetArguments()[0], val);
        if (ret == 0)
        {
            fill_int_reply(ctx.reply, val);
        }
        else
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
        }
        return 0;
    }

    int ArdbServer::SetEX(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        uint32 secs;
        if (!string_touint32(cmd.GetArguments()[1], secs))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        m_db->SetEx(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[2], secs);
        fill_status_reply(ctx.reply, "OK");
        return 0;
    }
    int ArdbServer::SetNX(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->SetNX(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1]);
        fill_int_reply(ctx.reply, ret);
        return 0;
    }
    int ArdbServer::SetRange(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int32 offset;
        if (!string_toint32(cmd.GetArguments()[1], offset))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        int ret = m_db->SetRange(ctx.currentDB, cmd.GetArguments()[0], offset, cmd.GetArguments()[2]);
        fill_int_reply(ctx.reply, ret);
        return 0;
    }
    int ArdbServer::Strlen(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->Strlen(ctx.currentDB, cmd.GetArguments()[0]);
        fill_int_reply(ctx.reply, ret);
        return 0;
    }

    int Ardb::Append(const DBID& db, const Slice& key, const Slice& value)
    {
        std::string prev_value;
        int ret = Get(db, key, prev_value);
        if (ret == 0 || ret == ERR_NOT_EXIST)
        {
            prev_value.append(value.data(), value.size());
            Set(db, key, prev_value);
            return prev_value.size();
        }
        return ret;
    }

    int Ardb::Incr(const DBID& db, const Slice& key, int64_t& value)
    {
        return Incrby(db, key, 1, value);
    }
    int Ardb::Incrby(const DBID& db, const Slice& key, int64_t increment, int64_t& value)
    {
        CommonMetaValue* meta = GetMeta(db, key, false);
        StringMetaValue* smeta = NULL;
        if (NULL != meta)
        {
            if (meta->header.type != STRING_META)
            {
                DELETE(meta);
                return ERR_INVALID_TYPE;
            }
            smeta = (StringMetaValue*) meta;
            smeta->value.ToNumber();
            if (smeta->value.type == BYTES_VALUE)
            {
                DELETE(smeta);
                return ERR_INVALID_TYPE;
            }
            smeta->value.Incrby(increment);
            value = smeta->value.integer_value;
            SetMeta(db, key, *smeta);
            DELETE(smeta);
        }
        else
        {
            StringMetaValue nsmeta;
            nsmeta.value.SetIntValue(increment);
            KeyObject k(key, KEY_META, db);
            SetMeta(k, nsmeta);
            value = increment;
        }
        return 0;
    }

    int Ardb::Decr(const DBID& db, const Slice& key, int64_t& value)
    {
        return Decrby(db, key, 1, value);
    }

    int Ardb::Decrby(const DBID& db, const Slice& key, int64_t decrement, int64_t& value)
    {
        return Incrby(db, key, 0 - decrement, value);
    }

    int Ardb::IncrbyFloat(const DBID& db, const Slice& key, double increment, double& value)
    {
        CommonMetaValue* meta = GetMeta(db, key, false);
        StringMetaValue* smeta = NULL;

        if (NULL != meta)
        {
            if (meta->header.type != STRING_META)
            {
                DELETE(meta);
                return ERR_INVALID_TYPE;
            }
            smeta = (StringMetaValue*) meta;
            smeta->value.ToNumber();
            if (smeta->value.type == BYTES_VALUE)
            {
                DELETE(smeta);
                return ERR_INVALID_TYPE;
            }
            smeta->value.IncrbyFloat(increment);
            value = smeta->value.double_value;
            DELETE(smeta);
        }
        else
        {
            StringMetaValue nsmeta;
            nsmeta.value.SetDoubleValue(increment);
            KeyObject k(key, KEY_META, db);
            SetMeta(k, nsmeta);
            value = increment;
        }
        return 0;
    }

    int Ardb::GetRange(const DBID& db, const Slice& key, int start, int end, std::string& v)
    {
        std::string value;
        int ret = Get(db, key, value);
        if (ret < 0)
        {
            return ret;
        }
        start = RealPosition(value, start);
        end = RealPosition(value, end);
        if (start > end)
        {
            return ERR_OUTOFRANGE;
        }
        v = value.substr(start, (end - start + 1));
        return ARDB_OK;
    }

    int Ardb::SetRange(const DBID& db, const Slice& key, int start, const Slice& v)
    {
        std::string value;
        int ret = Get(db, key, value);
        if (ret < 0)
        {
            return ret;
        }
        start = RealPosition(value, start);
        value.resize(start);
        value.append(v.data(), v.size());
        return Set(db, key, value) == 0 ? value.size() : 0;
    }

    int Ardb::GetSet(const DBID& db, const Slice& key, const Slice& value, std::string& v)
    {
        if (Get(db, key, v) < 0)
        {
            Set(db, key, value);
            return ERR_NOT_EXIST;
        }
        return Set(db, key, value);
    }

}

