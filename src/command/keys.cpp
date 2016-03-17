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
    int Ardb::ObjectLen(Context& ctx, KeyType type, const std::string& keystr)
    {
        RedisReply& reply = ctx.GetReply();
        KeyObject key(ctx.ns, KEY_META, keystr);
        ValueObject meta;
        int err = m_engine->Get(ctx, key, meta);
        if (err != 0)
        {
            if (err != ERR_ENTRY_NOT_EXIST)
            {
                reply.SetErrCode(err);
            }
            else
            {
                reply.SetInteger(0);
            }
        }
        else
        {
            if (meta.GetType() != type)
            {
                reply.SetErrCode(ERR_INVALID_TYPE);
            }
            else
            {
                if (meta.GetObjectLen() >= 0)
                {
                    reply.SetInteger(meta.GetObjectLen());
                }
                else
                {
                    KeyType ele_type = element_type(type);
                    int64_t len = 0;
                    KeyObject key(ctx.ns, ele_type, keystr);
                    Iterator* iter = m_engine->Find(ctx, key);
                    while (NULL != iter && iter->Valid())
                    {
                        KeyObject& field = iter->Key();
                        if (field.GetType() != ele_type || field.GetNameSpace() != key.GetNameSpace() || field.GetKey() != field.GetKey())
                        {
                            break;
                        }
                        len++;
                        iter->Next();
                    }
                    DELETE(iter);
                    reply.SetInteger(len);
                }
            }
        }
        return 0;
    }
    int Ardb::KeysCount(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }
    int Ardb::Randomkey(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }
    int Ardb::Scan(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::Keys(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::Rename(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::RenameNX(Context& ctx, RedisCommandFrame& cmd)
    {
        Rename(ctx, cmd);
        return 0;
    }

    int Ardb::Move(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::Type(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        KeyObject meta_key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject meta_value;
        int err = m_engine->Get(ctx, meta_key, meta_value);
        if (err != 0 && err != ERR_ENTRY_NOT_EXIST)
        {
            reply.SetErrCode(err);
            return false;
        }
        switch (meta_value.GetType())
        {
            case 0:
            {
                reply.SetString("none");
                break;
            }
            case KEY_HASH:
            {
                reply.SetString("hash");
                break;
            }
            case KEY_LIST:
            {
                reply.SetString("list");
                break;
            }
            case KEY_SET:
            {
                reply.SetString("set");
                break;
            }
            case KEY_ZSET:
            {
                reply.SetString("zset");
                break;
            }
            case KEY_STRING:
            {
                reply.SetString("string");
                break;
            }
            default:
            {
                reply.SetString("invalid");
                break;
            }
        }
        return 0;
    }

    int Ardb::Persist(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }


    int Ardb::MergeExpire(Context& ctx, const KeyObject& key, ValueObject& meta, int64 ms)
    {
        if(meta.GetType() == 0)
        {
            return ERR_NOTPERFORMED;
        }
        int64 ttl = ms + get_current_epoch_millis();
        if (meta.GetTTL() == 0)
        {
            meta.SetTTL(ttl);
        }
        else
        {
            KeyObject ttl_sort(key.GetNameSpace(), KEY_TTL_SORT, meta.GetTTL());
            ttl_sort.SetTTLKey(key.GetKey());
            m_engine->Del(ctx, ttl_sort);
        }
        KeyObject ttl_sort(key.GetNameSpace(), KEY_TTL_SORT, ttl);
        ValueObject empty;
        m_engine->Put(ctx, ttl_sort, empty);
        meta.SetTTL(ttl);
        return 0;
    }
    int Ardb::PExpireat(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        int64 mills;
        if (!string_toint64(cmd.GetArguments()[1], mills) || mills <= 0)
        {
            reply.SetErrCode(ERR_OUTOFRANGE);
            return 0;
        }
        switch(cmd.GetType())
        {
            case REDIS_CMD_PEXPIREAT:
            case REDIS_CMD_PEXPIREAT2:
            {
                break;
            }
            case REDIS_CMD_EXPIREAT:
            case REDIS_CMD_EXPIREAT2:
            {
                mills *= 1000;
                break;
            }
            case REDIS_CMD_EXPIRE:
            case REDIS_CMD_EXPIRE2:
            {
                mills *= 1000;
                mills  += get_current_epoch_millis();
                break;
            }
            case REDIS_CMD_PEXPIRE:
            case REDIS_CMD_PEXPIRE2:
            {
                mills  += get_current_epoch_millis();
                break;
            }
            default:
            {
                abort();
            }
        }
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        if (cmd.GetType() > REDIS_CMD_MERGE_BEGIN && !GetConf().redis_compatible)
        {
            Data merge_data;
            merge_data.SetInt64(mills);
            m_engine->Merge(ctx, key, REDIS_CMD_PEXPIREAT, merge_data);
            reply.SetStatusCode(STATUS_OK);
            return 0;
        }
        return 0;
    }
    int Ardb::PExpire(Context& ctx, RedisCommandFrame& cmd)
    {
        return PExpireat(ctx, cmd);
    }
    int Ardb::Expire(Context& ctx, RedisCommandFrame& cmd)
    {
        return PExpireat(ctx, cmd);
    }

    int Ardb::Expireat(Context& ctx, RedisCommandFrame& cmd)
    {
        return PExpireat(ctx, cmd);
    }

    int Ardb::PTTL(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        reply.SetInteger(0);  //default response
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject val;
        int err = m_engine->Get(ctx, key, val);
        if (0 != err)
        {
            if (err != ERR_ENTRY_NOT_EXIST)
            {
                reply.SetErrCode(err);
            }
            return 0;
        }
        if (cmd.GetType() == REDIS_CMD_PTTL)
        {
            reply.SetInteger(val.GetTTL());
        }
        else
        {
            reply.SetInteger(val.GetTTL() / 1000);
        }
        return 0;
    }
    int Ardb::TTL(Context& ctx, RedisCommandFrame& cmd)
    {
        return PTTL(ctx, cmd);
    }

    int Ardb::Exists(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_META, keystr);
        bool existed = m_engine->Exists(ctx, key);
        reply.SetInteger(existed ? 1 : 0);
        return 0;
    }

    int Ardb::MergeDel(Context& ctx,KeyObject& key, ValueObject& value)
    {
        if (value.GetType() == 0)
        {
            return ERR_NOTPERFORMED;
        }
        DelElements(ctx, key, value);
        if (value.GetTTL() > 0)
        {
            KeyObject ttl_key(key.GetNameSpace(), KEY_TTL_SORT, value.GetTTL());
            ttl_key.SetTTLKey(key.GetKey());
            m_engine->Del(ctx, ttl_key);
        }
        value.Clear();
        return 0;
    }

    int Ardb::DelElements(Context& ctx,KeyObject& key, ValueObject& value)
    {
    	//todo
        switch (value.GetType())
        {
            case 0:
            {
        	     break;
            }
            case KEY_STRING:
            {
                break;
            }
            case KEY_HASH:
            {
                break;
            }
            case KEY_SET:
            {
                break;
            }
            case KEY_LIST:
            {
                break;
            }
            case KEY_ZSET:
            {
                break;
            }
            default:
            {
                abort();
            }
        }
    	return 0;
    }

    int Ardb::Del(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        if (cmd.GetType() > REDIS_CMD_MERGE_BEGIN || !GetConf().redis_compatible)
        {
            for (size_t i = 0; i < cmd.GetArguments().size(); i++)
            {
                KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[i]);
                Data empty;
                m_engine->Merge(ctx, key, cmd.GetType(), empty);
            }
            reply.SetStatusCode(STATUS_OK);
            return 0;
        }
        KeyObjectArray ks;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
             KeyObject k(ctx.ns, KEY_META, cmd.GetArguments()[i]);
             ks.push_back(k);
        }
        ValueObjectArray vs;
        ErrCodeArray errs;
        int err = m_engine->MultiGet(ctx, ks, vs, errs);
        if (0 != err)
        {
             reply.SetErrCode(err);
             return 0;
        }
        int removed = 0;
        for (size_t i = 0; i < ks.size(); i++)
        {
              if (errs[i] == 0)
              {
                  DelElements(ctx, ks[i], vs[i]);
            	  err = m_engine->Del(ctx, ks[i]);
            	  removed++;
              }
        }
        reply.SetInteger(removed);
        return 0;
    }

    int Ardb::RawSet(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }
    int Ardb::RawDel(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

OP_NAMESPACE_END

