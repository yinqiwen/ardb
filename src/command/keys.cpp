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
        if (!CheckMeta(ctx, key, type, meta))
        {
            return 0;
        }
        if (meta.GetType() == 0)
        {
            reply.SetInteger(0);
            return 0;
        }
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
                if (field.GetType() != ele_type || field.GetNameSpace() != key.GetNameSpace() || field.GetKey() != key.GetKey())
                {
                    break;
                }
                len++;
                iter->Next();
            }
            DELETE(iter);
            reply.SetInteger(len);
        }
        return 0;
    }

    int Ardb::GetMinMax(Context& ctx, const KeyObject& key, ValueObject& meta, Iterator*& iter)
    {
        if (NULL != iter)
        {
            WARN_LOG("Not NULL iterator before getting min/max element.");
            return -1;
        }
        if (meta.GetType() == 0)
        {
            return 0;
        }
        KeyType ele_type = element_type((KeyType) meta.GetType());
        KeyObject start_element(ctx.ns, ele_type, key.GetKey());
        start_element.SetMember(meta.GetMin(), 0);
        iter = m_engine->Find(ctx, start_element);
        if (!meta.GetMin().IsNil() && !meta.GetMax().IsNil())
        {
            return 0;
        }
        if (NULL == iter || !iter->Valid())
        {
            DELETE(iter);
            return -1;
        }
        KeyObject& min_key = iter->Key();
        if (min_key.GetKey() != key.GetKey() || min_key.GetType() != ele_type || min_key.GetNameSpace() != key.GetNameSpace())
        {
            WARN_LOG("Invalid start iterator to fetch max element");
            DELETE(iter);
            return -1;
        }
        bool meta_changed = false;
        if (meta.GetMin().IsNil())
        {
            meta.SetMinData(min_key.GetElement(0));
            meta_changed = true;
        }
        if (meta.GetMax().IsNil())
        {
            iter->JumpToLast();
            if (!iter->Valid())
            {
                DELETE(iter);
                WARN_LOG("Invalid iterator to fetch max element");
                return -1;
            }
            KeyObject& iter_key = iter->Key();
            if (iter_key.GetType() != ele_type || iter_key.GetKey() != key.GetKey() || iter_key.GetNameSpace() != key.GetNameSpace())
            {
                DELETE(iter);
                WARN_LOG("Invalid iterator key to fetch max element");
                return -1;
            }
            meta.SetMaxData(iter_key.GetElement(0));
            meta_changed = true;
        }
        if (meta_changed)
        {
            m_engine->Put(ctx, key, meta);
        }
        start_element.SetMember(meta.GetMin(), 0);
        iter->Jump(start_element);
        return 0;
    }
    int Ardb::GetMinMax(Context& ctx, const KeyObject& key, KeyType expected, ValueObject& meta, Iterator*& iter)
    {
        m_engine->Get(ctx, key, meta);
        if (meta.GetType() > 0 && meta.GetType() != expected)
        {
            return -1;
        }
        return GetMinMax(ctx, key, meta, iter);
    }

    int Ardb::KeysCount(Context& ctx, RedisCommandFrame& cmd)
    {
        return Keys(ctx, cmd);
    }
    int Ardb::Randomkey(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }
    int Ardb::Scan(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        uint32 limit = 1000; //return max 1000 keys one time
        std::string pattern;
        if (cmd.GetArguments().size() > 1)
        {
            for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
            {
                if (!strcasecmp(cmd.GetArguments()[i].c_str(), "count"))
                {
                    if (i + 1 >= cmd.GetArguments().size() || !string_touint32(cmd.GetArguments()[i + 1], limit))
                    {
                        reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
                        return 0;
                    }
                    i++;
                }
                else if (!strcasecmp(cmd.GetArguments()[i].c_str(), "match"))
                {
                    if (i + 1 >= cmd.GetArguments().size())
                    {
                        reply.SetErrorReason("'MATCH' need one args followed");
                        return 0;
                    }
                    pattern = cmd.GetArguments()[i + 1];
                    i++;
                }
                else
                {
                    reply.SetErrorReason("Syntax error, try scan 0");
                    return 0;
                }
            }
        }
        uint32 scan_count_limit = limit * 10;
        uint32 scan_count = 0;

        return 0;
    }

    int Ardb::Keys(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& pattern = cmd.GetArguments()[0];
        ctx.flags.iterate_multi_keys = 1;
        std::string start;
        for (size_t i = 0; i < pattern.size(); i++)
        {
            if (pattern[i] != '*' && pattern[i] != '?' && pattern[i] != '[' && pattern[i] != '\\')
            {
                start.append(pattern[i], 1);
            }
            else
            {
                break;
            }
        }
        RedisReply& reply = ctx.GetReply();
        if (cmd.GetType() == REDIS_CMD_KEYS)
        {
            reply.ReserveMember(0);
        }
        else
        {
            reply.SetInteger(0);
        }
        bool noregex = start == pattern;
        int64_t match_count = 0;
        KeyObject startkey(ctx.ns, KEY_META, start);
        Iterator* iter = m_engine->Find(ctx, startkey);
        while (iter->Valid())
        {
            KeyObject& k = iter->Key();
            if (k.GetType() == KEY_META)
            {
                std::string keystr;
                k.GetKey().ToString(keystr);
                if (stringmatchlen(pattern.c_str(), pattern.size(), keystr.c_str(), keystr.size(), 0) == 1)
                {
                    match_count++;
                    if (cmd.GetType() == REDIS_CMD_KEYS)
                    {
                        RedisReply& r = reply.AddMember();
                        r.SetString(keystr);
                    }
                    if (noregex)
                    {
                        break;
                    }
                }
            }
            iter->Next();
        }
        DELETE(iter);
        if (cmd.GetType() == REDIS_CMD_KEYSCOUNT)
        {
            reply.SetInteger(match_count);
        }
        return 0;
    }

    int Ardb::MoveKey(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        Data srcdb, dstdb;
        const std::string& srckey = cmd.GetArguments()[0];
        std::string dstkey;
        srcdb = ctx.ns;
        if (cmd.GetType() == REDIS_CMD_RENAME || cmd.GetType() == REDIS_CMD_RENAMENX)
        {
            dstkey = cmd.GetArguments()[1];
            dstdb = srcdb;
        }
        else
        {
            dstkey = srckey;
            dstdb.SetString(cmd.GetArguments()[1], true);
        }
        bool nx = cmd.GetType() == REDIS_CMD_RENAMENX || cmd.GetType() == REDIS_CMD_MOVE;
        KeyObject src(srcdb, KEY_META, srckey);
        KeyObject dst(dstdb, KEY_META, dstkey);
        KeysLockGuard guard(src, dst);
        if (m_engine->Exists(ctx, dst))
        {
            if (nx)
            {
                reply.SetInteger(0);
                return 0;
            }
            DelKey(ctx, dst);
        }
        int64_t moved = 0;
        Iterator* iter = m_engine->Find(ctx, src);
        while(iter->Valid())
        {
            KeyObject& k = iter->Key();
            if(k.GetKey() != src.GetKey() || k.GetNameSpace() != src.GetNameSpace())
            {
                break;
            }
            k.SetNameSpace(dstdb);
            m_engine->Put(ctx, k, iter->Value());
            iter->Next();
        }
        DELETE(iter);
        reply.SetInteger(moved > 0 ?1:0);
        return 0;
    }

    int Ardb::Rename(Context& ctx, RedisCommandFrame& cmd)
    {
        return MoveKey(ctx, cmd);
    }

    int Ardb::RenameNX(Context& ctx, RedisCommandFrame& cmd)
    {
        return MoveKey(ctx, cmd);
    }

    int Ardb::Move(Context& ctx, RedisCommandFrame& cmd)
    {
        return MoveKey(ctx, cmd);
    }

    int Ardb::Type(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        KeyObject meta_key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject meta_value;
        KeyLockGuard guard(meta_key);
        if (!CheckMeta(ctx, meta_key, (KeyType) 0, meta_value))
        {
            return 0;
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
        return PExpireat(ctx, cmd);
    }

    int Ardb::MergeExpire(Context& ctx, const KeyObject& key, ValueObject& meta, int64 ms)
    {
        if (meta.GetType() == 0)
        {
            return ERR_NOTPERFORMED;
        }
        int64 ttl = ms + get_current_epoch_millis();
        meta.SetTTL(ttl);
        return 0;
    }
    int Ardb::PExpireat(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        int64 mills;
        if (cmd.GetArguments().size() == 2)
        {
            if (!string_toint64(cmd.GetArguments()[1], mills) || mills <= 0)
            {
                reply.SetErrCode(ERR_OUTOFRANGE);
                return 0;
            }
        }

        switch (cmd.GetType())
        {
            case REDIS_CMD_PERSIST:
            {
                mills = 0;
                break;
            }
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
                mills += get_current_epoch_millis();
                break;
            }
            case REDIS_CMD_PEXPIRE:
            case REDIS_CMD_PEXPIRE2:
            {
                mills += get_current_epoch_millis();
                break;
            }
            default:
            {
                abort();
            }
        }
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        if (!ctx.flags.redis_compatible)
        {
            Data merge_data;
            merge_data.SetInt64(mills);
            m_engine->Merge(ctx, key, REDIS_CMD_PEXPIREAT, merge_data);
            reply.SetStatusCode(STATUS_OK);
        }
        else
        {
            ValueObject meta_value;
            KeyLockGuard guard(key);
            if (!CheckMeta(ctx, key, (KeyType) 0, meta_value))
            {
                return 0;
            }
            if (meta_value.GetType() == 0)
            {
                reply.SetInteger(0);
            }
            else
            {
                meta_value.SetTTL(mills);
                m_engine->Put(ctx, key, meta_value);
                reply.SetInteger(1);
            }
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

    int Ardb::DelKey(Context& ctx, const KeyObject& meta_key)
    {
        Iterator* iter = NULL;
        int ret = DelKey(ctx, meta_key, iter);
        DELETE(iter);
        return ret;
    }

    int Ardb::DelKey(Context& ctx, const KeyObject& meta_key, Iterator*& iter)
    {
        if (NULL == iter)
        {
            iter = m_engine->Find(ctx, meta_key);
        }
        else
        {
            iter->Jump(meta_key);
        }
        int removed = 0;
        while (NULL != iter && iter->Valid())
        {
            KeyObject& k = iter->Key();
            const Data& kdata = k.GetKey();
            if (k.GetNameSpace().Compare(meta_key.GetNameSpace()) != 0 || kdata.StringLength() != meta_key.GetKey().StringLength() || strncmp(meta_key.GetKey().CStr(), kdata.CStr(), kdata.StringLength()) != 0)
            {
                break;
            }
            removed = 1;
            m_engine->Del(ctx, k);
            iter->Next();
        }
        //DELETE(iter);
        return removed;
    }

    int Ardb::Del(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        Iterator* iter = NULL;
        int removed = 0;
        ctx.flags.iterate_multi_keys = cmd.GetArguments().size() > 1 ? 1 : 0;
        for (size_t i = 0; i < cmd.GetArguments().size(); i++)
        {
            KeyObject meta(ctx.ns, KEY_META, cmd.GetArguments()[i]);
            KeyLockGuard guard(meta);
            removed += DelKey(ctx, meta, iter);
        }
        DELETE(iter);
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

