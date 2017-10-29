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
                if (field.GetType() != ele_type || field.GetNameSpace() != key.GetNameSpace()
                        || field.GetKey() != key.GetKey())
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
        if (meta.GetMax().IsNil())
        {
            ctx.flags.iterate_total_order = 1;
        }
        iter = m_engine->Find(ctx, start_element);
        if (!meta.GetMin().IsNil() && !meta.GetMax().IsNil())
        {
        	//DELETE(iter);
            return 0;
        }
        if (NULL == iter || !iter->Valid())
        {
            DELETE(iter);
            return -1;
        }
        KeyObject& min_key = iter->Key(true);
        if (min_key.GetKey() != key.GetKey() || min_key.GetType() != ele_type
                || min_key.GetNameSpace() != key.GetNameSpace())
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
            KeyObject& iter_key = iter->Key(true);
            if (iter_key.GetType() != ele_type || iter_key.GetKey() != key.GetKey()
                    || iter_key.GetNameSpace() != key.GetNameSpace())
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
            SetKeyValue(ctx, key, meta);
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
        RedisReply& reply = ctx.GetReply();
        KeyObject startkey(ctx.ns, KEY_META, "");
        ctx.flags.iterate_multi_keys = 1;
        ctx.flags.iterate_no_upperbound = 1;
        /*
         * for rocksdb, this flag must be set for right behavior
         */
        ctx.flags.iterate_total_order = 1;
        Iterator* iter = m_engine->Find(ctx, startkey);
        if (iter->Valid())
        {
            KeyObject& k = iter->Key();
            reply.SetString(k.GetKey());
        }
        DELETE(iter);
        return 0;
    }

    int Ardb::Scan(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        reply.ReserveMember(0);
        uint32 limit = 1000; //return max 1000 keys one time
        std::string pattern;
        KeyObject startkey;
        startkey.SetNameSpace(ctx.ns);
        uint32 cursor_pos = 0;
        std::string cursor_element;
        bool skip_first = false;
        Data nil;

        if (GetConf().rocksdb_scan_total_order)
        {
            ctx.flags.iterate_total_order = 1;
        }
        if (cmd.GetType() == REDIS_CMD_HSCAN)
        {
            cursor_pos = 1;
            FindElementByRedisCursor(cmd.GetArguments()[cursor_pos], cursor_element);
            startkey.SetType(KEY_HASH_FIELD);
            startkey.SetKey(cmd.GetArguments()[0]);
            if (cursor_element.empty())
            {
                startkey.SetHashField(nil);
            }
            else
            {
                startkey.SetHashField(cursor_element);
            }

        }
        else if (cmd.GetType() == REDIS_CMD_SSCAN)
        {
            cursor_pos = 1;
            FindElementByRedisCursor(cmd.GetArguments()[cursor_pos], cursor_element);
            startkey.SetType(KEY_SET_MEMBER);
            startkey.SetKey(cmd.GetArguments()[0]);
            if (cursor_element.empty())
            {
                startkey.SetSetMember(nil);
            }
            else
            {
                startkey.SetSetMember(cursor_element);
            }
        }
        else if (cmd.GetType() == REDIS_CMD_ZSCAN)
        {
            cursor_pos = 1;
            FindElementByRedisCursor(cmd.GetArguments()[cursor_pos], cursor_element);
            startkey.SetType(KEY_ZSET_SORT);
            startkey.SetKey(cmd.GetArguments()[0]);
            startkey.SetZSetMember(cursor_element);
        }
        else
        {
            cursor_pos = 0;
            FindElementByRedisCursor(cmd.GetArguments()[cursor_pos], cursor_element);
            startkey.SetType(KEY_META);
            startkey.SetKey(cursor_element);
            ctx.flags.iterate_multi_keys = 1;
            /*
             * for rocksdb, this flag must be set for right behavior
             */
            ctx.flags.iterate_total_order = 1;
        }

        skip_first = !cursor_element.empty();

        if (cmd.GetArguments().size() > cursor_pos + 1)
        {
            for (uint32 i = cursor_pos + 1; i < cmd.GetArguments().size(); i++)
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
                    if (pattern == "*")
                    {
                        pattern.clear();
                    }
                    i++;
                }
                else
                {
                    reply.SetErrorReason("Syntax error, try scan 0");
                    return 0;
                }
            }
        }
        RedisReply& r1 = reply.AddMember();
        RedisReply& r2 = reply.AddMember();
        r2.ReserveMember(0);
        uint32 scan_count_limit = limit * 10;
        uint32 scan_count = 0;
        int64_t result_count = 0;
        Iterator* iter = m_engine->Find(ctx, startkey);
        if (iter->Valid() && skip_first)
        {
            iter->Next();
        }
        std::string match_element;
        while (iter->Valid())
        {
            KeyObject& k = iter->Key();
            if (cmd.GetType() == REDIS_CMD_SCAN)
            {
                if (k.GetType() != KEY_META)
                {
                    //iter->Next();
                    KeyObject next(ctx.ns, KEY_END, k.GetKey());
                    iter->Jump(next);
                    continue;
                }
                k.GetKey().ToString(match_element);
            }
            else
            {
                if (k.GetType() != startkey.GetType() || k.GetKey() != startkey.GetKey()
                        || k.GetNameSpace() != startkey.GetNameSpace())
                {
                    break;
                }

                //if (!pattern.empty())
                {
                    if (k.GetType() == KEY_ZSET_SORT)
                    {
                        k.GetZSetMember().ToString(match_element);
                    }
                    else
                    {
                        k.GetElement(0).ToString(match_element);
                    }
                }
            }
            scan_count++;
            if (!pattern.empty())
            {
                if (stringmatchlen(pattern.c_str(), pattern.size(), match_element.c_str(), match_element.size(), 0)
                        != 1)
                {
                    iter->Next();
                    continue;
                }
            }
            result_count++;
            switch (k.GetType())
            {
                case KEY_META:
                {
                    RedisReply& rr = r2.AddMember();
                    rr.SetString(k.GetKey());
                    break;
                }
                case KEY_HASH_FIELD:
                {
                    RedisReply& rr = r2.AddMember();
                    rr.SetString(k.GetHashField());
                    RedisReply& rr1 = r2.AddMember();
                    rr1.SetString(iter->Value().GetHashValue());
                    break;
                }
                case KEY_ZSET_SORT:
                {
                    RedisReply& rr = r2.AddMember();
                    rr.SetString(k.GetZSetMember());
                    RedisReply& rr1 = r2.AddMember();
                    rr1.SetString(k.GetElement(0));
                    break;
                }
                case KEY_SET_MEMBER:
                {
                    RedisReply& rr = r2.AddMember();
                    rr.SetString(k.GetSetMember());
                    break;
                }
                default:
                {
                    break;
                }
            }
            if (scan_count >= scan_count_limit || result_count >= limit)
            {
                break;
            }
            iter->Next();
        }
        if (!iter->Valid())
        {
            r1.SetString("0");
        }
        else
        {
            std::string next = match_element;
            //next.append(1, 0);
            uint64 newcursor = GetNewRedisCursor(next);
            r1.SetString(stringfromll(newcursor));
        }
        DELETE(iter);
        return 0;
    }

    int Ardb::Keys(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& pattern = cmd.GetArguments()[0];
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
        ctx.flags.iterate_multi_keys = 1;
        ctx.flags.iterate_no_upperbound = 1;
        /*
         * for rocksdb, this flag must be set for right behavior
         */
        ctx.flags.iterate_total_order = 1;
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
                    /*
                     * limit keys output
                     */
                    if (match_count >= 10000 && cmd.GetType() == REDIS_CMD_KEYS)
                    {
                        reply.SetErrorReason("Too many keys for keys command, use 'scan' instead.");
                        break;
                    }
                    if (noregex)
                    {
                        break;
                    }
                }
            }
            if (iter->Value().GetType() != KEY_STRING)
            {
                std::string keystr;
                k.GetKey().ToString(keystr);
                keystr.append(1, 0);
                KeyObject next(ctx.ns, KEY_META, keystr);
                iter->Jump(next);
                continue;
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
            ctx.flags.create_if_notexist = 1;
        }
        bool nx = cmd.GetType() == REDIS_CMD_RENAMENX || cmd.GetType() == REDIS_CMD_MOVE;
        KeyObject src(srcdb, KEY_META, srckey);
        KeyObject dst(dstdb, KEY_META, dstkey);
        KeysLockGuard guard(ctx, src, dst);
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
        while (iter->Valid())
        {
            KeyObject& k = iter->Key();
            if (k.GetKey() != src.GetKey() || k.GetNameSpace() != src.GetNameSpace())
            {
                break;
            }
            k.SetNameSpace(dstdb);
            k.SetKey(dstkey);
            SetKeyValue(ctx, k, iter->Value());
            iter->Del();
            iter->Next();
            moved++;
        }
        DELETE(iter);
        if (cmd.GetType() == REDIS_CMD_RENAME)
        {
            reply.SetStatusCode(STATUS_OK);
        }
        else
        {
            reply.SetInteger(moved > 0 ? 1 : 0);
        }
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
        KeyLockGuard guard(ctx, meta_key);
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
        int64 ttl = ms;
        int64 old_ttl = meta.GetTTL();
        if (old_ttl == ttl)
        {
            return ERR_NOTPERFORMED;
        }
        meta.SetTTL(ttl);
        return 0;
    }
    int Ardb::PExpireat(Context& ctx, RedisCommandFrame& cmd)
    {
        if (ctx.flags.slave && GetConf().slave_ignore_expire)
        {
            return 0;
        }
        RedisReply& reply = ctx.GetReply();
        int64 mills;
        int64 now = get_current_epoch_millis();
        if (cmd.GetArguments().size() == 2)
        {
            if (!string_toint64(cmd.GetArguments()[1], mills))
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
                mills += now;
                break;
            }
            case REDIS_CMD_PEXPIRE:
            case REDIS_CMD_PEXPIRE2:
            {
                mills += now;
                break;
            }
            default:
            {
                FATAL_LOG("Invalid expire command:%d", cmd.GetType());
            }
        }
        int err = 0;
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        /* EXPIRE with negative TTL, or EXPIREAT with a timestamp into the past
         * should never be executed as a DEL when load the AOF or in the context
         * of a slave instance.
         *
         * Instead we take the other branch of the IF statement setting an expire
         * (possibly in the past) and wait for an explicit DEL from the master. */
        if (now > mills && GetConf().master_host.empty() && !IsLoadingData() && cmd.GetType() != REDIS_CMD_PERSIST)
        {
            if ((!ctx.flags.redis_compatible && m_engine->GetFeatureSet().support_merge) || m_engine->Exists(ctx, key))
            {
                DelKey(ctx, key);
                RedisCommandFrame del("del");
                del.AddArg(cmd.GetArguments()[0]);
                ctx.RewriteClientCommand(del);
                reply.SetInteger(0);
                return 0;
            }
        }
        if (!ctx.flags.redis_compatible && m_engine->GetFeatureSet().support_merge)
        {
            reply.SetStatusCode(STATUS_OK);
            Data merge_data;
            merge_data.SetInt64(mills);
            err = MergeKeyValue(ctx, key, REDIS_CMD_PEXPIREAT, DataArray(1, merge_data));
        }
        else
        {
            ValueObject meta_value;
            KeyLockGuard guard(ctx, key);
            if (!CheckMeta(ctx, key, (KeyType) 0, meta_value))
            {
                return 0;
            }
            reply.SetInteger(0);
            err = ERR_NOTPERFORMED;
            if (meta_value.GetType() != 0)
            {
                int64 old_ttl = meta_value.GetTTL();
                if (0 == MergeExpire(ctx, key, meta_value, mills))
                {
                    reply.SetInteger(1);
                    err = 0;
                    SetKeyValue(ctx, key, meta_value);
                    /*
                     * if the storage engine underly do NOT support custom compact filter,
                     * ttl key/value pair must be stored for the later expire scan worker.
                     */
                    if (!m_engine->GetFeatureSet().support_compactfilter)
                    {
                        SaveTTL(ctx, ctx.ns, cmd.GetArguments()[0], old_ttl, mills);
                    }
                }
            }
        }
        if (0 == err && !GetConf().master_host.empty()
                && (cmd.GetType() != REDIS_CMD_PEXPIREAT && cmd.GetType() != REDIS_CMD_PERSIST))
        {
            RedisCommandFrame pexpreat("pexpireat");
            pexpreat.AddArg(cmd.GetArguments()[0]);
            pexpreat.AddArg(stringfromll(mills));
            ctx.RewriteClientCommand(pexpreat);
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

    /*
     * The command returns -2 if the key does not exist.
     * The command returns -1 if the key exists but has no associated expire.
     */
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
            else
            {
                reply.SetInteger(-2);
            }
            return 0;
        }
        int64 ttl = val.GetTTL();
        if (ttl > 0)
        {
            if (cmd.GetType() == REDIS_CMD_PTTL)
            {
                ttl -= get_current_epoch_millis();
            }
            else
            {
                ttl = ttl / 1000 - time(NULL);

            }
            if (ttl < 0)
            {
                if (GetConf().master_host.empty())
                {
                    if (val.GetType() == KEY_STRING)
                    {
                        RemoveKey(ctx, key);
                    }
                    else
                    {
                        DelKey(ctx, key);
                    }
                    RedisCommandFrame del("del");
                    del.AddArg(cmd.GetArguments()[0]);
                    FeedReplicationBacklog(ctx, ctx.ns, del);
                    ttl = -2;
                }
                else
                {
                    ttl = -1;
                }
            }
        }
        else
        {
            ttl = -1;
        }
        reply.SetInteger(ttl);
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

    int Ardb::DelKey(Context& ctx, const std::string& key)
    {
        KeyObject meta_key(ctx.ns, KEY_META, key);
        return DelKey(ctx, meta_key);
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
        ValueObject meta_obj;
        if (0 == m_engine->Get(ctx, meta_key, meta_obj))
        {
            /*
             * need delete ttl sort key with ttl value
             */
            if (m_engine->GetFeatureSet().support_compactfilter)
            {
                int64 ttl = meta_obj.GetTTL();
                if (ttl > 0)
                {
                    Data tll_ns(TTL_DB_NSMAESPACE, false);
                    KeyObject new_ttl_key(tll_ns, KEY_TTL_SORT, "");
                    new_ttl_key.SetTTL(ttl);
                    new_ttl_key.SetTTLKeyNamespace(meta_key.GetNameSpace());
                    new_ttl_key.SetTTLKey(meta_key.GetKey().AsString());
                    m_engine->Del(ctx, new_ttl_key);
                }
            }
            if (meta_obj.GetType() == KEY_STRING)
            {
                int err = RemoveKey(ctx, meta_key);
                return err == 0 ? 1 : 0;
            }
        }
        else
        {
            return 0;
        }
        int removed = 0;
        if (m_engine->GetFeatureSet().support_delete_range
                && (meta_obj.GetObjectLen() < 0 || meta_obj.GetObjectLen() >= GetConf().range_delete_min_size))
        {
            KeyObject end(ctx.ns, KEY_END, meta_key.GetKey());
            m_engine->DelRange(ctx, meta_key, end);
            removed = 1;
        }
        else
        {
            if (NULL == iter)
            {
                iter = m_engine->Find(ctx, meta_key);
            }
            else
            {
                iter->Jump(meta_key);
            }
            while (NULL != iter && iter->Valid())
            {
                KeyObject& k = iter->Key();
                const Data& kdata = k.GetKey();
                if (k.GetNameSpace().Compare(meta_key.GetNameSpace()) != 0
                        || kdata.StringLength() != meta_key.GetKey().StringLength()
                        || strncmp(meta_key.GetKey().CStr(), kdata.CStr(), kdata.StringLength()) != 0)
                {
                    break;
                }
                removed = 1;
                iter->Del();
                //RemoveKey(ctx, k);
                iter->Next();
            }
        }

        if (removed > 0)
        {
            TouchWatchKey(ctx, meta_key);
            ctx.dirty++;
        }
        return removed;
    }

    int Ardb::Del(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        Iterator* iter = NULL;
        int removed = 0;
        ctx.flags.iterate_no_upperbound = cmd.GetArguments().size() > 1 ? 1 : 0;
        for (size_t i = 0; i < cmd.GetArguments().size(); i++)
        {
            KeyObject meta(ctx.ns, KEY_META, cmd.GetArguments()[i]);
            KeyLockGuard guard(ctx, meta);
            removed += DelKey(ctx, meta, iter);
        }
        DELETE(iter);

        reply.SetInteger(removed);
        return 0;
    }

    int Ardb::Dump(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        ObjectBuffer buffer;
        KeyObject meta(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        KeyLockGuard guard(ctx, meta);
        if (!buffer.RedisSave(ctx, cmd.GetArguments()[0], reply.str))
        {
            reply.Clear();
            return 0;
        }
        reply.type = REDIS_REPLY_STRING;
        return 0;
    }
    int Ardb::Restore(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        bool replace = false;
        int64 ttl = 0;
        bool delete_exist = true;
        if (cmd.GetArguments().size() == 4)
        {
            if (!strcasecmp(cmd.GetArguments()[3].c_str(), "replace"))
            {
                replace = true;
            }
            else
            {
                reply.SetErrCode(ERR_INVALID_SYNTAX);
                return 0;
            }
        }
        if (!string_toint64(cmd.GetArguments()[1], ttl) || ttl < 0)
        {
            reply.SetErrorReason("Invalid TTL value, must be >= 0");
            return 0;
        }
        if (ttl > 0)
        {
            ttl += get_current_epoch_millis();
        }
        ObjectBuffer buffer(cmd.GetArguments()[2]);
        if (!buffer.CheckReadPayload())
        {
            reply.SetErrorReason("DUMP payload version or checksum are wrong");
            return 0;
        }
        KeyObject meta(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        KeyLockGuard guard(ctx, meta);
        if (!replace)
        {
            if (m_engine->Exists(ctx, meta))
            {
                reply.SetErrorReason("-BUSYKEY Target key name already exists.");
                return 0;
            }
            delete_exist = false;  //no need to delete
        }
        if (delete_exist)
        {
            DelKey(ctx, meta);
        }
        ctx.flags.create_if_notexist = 1;
        if (buffer.RedisLoad(ctx, cmd.GetArguments()[0], ttl))
        {
            //printf("###restore ss for key:%s at %s\n", cmd.GetArguments()[0].c_str(), ctx.ns.AsString().c_str());
            reply.SetStatusCode(STATUS_OK);
        }
        else
        {
            reply.SetErrorReason("Bad data format");
        }
        return 0;
    }

    int Ardb::Touch(Context& ctx, RedisCommandFrame& cmd)
    {
        int count = 0;
        for (size_t i = 0; i < cmd.GetArguments().size(); i++)
        {
            KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[i]);
            ValueObject meta;
            KeyLockGuard guard(ctx, key);
            if (!CheckMeta(ctx, key, (KeyType) 0, meta))
            {
                return 0;
            }
            if (meta.GetType() == 0)
            {
                continue;
            }
            count++;
        }
        ctx.GetReply().SetInteger(count);
        return 0;
    }

OP_NAMESPACE_END

