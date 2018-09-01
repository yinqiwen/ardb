/*
 *Copyright (c) 2013-2018, yinqiwen <yinqiwen@gmail.com>
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
#include "util/string_helper.hpp"
#include "util/lru.hpp"

#define GTABLE_IDLE   0
#define GTABLE_INUSE  1
#define GTABLE_ERASED 2


OP_NAMESPACE_BEGIN
/* Parse a stream ID in the format given by clients to Redis, that is
 * <ms>-<seq>, and converts it into a streamID structure. If
 * the specified ID is invalid C_ERR is returned and an error is reported
 * to the client, otherwise C_OK is returned. The ID may be in incomplete
 * form, just stating the milliseconds time part of the stream. In such a case
 * the missing part is set according to the value of 'missing_seq' parameter.
 * The IDs "-" and "+" specify respectively the minimum and maximum IDs
 * that can be represented.
 *
 * If 'c' is set to NULL, no reply is sent to the client. */
    static int streamParseIDOrReply(Context& ctx, const std::string& v, StreamID& id, uint64_t missing_seq)
    {
        char buf[128];
        char * dot = NULL;
        if (v.size() > sizeof(buf) - 1) goto invalid;
        memcpy(buf, v.c_str(), v.size());
        buf[v.size()] = '\0';

        /* Handle the "-" and "+" special cases. */
        if (buf[0] == '-' && buf[1] == '\0')
        {
            id.ms = 0;
            id.seq = 0;
            return 0;
        }
        else if (buf[0] == '+' && buf[1] == '\0')
        {
            id.ms = UINT64_MAX;
            id.seq = UINT64_MAX;
            return 0;
        }

        /* Parse <ms>-<seq> form. */
        dot = strchr(buf, '-');
        if (dot) *dot = '\0';
        uint64_t ms, seq;
        if (!str_touint64(buf, ms))
        {
            goto invalid;
        }
        if (dot && !str_touint64(dot + 1, seq))
        {
            goto invalid;
        }
        if (!dot) seq = missing_seq;
        id.ms = ms;
        id.seq = seq;
        return 0;

        invalid: ctx.GetReply().SetErrorReason("Invalid stream ID specified as stream command argument:" + v);
        return -1;
    }

    static void replyStreamID(RedisReply& r, const StreamID& id)
    {
        char reply_buffer[256];
        sprintf(reply_buffer, "%" PRIu64 "-%" PRIu64 , id.ms, id.seq);
        r.SetStatusString(reply_buffer);
    }

    static void replyStreamElement(RedisReply& reply, const StreamID& id, ValueObject& meta, ValueObject& ele)
    {
        RedisReply& rr = reply.AddMember();
        replyStreamID(rr, id);
        RedisReply& fvs = reply.AddMember();
        fvs.ReserveMember(0);
        for (int64_t i = 0; i < ele.GetStreamFieldLength(); i++)
        {
            if (ele.ElementSize() == (size_t)ele.GetStreamFieldLength() + 1)
            {
                fvs.AddMember().SetString(meta.GetStreamMetaFieldNames()[i]);
            }
            else
            {
                fvs.AddMember().SetString(ele.GetStreamFieldName(i));
            }
            fvs.AddMember().SetString(ele.GetStreamFieldValue(i));
        }
    }
    static void replyHelp(RedisReply& reply, const std::string& cmd, const char** help)
    {
        std::string ncmd = string_toupper(cmd);
        ncmd.append(" <subcommand> arg arg ... arg. Subcommands are:");
        reply.AddMember().SetStatusString(ncmd);
        int blen = 0;
        while (help[blen])
        {
            reply.AddMember().SetStatusString(help[blen++]);
        }
    }

    /* Generate the next stream item ID given the previous one. If the current
     * milliseconds Unix time is greater than the previous one, just use this
     * as time part and start with sequence part of zero. Otherwise we use the
     * previous time (and never go backward) and increment the sequence. */
    static void streamNextID(StreamID& last_id, StreamID& new_id)
    {
        uint64_t ms = get_current_epoch_millis();
        if (ms > last_id.ms)
        {
            new_id.ms = ms;
            new_id.seq = 0;
        }
        else
        {
            new_id.ms = last_id.ms;
            new_id.seq = last_id.seq + 1;
        }
    }

    typedef LRUCache<KeyPrefix, StreamGroupTable*> StreamGroupCache;
    typedef TreeSet<StreamGroupTable*>::Type GroupTableSet;
    static StreamGroupCache g_stream_groups;
    static ThreadMutex g_stream_groups_mutex;
    static GroupTableSet g_retired_groups;
    static ThreadMutex g_retired_groups_mutex;

    static void addRetiredGroup(StreamGroupTable* g)
    {
        LockGuard<ThreadMutex> guard(g_retired_groups_mutex);
        g_retired_groups.insert(g);
    }


    static void clearGroup(StreamGroupMeta* g)
    {
        ConsumerTable::iterator cit = g->consumers.begin();
        while (cit != g->consumers.end())
        {
            DELETE(cit->second);
            cit++;
        }
        PELTable::iterator pit = g->consumer_pels.begin();
        while (pit != g->consumer_pels.end())
        {
            DELETE(pit->second);
            pit++;
        }
        DELETE(g);
    }
    static void clearGroupTable(StreamGroupTable* g)
    {
        if (NULL == g)
        {
            return;
        }
        StreamGroupTable::iterator git = g->begin();
        while (git != g->end())
        {
            clearGroup(git->second);
            git++;
        }
        DELETE(g);
    }
    static void addNACK(StreamGroupMeta* g, const std::string& consumer, StreamID sid, StreamNACK* nack)
    {
        StreamConsumerMeta* c = NULL;
        ConsumerTable::iterator found = g->consumers.find((consumer));
        if (found == g->consumers.end())
        {
            c = new StreamConsumerMeta;
            c->name = consumer;
            c->seen_time = get_current_epoch_millis();
            g->consumers[consumer] = c;
        }
        else
        {
            c = found->second;
        }
        if(c->seen_time < (int64_t)nack->delivery_time)
        {
            c->seen_time = nack->delivery_time;
        }
        nack->consumer = c;
        c->pels[sid] = nack;
        g->consumer_pels[sid] = nack;
    }

    static void release_gtable(void* gtable)
    {
        if(NULL == gtable)
        {
            return;
        }
        ((StreamGroupTable*)gtable)->DecRef();
    }

    int Ardb::StreamDel(Context& ctx, const KeyObject& key)
    {
        KeyPrefix prefix;
        prefix.key = key.GetKey();
        prefix.ns = key.GetNameSpace();
        LockGuard<ThreadMutex> guard(g_stream_groups_mutex);
        StreamGroupTable* gtable = NULL;
        if (g_stream_groups.Erase(prefix, gtable))
        {
            clearGroupTable(gtable);
        }
        return 0;
    }

    void Ardb::ClearRetiredStreamCache()
    {
        LockGuard<ThreadMutex> guard(g_retired_groups_mutex);
        if(!g_retired_groups.empty())
        {
            GroupTableSet::iterator sit = g_retired_groups.begin();
            while(sit != g_retired_groups.end())
            {
                StreamGroupTable* g = *sit;
                if(g->ref == 0)
                {
                    DEBUG_LOG("Clear retired stream group cache data.");
                    clearGroupTable(g);
                    sit = g_retired_groups.erase(sit);
                    continue;
                }
                sit++;
            }
        }
    }

    StreamGroupTable* Ardb::StreamLoadGroups(Iterator* iter)
    {
        StreamGroupTable* gtable = new StreamGroupTable;
        StreamGroupMeta* gmeta = NULL;
        while (iter->Valid())
        {
            KeyObject& ik = iter->Key(false);
            if (ik.GetType() != KEY_STREAM_PEL)
            {
                break;
            }
            std::string group_name, consumer_name;
            ik.GetStreamGroup().ToString(group_name);
            if (NULL == gmeta || gmeta->name != group_name)
            {
                gmeta = new StreamGroupMeta;
                gmeta->name = group_name;
                gmeta->lastid.Decode(iter->Value(false).GetStreamGroupStreamId());
                gtable->insert(StreamGroupTable::value_type(group_name, gmeta));
            }
            StreamID sid = ik.GetStreamPELId();
            if (!sid.Empty())
            {
                ValueObject& v = iter->Value(false);
                v.GetConsumerName().ToString(consumer_name);
                StreamNACK* nack = new StreamNACK;
                nack->delivery_count = v.GetNACKDeliveryCount().GetInt64();
                nack->delivery_time = v.GetNACKDeliveryTime().GetInt64();
                addNACK(gmeta, consumer_name, sid, nack);
            }
            iter->Next();
        }
        if (gtable->size() == 0)
        {
            DELETE(gtable);
        }
        return gtable;
    }

    StreamGroupTable* Ardb::StreamLoadGroups(Context& ctx, const KeyPrefix& gk, bool create_ifnotexist)
    {
        StreamGroupTable* gtable = NULL;
        LockGuard<ThreadMutex> guard(g_stream_groups_mutex);
        g_stream_groups.SetMaxCacheSize(GetConf().stream_lru_cache_size);
        if (g_stream_groups.Get(gk, gtable))
        {
            return gtable;
        }

        KeyObject sk(gk.ns, KEY_STREAM_PEL, gk.key);
        StreamGroupMeta* gmeta = NULL;
        Iterator* iter = m_engine->Find(ctx, sk);
        gtable = StreamLoadGroups(iter);
        while (iter->Valid())
        {
            KeyObject& ik = iter->Key(false);
            if (ik.GetType() != KEY_STREAM_PEL || ik.GetNameSpace() != sk.GetNameSpace() || ik.GetKey() != sk.GetKey())
            {
                break;
            }
            std::string group_name, consumer_name;
            ik.GetStreamGroup().ToString(group_name);
            if (NULL == gmeta || gmeta->name != group_name)
            {
                gmeta = new StreamGroupMeta;
                gmeta->name = group_name;
                gmeta->lastid.Decode(iter->Value(false).GetStreamGroupStreamId());
                gtable->insert(StreamGroupTable::value_type(group_name, gmeta));
            }
            StreamID sid = ik.GetStreamPELId();
            if (!sid.Empty())
            {
                ValueObject& v = iter->Value(false);
                v.GetConsumerName().ToString(consumer_name);
                StreamNACK* nack = new StreamNACK;
                nack->delivery_count = v.GetNACKDeliveryCount().GetInt64();
                nack->delivery_time = v.GetNACKDeliveryTime().GetInt64();
                addNACK(gmeta, consumer_name, sid, nack);
            }
            iter->Next();
        }
        if(create_ifnotexist)
        {
            if(NULL == gtable)
            {
                gtable = new StreamGroupTable;
            }
        }
        if(NULL != gtable)
        {
            StreamGroupCache::CacheEntry erased;
            g_stream_groups.Insert(gk, gtable, erased);
            if (NULL != erased.second)
            {
                StreamGroupTable* g = erased.second;
                addRetiredGroup(g);
            }
            gtable->IncRef();
            ctx.AddPostCmdFunc(release_gtable, gtable);
        }
        return gtable;
    }

    StreamGroupTable* Ardb::StreamLoadGroups(Context& ctx, const std::string& key, bool create_ifnotexist)
    {
        KeyPrefix gk;
        gk.key.SetString(key, false, true);
        gk.ns = ctx.ns;
        return StreamLoadGroups(ctx, gk, create_ifnotexist);
    }

    StreamGroupMeta* Ardb::StreamLoadGroup(Context& ctx, const KeyPrefix& gk, const std::string& group)
    {
        StreamGroupTable* gtable = StreamLoadGroups(ctx, gk, false);
        if (NULL != gtable)
        {
            StreamGroupTable::iterator git = gtable->find(group);
            if (git == gtable->end())
            {
                return NULL;
            }
            return git->second;
        }
        return NULL;
    }

    StreamGroupMeta* Ardb::StreamLoadGroup(Context& ctx, const std::string& key, const std::string& group)
    {
        KeyPrefix gk;
        gk.key.SetString(key, false, true);
        gk.ns = ctx.ns;
        return StreamLoadGroup(ctx, gk, group);
    }

    int Ardb::StreamUpdateNACK(Context& ctx, const std::string& key, const std::string& group,
            const std::string& consumer, const StreamID& id, StreamGroupMeta* group_meta, StreamNACK* nack)
    {
        if (NULL != nack->consumer && nack->consumer->name != consumer)
        {
            nack->consumer->pels.erase(id);
            nack->consumer = NULL;
        }
        ConsumerTable::iterator found = group_meta->consumers.find(consumer);
        StreamConsumerMeta* consumer_meta = NULL;
        if (found != group_meta->consumers.end())
        {
            consumer_meta = found->second;
        }
        nack->consumer = consumer_meta;
        group_meta->consumer_pels[id] = nack;
        if (NULL != consumer_meta)
        {
            consumer_meta->pels[id] = nack;
            consumer_meta->seen_time = nack->delivery_time;
        }
        KeyObject k(ctx.ns, KEY_STREAM_PEL, key);
        k.SetStreamGroup(group);
        k.SetStreamPELId(id);
        ValueObject v;
        v.SetType(KEY_STREAM_PEL);
        v.GetNACKConsumer().SetString(consumer, false, true);
        v.GetNACKDeliveryCount().SetInt64(nack->delivery_count);
        v.GetNACKDeliveryTime().SetInt64(nack->delivery_time);
        m_engine->Put(ctx, k, v);
        /* We need to generate an XCLAIM that will work in a idempotent fashion:
         *
         * XCLAIM <key> <group> <consumer> 0 <id> TIME <milliseconds-unix-time>
         *        RETRYCOUNT <count> FORCE JUSTID.
         *
         * Note that JUSTID is useful in order to avoid that XCLAIM will do
         * useless work in the slave side, trying to fetch the stream item. */
        if (GetConf().master_host.empty())
        {
            RedisCommandFrame xclaim;
            xclaim.SetCommand("XCLAIM");
            xclaim.GetMutableArguments().push_back(key);
            xclaim.GetMutableArguments().push_back(group);
            xclaim.GetMutableArguments().push_back(consumer);
            xclaim.GetMutableArguments().push_back("0");
            char tmp[512];
            sprintf(tmp, "%" PRIu64 "-%" PRIu64 , id.ms, id.seq);
            xclaim.GetMutableArguments().push_back(tmp);
            xclaim.GetMutableArguments().push_back("TIME");
            xclaim.GetMutableArguments().push_back(stringfromll(nack->delivery_time));
            xclaim.GetMutableArguments().push_back("RETRYCOUNT");
            xclaim.GetMutableArguments().push_back(stringfromll(nack->delivery_count));
            xclaim.GetMutableArguments().push_back("FORCE");
            xclaim.GetMutableArguments().push_back("JUSTID");
            FeedReplicationBacklog(ctx, ctx.ns, xclaim);
        }
        return 0;
    }

    int Ardb::StreamCreateNACK(Context& ctx, const std::string& key, const std::string& group,
            const std::string& consumer, const StreamID& id, StreamGroupMeta* group_meta)
    {
        StreamNACK* nack = new StreamNACK;
        nack->delivery_count = 1;
        nack->delivery_time = (int64_t) (get_current_epoch_millis());
        return StreamUpdateNACK(ctx, key, group, consumer, id, group_meta, nack);
    }
    int Ardb::StreamDelItem(Context& ctx, const std::string& key, const StreamID& id)
    {
        KeyObject k(ctx.ns, KEY_STREAM_ELEMENT, key);
        k.SetStreamID(id);
        ValueObject tmp;
        if (!m_engine->Exists(ctx, k, tmp))
        {
            return 0;
        }
        m_engine->Del(ctx, k);
        return 1;
    }

    int Ardb::StreamCreateCG(Context& ctx, const std::string& key, const std::string& group, const StreamID& id,
            ValueObject& v)
    {
        KeyObject keyobj(ctx.ns, KEY_META, key);
        KeyLockGuard guard(ctx, keyobj);
        KeyObject k(ctx.ns, KEY_STREAM_PEL, key);
        k.SetStreamGroup(group);
        if (0 == m_engine->Get(ctx, k, v))
        {
            //already exist
            return -1;
        }
        v.SetType(KEY_STREAM_PEL);
        id.Encode(v.GetStreamGroupStreamId());
        m_engine->Put(ctx, k, v);

        StreamGroupTable* gs = StreamLoadGroups(ctx, key, true);
        if (NULL != gs)
        {
            StreamGroupMeta* gmeta = new StreamGroupMeta;
            gmeta->name = group;
            gmeta->lastid = id;
            gs->insert(StreamGroupTable::value_type(group, gmeta));
        }
        return 0;
    }
    int Ardb::StreamMinMaxID(Context& ctx, const std::string& key, StreamID& min, StreamID& max, ValueObject& minv,
            ValueObject& maxv)
    {
        KeyObject ck(ctx.ns, KEY_STREAM_ELEMENT, key);
        Iterator* citer = m_engine->Find(ctx, ck);
        if (!citer->Valid())
        {
            DELETE(citer);
            return -1;
        }
        KeyObject& ik = citer->Key(false);
        if (ik.GetType() != KEY_STREAM_ELEMENT || ik.GetKey() != ck.GetKey())
        {
            DELETE(citer);
            return -1;
        }
        min = ik.GetStreamID();
        max = min;
        minv = citer->Value(true);
        if (maxv.GetType() == 0)
        {
            maxv = minv;
        }
        KeyObject next(ctx.ns, KEY_STREAM_ELEMENT + 1, key);
        citer->Jump(next);
        citer->Prev();
        if (citer->Valid())
        {
            KeyObject& nk = citer->Key(false);
            if (nk.GetType() == KEY_STREAM_ELEMENT && nk.GetKey() == ck.GetKey())
            {
                max = nk.GetStreamID();
                maxv = citer->Value(true);
            }
        }
        DELETE(citer);
        return 0;
    }
    int Ardb::StreamAddConsumer(Context& ctx, const std::string& consumer, StreamGroupMeta* group_meta)
    {
        ConsumerTable::iterator found = group_meta->consumers.find(consumer);
        if (found == group_meta->consumers.end())
        {
            StreamConsumerMeta* c = new StreamConsumerMeta;
            c->name = consumer;
            c->seen_time = get_current_epoch_millis();
            group_meta->consumers[consumer] = c;
        }
        return 0;
    }
    int64_t Ardb::StreamDelGroup(Context& ctx, const std::string& key, const std::string& group)
    {
        StreamGroupTable* gtable = StreamLoadGroups(ctx, key, false);
        if (NULL == gtable)
        {
            return 0;
        }
        StreamGroupTable::iterator git = gtable->find(group);
        if (git == gtable->end())
        {
            return 0;
        }
        StreamGroupMeta* gmeta = git->second;
        if (NULL == gmeta)
        {
            return 0;
        }
        int64_t pel_counter = gmeta->consumer_pels.size();
        PELTable::iterator pit = gmeta->consumer_pels.begin();
        while (pit != gmeta->consumer_pels.end())
        {
            KeyObject gk(ctx.ns, KEY_STREAM_PEL, key);
            gk.SetStreamGroup(group);
            gk.SetStreamPELId(pit->first);
            m_engine->Del(ctx, gk);
            gmeta->consumer_pels.erase(pit->first);
            DELETE(pit->second);
            pit++;
        }
        clearGroup(gmeta);
        KeyObject gk(ctx.ns, KEY_STREAM_PEL, key);
        gk.SetStreamGroup(group);
        m_engine->Del(ctx, gk);
        gtable->erase(git);
        return pel_counter;
    }
    int64_t Ardb::StreamDelConsumer(Context& ctx, const std::string& key, const std::string& group,
            const std::string& consumer)
    {
        StreamGroupMeta* gmeta = StreamLoadGroup(ctx, key, group);
        if (NULL == gmeta)
        {
            return 0;
        }
        ConsumerTable::iterator cfound = gmeta->consumers.find(consumer);
        if (cfound == gmeta->consumers.end())
        {
            return 0;
        }
        StreamConsumerMeta* c = cfound->second;
        if (NULL == c)
        {
            gmeta->consumers.erase(cfound);
            return 0;
        }
        int64_t pel_counter = c->pels.size();
        PELTable::iterator pit = c->pels.begin();
        while (pit != c->pels.end())
        {
            KeyObject gk(ctx.ns, KEY_STREAM_PEL, key);
            gk.SetStreamGroup(group);
            gk.SetStreamPELId(pit->first);
            m_engine->Del(ctx, gk);
            gmeta->consumer_pels.erase(pit->first);
            DELETE(pit->second);
            pit++;
        }
        gmeta->consumers.erase(cfound);
        DELETE(c);
        return pel_counter;
    }

    int64_t Ardb::StreamTrimByLength(Context& ctx, const std::string& key, ValueObject& meta, size_t maxlen, int approx)
    {
        int64_t trimed = 0;
        KeyObject start(ctx.ns, KEY_STREAM_ELEMENT, key);
        Iterator* iter = m_engine->Find(ctx, start);
        WriteBatchGuard batch(ctx, m_engine);
        while (iter->Valid() && meta.GetObjectLen() > (int64_t)maxlen)
        {
            KeyObject& ik = iter->Key(false);
            if (ik.GetType() != KEY_STREAM_ELEMENT || ik.GetNameSpace() != start.GetNameSpace()
                    || ik.GetKey() != start.GetKey())
            {
                break;
            }
            trimed++;
            meta.SetObjectLen(meta.GetObjectLen() - 1);
            iter->Del();
            iter->Next();
        }
        DELETE(iter);
        return trimed;
    }

    /* XADD key [MAXLEN <count>] <ID or *> [field value] [field value] ... */
    int Ardb::XAdd(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.flags.create_if_notexist = 1;
        RedisReply& reply = ctx.GetReply();
        const std::string& keystr = cmd.GetArguments()[0];
        KeyObject key(ctx.ns, KEY_META, keystr);
        {
            KeyLockGuard guard(ctx, key);
            ValueObject meta;
            if (!CheckMeta(ctx, keystr, KEY_STREAM, meta))
            {
                return 0;
            }
            StreamID id;
            int64_t maxlen = 0; /* 0 means no maximum length. */
            int approx_maxlen = 0; /* If 1 only delete whole radix tree nodes, so
             the maxium length is not applied verbatim. */
            //int maxlen_arg_idx = 0; /* Index of the count in MAXLEN, for rewriting. */
            bool id_given = false;
            size_t i = 1;
            for (; i < cmd.GetArguments().size(); i++)
            {
                int moreargs = (cmd.GetArguments().size() - i); /* Number of additional arguments. */
                const char *opt = cmd.GetArguments()[i].c_str();
                if (opt[0] == '*' && opt[1] == '\0')
                {
                    /* This is just a fast path for the common case of auto-ID
                     * creation. */
                    break;
                }
                else if (!strcasecmp(opt, "maxlen") && moreargs)
                {
                    const char *next = cmd.GetArguments()[i + 1].c_str();
                    /* Check for the form MAXLEN ~ <count>. */
                    if (moreargs >= 2 && next[0] == '~' && next[1] == '\0')
                    {
                        approx_maxlen = 1;
                        i++;
                    }
                    if (!string_toint64(cmd.GetArguments()[i + 1], maxlen))
                    {
                        reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
                        return 0;
                    }
                    i++;
                    //maxlen_arg_idx = i;
                }
                else
                {
                    /* If we are here is a syntax error or a valid ID. */
                    if (streamParseIDOrReply(ctx, cmd.GetArguments()[i], id, 0) != 0) return 0;
                    id_given = true;
                    break;
                }
            }
            size_t field_pos = i + 1;
            size_t id_pos = i;

            /* Check arity. */
            if ((cmd.GetArguments().size() - field_pos) < 2 || ((cmd.GetArguments().size() - field_pos) % 2) == 1)
            {
                ctx.GetReply().SetErrorReason("wrong number of arguments for XADD");
                return 0;
            }
            bool create_stream = false;
            if (meta.GetType() == 0)
            {
                meta.SetType(KEY_STREAM);
                meta.SetObjectLen(1);
                create_stream = true;
            }
            else
            {
                meta.SetObjectLen(meta.GetObjectLen() + 1);
            }
            if (id_given && id.Compare(meta.GetMetaObject().stream_last_id) <= 0)
            {
                ctx.GetReply().SetErrorReason("The ID specified in XADD is equal or smaller than the "
                        "target stream top item");
                return 0;
            }
            if (!id_given)
            {
                streamNextID(meta.GetMetaObject().stream_last_id, id);
            }
            meta.GetMetaObject().stream_last_id = id;
            KeyObject stream_ele(ctx.ns, KEY_STREAM_ELEMENT, keystr);
            stream_ele.SetStreamID(id);
            ValueObject stream_ele_val;
            stream_ele_val.SetType(KEY_STREAM_ELEMENT);
            int stream_meta_fields_equal = -1; // -1: not compared 0: not equal  1:  equal
            size_t field_len = (cmd.GetArguments().size() - field_pos) / 2;
            stream_ele_val.SetStreamFieldLength(field_len);
            DataArray& meta_field_names = meta.GetStreamMetaFieldNames();
            if (meta_field_names.size() != field_len)
            {
                stream_meta_fields_equal = 0;
            }
            for (size_t i = field_pos, field_idx = 0; i < cmd.GetArguments().size(); i += 2, field_idx++)
            {
                if (create_stream)
                {
                    Data f;
                    f.SetString(cmd.GetArguments()[i], false, false);
                    meta_field_names.push_back(f);
                    stream_meta_fields_equal = 1;
                }
                else
                {
                    if (-1 == stream_meta_fields_equal)
                    {
                        if (field_idx >= meta_field_names.size())
                        {
                            stream_meta_fields_equal = 0;
                        }
                        else
                        {
                            std::string tmp;
                            if (cmd.GetArguments()[i] != meta_field_names[field_idx].ToString(tmp))
                            {
                                stream_meta_fields_equal = 0;
                            }
                        }
                    }
                }
                stream_ele_val.GetStreamFieldValue(field_idx).SetString(cmd.GetArguments()[i + 1], true, false);
            }
            if (-1 == stream_meta_fields_equal)
            {
                stream_meta_fields_equal = 1;
            }
            if (0 == stream_meta_fields_equal)
            {
                for (size_t i = field_pos, field_idx = 0; i < cmd.GetArguments().size(); i += 2, field_idx++)
                {
                    stream_ele_val.GetStreamFieldName(field_idx).SetString(cmd.GetArguments()[i], false, false);
                }
            }
            SetKeyValue(ctx, stream_ele, stream_ele_val);
            SetKeyValue(ctx, key, meta);
            replyStreamID(reply, id);

            //trim stream
            if (approx_maxlen && meta.GetObjectLen() > maxlen)
            {
                StreamTrimByLength(ctx, keystr, meta, maxlen, approx_maxlen);
            }
            cmd.ClearRawProtocolData();
            std::string idstr;
            id.ToString(idstr);
            cmd.GetMutableArguments()[id_pos].assign(idstr);
        }
        SignalKeyAsReady(ctx, keystr);
        return 0;
    }

    /* XACK <key> <group> <id> <id> ... <id>
     *
     * Acknowledge a message as processed. In practical terms we just check the
     * pendine entries list (PEL) of the group, and delete the PEL entry both from
     * the group and the consumer (pending messages are referenced in both places).
     *
     * Return value of the command is the number of messages successfully
     * acknowledged, that is, the IDs we were actually able to resolve in the PEL.
     */
    int Ardb::XACK(Context& ctx, RedisCommandFrame& cmd)
    {
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        KeyLockGuard guard(ctx, key);
        StreamGroupMeta* group = StreamLoadGroup(ctx, cmd.GetArguments()[0], cmd.GetArguments()[1]);
        if (NULL == group)
        {
            ctx.GetReply().SetInteger(0);
            return 0;
        }
        int acknowledged = 0;
        for (size_t i = 2; i < cmd.GetArguments().size(); i++)
        {
            KeyObject pkey(ctx.ns, KEY_STREAM_PEL, cmd.GetArguments()[0]);
            pkey.SetStreamGroup(cmd.GetArguments()[1]);
            StreamID id;
            if (streamParseIDOrReply(ctx, cmd.GetArguments()[i], id, 0) != 0)
            {
                return 0;
            }
            pkey.SetStreamPELId(id);
            PELTable::iterator found = group->consumer_pels.find(id);
            if (found != group->consumer_pels.end())
            {
                acknowledged++;
                m_engine->Del(ctx, pkey);
                StreamNACK* nack = found->second;
                if (NULL != nack->consumer)
                {
                    nack->consumer->pels.erase(id);
                    nack->consumer->seen_time = get_current_epoch_millis();
                }
                DELETE(nack);
                group->consumer_pels.erase(found);
            }

        }
        ctx.GetReply().SetInteger(acknowledged);
        return 0;
    }

    /* -----------------------------------------------------------------------
     * Consumer groups commands
     * ----------------------------------------------------------------------- */

    /* XGROUP CREATE <key> <groupname> <id or $>
     * XGROUP SETID <key> <id or $>
     * XGROUP DELGROUP <key> <groupname>
     * XGROUP DELCONSUMER <key> <groupname> <consumername> */
    int Ardb::XGroup(Context& ctx, RedisCommandFrame& cmd)
    {
        const char *help[] = { "CREATE      <key> <groupname> <id or $>  -- Create a new consumer group.",
                "SETID       <key> <groupname> <id or $>  -- Set the current group ID.",
                "DELGROUP    <key> <groupname>            -- Remove the specified group.",
                "DELCONSUMER <key> <groupname> <consumer> -- Remove the specified conusmer.",
                "HELP                                     -- Prints this help.",
                NULL };
        //const char* grpname = NULL;
        const char *opt = cmd.GetArguments()[0].c_str(); /* Subcommand name. */
        ValueObject meta;
        /* Lookup the key now, this is common for all the subcommands but HELP. */
        if (cmd.GetArguments().size() >= 3)
        {
            KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[1]);
            KeyLockGuard guard(ctx, key);
            if (!CheckMeta(ctx, cmd.GetArguments()[1], KEY_STREAM, meta))
            {
                return 0;
            }
            if (meta.GetType() == 0)
            {
                ctx.GetReply().SetErrCode(ERR_ENTRY_NOT_EXIST);
                return 0;
            }
            //grpname = cmd.GetArguments()[2].c_str();

            /* Certain subcommands require the group to exist. */
            if ((!strcasecmp(opt, "SETID") || !strcasecmp(opt, "DELGROUP") || !strcasecmp(opt, "DELCONSUMER"))
                    && (NULL == StreamLoadGroup(ctx, cmd.GetArguments()[1], cmd.GetArguments()[2])))
            {
                std::string err = "NOGROUP No such consumer group '" + cmd.GetArguments()[2] + "' for key name '"
                        + cmd.GetArguments()[1] + "'";
                ctx.GetReply().SetErrorReason(err);
                return 0;
            }
        }

        /* Dispatch the different subcommands. */
        if (!strcasecmp(opt, "CREATE") && cmd.GetArguments().size() == 4)
        {
            StreamID id;
            if (!strcmp(cmd.GetArguments()[3].c_str(), "$"))
            {
                id = meta.GetStreamLastId();
            }
            else if (streamParseIDOrReply(ctx, cmd.GetArguments()[3], id, 0) != 0)
            {
                return 0;
            }
            ValueObject cg;
            if (0 == StreamCreateCG(ctx, cmd.GetArguments()[1], cmd.GetArguments()[2], id, cg))
            {
                ctx.GetReply().SetStatusCode(STATUS_OK);
            }
            else
            {
                ctx.GetReply().SetErrorReason("BUSYGROUP Consumer Group name already exists");
            }
            return 0;
        }
        else if (!strcasecmp(opt, "SETID") && cmd.GetArguments().size() == 4)
        {
        }
        else if (!strcasecmp(opt, "DELGROUP") && cmd.GetArguments().size() == 3)
        {
            KeyObject k(ctx.ns, KEY_META, cmd.GetArguments()[1]);
            KeyLockGuard guard(ctx, k);
            int64_t pending = StreamDelGroup(ctx, cmd.GetArguments()[1], cmd.GetArguments()[2]);
            ctx.GetReply().SetInteger(pending);
        }
        else if (!strcasecmp(opt, "DELCONSUMER") && cmd.GetArguments().size() == 4)
        {
            /* Delete the consumer and returns the number of pending messages
             * that were yet associated with such a consumer. */
            KeyObject k(ctx.ns, KEY_META, cmd.GetArguments()[1]);
            KeyLockGuard guard(ctx, k);
            int64_t pending = StreamDelConsumer(ctx, cmd.GetArguments()[1], cmd.GetArguments()[2],
                    cmd.GetArguments()[3]);
            ctx.GetReply().SetInteger(pending);
        }
        else if (!strcasecmp(opt, "HELP"))
        {
            replyHelp(ctx.GetReply(), cmd.GetCommand(), help);
        }
        else
        {
            ctx.GetReply().SetErrCode(ERR_INVALID_SYNTAX);
        }
        return 0;
    }
    /* XDEL <key> [<ID1> <ID2> ... <IDN>]
     *
     * Removes the specified entries from the stream. Returns the number
     * of items actaully deleted, that may be different from the number
     * of IDs passed in case certain IDs do not exist. */
    int Ardb::XDel(Context& ctx, RedisCommandFrame& cmd)
    {
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        KeyLockGuard guard(ctx, key);
        ValueObject meta;
        if (!CheckMeta(ctx, cmd.GetArguments()[0], KEY_STREAM, meta))
        {
            return 0;
        }
        if (0 == meta.GetType())
        {
            ctx.GetReply().SetInteger(0);
            return 0;
        }

        /* We need to sanity check the IDs passed to start. Even if not
         * a big issue, it is not great that the command is only partially
         * executed becuase at some point an invalid ID is parsed. */
        StreamID id;
        for (size_t j = 1; j < cmd.GetArguments().size(); j++)
        {
            if (streamParseIDOrReply(ctx, cmd.GetArguments()[j], id, 0) != 0) return 0;
        }

        /* Actaully apply the command. */
        int deleted = 0;
        WriteBatchGuard batch(ctx, m_engine);
        for (size_t j = 1; j < cmd.GetArguments().size(); j++)
        {
            streamParseIDOrReply(ctx, cmd.GetArguments()[j], id, 0); /* Retval already checked. */
            deleted += StreamDelItem(ctx, cmd.GetArguments()[0], id);
        }
        ctx.GetReply().SetInteger(deleted);
        return 0;
    }
    /* General form: XTRIM <key> [... options ...]
     *
     * List of options:
     *
     * MAXLEN [~] <count>       -- Trim so that the stream will be capped at
     *                             the specified length. Use ~ before the
     *                             count in order to demand approximated trimming
     *                             (like XADD MAXLEN option).
     */

#define TRIM_STRATEGY_NONE 0
#define TRIM_STRATEGY_MAXLEN 1
    int Ardb::XTrim(Context& ctx, RedisCommandFrame& cmd)
    {
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        KeyLockGuard guard(ctx, key);
        ValueObject meta;
        if (!CheckMeta(ctx, cmd.GetArguments()[0], KEY_STREAM, meta))
        {
            return 0;
        }
        if (0 == meta.GetType())
        {
            ctx.GetReply().SetInteger(0);
            return 0;
        }

        /* Argument parsing. */
        int trim_strategy = TRIM_STRATEGY_NONE;
        int64_t maxlen = 0; /* 0 means no maximum length. */
        int approx_maxlen = 0; /* If 1 only delete whole radix tree nodes, so
         the maxium length is not applied verbatim. */

        /* Parse options. */
        size_t i = 1; /* Start of options. */
        for (; i < cmd.GetArguments().size(); i++)
        {
            int moreargs = (cmd.GetArguments().size() - i);
            /* Number of additional arguments. */
            const char *opt = cmd.GetArguments()[i].c_str();
            if (!strcasecmp(opt, "maxlen") && moreargs)
            {
                trim_strategy = TRIM_STRATEGY_MAXLEN;
                const char *next = cmd.GetArguments()[i + 1].c_str();
                /* Check for the form MAXLEN ~ <count>. */
                if (moreargs >= 2 && next[0] == '~' && next[1] == '\0')
                {
                    approx_maxlen = 1;
                    i++;
                }
                if (!string_toint64(cmd.GetArguments()[i + 1], maxlen))
                {
                    ctx.GetReply().SetErrCode(ERR_INVALID_INTEGER_ARGS);
                    return 0;
                }
                i++;
            }
            else
            {
                ctx.GetReply().SetErrCode(ERR_INVALID_SYNTAX);
                return 0;
            }
        }

        /* Perform the trimming. */
        int64_t deleted = 0;
        if (trim_strategy == TRIM_STRATEGY_MAXLEN)
        {
            deleted = StreamTrimByLength(ctx, cmd.GetArguments()[0], meta, maxlen, approx_maxlen);
            ctx.GetReply().SetInteger(deleted);
            return 0;
        }
        else
        {
            ctx.GetReply().SetErrorReason("XTRIM called without an option to trim the stream");
            return 0;
        }
    }

    int Ardb::XLen(Context& ctx, RedisCommandFrame& cmd)
    {
        ValueObject meta;
        if (!CheckMeta(ctx, cmd.GetArguments()[0], KEY_STREAM, meta))
        {
            return 0;
        }
        if (0 == meta.GetType())
        {
            ctx.GetReply().SetInteger(0);
            return 0;
        }
        ctx.GetReply().SetInteger(meta.GetObjectLen());
        return 0;
    }

    /* XRANGE key start end [COUNT <n>] */
    /* XREVRANGE key end start [COUNT <n>] */
    int Ardb::XRange(Context& ctx, RedisCommandFrame& cmd)
    {
        bool rev = cmd.GetType() == REDIS_CMD_XREVRANGE;
        StreamID startid, endid;
        int64_t count = 0;
        const std::string& startarg = rev ? cmd.GetArguments()[2] : cmd.GetArguments()[1];
        const std::string& endarg = rev ? cmd.GetArguments()[1] : cmd.GetArguments()[2];

        if (0 != streamParseIDOrReply(ctx, startarg, startid, 0)) return 0;
        if (0 != streamParseIDOrReply(ctx, endarg, endid, UINT64_MAX)) return 0;

        /* Parse the COUNT option if any. */
        if (cmd.GetArguments().size() > 3)
        {
            for (size_t j = 3; j < cmd.GetArguments().size(); j++)
            {
                int additional = cmd.GetArguments().size() - j - 1;
                if (strcasecmp(cmd.GetArguments()[j].c_str(), "COUNT") == 0 && additional >= 1)
                {
                    if (!string_toint64(cmd.GetArguments()[j + 1], count))
                    {
                        ctx.GetReply().SetErrCode(ERR_INVALID_INTEGER_ARGS);
                        return 0;
                    }
                    if (count < 0) count = 0;
                    j++; /* Consume additional arg. */
                }
                else
                {
                    ctx.GetReply().SetErrCode(ERR_INVALID_SYNTAX);
                    return 0;
                }
            }
        }
        ValueObject meta;
        if (!CheckMeta(ctx, cmd.GetArguments()[0], KEY_STREAM, meta))
        {
            return 0;
        }
        ctx.GetReply().ReserveMember(0);
        if (0 == meta.GetType())
        {
            return 0;
        }
        KeyObject key(ctx.ns, KEY_STREAM_ELEMENT, cmd.GetArguments()[0]);
        if (rev)
        {
            key.SetStreamID(endid);
        }
        else
        {
            key.SetStreamID(startid);
        }
        ctx.flags.iterate_total_order = 1;
        Iterator* iter = m_engine->Find(ctx, key);
        if (rev)
        {
            if (!iter->Valid() || iter->Key(false).GetType() != KEY_STREAM_ELEMENT)
            {
                iter->JumpToLast();
            }
        }
        bool first_iter = true;
        RedisReply& reply = ctx.GetReply();
        while (iter->Valid())
        {
            if (count > 0)
            {
                if (reply.MemberSize() >= (size_t)count)
                {
                    break;
                }
            }
            KeyObject& ik = iter->Key(false);
            if (ik.GetType() != KEY_STREAM_ELEMENT)
            {
                break;
            }
            StreamID iid = ik.GetStreamID();
            if (rev && first_iter) //first for rev
            {
                first_iter = false;
                if (iid.Compare(endid) > 0)
                {
                    iter->Prev();
                    continue;
                }
            }

            if (rev && iid.Compare(startid) < 0)
            {
                break;
            }
            if (iid.Compare(endid) > 0)
            {
                break;
            }
            ValueObject& iv = iter->Value(false);
            RedisReply& rr = reply.AddMember();
            replyStreamID(rr.AddMember(), iid);
            RedisReply& fvs = rr.AddMember();
            fvs.ReserveMember(0);
            for (int64_t i = 0; i < iv.GetStreamFieldLength(); i++)
            {
                if (iv.ElementSize() == (size_t)iv.GetStreamFieldLength() + 1)
                {
                    fvs.AddMember().SetString(meta.GetStreamMetaFieldNames()[i]);
                }
                else
                {
                    fvs.AddMember().SetString(iv.GetStreamFieldName(i));
                }
                fvs.AddMember().SetString(iv.GetStreamFieldValue(i));
            }
            if (rev)
            {
                iter->Prev();
            }
            else
            {
                iter->Next();
            }
        }
        DELETE(iter);
        return 0;
    }
    int Ardb::XRevRange(Context& ctx, RedisCommandFrame& cmd)
    {
        return XRange(ctx, cmd);
    }

    /* XCLAIM <key> <group> <consumer> <min-idle-time> <ID-1> <ID-2>
     *        [IDLE <milliseconds>] [TIME <mstime>] [RETRYCOUNT <count>]
     *        [FORCE] [JUSTID]
     *
     * Gets ownership of one or multiple messages in the Pending Entries List
     * of a given stream consumer group.
     *
     * If the message ID (among the specified ones) exists, and its idle
     * time greater or equal to <min-idle-time>, then the message new owner
     * becomes the specified <consumer>. If the minimum idle time specified
     * is zero, messages are claimed regardless of their idle time.
     *
     * All the messages that cannot be found inside the pending entires list
     * are ignored, but in case the FORCE option is used. In that case we
     * create the NACK (representing a not yet acknowledged message) entry in
     * the consumer group PEL.
     *
     * This command creates the consumer as side effect if it does not yet
     * exists. Moreover the command reset the idle time of the message to 0,
     * even if by using the IDLE or TIME options, the user can control the
     * new idle time.
     *
     * The options at the end can be used in order to specify more attributes
     * to set in the representation of the pending message:
     *
     * 1. IDLE <ms>:
     *      Set the idle time (last time it was delivered) of the message.
     *      If IDLE is not specified, an IDLE of 0 is assumed, that is,
     *      the time count is reset because the message has now a new
     *      owner trying to process it.
     *
     * 2. TIME <ms-unix-time>:
     *      This is the same as IDLE but instead of a relative amount of
     *      milliseconds, it sets the idle time to a specific unix time
     *      (in milliseconds). This is useful in order to rewrite the AOF
     *      file generating XCLAIM commands.
     *
     * 3. RETRYCOUNT <count>:
     *      Set the retry counter to the specified value. This counter is
     *      incremented every time a message is delivered again. Normally
     *      XCLAIM does not alter this counter, which is just served to clients
     *      when the XPENDING command is called: this way clients can detect
     *      anomalies, like messages that are never processed for some reason
     *      after a big number of delivery attempts.
     *
     * 4. FORCE:
     *      Creates the pending message entry in the PEL even if certain
     *      specified IDs are not already in the PEL assigned to a different
     *      client. However the message must be exist in the stream, otherwise
     *      the IDs of non existing messages are ignored.
     *
     * 5. JUSTID:
     *      Return just an array of IDs of messages successfully claimed,
     *      without returning the actual message.
     *
     * The command returns an array of messages that the user
     * successfully claimed, so that the caller is able to understand
     * what messages it is now in charge of. */
    int Ardb::XClaim(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& keystr = cmd.GetArguments()[0];
        const std::string& group_name = cmd.GetArguments()[1];
        const std::string& consumer_name = cmd.GetArguments()[2];

        KeyObject key(ctx.ns, KEY_META, keystr);
        ValueObject meta;
        KeyLockGuard lock(ctx, key);
        if (!CheckMeta(ctx, key, KEY_STREAM, meta))
        {
            return 0;
        }
        StreamGroupMeta* group_meta = StreamLoadGroup(ctx, keystr, group_name);
        if (NULL == group_meta)
        {
            std::string err = "NOGROUP No such key '" + cmd.GetArguments()[0] + "' or consumer group '"
                    + cmd.GetArguments()[1] + "'";
            ctx.GetReply().SetErrorReason(err);
            return 0;
        }

        int64_t minidle; /* Minimum idle time argument. */
        int64_t retrycount = -1; /* -1 means RETRYCOUNT option not given. */
        if (!string_toint64(cmd.GetArguments()[3], minidle))
        {
            ctx.GetReply().SetErrorReason("Invalid min-idle-time argument for XCLAIM");
            return 0;
        }
        if (minidle < 0) minidle = 0;

        /* Start parsing the IDs, so that we abort ASAP if there is a syntax
         * error: the return value of this command cannot be an error in case
         * the client successfully claimed some message, so it should be
         * executed in a "all or nothing" fashion. */
        size_t j;
        std::vector<StreamID> ids;
        for (j = 4; j < cmd.GetArguments().size(); j++)
        {
            StreamID id;
            if (streamParseIDOrReply(ctx, cmd.GetArguments()[j], id, 0) != 0) return 0;
            ids.push_back(id);
        }
        ctx.GetReply().Clear();

        //int last_id_arg = j - 1; /* Next time we iterate the IDs we now the range. */
        int force = 0;
        int justid = 0;
        /* If we stopped because some IDs cannot be parsed, perhaps they
         * are trailing options. */
        int64_t now = get_current_epoch_millis();
        int64_t deliverytime = -1; /* -1 means IDLE/TIME options not given. */
        for (; j < cmd.GetArguments().size(); j++)
        {
            int moreargs = (cmd.GetArguments().size() - 1) - j; /* Number of additional arguments. */
            const char *opt = cmd.GetArguments()[j].c_str();
            if (!strcasecmp(opt, "FORCE"))
            {
                force = 1;
            }
            else if (!strcasecmp(opt, "JUSTID"))
            {
                justid = 1;
            }
            else if (!strcasecmp(opt, "IDLE") && moreargs)
            {
                j++;
                if (!string_toint64(cmd.GetArguments()[j], deliverytime))
                {
                    ctx.GetReply().SetErrorReason("Invalid IDLE option argument for XCLAIM");
                    return 0;
                }
                deliverytime = now - deliverytime;
            }
            else if (!strcasecmp(opt, "TIME") && moreargs)
            {
                j++;
                if (!string_toint64(cmd.GetArguments()[j], deliverytime))
                {
                    ctx.GetReply().SetErrorReason("Invalid TIME option argument for XCLAIM");
                    return 0;
                }
            }
            else if (!strcasecmp(opt, "RETRYCOUNT") && moreargs)
            {
                j++;
                if (!string_toint64(cmd.GetArguments()[j], retrycount))
                {
                    ctx.GetReply().SetErrorReason("Invalid RETRYCOUNT option argument for XCLAIM");
                    return 0;
                }
            }
            else
            {
                ctx.GetReply().SetErrorReason("Unrecognized XCLAIM option '" + cmd.GetArguments()[j] + "'");
                return 0;
            }
        }

        if (deliverytime != -1)
        {
            /* If a delivery time was passed, either with IDLE or TIME, we
             * do some sanity check on it, and set the deliverytime to now
             * (which is a sane choice usually) if the value is bogus.
             * To raise an error here is not wise because clients may compute
             * the idle time doing some math startin from their local time,
             * and this is not a good excuse to fail in case, for instance,
             * the computed time is a bit in the future from our POV. */
            if (deliverytime < 0 || deliverytime > now) deliverytime = now;
        }
        else
        {
            /* If no IDLE/TIME option was passed, we want the last delivery
             * time to be now, so that the idle time of the message will be
             * zero. */
            deliverytime = now;
        }
        //StreamAddConsumer(ctx, gk, vs[1], consumer_name);
        for (size_t i = 0; i < ids.size(); i++)
        {
            PELTable::iterator found = group_meta->consumer_pels.find(ids[i]);
            bool exist = found != group_meta->consumer_pels.end();
            if (!exist && force)
            {
                KeyObject selement(ctx.ns, KEY_STREAM_ELEMENT, cmd.GetArguments()[0]);
                selement.SetStreamID(ids[i]);
                ValueObject tmp;
                if (!m_engine->Exists(ctx, selement, tmp))
                {
                    continue;
                }
                StreamCreateNACK(ctx, cmd.GetArguments()[0], group_name, consumer_name, ids[i], group_meta);
            }
            if (exist)
            {
                StreamNACK* nack = found->second;
                /* We need to check if the minimum idle time requested
                 * by the caller is satisfied by this entry. */
                if (minidle)
                {
                    int64_t this_idle = now - nack->delivery_time;
                    if (this_idle < minidle) continue;
                }
                /* Remove the entry from the old consumer.
                 * Note that nack->consumer is NULL if we created the
                 * NACK above because of the FORCE option. */
                if (NULL != nack->consumer)
                {
                    nack->consumer->pels.erase(ids[i]);
                }
                nack->delivery_time = deliverytime;
                /* Set the delivery attempts counter if given. */
                if (retrycount >= 0)
                {
                    nack->delivery_count = retrycount;
                }
                /* Add the entry in the new cosnumer local PEL. */
                StreamUpdateNACK(ctx, cmd.GetArguments()[0], group_name, consumer_name, ids[i], group_meta, nack);

                /* Send the reply for this entry. */
                if (justid)
                {
                    RedisReply& r = ctx.GetReply().AddMember();
                    replyStreamID(r, ids[i]);
                }
                else
                {
                    RedisReply& r = ctx.GetReply().AddMember();
                    KeyObject selement(ctx.ns, KEY_STREAM_ELEMENT, cmd.GetArguments()[0]);
                    selement.SetStreamID(ids[i]);
                    ValueObject ele;
                    if (0 != m_engine->Get(ctx, selement, ele))
                    {
                        continue;
                    }
                    replyStreamElement(r, ids[i], meta, ele);
                }
            }
        }
        return 0;
    }

    /* XPENDING <key> <group> [<start> <stop> <count>] [<consumer>]
     *
     * If start and stop are omitted, the command just outputs information about
     * the amount of pending messages for the key/group pair, together with
     * the minimum and maxium ID of pending messages.
     *
     * If start and stop are provided instead, the pending messages are returned
     * with informations about the current owner, number of deliveries and last
     * delivery time and so forth. */
    int Ardb::XPending(Context& ctx, RedisCommandFrame& cmd)
    {
        int justinfo = cmd.GetArguments().size() == 2; /* Without the range just outputs general
         informations about the PEL. */
        const std::string& keystr = cmd.GetArguments()[0];
        const std::string& group = cmd.GetArguments()[1];
        const std::string& consumer = cmd.GetArguments().size() == 6 ? cmd.GetArguments()[5] : "";
        int64_t count;
        StreamID startid, endid;
        if (cmd.GetArguments().size() != 2 && cmd.GetArguments().size() != 5 && cmd.GetArguments().size() != 6)
        {
            ctx.GetReply().SetErrCode(ERR_INVALID_SYNTAX);
            return 0;
        }
        if (cmd.GetArguments().size() >= 5)
        {
            if (!string_toint64(cmd.GetArguments()[4], count))
            {
                ctx.GetReply().SetErrCode(ERR_INVALID_INTEGER_ARGS);
                return 0;
            }
            if (streamParseIDOrReply(ctx, cmd.GetArguments()[2], startid, 0) != 0) return 0;
            if (streamParseIDOrReply(ctx, cmd.GetArguments()[3], endid, 0) != 0) return 0;
        }
        KeyObject key(ctx.ns, KEY_META, keystr);
        //ValueObject meta;
        KeyLockGuard lock(ctx, key);
//        if (!CheckMeta(ctx, key, KEY_STREAM, meta))
//        {
//            return 0;
//        }
        StreamGroupMeta* group_meta = StreamLoadGroup(ctx, keystr, group);
        if (NULL == group_meta)
        {
            std::string err = "NOGROUP No such key '" + cmd.GetArguments()[0] + "' or consumer group '"
                    + cmd.GetArguments()[1] + "'";
            ctx.GetReply().SetErrorReason(err);
            return 0;
        }

        if (justinfo)
        {
            ctx.GetReply().ReserveMember(4);
            RedisReply& count_reply = ctx.GetReply().MemberAt(0);
            RedisReply& start_reply = ctx.GetReply().MemberAt(1);
            RedisReply& end_reply = ctx.GetReply().MemberAt(2);
            RedisReply& consumers_reply = ctx.GetReply().MemberAt(3);
            if (0 == group_meta->consumer_pels.size())
            {
                count_reply.SetInteger(0);
                end_reply.Clear();
                start_reply.Clear();
                consumers_reply.ReserveMember(-1);
            }
            else
            {
                replyStreamID(start_reply, group_meta->StartId());
                replyStreamID(end_reply, group_meta->EndId());
                count_reply.SetInteger(group_meta->consumer_pels.size());
                consumers_reply.ReserveMember(0);
                ConsumerTable::iterator cit = group_meta->consumers.begin();
                while (cit != group_meta->consumers.end())
                {
                    StreamConsumerMeta* c = cit->second;
                    RedisReply& rr = consumers_reply.AddMember();
                    rr.AddMember().SetString(c->name);
                    rr.AddMember().SetInteger(c->pels.size());
                    cit++;
                }
            }
        }
        else /* XPENDING <key> <group> <start> <stop> <count> [<consumer>] variant. */
        {
            ctx.GetReply().ReserveMember(0);
            PELTable* pel_table = &group_meta->consumer_pels;
            if (!consumer.empty())
            {
                ConsumerTable::iterator cit = group_meta->consumers.find(consumer);
                if (cit == group_meta->consumers.end())
                {
                    return 0;
                }
                pel_table = &(cit->second->pels);
            }

            int64_t now = get_current_epoch_millis();
            PELTable::iterator pit = pel_table->begin();
            while (pit != pel_table->end())
            {
                StreamID sid = pit->first;
                StreamNACK* nack = pit->second;
                RedisReply& r = ctx.GetReply().AddMember();
                r.ReserveMember(4);
                replyStreamID(r.MemberAt(0), sid);
                r.MemberAt(1).SetString(NULL != nack->consumer ? nack->consumer->name : "");
                int64_t elapsed = now - nack->delivery_time;
                if (elapsed < 0) elapsed = 0;
                r.MemberAt(2).SetInteger(elapsed);
                /* Number of deliveries. */
                r.MemberAt(3).SetInteger(nack->delivery_count);
                pit++;
            }
        }
        return 0;
    }

    /* XINFO CONSUMERS key group
     * XINFO GROUPS <key>
     * XINFO STREAM <key>
     * XINFO <key> (alias of XINFO STREAM key)
     * XINFO HELP. */
    int Ardb::XInfo(Context& ctx, RedisCommandFrame& cmd)
    {
        const char *help[] = { "CONSUMERS <key> <groupname>  -- Show consumer groups of group <groupname>.",
                "GROUPS <key>                 -- Show the stream consumer groups.",
                "STREAM <key>                 -- Show information about the stream.",
                "<key>                        -- Alias for STREAM <key>.",
                "HELP                         -- Print this help.",
                NULL };
        /* HELP is special. Handle it ASAP. */
        if (!strcasecmp(cmd.GetArguments()[0].c_str(), "HELP"))
        {
            replyHelp(ctx.GetReply(), cmd.GetCommand(), help);
            return 0;
        }
        const char* opt = NULL;
        const char* keystr = NULL;
        /* Handle the fact that no subcommand means "STREAM". */
        if (cmd.GetArguments().size() == 1)
        {
            opt = "STREAM";
            keystr = cmd.GetArguments()[0].c_str();
        }
        else
        {
            opt = cmd.GetArguments()[0].c_str();
            keystr = cmd.GetArguments()[1].c_str();
        }
        KeyObject key(ctx.ns, KEY_META, keystr);
        KeyLockGuard lock(ctx, key);
        ValueObject meta;
        if (!CheckMeta(ctx, key, KEY_STREAM, meta))
        {
            return 0;
        }
        if (meta.GetType() == 0)
        {
            ctx.GetReply().SetErrCode(ERR_ENTRY_NOT_EXIST);
            return 0;
        }
        int64_t now = get_current_epoch_millis();
        RedisReply& r = ctx.GetReply();
        if (!strcasecmp(opt, "CONSUMERS") && cmd.GetArguments().size() == 3)
        {
            StreamGroupMeta* group_meta = StreamLoadGroup(ctx, keystr, cmd.GetArguments()[2]);
            if (NULL == group_meta)
            {
                std::string err = "NOGROUP No such key '" + cmd.GetArguments()[1] + "' or consumer group '"
                        + cmd.GetArguments()[1] + "'";
                ctx.GetReply().SetErrorReason(err);
                return 0;
            }
            r.ReserveMember(0);
            ConsumerTable::iterator cit = group_meta->consumers.begin();
            while (cit != group_meta->consumers.end())
            {
                StreamConsumerMeta* c = cit->second;
                RedisReply& rr = r.AddMember();
                rr.AddMember().SetStatusString("name");
                rr.AddMember().SetString(c->name);
                rr.AddMember().SetStatusString("pending");
                rr.AddMember().SetInteger(c->pels.size());
                rr.AddMember().SetStatusString("idle");
                int64_t idle = now - c->seen_time;
                if (idle < 0) idle = 0;
                rr.AddMember().SetInteger(idle);
                cit++;
            }
        }
        else if (!strcasecmp(opt, "GROUPS") && cmd.GetArguments().size() == 2)
        {
            ctx.GetReply().ReserveMember(0);
            StreamGroupTable* gs = StreamLoadGroups(ctx, keystr, false);
            if (NULL != gs)
            {
                StreamGroupTable::iterator git = gs->begin();
                while (git != gs->end())
                {
                    StreamGroupMeta* g = git->second;
                    RedisReply& rr = r.AddMember();
                    rr.AddMember().SetStatusString("name");
                    rr.AddMember().SetString(g->name);
                    rr.AddMember().SetStatusString("consumers");
                    rr.AddMember().SetInteger(g->consumers.size());
                    rr.AddMember().SetStatusString("pending");
                    rr.AddMember().SetInteger(g->consumer_pels.size());
                    git++;
                }
            }

        }
        else if (cmd.GetArguments().size() == 1 || (!strcasecmp(opt, "STREAM") && cmd.GetArguments().size() == 2))
        {
            ctx.GetReply().AddMember().SetStatusString("length");
            ctx.GetReply().AddMember().SetInteger(meta.GetObjectLen());
            ctx.GetReply().AddMember().SetStatusString("radix-tree-keys");
            ctx.GetReply().AddMember().SetInteger(meta.GetObjectLen());
            ctx.GetReply().AddMember().SetStatusString("radix-tree-nodes");
            ctx.GetReply().AddMember().SetInteger(meta.GetObjectLen());
            ctx.GetReply().AddMember().SetStatusString("groups");
            StreamGroupTable* gs = StreamLoadGroups(ctx, keystr, false);
            ctx.GetReply().AddMember().SetInteger(NULL == gs ? 0 : gs->size());
            StreamID min, max;
            ValueObject minv, maxv;
            StreamMinMaxID(ctx, keystr, min, max, minv, maxv);
            if (min.Empty())
            {
                ctx.GetReply().AddMember().Clear();
                ctx.GetReply().AddMember().Clear();
            }
            else
            {
                ctx.GetReply().AddMember().SetStatusString("first-entry");
                replyStreamElement(ctx.GetReply().AddMember(), min, meta, minv);
                ctx.GetReply().AddMember().SetStatusString("last-entry");
                replyStreamElement(ctx.GetReply().AddMember(), max, meta, maxv);
            }
        }
        else
        {
            ctx.GetReply().SetErrorReason("syntax error, try 'XINFO HELP'");
        }
        return 0;
    }

    /* XREAD [BLOCK <milliseconds>] [COUNT <count>] STREAMS key_1 key_2 ... key_N
     *       ID_1 ID_2 ... ID_N
     *
     * This function also implements the XREAD-GROUP command, which is like XREAD
     * but accepting the [GROUP group-name consumer-name] additional option.
     * This is useful because while XREAD is a read command and can be called
     * on slaves, XREAD-GROUP is not. */
#define XREAD_BLOCKED_DEFAULT_COUNT 1000
    int Ardb::XRead(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.flags.no_wal = 1;
        int xreadgroup = cmd.GetCommand().size() >= 10; /* XREAD or XREADGROUP? */
        /* Parse arguments. */
        int64_t timeout = -1, count = 0;
        size_t argc = cmd.GetArguments().size();
        int streams_arg = 0;
        size_t streams_count = 0;
        std::string groupname, consumername;
        int noack = 0;
        for (size_t i = 0; i < argc; i++)
        {
            int moreargs = argc - i - 1;
            const char *o = cmd.GetArguments()[i].c_str();
            if (!strcasecmp(o, "BLOCK") && moreargs)
            {
                i++;
                if (!string_toint64(cmd.GetArguments()[i], timeout))
                {
                    ctx.GetReply().SetErrCode(ERR_INVALID_INTEGER_ARGS);
                    return 0;
                }
            }
            else if (!strcasecmp(o, "COUNT") && moreargs)
            {
                i++;
                if (!string_toint64(cmd.GetArguments()[i], count))
                {
                    ctx.GetReply().SetErrCode(ERR_INVALID_INTEGER_ARGS);
                    return 0;
                }
                if (count < 0) count = 0;
            }
            else if (!strcasecmp(o, "STREAMS") && moreargs)
            {
                streams_arg = i + 1;
                streams_count = (argc - streams_arg);
                if ((streams_count % 2) != 0)
                {
                    ctx.GetReply().SetErrorReason("Unbalanced XREAD list of streams: "
                            "for each stream key an ID or '$' must be "
                            "specified.");
                    return 0;
                }
                streams_count /= 2; /* We have two arguments for each stream. */
                break;
            }
            else if (!strcasecmp(o, "GROUP") && moreargs >= 2)
            {
                if (!xreadgroup)
                {
                    ctx.GetReply().SetErrorReason("The GROUP option is only supported by "
                            "XREADGROUP. You called XREAD instead.");
                    return 0;
                }
                groupname = cmd.GetArguments()[i + 1];
                consumername = cmd.GetArguments()[i + 2];
                i += 2;
            }
            else if (!strcasecmp(o, "NOACK"))
            {
                if (!xreadgroup)
                {
                    ctx.GetReply().SetErrorReason("The NOACK option is only supported by "
                            "XREADGROUP. You called XREAD instead.");
                    return 0;
                }
                noack = 1;
            }
            else
            {
                ctx.GetReply().SetErrCode(ERR_INVALID_SYNTAX);
                return 0;
            }
        }
        /* STREAMS option is mandatory. */
        if (streams_arg == 0)
        {
            ctx.GetReply().SetErrCode(ERR_INVALID_SYNTAX);
            return 0;
        }

        /* If the user specified XREADGROUP then it must also
         * provide the GROUP option. */
        if (xreadgroup && groupname.empty())
        {
            ctx.GetReply().SetErrorReason("Missing GROUP option for XREADGROUP");
            return 0;
        }
        std::vector<StreamGroupMeta*> groups;
        ValueObjectArray streams;
        std::vector<StreamID> ids;
        groups.resize(streams_count);
        streams.resize(streams_count);
        ids.resize(streams_count);
        for (size_t i = streams_arg + streams_count; i < argc; i++)
        {
            /* Specifying "$" as last-known-id means that the client wants to be
             * served with just the messages that will arrive into the stream
             * starting from now. */
            size_t id_idx = i - streams_arg - streams_count;
            const std::string& stream_key = cmd.GetArguments()[i - streams_count];
            /* If a group was specified, than we need to be sure that the
             * key and group actually exist. */
            if (!groupname.empty())
            {
                if (!CheckMeta(ctx, stream_key, KEY_STREAM, streams[id_idx]))
                {
                    return 0;
                }
                StreamGroupMeta* group_meta = StreamLoadGroup(ctx, stream_key, groupname);
                if (streams[id_idx].GetType() == 0 || NULL == group_meta)
                {
                    std::string err = "NOGROUP No such key '" + stream_key + "' or consumer "
                            "group '" + groupname + "' in XREADGROUP with GROUP "
                            "option";
                    ctx.GetReply().SetErrorReason(err);
                    return 0;
                }
                groups[id_idx] = group_meta;
            }
            if (strcmp(cmd.GetArguments()[i].c_str(), "$") == 0)
            {
                if (!CheckMeta(ctx, stream_key, KEY_STREAM, streams[id_idx]))
                {
                    return 0;
                }
                if (streams[id_idx].GetType() > 0)
                {
                    ids[id_idx] = streams[id_idx].GetStreamLastId();
                }
                else
                {
                    ids[id_idx].ms = 0;
                    ids[id_idx].seq = 0;
                }
                continue;
            }
            else if (strcmp(cmd.GetArguments()[i].c_str(), ">") == 0)
            {
                if (!xreadgroup || groupname.empty())
                {
                    ctx.GetReply().SetErrorReason("The > ID can be specified only when calling "
                            "XREADGROUP using the GROUP <group> "
                            "<consumer> option.");
                    return 0;
                }
                ids[id_idx] = groups[id_idx]->lastid;
                continue;
            }
            if (streamParseIDOrReply(ctx, cmd.GetArguments()[i], ids[id_idx], 0) != 0) return 0;
        }

        int64_t reply_count = 0;
        /* Try to serve the client synchronously. */
        StringArray blist_keys;
        for (size_t i = 0; i < streams_count; i++)
        {
            const std::string& stream_key_str = cmd.GetArguments()[streams_arg + i];
            KeyObject stream_key(ctx.ns, KEY_META, stream_key_str);
            if (0 == streams[i].GetType() && groupname.empty() && !CheckMeta(ctx, stream_key, KEY_STREAM, streams[i]))
            {
                return 0;
            }
            blist_keys.push_back(stream_key_str);
            if (0 == streams[i].GetType()) continue;
            StreamID& gt = ids[i]; /* ID must be greater than this. */
            if (streams[i].GetStreamLastId().ms > gt.ms
                    || (streams[i].GetStreamLastId().ms == gt.ms && streams[i].GetStreamLastId().seq > gt.seq))
            {
                /* streamReplyWithRange() handles the 'start' ID as inclusive,
                 * so start from the next ID, since we want only messages with
                 * IDs greater than start. */
                StreamID start = gt;
                start.seq++; /* uint64_t can't overflow in this context. */

                /* Emit the two elements sub-array consisting of the name
                 * of the stream and the data we extracted from it. */
                RedisReply& rr = ctx.GetReply().AddMember();
                RedisReply& r1 = rr.AddMember();
                r1.SetString(stream_key_str);
                RedisReply& r2 = rr.AddMember();

                if (!groupname.empty())
                {
                    StreamAddConsumer(ctx, consumername, groups[i]);
                    if (start.Compare(groups[i]->lastid) <= 0)
                    {
                        reply_count++;
                        r2.ReserveMember(0);
                        StreamConsumerMeta* c = groups[i]->consumers[consumername];
                        PELTable::iterator pit = c->pels.lower_bound(start);
                        while (pit != c->pels.end())
                        {
                            StreamID sid = pit->first;
                            KeyObject sk(ctx.ns, KEY_STREAM_ELEMENT, stream_key_str);
                            ValueObject sv;
                            if (0 == m_engine->Get(ctx, sk, sv))
                            {
                                replyStreamElement(r2.AddMember(), sid, streams[i], sv);
                                StreamNACK* nack = pit->second;
                                nack->delivery_count++;
                                nack->delivery_time = get_current_epoch_millis();
                                StreamUpdateNACK(ctx, stream_key_str, groupname, consumername, sid, groups[i], nack);
                            }
                            else
                            {
                                RedisReply& ele_reply = r2.AddMember();
                                RedisReply& rr1 = ele_reply.AddMember();
                                replyStreamID(rr1, sid);
                                RedisReply& rr2 = ele_reply.AddMember();
                                rr2.ReserveMember(-1);
                            }
                            pit++;
                        }
                        continue;
                    }
                }
                StreamGroupMeta* group_meta = groups[i];
                KeyObject sk(ctx.ns, KEY_STREAM_ELEMENT, stream_key_str);
                sk.SetStreamID(start);
                Iterator* iter = m_engine->Find(ctx, sk);
                WriteBatchGuard batch(ctx, m_engine);
                int64_t ele_count = 0;
                bool group_meta_updated = false;
                while (iter->Valid())
                {
                    KeyObject& ik = iter->Key(false);
                    if (ik.GetType() != KEY_STREAM_ELEMENT || ik.GetNameSpace() != sk.GetNameSpace()
                            || ik.GetKey() != sk.GetKey())
                    {
                        break;
                    }
                    StreamID id = ik.GetStreamID();
                    ValueObject& iv = iter->Value(false);
                    replyStreamElement(r2.AddMember(), id, streams[i], iv);
                    ele_count++;
                    reply_count++;
                    if (NULL != group_meta && id.Compare(group_meta->lastid) > 0)
                    {
                        group_meta->lastid = id;
                        group_meta_updated = true;
                    }
                    if (NULL != group_meta && !noack)
                    {
                        StreamCreateNACK(ctx, stream_key_str, groupname, consumername, id, group_meta);
                    }
                    if (count > 0 && ele_count >= count)
                    {
                        break;
                    }
                    iter->Next();
                }
                DELETE(iter);
                if (group_meta_updated)
                {
                    KeyObject gk(ctx.ns, KEY_STREAM_PEL, stream_key_str);
                    gk.SetStreamGroup(groupname);
                    ValueObject gv;
                    gv.SetType(KEY_STREAM_PEL);
                    group_meta->lastid.Encode(gv.GetStreamGroupStreamId());
                    m_engine->Put(ctx, gk, gv);
                }
            }
        }
        if (reply_count > 0)
        {
            return 0;
        }
        /* Block if needed. */
        ctx.GetReply().ReserveMember(-1);
        if (timeout != -1)
        {
            /* If we are inside a MULTI/EXEC and the list is empty the only thing
             * we can do is treating it as a timeout (even with timeout 0). */
            if (ctx.InTransaction())
            {
                return 0;
            }
            ctx.GetReply().type = 0; //wait
            ctx.GetBPop().GetStreamTarget().ids = ids;
            AnyArray idptrs;
            BuildAnyArray(ctx.GetBPop().GetStreamTarget().ids, idptrs);
            BlockForKeys(ctx, blist_keys, idptrs, KEY_STREAM, timeout);
            ctx.GetBPop().GetStreamTarget().xread_count = count ? count : XREAD_BLOCKED_DEFAULT_COUNT;
            ctx.GetBPop().GetStreamTarget().group = groupname;
            ctx.GetBPop().GetStreamTarget().consumer = consumername;
            ctx.GetBPop().GetStreamTarget().noack = noack ? true : false;
        }
        return 0;
    }

    int Ardb::WakeClientsBlockingOnStream(Context& ctx, const KeyPrefix& ready_key, Context& unblock_client)
    {
        RedisReply* r = NULL;
        NEW(r, RedisReply);
        RedisReply& rr = r->AddMember();
        RedisReply& r1 = rr.AddMember();
        RedisReply& r2 = rr.AddMember();
        r1.SetString(ready_key.key);
        r2.ReserveMember(-1);
        KeyObject key(ready_key.ns, KEY_META, ready_key.key);
        ValueObject meta;
        CheckMeta(ctx, key, KEY_STREAM, meta);
        StreamGroupMeta* group_meta = NULL;
        if (!unblock_client.GetBPop().GetStreamTarget().group.empty())
        {
            group_meta = StreamLoadGroup(unblock_client, ready_key, unblock_client.GetBPop().GetStreamTarget().group);
            if (NULL == group_meta)
            {
                UnblockKeys(unblock_client, false, r);
                return 0;
            }
        }
        Context stream_ctx;
        stream_ctx.ns = ready_key.ns;
        std::string stream_key;
        ready_key.key.ToString(stream_key);

        KeyObject sk(ready_key.ns, KEY_STREAM_ELEMENT, ready_key.key);
        BlockingState::BlockKeyTable::iterator found = unblock_client.GetBPop().keys.find(ready_key);
        if (found != unblock_client.GetBPop().keys.end())
        {
            StreamID sid = *(const StreamID*) found->second;
            sid.seq++;
            sk.SetStreamID(sid);
        }
        ctx.flags.iterate_total_order = 1;
        Iterator* iter = m_engine->Find(ctx, sk);
        WriteBatchGuard batch(ctx, m_engine);
        int64_t ele_count = 0;
        bool group_meta_updated = false;
        while (iter->Valid())
        {
            KeyObject& ik = iter->Key(false);
            if (ik.GetType() != KEY_STREAM_ELEMENT || ik.GetNameSpace() != sk.GetNameSpace()
                    || ik.GetKey() != sk.GetKey())
            {
                break;
            }
            StreamID id = ik.GetStreamID();
            ValueObject& iv = iter->Value(false);
            RedisReply& ele_reply = r2.AddMember();
            replyStreamElement(ele_reply, id, meta, iv);
            ele_count++;
            if (NULL != group_meta && id.Compare(group_meta->lastid) > 0)
            {
                group_meta->lastid = id;
                group_meta_updated = true;
            }
            if (NULL != group_meta && !unblock_client.GetBPop().GetStreamTarget().noack)
            {
                StreamCreateNACK(stream_ctx, stream_key, unblock_client.GetBPop().GetStreamTarget().group,
                        unblock_client.GetBPop().GetStreamTarget().consumer, id, group_meta);
            }
            if ((size_t)ele_count >= unblock_client.GetBPop().GetStreamTarget().xread_count)
            {
                break;
            }
            iter->Next();
        }
        DELETE(iter);
        if (group_meta_updated)
        {
            KeyObject gk(ready_key.ns, KEY_STREAM_PEL, ready_key.key);
            gk.SetStreamGroup(unblock_client.GetBPop().GetStreamTarget().group);
            ValueObject gv;
            gv.SetType(KEY_STREAM_PEL);
            group_meta->lastid.Encode(gv.GetStreamGroupStreamId());
            m_engine->Put(ctx, gk, gv);
        }
        UnblockKeys(unblock_client, false, r);
        return 0;
    }
OP_NAMESPACE_END
