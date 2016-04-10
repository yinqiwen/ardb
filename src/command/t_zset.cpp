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
#include <float.h>

/* This generic command implements both ZADD and ZINCRBY. */
#define ZADD_NONE 0
#define ZADD_INCR (1<<0)    /* Increment the score instead of setting it. */
#define ZADD_NX (1<<1)      /* Don't touch elements not already existing. */
#define ZADD_XX (1<<2)      /* Only touch elements already exisitng. */
#define ZADD_CH (1<<3)      /* Return num of elements added or updated. */

OP_NAMESPACE_BEGIN

    int Ardb::ZAdd(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.flags.create_if_notexist = 1;
	    int flags = 0;
	    int added = 0;      /* Number of new elements added. */
	    int updated = 0;    /* Number of elements with updated score. */
	    int processed = 0;  /* Number of elements processed, may remain zero with
	                               options like XX. */
	    size_t scoreidx = 1;
	    while(scoreidx < cmd.GetArguments().size())
	    {
	    	const char* opt = cmd.GetArguments()[scoreidx].c_str();
	    	if (!strcasecmp(opt,"nx")) flags |= ZADD_NX;
	    	else if (!strcasecmp(opt,"xx")) flags |= ZADD_XX;
	    	else if (!strcasecmp(opt,"ch")) flags |= ZADD_CH;
	    	else if (!strcasecmp(opt,"incr")) flags |= ZADD_INCR;
	        else break;
	    	scoreidx++;
	    }
        /* Turn options into simple to check vars. */
        int incr = (flags & ZADD_INCR) != 0;
        int nx = (flags & ZADD_NX) != 0;
        int xx = (flags & ZADD_XX) != 0;
        int ch = (flags & ZADD_CH) != 0;
        RedisReply& reply = ctx.GetReply();
        size_t elements = cmd.GetArguments().size() - scoreidx;
        if(elements% 2 != 0)
		{
        	reply.SetErrCode(ERR_INVALID_SYNTAX);
			return 0;
		}
        if (nx && xx) {
        	reply.SetErrorReason("XX and NX options at the same time are not compatible");
            return 0;
        }
        elements /= 2; /* Now this holds the number of score-element pairs. */
        if (incr && elements > 1) {
        	reply.SetErrorReason("INCR option supports a single increment-element pair");
            return 0;
        }
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject meta;
        KeyLockGuard guard(key);
        if(!CheckMeta(ctx, key, KEY_ZSET, meta))
        {
        	return 0;
        }
        if(xx && meta.GetType() == 0)
        {
        	return 0;
        }
        for(size_t i = 0; i < elements; i++)
        {
        	KeyObject ele(ctx.ns, KEY_ZSET_SCORE, cmd.GetArguments()[0]);
        	ele.SetZSetMember(cmd.GetArguments()[scoreidx + i * 2 + 1]);
        	const std::string& scorestr = cmd.GetArguments()[scoreidx + i * 2 ];
        	double score,current_score;
        	if(!string_todouble(scorestr, score))
        	{
        		reply.SetErrCode(ERR_INVALID_FLOAT_ARGS);
        		return 0;
        	}
        	ValueObject ele_value;
        	if(0 == m_engine->Get(ctx, ele, ele_value))
        	{
        		if(nx) continue;
        		current_score = ele_value.GetZSetScore();
                if(incr)
                {
                	score += current_score;
                }

                processed++;
                if(score != current_score)
                {
                	KeyObject old_sort_key(ctx.ns, KEY_ZSET_SORT, cmd.GetArguments()[0]);
                	old_sort_key.SetZSetMember(cmd.GetArguments()[scoreidx + i * 2 + 1]);
                	old_sort_key.SetZSetScore(current_score);
                	m_engine->Del(ctx, old_sort_key);
                	updated++;
                }else
                {
                	continue;
                }
        	}else
        	{
        		if(xx)
        		{
        			continue;
        		}
        		added++;
        		processed++;
        	}
          	KeyObject new_sort_key(ctx.ns, KEY_ZSET_SORT, cmd.GetArguments()[0]);
          	new_sort_key.SetZSetMember(cmd.GetArguments()[scoreidx + i * 2 + 1]);
          	new_sort_key.SetZSetScore(score);
          	ValueObject empty;
          	empty.SetType(KEY_ZSET_SORT);
          	m_engine->Put(ctx, new_sort_key, empty);
          	ele_value.SetType(KEY_ZSET_SCORE);
          	ele_value.SetZSetScore(score);
          	m_engine->Put(ctx, ele, ele_value);
        }
        meta.SetObjectLen(meta.GetObjectLen() + added);
        return 0;
    }

    int Ardb::ZCard(Context& ctx, RedisCommandFrame& cmd)
    {
        return ObjectLen(ctx, KEY_ZSET, cmd.GetArguments()[0]);
    }

    int Ardb::ZCount(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::ZIncrby(Context& ctx, RedisCommandFrame& cmd)
    {
        return 0;
    }

    int Ardb::ZRange(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }
    int Ardb::ZRevRange(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }


    int Ardb::ZRangeByScore(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    int Ardb::ZRevRangeByScore(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    int Ardb::ZRank(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    int Ardb::ZRevRank(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    int Ardb::ZRem(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    int Ardb::ZRemRangeByRank(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    int Ardb::ZRemRangeByScore(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    int Ardb::ZInterStore(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    int Ardb::ZUnionStore(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    int Ardb::ZScan(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    int Ardb::ZScore(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    int Ardb::ZLexCount(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    int Ardb::ZRangeByLex(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    int Ardb::ZRemRangeByLex(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

OP_NAMESPACE_END
