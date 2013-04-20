/*
 * InternalMessageDecoder.hpp
 *
 *  Created on: 2011-1-29
 *      Author: wqy
 */

#ifndef REDIS_REPLY_ENCODER_HPP_
#define REDIS_REPLY_ENCODER_HPP_
#include "channel/all_includes.hpp"
#include <deque>
#include <string>

#define REDIS_REPLY_STRING 1
#define REDIS_REPLY_ARRAY 2
#define REDIS_REPLY_INTEGER 3
#define REDIS_REPLY_NIL 4
#define REDIS_REPLY_STATUS 5
#define REDIS_REPLY_ERROR 6

#define REDIS_REPLY_DOUBLE 1001

namespace ardb
{
    namespace codec
    {
	struct RedisReply
	{
			int type;
			std::string str;
			int64_t integer;
			double double_value;
			std::deque<RedisReply> elements;
			RedisReply() :
					type(0), integer(0), double_value(0)
			{
			}
			RedisReply(uint64 v) :
					type(REDIS_REPLY_INTEGER), integer(v), double_value(0)
			{
			}
			RedisReply(double v) :
					type(REDIS_REPLY_DOUBLE), integer(v), double_value(v)
			{
			}
			RedisReply(const std::string& v) :
					type(REDIS_REPLY_STRING), str(v), integer(0), double_value(
					        0)
			{
			}
			void Clear()
			{
				type = REDIS_REPLY_NIL;
				integer = 0;
				double_value = 0;
				str.clear();
				elements.clear();
			}
	};

        class RedisReplyEncoder: public ChannelDownstreamHandler<RedisReply>
        {
            private:
                bool WriteRequested(ChannelHandlerContext& ctx, MessageEvent<
                		RedisReply>& e);
            public:
                static bool Encode(Buffer& buf, RedisReply& reply);
        };
    }
}

#endif /* INTERNALMESSAGEDECODER_HPP_ */
