/*
 * InternalMessageEncoder.cpp
 *
 *  Created on: 2011-2-12
 *      Author: qiyingwang
 */

#include "redis_reply_encoder.hpp"
#include "util/buffer_helper.hpp"

#include <limits.h>

using ardb::BufferHelper;
using namespace ardb::codec;
using namespace ardb;

bool RedisReplyEncoder::Encode(Buffer& buf, RedisReply& reply)
{
	switch (reply.type)
	{
	case REDIS_REPLY_NIL:
	{
		buf.Printf("$-1\r\n");
		break;
	}
	case REDIS_REPLY_STRING:
	{
		buf.Printf("$%d\r\n", reply.str.size());
		if (reply.str.size() > 0)
		{
			buf.Printf("%s\r\n", reply.str.c_str());
		}
		else
		{
			buf.Printf("\r\n");
		}
		break;
	}
	case REDIS_REPLY_ERROR:
	{
		buf.Printf("-%s\r\n", reply.str.c_str());
		break;
	}
	case REDIS_REPLY_INTEGER:
	{
		buf.Printf(":%lld\r\n", reply.integer);
		break;
	}
	case REDIS_REPLY_DOUBLE:
	{
		std::string doubleStrValue;
		fast_dtoa(reply.double_value, 9, doubleStrValue);
		buf.Printf("$%d\r\n", doubleStrValue.size());
		buf.Printf("%s\r\n", doubleStrValue.c_str());
		break;
	}
	case REDIS_REPLY_ARRAY:
	{
		buf.Printf("*%d\r\n", reply.elements.size());
		size_t i = 0;
		while (i < reply.elements.size())
		{
			if(!RedisReplyEncoder::Encode(buf, reply.elements[i]))
			{
				return false;
			}
			i++;
		}
		break;
	}
	case REDIS_REPLY_STATUS:
	{
		buf.Printf("+%s\r\n", reply.str.c_str());
		break;
	}
	default:
	{
		ERROR_LOG("Recv unexpected redis reply type:%d", reply.type);
		return false;
	}
	}
	return true;
}

bool RedisReplyEncoder::WriteRequested(ChannelHandlerContext& ctx,
		MessageEvent<RedisReply>& e)
{
	RedisReply* msg = e.GetMessage();
	//consider using static object?
	//static Buffer buffer(1024);
	static Buffer buffer(1024);
	buffer.Clear();
	if (Encode(buffer, *msg))
	{
		return ctx.GetChannel()->Write(buffer);
	}
	return false;

}
