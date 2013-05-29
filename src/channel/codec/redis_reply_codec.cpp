/*
 * InternalMessageEncoder.cpp
 *
 *  Created on: 2011-2-12
 *      Author: qiyingwang
 */

#include "redis_reply_codec.hpp"
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
				if (!RedisReplyEncoder::Encode(buf, reply.elements[i]))
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
	Buffer buffer(1024);
	if (Encode(buffer, *msg))
	{
		return ctx.GetChannel()->Write(buffer);
	}
	return false;
}

//==================================Decoder==========================================
static const char* kCRLF = "\r\n";
static int decode_reply(Buffer& buffer, RedisReply& msg)
{
	uint32 mark = buffer.GetReadIndex();
	int index = buffer.IndexOf(kCRLF, 2);
	if (-1 == index)
	{
		return 0;
	}
	char type;
	buffer.ReadByte(type);
	switch (type)
	{
		case '$':
		{
			int64 len;
			if (!raw_toint64(buffer.GetRawReadBuffer(),
			        index - buffer.GetReadIndex(), len))
			{
				return -1;
			}
			if (len == -1)
			{
				msg.type = REDIS_REPLY_NIL;
				buffer.SetReadIndex(index + 2);
			}
			else
			{
				buffer.SetReadIndex(index + 2);
				msg.type = REDIS_REPLY_STRING;
				if (buffer.ReadableBytes() < (uint32)(len + 2))
				{
					break;
				}
				msg.str.assign(buffer.GetRawReadBuffer(), len);
				buffer.SkipBytes(len + 2);
			}
			return 1;
		}
		case '+':
		{
			msg.type = REDIS_REPLY_STATUS;
			msg.str.assign(buffer.GetRawReadBuffer(),
			        index - buffer.GetReadIndex());
			buffer.SetReadIndex(index + 2);
			return 1;
		}
		case '-':
		{
			msg.type = REDIS_REPLY_ERROR;
			msg.str.assign(buffer.GetRawReadBuffer(),
			        index - buffer.GetReadIndex());
			buffer.SetReadIndex(index + 2);
			return 1;
		}
		case ':':
		{
			msg.type = REDIS_REPLY_INTEGER;
			int64 i;
			if (!raw_toint64(buffer.GetRawReadBuffer(),
			        index - buffer.GetReadIndex(), i))
			{
				return -1;
			}
			msg.integer = i;
			buffer.SetReadIndex(index + 2);
			return 1;
		}
		case '*':
		{
			int64 len;
			if (!raw_toint64(buffer.GetRawReadBuffer(),
			        index - buffer.GetReadIndex(), len))
			{
				return -1;
			}
			msg.type = REDIS_REPLY_ARRAY;
			buffer.SetReadIndex(index + 2);
			for (int i = 0; i < len; i++)
			{
				RedisReply r;
				int ret = decode_reply(buffer, r);
				if (ret < 0)
				{
					return ret;
				}
				else if (ret == 0)
				{
					break;
				}
				else
				{
					msg.elements.push_back(r);
				}
			}
			return 1;
		}
		default:
		{
			return -1;
		}
	}
	buffer.SetReadIndex(mark);
	return 0;
}
bool RedisReplyDecoder::Decode(ChannelHandlerContext& ctx, Channel* channel,
        Buffer& buffer, RedisReply& msg)
{
	int ret = decode_reply(buffer, msg);
	if (ret < 0)
	{
		//Close conn if recv invalid msg.
		channel->Close();
	}
	return ret > 0;
}
