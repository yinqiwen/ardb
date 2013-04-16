/*
 * redis_frame_decoder.cpp
 *
 *  Created on: 2011-7-14
 *      Author: qiyingwang
 */
#include "util/buffer_helper.hpp"
#include "redis_frame_decoder.hpp"
#include "util/exception/api_exception.hpp"

#include <limits.h>

using ardb::BufferHelper;
using namespace ardb::codec;
using namespace ardb;

/* Client request types */
static const uint32 REDIS_REQ_INLINE = 1;
static const uint32 REDIS_REQ_MULTIBULK = 2;
static const char* kCRLF = "\r\n";

void RedisCommandFrame::FillNextArgument(Buffer& buf, size_t len)
{
	const char* str = buf.GetRawReadBuffer();
	buf.AdvanceReadIndex(len);
//	if(m_cmd.empty()){
//		m_cmd.append(str, len);
//		return;
//	}
	m_args.push_back(std::string(str, len));
}

std::string* RedisCommandFrame::GetArgument(uint32 index)
{
	if (index >= m_args.size())
	{
		return NULL;
	}
	return &(m_args[index]);
}

void RedisCommandFrame::Clear()
{
	m_args.clear();
}


RedisCommandFrame::~RedisCommandFrame()
{
	Clear();
}

int RedisFrameDecoder::ProcessInlineBuffer(ChannelHandlerContext& ctx,
        Buffer& buffer, RedisCommandFrame& frame)
{
	int index = buffer.IndexOf(kCRLF, 2);
	if (-1 == index)
	{
		return 0;
	}
	while (true)
	{
		char ch;
		if (!buffer.ReadByte(ch))
		{
			break;
		}
		if (ch == ' ')
		{
			continue;
		}
		buffer.AdvanceReadIndex(-1);
		int current = buffer.GetReadIndex();
		if (current == index)
		{
			buffer.AdvanceReadIndex(2); //skip "\r\n"
			break;
		}
		int space_index = buffer.IndexOf(" ", 1, current, index);
		if (-1 == space_index)
		{
			break;
		}
		frame.FillNextArgument(buffer, space_index - current);
		buffer.AdvanceReadIndex(1); //skip space char
	}
	int current = buffer.GetReadIndex();
	if (current < index)
	{
		frame.FillNextArgument(buffer, index - current);
		buffer.AdvanceReadIndex(2); //skip "\r\n"
	}
	return 1;
}

int RedisFrameDecoder::ProcessMultibulkBuffer(ChannelHandlerContext& ctx,
        Buffer& buffer, RedisCommandFrame& frame)
{
	int index = buffer.IndexOf(kCRLF, 2);
	if (-1 == index)
	{
		return 0;
	}
	char *eptr = NULL;
	const char* raw = buffer.GetRawReadBuffer();
	uint32 multibulklen = strtol(raw, &eptr, 10);
	if (multibulklen <= 0)
	{
		buffer.SetReadIndex(index + 2);
		return 1;
	}
	else if (multibulklen > 1024 * 1024)
	{
		APIException ex("Protocol error: invalid multibulk length");
		fire_exception_caught(ctx.GetChannel(), ex);
		return -1;
	}
	buffer.SetReadIndex(index + 2);
	while (multibulklen > 0)
	{
		int newline_index = buffer.IndexOf(kCRLF, 2);
		if (-1 == newline_index)
		{
			return 0;
		}
		char ch;
		if (!buffer.ReadByte(ch))
		{
			return 0;
		}
		if (ch != '$')
		{
			char temp[100];
			sprintf(temp, "Protocol error: expected '$', , got '%c'", ch);
			APIException ex(temp);
			fire_exception_caught(ctx.GetChannel(), ex);
			return -1;
		}
		if (!buffer.Readable())
		{
			return 0;
		}
		const char* raw = buffer.GetRawReadBuffer();
		uint32 arglen = (uint32) strtol(raw, &eptr, 10);
		if (eptr[0] != '\r' || arglen == LONG_MIN || arglen == LONG_MAX
		        || arglen < 0 || arglen > 512 * 1024 * 1024)
		{
			APIException ex("Protocol error: invalid bulk length");
			fire_exception_caught(ctx.GetChannel(), ex);
			return -1;
		}
		buffer.SetReadIndex(newline_index + 2);
		if (!buffer.Readable())
		{
			return 0;
		}
		if (buffer.ReadableBytes() < (arglen + 2))
		{
			return 0;
		}
		frame.FillNextArgument(buffer, arglen);
		//Buffer* arg = frame.GetNextArgument(arglen);
		//buffer.Read(arg, arglen);
		char tempchs[2];
		buffer.Read(tempchs, 2);
		if (tempchs[0] != '\r' || tempchs[1] != '\n')
		{
			APIException ex("CRLF expected after argument.");
			fire_exception_caught(ctx.GetChannel(), ex);
			return -1;
		}
		//buffer.AdvanceReadIndex(arglen + 2);
		multibulklen--;
	}
	return 1;
}

bool RedisFrameDecoder::Decode(ChannelHandlerContext& ctx, Channel* channel,
        Buffer& buffer, RedisCommandFrame& msg)
{
	int reqtype = -1;
	size_t mark_read_index = buffer.GetReadIndex();
	char ch;
	if (buffer.ReadByte(ch))
	{
		int ret = -1;
		if (ch == '*')
		{
			reqtype = REDIS_REQ_MULTIBULK;
			msg.m_is_inline = false;
			ret = ProcessMultibulkBuffer(ctx, buffer, msg);
		}
		else
		{
			reqtype = REDIS_REQ_INLINE;
			msg.m_is_inline = true;
			buffer.AdvanceReadIndex(-1);
			ret = ProcessInlineBuffer(ctx, buffer, msg);
		}
		if (ret > 0)
		{
			return true;
		}
		else
		{
			msg.Clear();
			if (0 == ret)
			{
				buffer.SetReadIndex(mark_read_index);
			}
			return false;
		}
	}
	return false;
}
