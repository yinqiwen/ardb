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

using rddb::BufferHelper;
using namespace rddb::codec;
using namespace rddb;

/* Client request types */
static const uint32 REDIS_REQ_INLINE = 1;
static const uint32 REDIS_REQ_MULTIBULK = 2;
static const char* kCRLF = "\r\n";

Buffer* RedisCommandFrame::GetNextArgument(size_t len)
{
	Buffer* buf = NULL;
	NEW(buf, Buffer);
	if (NULL != buf)
	{
		buf->EnsureWritableBytes(len);
	}
	m_args.push_back(buf);
	return buf;
}

Buffer* RedisCommandFrame::GetArgument(uint32 index)
{
	if (index >= m_args.size())
	{
		return NULL;
	}
	return m_args[index];
}

void RedisCommandFrame::Clear()
{
	ArgumentArray::iterator it = m_args.begin();
	while (it != m_args.end())
	{
		Buffer* arg = *it;
		DELETE(arg);
		it++;
	}
}

int RedisCommandFrame::EncodeRawProtocol(Buffer& buf)
{
	if (m_is_inline)
	{
		ArgumentArray::iterator it = m_args.begin();
		while (it != m_args.end())
		{
			Buffer* arg = *it;
			uint32 datalen = arg->ReadableBytes();
			buf.Write(arg, datalen);
			arg->AdvanceReadIndex(0 - datalen);
			it++;
			if (it != m_args.end())
			{
				buf.Printf(" ");
			}
		}
		buf.Write("\r\n", 2);
		return 0;
	} else
	{
		buf.Printf("*%d\r\n", m_args.size());
		ArgumentArray::iterator it = m_args.begin();
		while (it != m_args.end())
		{
			Buffer* arg = *it;
			uint32 datalen = arg->ReadableBytes();
			buf.Printf("$%d\r\n", datalen);
			buf.Write(arg, datalen);
			buf.Write("\r\n", 2);
			arg->AdvanceReadIndex(0 - datalen);
			it++;
		}
		return 0;
	}
}

int RedisCommandFrame::EncodeToBuffer(Buffer& buf)
{
	char ch = m_is_inline ? 1 : 0;
	buf.WriteByte(ch);
	size_t size = m_args.size();
	BufferHelper::WriteFixUInt32(buf, size);
	ArgumentArray::iterator it = m_args.begin();
	while (it != m_args.end())
	{
		Buffer* item = *it;
		size_t datalen = item->ReadableBytes();
		BufferHelper::WriteFixUInt32(buf, datalen);
		buf.Write(item, datalen);
		item->AdvanceReadIndex(0 - datalen);
		it++;
	}
	return 0;
}
int RedisCommandFrame::DecodeFromBuffer(Buffer& buf)
{
	char ch;
	if (!buf.ReadByte(ch))
	{
		return -1;
	}
	m_is_inline = ch == 1 ? true : false;
	uint32 len;
	if (!BufferHelper::ReadFixUInt32(buf, len))
	{
		return -1;
	}
	for (uint32 i = 0; i < len; i++)
	{
		uint32 arglen;
		if (!BufferHelper::ReadFixUInt32(buf, arglen))
		{
			return -1;
		}
		Buffer* item = GetNextArgument(arglen);
		if (-1 == buf.Read(item, arglen))
		{
			return -1;
		}
	}
	return 0;
}

RedisCommandFrame::~RedisCommandFrame()
{
	Clear();
}

int RedisFrameDecoder::ProcessInlineBuffer(ChannelHandlerContext& ctx,
		Buffer& buffer, RedisCommandFrame* frame)
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
		int space_index = buffer.IndexOf(" ", 1, current, index - 2);
		if (-1 == space_index)
		{
			break;
		}

		Buffer* arg = frame->GetNextArgument(space_index - current);
		buffer.Read(arg, space_index - current);
		buffer.AdvanceReadIndex(1); //skip space char
	}
	int current = buffer.GetReadIndex();
	if (current < index)
	{
		Buffer* arg = frame->GetNextArgument(index - current);
		buffer.Read(arg, index - current);
		buffer.AdvanceReadIndex(2); //skip "\r\n"
	}
	return 1;
}

int RedisFrameDecoder::ProcessMultibulkBuffer(ChannelHandlerContext& ctx,
		Buffer& buffer, RedisCommandFrame* frame)
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
	} else if (multibulklen > 1024 * 1024)
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
		Buffer* arg = frame->GetNextArgument(arglen);
		buffer.Read(arg, arglen);
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

FrameDecodeResult<RedisCommandFrame> RedisFrameDecoder::Decode(
		ChannelHandlerContext& ctx, Channel* channel, Buffer& buffer)
{
	int reqtype = -1;
	size_t mark_read_index = buffer.GetReadIndex();
	char ch;
	if (buffer.ReadByte(ch))
	{
		RedisCommandFrame* frame = NULL;
		NEW(frame, RedisCommandFrame);
		int ret = -1;
		if (ch == '*')
		{
			reqtype = REDIS_REQ_MULTIBULK;
			frame->m_is_inline = false;
			ret = ProcessMultibulkBuffer(ctx, buffer, frame);
		} else
		{
			reqtype = REDIS_REQ_INLINE;
			frame->m_is_inline = true;
			buffer.AdvanceReadIndex(-1);
			ret = ProcessInlineBuffer(ctx, buffer, frame);
		}
		if (ret > 0)
		{
			return FrameDecodeResult<RedisCommandFrame>(frame,
					rddb::StandardDestructor<RedisCommandFrame>);
		} else
		{
			DELETE(frame);
			if (0 == ret)
			{
				buffer.SetReadIndex(mark_read_index);
			}
			return FrameDecodeResult<RedisCommandFrame>();
		}

	}
	return FrameDecodeResult<RedisCommandFrame>();
}
