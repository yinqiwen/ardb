/*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
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

#include "buffer/buffer_helper.hpp"
#include "channel/all_includes.hpp"
#include "redis_command_codec.hpp"
#include "util/exception/api_exception.hpp"

#include <limits.h>

using ardb::BufferHelper;
using namespace ardb::codec;
using namespace ardb;

/* Client request types */
static const uint32 REDIS_REQ_INLINE = 1;
static const uint32 REDIS_REQ_MULTIBULK = 2;
static const char* kCRLF = "\r\n";

int RedisCommandDecoder::ProcessInlineBuffer(Buffer& buffer, RedisCommandFrame& frame)
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

static inline int readBulkLen(Buffer& buffer, int& len)
{
    char *eptr = NULL;
    const char* raw = buffer.GetRawReadBuffer();
    int tmp = strtol(raw, &eptr, 10);
    if ((eptr - raw) > (buffer.ReadableBytes() - 2))
    {
        return 0;
    }
    if (*eptr != '\r' || *(eptr + 1) != '\n')
    {
        return -1;
    }
    len = tmp;
    buffer.AdvanceReadIndex(eptr - raw + 2);
    return 1;
}

#define THROW_DECODE_EX(str)  do{\
        if (NULL != channel)\
        {\
              APIException ex(str);\
              fire_exception_caught(channel, ex);\
              ERROR_LOG("Exception:%s occured.", str);\
        }\
}while(0)

int RedisCommandDecoder::ProcessMultibulkBuffer(Channel* channel, Buffer& buffer, RedisCommandFrame& frame)
{
    if (buffer.ReadableBytes() < 3)  //at least  '0\r\n'
    {
        return 0;
    }
    int multibulklen = 0;
    int read_len_ret = readBulkLen(buffer, multibulklen);
    if (read_len_ret == 0)
    {
        return 0;
    }
    else if (read_len_ret < 0)
    {
        THROW_DECODE_EX("Protocol error: expected CRLF at bulk length end");
        return -1;
    }
    if (multibulklen > 512 * 1024 * 1024)
    {
        THROW_DECODE_EX("Protocol error: invalid multibulk length");
        return -1;
    }
    int parsed_args = 0;
    while (parsed_args < multibulklen)
    {
        if (buffer.ReadableBytes() < 4)  //at least '$0\r\n'
        {
            return 0;
        }
        char expected = 0;
        buffer.ReadByte(expected);
        if (expected != '$')
        {
            if (NULL != channel)
            {
                char temp[100];
                sprintf(temp, "Protocol error: expected '$', , got '%c'", buffer.GetRawReadBuffer()[0]);
                THROW_DECODE_EX(temp);
            }
            return -1;
        }
        int arglen = 0;
        read_len_ret = readBulkLen(buffer, arglen);
        if (read_len_ret == 0)
        {
            return 0;
        }
        else if (read_len_ret < 0)
        {
            THROW_DECODE_EX("Protocol error: expected CRLF at bulk length end");
            return -1;
        }
        if (arglen > 512 * 1024 * 1024)
        {
            THROW_DECODE_EX("Protocol error: invalid bulk length");
            return -1;
        }
        if (buffer.ReadableBytes() < (arglen + 2))
        {
            return 0;
        }
        if (buffer.GetRawReadBuffer()[arglen] != '\r' || buffer.GetRawReadBuffer()[arglen + 1] != '\n')
        {
            THROW_DECODE_EX("Protocol error: expected CRLF at bulk end.");
            return -1;
        }
        frame.FillNextArgument(buffer, arglen);
        buffer.AdvanceReadIndex(2);
        parsed_args++;
    }
    return 1;
}

bool RedisCommandDecoder::Decode(Channel* channel, Buffer& buffer, RedisCommandFrame& msg)
{
    while (buffer.GetRawReadBuffer()[0] == '\r' || buffer.GetRawReadBuffer()[0] == '\n')
    {
        buffer.AdvanceReadIndex(1);
    }
    size_t mark_read_index = buffer.GetReadIndex();
    char ch;
    if (buffer.ReadByte(ch))
    {
        int ret = -1;
        if (ch == '*')
        {
            //reqtype = REDIS_REQ_MULTIBULK;
            msg.m_is_inline = false;
            ret = ProcessMultibulkBuffer(channel, buffer, msg);
        }
        else
        {
            //reqtype = REDIS_REQ_INLINE;
            msg.m_is_inline = true;
            buffer.AdvanceReadIndex(-1);
            ret = ProcessInlineBuffer(buffer, msg);
        }
        if (ret > 0)
        {
            msg.m_raw_data_size = buffer.GetReadIndex() - mark_read_index;
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
    buffer.SetReadIndex(mark_read_index);
    return false;
}

bool RedisCommandDecoder::Decode(ChannelHandlerContext& ctx, Channel* channel, Buffer& buffer, RedisCommandFrame& msg)
{
    return Decode(channel, buffer, msg);
}

//===================================encoder==============================
bool RedisCommandEncoder::Encode(Buffer& buf, const RedisCommandFrame& cmd)
{
    buf.Printf("*%d\r\n", cmd.GetArguments().size() + 1);
    buf.Printf("$%d\r\n", cmd.GetCommand().size());
    buf.Write(cmd.GetCommand().data(), cmd.GetCommand().size());
    buf.Write("\r\n", 2);
    for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
    {
        const std::string* arg = cmd.GetArgument(i);
        buf.Printf("$%d\r\n", arg->size());
        buf.Write(arg->data(), arg->size());
        buf.Write("\r\n", 2);
    }
    return true;
}

bool RedisCommandEncoder::WriteRequested(ChannelHandlerContext& ctx, MessageEvent<RedisCommandFrame>& e)
{
    RedisCommandFrame* msg = e.GetMessage();
    m_buffer.Clear();
    if (Encode(m_buffer, *msg))
    {
        return ctx.GetChannel()->Write(m_buffer);
    }
    return false;
}
