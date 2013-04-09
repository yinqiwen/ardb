/*
 * IntegerHeaderFrameDecoder.cpp
 *
 *  Created on: 2011-1-25
 *      Author: qiyingwang
 */
#include "channel/all_includes.hpp"

using namespace ardb::codec;

//static void DestoryByteBuffer(ByteBuffer* obj)
//{
//	ChannelBufferPool::Release(obj);
//}

bool IntegerHeaderFrameDecoder::Decode(ChannelHandlerContext& ctx, Channel* channel,
        Buffer& buffer, Buffer& msg)
{
	if (buffer.ReadableBytes() < 4)
	{
		return false;
	}
	size_t read_idx = buffer.GetReadIndex();
	uint32 len;
	buffer.Read(&len, sizeof(len));
	if(m_header_network_order)
	{
	    len = ntohl(len);
	}
	if (buffer.ReadableBytes() < (len - sizeof(len)))
	{
		buffer.SetReadIndex(read_idx);
		return false;
	}
	if(!m_remove_len_header)
	{
		msg.Write(&len, sizeof(uint32));
	}
	msg.EnsureWritableBytes(m_header_calc_in_length?len - sizeof(len):len);
	msg.Write(&buffer, m_header_calc_in_length?len - sizeof(len):len);
	//FrameDecodeResult<Buffer> result(msgbuf, StandardDestructor<Buffer>);
	return true;
}
