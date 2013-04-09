/*
 * delimiter_frame_decoder.cpp
 *
 *  Created on: 2011-3-16
 *      Author: qiyingwang
 */
#include "channel/all_includes.hpp"
#include "util/exception/api_exception.hpp"

using namespace ardb::codec;
using namespace ardb;

Delimiters CommonDelimiters::m_lineDelimiter;
Delimiters CommonDelimiters::m_nulDelimiter;
Delimiters CommonDelimiters::m_CRDelimiter;
Delimiters CommonDelimiters::m_LFDelimiter;
Delimiters CommonDelimiters::m_CRLFDelimiter;

bool CommonDelimiters::m_inited = false;

//static void DestoryByteBuffer(ByteBuffer* obj)
//{
//	ChannelBufferPool::Release(obj);
//}

void CommonDelimiters::Init()
{
	if (!m_inited)
	{
		m_lineDelimiter.push_back("\r\n");
		m_lineDelimiter.push_back("\n");
		m_CRDelimiter.push_back("\r");
		m_LFDelimiter.push_back("\n");
		m_CRLFDelimiter.push_back("\r\n");
		std::string nul("\0", 1);
		m_nulDelimiter.push_back(nul);
		m_inited = true;
	}
}

const Delimiters& CommonDelimiters::lineDelimiter()
{
	Init();
	return m_lineDelimiter;
}
const Delimiters& CommonDelimiters::nulDelimiter()
{
	Init();
	return m_nulDelimiter;
}

const Delimiters& CommonDelimiters::CRLFDelimiter()
{
	Init();
	return m_CRLFDelimiter;
}
const Delimiters& CommonDelimiters::CRDelimiter()
{
	Init();
	return m_CRDelimiter;
}
const Delimiters& CommonDelimiters::LFDelimiter()
{
	Init();
	return m_LFDelimiter;
}

DelimiterBasedFrameDecoder::DelimiterBasedFrameDecoder(uint32 maxFrameLength,
		const Delimiters& delimiters, bool stripDelimiter) :
		m_delimiters(delimiters), m_maxFrameLength(maxFrameLength), m_stripDelimiter(
				stripDelimiter), m_discardingTooLongFrame(false)
{
	if (m_delimiters.empty())
	{
		assert("Delimiters is empty!");
	}
}

bool DelimiterBasedFrameDecoder::Decode(ChannelHandlerContext& ctx,
		Channel* channel, Buffer& buffer, Buffer& frame)
{
	const char* minDelim = NULL;
	Delimiters::iterator it = m_delimiters.begin();
	uint32 minFrameLength = 0xffffffff;
	uint32 minDelimLength = 0;
	while (it != m_delimiters.end())
	{
		const char* delim = it->c_str();
		size_t read_idx = buffer.GetReadIndex();
		int frameLength = buffer.IndexOf(delim, it->size());
		if (frameLength >= 0 && (uint32) frameLength < minFrameLength)
		{
			minFrameLength = frameLength - read_idx;
			minDelim = delim;
			minDelimLength = it->size();
		}
		it++;
	}

	if (minDelim != NULL)
	{
		if (m_discardingTooLongFrame)
		{
			// We've just finished discarding a very large frame.
			// Go back to the initial state.
			m_discardingTooLongFrame = false;
			buffer.SkipBytes(minFrameLength + minDelimLength);

			// TODO Let user choose when the exception should be raised - early or late?
			//      If early, fail() should be called when discardingTooLongFrame is set to true.
			APIException exception("Frame length exceeds limit!");
			fire_exception_caught(channel, exception);
			return false;
		}

		if (minFrameLength > m_maxFrameLength)
		{
			// Discard read frame.
			buffer.SkipBytes(minFrameLength + minDelimLength);
			APIException exception("Frame length exceeds limit!");
			fire_exception_caught(channel, exception);
			return false;
		}
		//Buffer* frame = NULL;
		//NEW(frame, Buffer);
		frame.EnsureWritableBytes(
				m_stripDelimiter ?
						minFrameLength : minFrameLength + minDelimLength);
		if (m_stripDelimiter)
		{
			frame.Write(&buffer, minFrameLength);
			buffer.SkipBytes(minDelimLength);
		} else
		{
			frame.Write(&buffer, minFrameLength + minDelimLength);
		}

		return true;
	} else
	{
		if (!m_discardingTooLongFrame)
		{
			if (buffer.ReadableBytes() > m_maxFrameLength)
			{
				//ERROR_LOG("Discard too long frame greater than %u bytes.", m_maxFrameLength);
				// Discard the content of the buffer until a delimiter is found.
				buffer.Clear();
				m_discardingTooLongFrame = true;
			}
		} else
		{
			// Still discarding the buffer since a delimiter is not found.
			buffer.Clear();
		}
		return false;
	}
}
