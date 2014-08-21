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

#ifndef DELIMITER_FRAME_DECODER_HPP_
#define DELIMITER_FRAME_DECODER_HPP_
#include "channel/codec/stack_frame_decoder.hpp"
#include <string>
#include <vector>
namespace ardb
{
	namespace codec
	{
		typedef std::vector<std::string> Delimiters;
		/**
		 * \r\n as message delimiter
		 * +--------------+
		 * | ABC\r\nDEF\r\n |
		 * +--------------+
		 * DelimiterBasedFrameDecoder
		 */
		class DelimiterBasedFrameDecoder: public StackFrameDecoder<Buffer>
		{
			private:
				Delimiters m_delimiters;
				uint32 m_maxFrameLength;
				bool m_stripDelimiter;
				bool m_discardingTooLongFrame;
			protected:
				bool Decode(ChannelHandlerContext& ctx, Channel* channel,
						Buffer& buffer, Buffer& msg);
			public:
				DelimiterBasedFrameDecoder(uint32 maxFrameLength,
						const Delimiters& delimiters, bool stripDelimiter =
								false);

		};

		class CommonDelimiters
		{
			private:
				static Delimiters m_lineDelimiter;
				static Delimiters m_CRDelimiter;
				static Delimiters m_LFDelimiter;
				static Delimiters m_CRLFDelimiter;
				static Delimiters m_nulDelimiter;
				static bool m_inited;
				static void Init();
			public:
				static const Delimiters& lineDelimiter();
				static const Delimiters& nulDelimiter();
				static const Delimiters& CRLFDelimiter();
				static const Delimiters& CRDelimiter();
				static const Delimiters& LFDelimiter();
		};
	}
}

#endif /* DELIMITER_FRAME_DECODER_HPP_ */
