/*
 * delimiter_frame_decoder.hpp
 *
 *  Created on: 2011-3-15
 *      Author: qiyingwang
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
