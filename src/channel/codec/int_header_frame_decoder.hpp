/*
 * IntegerHeaderFrameDecoder.hpp
 *
 *  Created on: 2011-1-25
 *      Author: qiyingwang
 */

#ifndef NOVA_INTEGERHEADERFRAMEDECODER_HPP_
#define NOVA_INTEGERHEADERFRAMEDECODER_HPP_
#include "channel/codec/stack_frame_decoder.hpp"
#include <utility>

using std::pair;

namespace ardb
{
	namespace codec
	{
		/**
		 *  MESSAGE FORMAT
		 *  ==============
		 *
		 *  Offset:  0        4                   (Length + 4)
		 *           +--------+------------------------+
		 *  Fields:  | Length | Actual message content |
		 *           +--------+------------------------+
		 *
		 */
		class IntegerHeaderFrameDecoder: public StackFrameDecoder<Buffer>
		{
			protected:
				bool m_header_network_order;
				bool m_header_calc_in_length;
				bool m_remove_len_header;
//                    FrameDecodeResult<Buffer> Decode(
//                            ChannelHandlerContext& ctx, Channel* channel,
//                            Buffer& buffer);
				bool Decode(ChannelHandlerContext& ctx, Channel* channel,
						Buffer& buffer, Buffer& msg);
			public:
				IntegerHeaderFrameDecoder() :
						m_header_network_order(true), m_header_calc_in_length(
								true), m_remove_len_header(true)
				{
				}
				void SetHeaderNetworkOrder(bool value)
				{
					m_header_network_order = value;
				}
				void SetHeaderCalcInLength(bool value)
				{
					m_header_calc_in_length = value;
				}
				void SetRemoveLengthHeader(bool value)
				{
					m_remove_len_header = value;
				}
		};
	}
}
#endif /* INTEGERHEADERFRAMEDECODER_HPP_ */
