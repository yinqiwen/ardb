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
