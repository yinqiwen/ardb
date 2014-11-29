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

#ifndef NOVA_INTEGERFRAMEDECODER_HPP_
#define NOVA_INTEGERFRAMEDECODER_HPP_
#include "channel/codec/stack_frame_decoder.hpp"
#include "util/network_helper.hpp"
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
		class UInt64FrameDecoder: public StackFrameDecoder<uint64>
		{
			protected:
				bool m_header_network_order;
				bool Decode(ChannelHandlerContext& ctx, Channel* channel,
				        Buffer& buffer, uint64& msg)
				{
					if (buffer.ReadableBytes() < sizeof(uint64))
					{
						return false;
					}
					buffer.Read(&msg, sizeof(uint64));
					if (m_header_network_order)
					{
						msg = ntoh_u64(msg);
					}
					return true;
				}
			public:
				UInt64FrameDecoder() :
						m_header_network_order(false)
				{
				}
				void SetHeaderNetworkOrder(bool value)
				{
					m_header_network_order = value;
				}
		};
	}
}
#endif /* INTEGERHEADERFRAMEDECODER_HPP_ */
