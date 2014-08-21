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

#ifndef NOVA_CHANNELHANDLERCONTEXT_HPP_
#define NOVA_CHANNELHANDLERCONTEXT_HPP_
#include <string>
#include "channel/channel_handler.hpp"
#include "channel/timer/timer.hpp"
using std::string;

namespace ardb
{
	class ChannelPipeline;
	class Channel;
	class ChannelHandlerContext
	{
		private:
			ChannelHandlerContext* m_prev;
			ChannelHandlerContext* m_next;
			ChannelPipeline& m_pipeline;
			string m_name;
			ChannelHandler* m_handler;
			bool m_canHandleUpstream;
			bool m_canHandleDownstream;
		public:
			ChannelHandlerContext(ChannelPipeline& pipeline,
					ChannelHandlerContext* prev, ChannelHandlerContext* next,
					const string& name, ChannelHandler* handler);
			inline const string& GetName()
			{
				return m_name;
			}
			inline bool CanHandleUpstream()
			{
				return m_canHandleUpstream;
			}
			inline bool CanHandleDownstream()
			{
				return m_canHandleDownstream;
			}
			inline ChannelHandlerContext* GetPrev()
			{
				return m_prev;
			}
			inline void SetPrev(ChannelHandlerContext* prev)
			{
				m_prev = prev;
			}
			inline ChannelHandlerContext* GetNext()
			{
				return m_next;
			}
			inline void SetNext(ChannelHandlerContext* next)
			{
				m_next = next;
			}
			inline ChannelHandler* GetHandler()
			{
				return m_handler;
			}
			inline ChannelPipeline& GetPipeline()
			{
				return m_pipeline;
			}
			Timer& GetTimer();
			Channel* GetChannel();

			/**
			 * Send event to next downstream handler
			 * @see channel_pipeline.hpp
			 */
			template<typename T>
			bool SendDownstream(T& e);

			/**
			 *  Send event to next upstream handler
			 * @see channel_pipeline.hpp
			 */
			template<typename T>
			bool SendUpstream(T& e);
	};
}

#endif /* CHANNELHANDLERCONTEXT_HPP_ */
