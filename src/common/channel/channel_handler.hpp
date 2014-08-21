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

#ifndef NOVA_CHANNELHANDLER_HPP_
#define NOVA_CHANNELHANDLER_HPP_
#include <stdexcept>
#include <typeinfo>
#include <set>
#include "channel/channel_state_event.hpp"
#include "channel/exception_event.hpp"
#include "channel/message_event.hpp"
namespace ardb
{
	class ChannelHandlerContext;
	class ChannelHandler
	{
		public:
			virtual bool CanHandleUpstream()
			{
				return false;
			}
			virtual bool CanHandleDownstream()
			{
				return false;
			}
			virtual bool HandleStreamEvent(ChannelHandlerContext& ctx,
					ChannelStateEvent& e) = 0;
			virtual bool HandleStreamEvent(ChannelHandlerContext& ctx,
					ExceptionEvent& e) = 0;

			virtual ~ChannelHandler()
			{
			}
	};

//	typedef std::tr1::unordered_set<ChannelHandler*> ChannelHandlerSet;
//	//typedef std::set<ChannelHandler*> ChannelHandlerSet;
//	template<typename T>
//	class ChannelHandlerHelper
//	{
//		private:
//			static ChannelHandlerSet m_down_handler_set;
//			static ChannelHandlerSet m_up_handler_set;
//		public:
//			static inline bool CanHandleDownMessageEvent(
//					ChannelHandler* handler)
//			{
//				if (m_down_handler_set.find(handler)
//						!= m_down_handler_set.end())
//				{
//					return true;
//				}
//				return false;
//			}
//			static inline bool CanHandleUpMessageEvent(ChannelHandler* handler)
//			{
//				if (m_up_handler_set.find(handler) != m_up_handler_set.end())
//				{
//					return true;
//				}
//				return false;
//			}
//			static inline void RegisterHandler(ChannelHandler* handler)
//			{
//				if (handler->CanHandleDownstream())
//				{
//					m_down_handler_set.insert(handler);
//				}
//				if (handler->CanHandleUpstream())
//				{
//					m_up_handler_set.insert(handler);
//				}
//			}
//			static inline void UnregisterHandler(ChannelHandler* handler)
//			{
//				m_down_handler_set.erase(handler);
//				m_up_handler_set.erase(handler);
//			}
//
//	};
//	template<typename T> ChannelHandlerSet ChannelHandlerHelper<T>::m_down_handler_set;
//	template<typename T> ChannelHandlerSet ChannelHandlerHelper<T>::m_up_handler_set;

	/**
	 *
	 */
	template<typename T>
	class AbstractChannelHandler: public ChannelHandler
	{
		public:
			virtual ~AbstractChannelHandler()
			{
				//ChannelHandlerHelper<T>::UnregisterHandler(this);
			}
	};

}

#endif /* CHANNELHANDLER_HPP_ */
