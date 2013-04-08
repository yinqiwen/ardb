/*
 * ChannelHandler.hpp
 *
 *  Created on: 2011-1-7
 *      Author: qiyingwang
 */

#ifndef NOVA_CHANNELHANDLER_HPP_
#define NOVA_CHANNELHANDLER_HPP_
#include <stdexcept>
#include <set>
#include <tr1/unordered_set>
#include "channel/channel_state_event.hpp"
#include "channel/exception_event.hpp"
#include "channel/message_event.hpp"
//#include "channel/channel_handler_context.hpp"
namespace rddb
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

	typedef std::tr1::unordered_set<ChannelHandler*> ChannelHandlerSet;
	//typedef std::set<ChannelHandler*> ChannelHandlerSet;
	template<typename T>
	class ChannelHandlerHelper
	{
		private:
			static ChannelHandlerSet m_down_handler_set;
			static ChannelHandlerSet m_up_handler_set;
		public:
			static inline bool CanHandleDownMessageEvent(
					ChannelHandler* handler)
			{
				if (m_down_handler_set.find(handler)
						!= m_down_handler_set.end())
				{
					return true;
				}
				return false;
			}
			static inline bool CanHandleUpMessageEvent(ChannelHandler* handler)
			{
				if (m_up_handler_set.find(handler) != m_up_handler_set.end())
				{
					return true;
				}
				return false;
			}
			static inline void RegisterHandler(ChannelHandler* handler)
			{
				if (handler->CanHandleDownstream())
				{
					m_down_handler_set.insert(handler);
				}
				if (handler->CanHandleUpstream())
				{
					m_up_handler_set.insert(handler);
				}
			}
			static inline void UnregisterHandler(ChannelHandler* handler)
			{
				m_down_handler_set.erase(handler);
				m_up_handler_set.erase(handler);
			}

	};
	template<typename T> ChannelHandlerSet ChannelHandlerHelper<T>::m_down_handler_set;
	template<typename T> ChannelHandlerSet ChannelHandlerHelper<T>::m_up_handler_set;

	/**
	 * 所有handler的模版基类
	 */
	template<typename T>
	class AbstractChannelHandler: public ChannelHandler
	{
		public:
			virtual ~AbstractChannelHandler()
			{
				ChannelHandlerHelper<T>::UnregisterHandler(this);
			}
	};

}

#endif /* CHANNELHANDLER_HPP_ */
