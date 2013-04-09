/*
 * ChannelStateEvent.hpp
 *
 *  Created on: 2011-1-8
 *      Author: wqy
 */

#ifndef NOVA_CHANNELSTATEEVENT_HPP_
#define NOVA_CHANNELSTATEEVENT_HPP_
#include "channel/channel_event.hpp"

namespace ardb
{
	enum ChannelState
	{
		OPEN = 1, BOUND = 2, CONNECTED = 3, CLOSED = 4, FLUSH = 5
	};

	/**
	 * Channel状态变更event，打开/关闭 socket属于此类event
	 */
	class ChannelEventFactory;
	class ChannelStateEvent: public ChannelEvent
	{
		private:
			ChannelState m_state;
			Address* m_addr;

			friend class ChannelEventFactory;
		public:
			inline ChannelStateEvent(Channel* ch, ChannelState state,
					Address* raw, bool is_up) :
					ChannelEvent(ch, is_up), m_state(state), m_addr(raw)
			{
			}
			inline ChannelState GetState()
			{
				return m_state;
			}
			inline Address* GetAddress()
			{
				return m_addr;
			}
			inline ~ChannelStateEvent()
			{
			}
	};
}

#endif /* CHANNELSTATEEVENT_HPP_ */
