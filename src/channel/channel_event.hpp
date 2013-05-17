/*
 * ChannelEvent.hpp
 *
 *  Created on: 2011-1-7
 *      Author: qiyingwang
 */

#ifndef NOVA_CHANNELEVENT_HPP_
#define NOVA_CHANNELEVENT_HPP_
#include "common.hpp"
namespace ardb
{
	class Channel;
	class ChannelEvent
	{
		protected:
			Channel* m_ch;
			bool m_is_upstream;
			friend class ChannelEventFactory;
		public:
			inline ChannelEvent(Channel* ch, bool is_up) :
					m_ch(ch), m_is_upstream(is_up)
			{
			}
			inline bool IsUpstreamEvent()
			{
				return m_is_upstream;
			}
			inline bool IsDownstreamEvent()
			{
				return !m_is_upstream;
			}
			inline Channel* GetChannel()
			{
				return m_ch;
			}
			virtual ~ChannelEvent()
			{
			}
	};
}

#endif /* CHANNELEVENT_HPP_ */
