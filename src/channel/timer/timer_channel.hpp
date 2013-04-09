/*
 * timer_channel.hpp
 *
 *  Created on: 2011-3-27
 *      Author: wqy
 */

#ifndef TIMER_CHANNEL_HPP_
#define TIMER_CHANNEL_HPP_
#include "channel/channel.hpp"
#include "timer.hpp"

namespace ardb
{
	class TimerChannel: public Timer, public Channel
	{
		protected:
			static int TimeoutCB(struct aeEventLoop *eventLoop, long long id,
					void *clientData);
			void OnScheduled(TimerTask* task);
			long long m_timer_id;
		public:
			TimerChannel(ChannelService& service) :
					Channel(NULL, service), m_timer_id(-1)
			{
			}
			~TimerChannel();
	};
}

#endif /* TIMER_CHANNEL_HPP_ */
