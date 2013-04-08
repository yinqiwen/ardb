/*
 * ExceptionEvent.hpp
 *
 *  Created on: 2011-1-10
 *      Author: qiyingwang
 */

#ifndef NOVA_EXCEPTIONEVENT_HPP_
#define NOVA_EXCEPTIONEVENT_HPP_
#include "channel/channel_event.hpp"
#include "util/exception/api_exception.hpp"

namespace rddb
{
	class ChannelEventFactory;
	class ExceptionEvent: public ChannelEvent
	{
		private:
			APIException m_exception;

			friend class ChannelEventFactory;
		public:
			inline ExceptionEvent(Channel* ch, const APIException& exception,
					bool is_up) :
					ChannelEvent(ch, is_up), m_exception(exception)
			{
			}
			inline APIException& GetException()
			{
				return m_exception;
			}
			inline ~ExceptionEvent()
			{

			}
	};
}

#endif /* EXCEPTIONEVENT_HPP_ */
