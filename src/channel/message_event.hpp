/*
 * MessageEvent.hpp
 *
 *  Created on: 2011-1-9
 *      Author: wqy
 */

#ifndef NOVA_MESSAGEEVENT_HPP_
#define NOVA_MESSAGEEVENT_HPP_
#include "common.hpp"
#include "channel/channel_event.hpp"
namespace rddb
{
	template<typename T>
	class MessageEvent: public ChannelEvent
	{
		private:
			T* m_msg;
			typename Type<T>::Destructor* messageDestructor;
		public:
			inline MessageEvent(Channel* ch, T* msg,
					typename Type<T>::Destructor* d, bool is_up) :
					ChannelEvent(ch, is_up), m_msg(msg), messageDestructor(d)
			{
			}
//                ChannelEventType GetType()
//                {
//                    return CHANNEL_MESSAGE_EVENT;
//                }
			inline T* GetMessage()
			{
				return m_msg;
			}
			inline void SetMessage(T* msg)
			{
				m_msg = msg;
			}
			inline void Unreference()
			{
				messageDestructor = NULL;
			}
			inline ~MessageEvent()
			{
				if (NULL != messageDestructor)
				{
					messageDestructor(m_msg);
				}
			}
	};
}

#endif /* MESSAGEEVENT_HPP_ */
