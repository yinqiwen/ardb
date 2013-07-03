/*
 * stat.hpp
 *
 *  Created on: 2013?7?3?
 *      Author: wqy
 */

#ifndef STAT_HPP_
#define STAT_HPP_

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include "common.hpp"
#include "util/thread/thread_mutex_lock.hpp"

namespace ardb
{
	class ServerStat
	{
		private:
			size_t m_recved_req;
			size_t m_sent_reply;
#ifdef HAVE_ATOMIC
			//do nothing
#else
			ThreadMutexLock m_lock;
#endif
			void IncValue(size_t& v)
			{
#ifdef HAVE_ATOMIC
				__sync_add_and_fetch(&v, 1);
#else
				m_lock.Lock();
				v++;
				m_lock.Unlock();
#endif
			}
		public:
			ServerStat() :
					m_recved_req(0), m_sent_reply(0)
			{
			}
			void IncRecvReq()
			{
				IncValue(m_recved_req);
			}
			void IncSentReply()
			{
				IncValue(m_sent_reply);
			}
			size_t GetSentReplyCount()
			{
				return m_sent_reply;
			}
			size_t GetRecvReqCount()
			{
				return m_recved_req;
			}
	};
}

#endif /* STAT_HPP_ */
