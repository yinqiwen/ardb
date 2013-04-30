/*
 * ThreadCondition.hpp
 *
 *  Created on: 2011-1-12
 *      Author: wqy
 */

#ifndef NOVA_THREADCONDITION_HPP_
#define NOVA_THREADCONDITION_HPP_
#include <pthread.h>

namespace ardb
{
	class ThreadCondition
	{
		protected:
			pthread_cond_t m_cond;
		public:
			ThreadCondition()
			{
				pthread_cond_init(&m_cond, NULL);
			}
			virtual ~ThreadCondition()
			{
				pthread_cond_destroy(&m_cond);
			}
			pthread_cond_t& GetRawCondition()
			{
				return m_cond;
			}

	};
}

#endif /* THREADCONDITION_HPP_ */
