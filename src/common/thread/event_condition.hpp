/*
 * event_condition.hpp
 *
 *  Created on: Feb 26, 2014
 *      Author: wangqiying
 */

#ifndef EVENT_CONDITION_HPP_
#define EVENT_CONDITION_HPP_

#include <unistd.h>
#include <stdint.h>

namespace ardb
{
    class EventCondition
    {
        private:
            int m_read_fd;
            int m_write_fd;
            volatile uint32_t m_waiting_num;
        public:
            EventCondition();
            int Wait();
            int Notify();
            int NotifyAll();
            ~EventCondition();
    };
}

#endif /* EVENT_CONDITION_HPP_ */
