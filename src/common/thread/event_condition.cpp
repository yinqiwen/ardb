/*
 * event_condition.cpp
 *
 *  Created on: Feb 26, 2014
 *      Author: wangqiying
 */
#include "event_condition.hpp"
#include "util/atomic.hpp"
#include <assert.h>

namespace ardb
{
    EventCondition::EventCondition() :
            m_read_fd(-1), m_write_fd(-1), m_waiting_num(0)
    {
        int pipefd[2];
        int ret = pipe(pipefd);
        assert(ret == 0);
        m_read_fd = pipefd[0];
        m_write_fd = pipefd[1];
    }
    int EventCondition::Wait()
    {
        //atomic_add_uint32(&m_waiting_num, 1);
        char buf;
        int ret;
        while ((ret = read(m_read_fd, &buf, 1)) != 1)
            ;
        //atomic_sub_uint32(&m_waiting_num, 1);
        return buf;
    }
    int EventCondition::Notify()
    {
        //if (m_waiting_num > 0)
        {
            write(m_write_fd, "N", 1);
        }
        return 0;
    }
    int EventCondition::NotifyAll()
    {
        if (m_waiting_num > 0)
        {
            uint32_t num = m_waiting_num;
            char buf[num];
            write(m_write_fd, buf, num);
        }
        return 0;
    }

    EventCondition::~EventCondition()
    {
        close(m_read_fd);
        close(m_write_fd);
    }
}

