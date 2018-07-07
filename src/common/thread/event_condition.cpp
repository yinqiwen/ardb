/*
 *Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 *
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 *THE POSSIBILITY OF SUCH DAMAGE.
 */
#include "event_condition.hpp"
#include "util/atomic.hpp"
#include <assert.h>

namespace ardb
{
    EventCondition::EventCondition()
            : m_read_fd(-1), m_write_fd(-1), m_waiting_num(0)
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
            if (write(m_write_fd, "N", 1))
            {
            }
        }
        return 0;
    }
    int EventCondition::NotifyAll()
    {
        if (m_waiting_num > 0)
        {
            uint32_t num = m_waiting_num;
            char buf[num];
            if (write(m_write_fd, buf, num))
            {
            }
        }
        return 0;
    }

    EventCondition::~EventCondition()
    {
        close(m_read_fd);
        close(m_write_fd);
    }
}

