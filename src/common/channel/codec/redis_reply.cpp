/*
 * redis_reply.cpp
 *
 *  Created on: 2014Äê8ÔÂ19ÈÕ
 *      Author: wangqiying
 */
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

#include "redis_reply.hpp"

namespace ardb
{
    namespace codec
    {
        RedisReplyPool::RedisReplyPool(uint32 size) :
                m_max_size(size), m_cursor(0)
        {
            SetMaxSize(size);
        }
        void RedisReplyPool::SetMaxSize(uint32 size)
        {
            if (size > 0 && size != m_max_size)
            {
                elements.resize(size);
                m_max_size = size;
                for (uint32 i = 0; i < m_max_size; i++)
                {
                    elements[i].SetPool(this);
                }
            }
        }
        RedisReply& RedisReplyPool::Allocate()
        {
            if (m_cursor >= elements.size())
            {
                while (m_cursor - elements.size() >= pending.size())
                {
                    RedisReply r;
                    r.SetPool(this);
                    pending.push_back(r);
                }
                RedisReply& rr = pending[m_cursor - elements.size()];
                m_cursor++;
                return rr;
            }
            RedisReply& r = elements[m_cursor++];
            r.Clear();
            return r;
        }
        void RedisReplyPool::Clear()
        {
            m_cursor = 0;
            if (!pending.empty())
            {
                pending.clear();
            }
        }

        void RedisReply::SetPool(RedisReplyPool* p)
        {
            if (NULL != p)
            {
                pool = p;
            }
        }

        long double RedisReply::GetDouble()
        {
            if (type == REDIS_REPLY_DOUBLE)
            {
                return *(long double*) (&integer);
            }
            return 0;
        }
        void RedisReply::SetDouble(long double v)
        {
            type = REDIS_REPLY_DOUBLE;
            *(long double*) (&integer) = v;
        }

        RedisReply& RedisReply::AddMember(bool tail)
        {
            type = REDIS_REPLY_ARRAY;
            if (NULL == elements)
            {
                elements = new std::deque<RedisReply*>;
            }
            RedisReply* reply = NULL;
            if (NULL == pool)
            {
                NEW(reply, RedisReply);
            }
            else
            {
                reply = &(pool->Allocate());
            }
            if (tail)
            {
                elements->push_back(reply);
            }
            else
            {
                elements->push_front(reply);
            }
            return *reply;
        }
        void RedisReply::ReserveMember(size_t num)
        {
            type = REDIS_REPLY_ARRAY;
            for (size_t i = 0; i < num; i++)
            {
                AddMember();
            }
        }
        size_t RedisReply::MemberSize()
        {
            if (NULL == elements)
            {
                return 0;
            }
            return elements->size();
        }
        RedisReply& RedisReply::MemberAt(uint32 i)
        {
            return *(elements->at(i));
        }
        void RedisReply::Clear()
        {
            type = REDIS_REPLY_NIL;
            integer = 0;
            str.clear();
            if (NULL == pool && NULL != elements)
            {
                for (int i = 0; i < elements->size(); i++)
                {
                    DELETE(elements->at(i));
                }
            }
            DELETE(elements);
        }
        RedisReply::~RedisReply()
        {
            DELETE(elements);
        }

        void reply_status_string(int code, std::string& str)
        {
            switch(code)
            {
                case STATUS_OK:
                {
                    str.assign("OK", 2);
                    break;
                }
                case STATUS_PONG:
                {
                    str.assign("PONG", 4);
                    break;
                }
                case STATUS_QUEUED:
                {
                    str.assign("QUEUED", 5);
                    break;
                }
                default:
                {
                    abort();
                }
            }
        }
        void reply_error_string(int code, std::string& str)
        {
            switch(code)
            {
                case STATUS_OK:
                {
                    str.assign("OK", 2);
                    break;
                }
                case STATUS_PONG:
                {
                    str.assign("PONG", 4);
                    break;
                }
                case STATUS_QUEUED:
                {
                    str.assign("QUEUED", 5);
                    break;
                }
                default:
                {
                    abort();
                }
            }
        }
    }
}

