/*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
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

#ifndef REDIS_REPLY_HPP_
#define REDIS_REPLY_HPP_

#include "common.hpp"
#include <deque>
#include <vector>
#include <string>

#define REDIS_REPLY_STRING 1
#define REDIS_REPLY_ARRAY 2
#define REDIS_REPLY_INTEGER 3
#define REDIS_REPLY_NIL 4
#define REDIS_REPLY_STATUS 5
#define REDIS_REPLY_ERROR 6

#define REDIS_REPLY_DOUBLE 1001

#define REDIS_REPLY_STATUS_OK 2001
#define REDIS_REPLY_STATUS_PONG 2002

#define FIRST_CHUNK_FLAG  0x01
#define LAST_CHUNK_FLAG  0x02

namespace ardb
{
    namespace codec
    {
        struct RedisDumpFileChunk
        {
                int64 len;
                uint32 flag;
                std::string chunk;
                RedisDumpFileChunk() :
                        len(0), flag(0)
                {
                }
                bool IsLastChunk()
                {
                    return (flag & LAST_CHUNK_FLAG) == (LAST_CHUNK_FLAG);
                }
                bool IsFirstChunk()
                {
                    return (flag & FIRST_CHUNK_FLAG) == (FIRST_CHUNK_FLAG);
                }
        };

        class RedisReplyPool;
        struct RedisReply
        {

            public:
                int type;
                std::string str;

                /*
                 * If the type is REDIS_REPLY_STRING, and the str's length is large,
                 * the integer value also used to identify chunk state.
                 */
                int64_t integer;
                double double_value;
                std::deque<RedisReply*>* elements;

                RedisReplyPool* pool;  //use object pool if reply is array with hundreds of elements
                bool self_pool;
                RedisReply() :
                        type(0), integer(0), double_value(0), elements(NULL),pool(NULL),self_pool(false)
                {
                }
                RedisReply(uint64 v) :
                        type(REDIS_REPLY_INTEGER), integer(v), double_value(0), elements(NULL),pool(NULL),self_pool(false)
                {
                }
                RedisReply(double v) :
                        type(REDIS_REPLY_DOUBLE), integer(0), double_value(v), elements(NULL),pool(NULL),self_pool(false)
                {
                }
                RedisReply(const std::string& v) :
                        type(REDIS_REPLY_STRING), str(v), integer(0), double_value(0), elements(NULL),pool(NULL),self_pool(false)
                {
                }
                void SetPool(RedisReplyPool* pool);
                RedisReplyPool& GetPool();
                RedisReply& AddMember(bool tail = true);
                void ReserveMember(size_t num);
                size_t MemberSize();
                RedisReply& MemberAt(uint32 i);
                void Clear();
                void Clone(const RedisReply& r)
                {
                    type = r.type;
                    integer = r.integer;
                    str = r.str;
                    double_value = r.double_value;
                    if (r.elements != NULL && !r.elements->empty())
                    {
                        for (uint32 i = 0; i < r.elements->size(); i++)
                        {
                            RedisReply& rr = AddMember();
                            rr.Clone(*(r.elements->at(i)));
                        }
                    }
                }
                ~RedisReply();
//            private:
//                RedisReply(const RedisReply& r) :
//                        type(0), integer(0), double_value(0), elements(NULL),pool(NULL),self_pool(false)
//                {
//                }
//                RedisReply& operator=(const RedisReply& r)
//                {
//                    return *this;
//                }
        };

        class RedisReplyPool
        {
            private:
                uint32 m_max_size;
                uint32 m_cursor;
                std::vector<RedisReply> elements;
                std::deque<RedisReply> pending;
            public:
                RedisReplyPool(uint32 size= 5);
                void SetMaxSize(uint32 size);
                RedisReply& Allocate();
                void Clear();
        };
    }
}

#endif /* REDIS_REPLY_HPP_ */
