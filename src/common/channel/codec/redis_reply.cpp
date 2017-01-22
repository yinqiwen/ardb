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
#include "db/engine.hpp"

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
                rr.Clear();
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

        double RedisReply::GetDouble()
        {
            if (type == REDIS_REPLY_DOUBLE)
            {
                return *(double*) (&integer);
            }
            return 0;
        }
        void RedisReply::SetDouble(double v)
        {
            type = REDIS_REPLY_DOUBLE;
            *(double*) (&integer) = v;
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
        void RedisReply::ReserveMember(int64_t num)
        {
            Clear();
            type = REDIS_REPLY_ARRAY;
            integer = num;
            for (size_t i = 0; num > 0 && i < num; i++)
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
            if (NULL == pool && NULL != elements)
            {
                for (int i = 0; i < elements->size(); i++)
                {
                    DELETE(elements->at(i));
                }
            }
            DELETE(elements);
            type = REDIS_REPLY_NIL;
            integer = 0;
            str.clear();
        }
        const std::string& RedisReply::Error()
        {
            if (str.empty())
            {
                reply_error_string(integer, str);
            }
            return str;
        }
        const std::string& RedisReply::Status()
        {
            if (str.empty())
            {
                reply_status_string(integer, str);
            }
            return str;
        }
        RedisReply::~RedisReply()
        {
            Clear();
        }

        void clone_redis_reply(RedisReply& src, RedisReply& dst)
        {
            dst.integer = src.integer;
            dst.type = src.type;
            switch (src.type)
            {
                case REDIS_REPLY_STATUS:
                case REDIS_REPLY_STRING:
                case REDIS_REPLY_ERROR:
                {
                    dst.str = src.str;
                    break;
                }
                case REDIS_REPLY_ARRAY:
                {
                    for (size_t i = 0; i < src.MemberSize(); i++)
                    {
                        RedisReply& child = src.MemberAt(i);
                        RedisReply& clone_child = dst.AddMember();
                        clone_redis_reply(child, clone_child);
                    }
                    break;
                }
                default:
                {
                    break;
                }
            }
        }

        void reply_status_string(int code, std::string& str)
        {
            switch (code)
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
                    str.assign("QUEUED", 6);
                    break;
                }
                case STATUS_NOKEY:
                {
                    str.assign("NOKEY", 5);
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
            switch (code)
            {
                case ERR_ENTRY_NOT_EXIST:
                {
                    str.assign("no such key");
                    break;
                }
                case ERR_INVALID_FLOAT_ARGS:
                {
                    str.assign("value is not a valid float or out of range");
                    break;
                }
                case ERR_INVALID_SYNTAX:
                {
                    str.assign("syntax error");
                    break;
                }
                case ERR_NOTPERFORMED:
                {
                    str.assign("not performed");
                    break;
                }
                case ERR_STRING_EXCEED_LIMIT:
                {
                    str.assign("string exceeds maximum allowed size (512MB)");
                    break;
                }
                case ERR_NOSCRIPT:
                {
                    str.assign("-NOSCRIPT No matching script. Please use EVAL.");
                    break;
                }
                case ERR_NOTSUPPORTED:
                {
                    str.assign("not supported");
                    break;
                }
                case ERR_INVALID_ARGS:
                {
                    str.assign("wrong number of arguments");
                    break;
                }
                case ERR_KEY_EXIST:
                {
                    str.assign("key exist");
                    break;
                }
                case ERR_INVALID_INTEGER_ARGS:
                {
                    str.assign("value is not an integer or out of range");
                    break;
                }
                case ERR_LOADING:
                {
                    str.assign("-LOADING Ardb is loading the snapshot file.");
                    break;
                }
                case ERR_MASTER_DOWN:
                {
                    str.assign("-MASTERDOWN Link with MASTER is down and slave-serve-stale-data is set to 'no'.");
                    break;
                }
                case ERR_READONLY_SLAVE:
                {
                    str.assign("-READONLY You can't write against a read only slave.");
                    break;
                }
                case ERR_WRONG_TYPE:
                {
                    str.assign("-WRONGTYPE Operation against a key holding the wrong kind of value.");
                    break;
                }
                case ERR_NOREPLICAS:
                {
                    str.assign("-NOREPLICAS Not enough good slaves to write.");
                    break;
                }
                case ERR_EXEC_ABORT:
                {
                    str.assign("EXECABORT Transaction discarded because of previous errors.");
                    break;
                }
                case ERR_UNSUPPORT_DIST_UNIT:
                {
                    str.assign("unsupported unit provided. please use m, km, ft, mi.");
                    break;
                }
                case ERR_SCORE_NAN:
                {
                    str.assign("resulting score is not a number (NaN)");
                    break;
                }
                case ERR_OUTOFRANGE:
                {
                    str.assign("value is out of range");
                    break;
                }
                case ERR_BIT_OFFSET_OUTRANGE:
                {
                    str.assign("bit offset is not an integer or out of range");
                    break;
                }
                case ERR_BIT_OUTRANGE:
                {
                    str.assign("bit is not an integer or out of range");
                    break;
                }
                case ERR_CORRUPTED_HLL_OBJECT:
                {
                    str.assign("INVALIDOBJ Corrupted HLL object detected");
                    break;
                }
                case ERR_INVALID_HLL_STRING:
                {
                    str.assign("WRONGTYPE Key is not a valid HyperLogLog string value.");
                    break;
                }
                default:
                {
                    str = g_engine->GetErrorReason(code);
                    if (str.empty())
                    {
                        char tmp[256];
                        sprintf(tmp, "##Unknown error code:%d", code);
                        str = tmp;
                    }
                    break;
                }
            }
        }
    }
}

