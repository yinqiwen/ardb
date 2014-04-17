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

#ifndef REDIS_COMMAND_HPP_
#define REDIS_COMMAND_HPP_

#include <deque>
#include <string>

namespace ardb
{
    namespace codec
    {

        enum RedisCommandType
        {
            REDIS_CMD_INVALID = 0,
            REDIS_CMD_PING = 1,
            REDIS_CMD_MULTI = 2,
            REDIS_CMD_EXEC = 3,
            REDIS_CMD_WATCH = 4,
            REDIS_CMD_UNWATCH = 5,
            REDIS_CMD_SUBSCRIBE = 6,
            REDIS_CMD_UNSUBSCRIBE = 7,
            REDIS_CMD_IMPORT = 8,
            REDIS_CMD_PSUBSCRIBE = 9,
            REDIS_CMD_PUBLISH = 10,
            REDIS_CMD_INFO = 11,
            REDIS_CMD_SAVE = 12,
            REDIS_CMD_BGSAVE = 13,
            REDIS_CMD_LASTSAVE = 14,
            REDIS_CMD_SLOWLOG = 15,
            REDIS_CMD_DBSIZE = 16,
            REDIS_CMD_CONFIG = 17,
            REDIS_CMD_CLIENT = 18,
            REDIS_CMD_FLUSHDB = 19,
            REDIS_CMD_FLUSHALL = 20,
            REDIS_CMD_COMPACTDB = 21,
            REDIS_CMD_COMPACTALL = 22,
            REDIS_CMD_TIME = 23,
            REDIS_CMD_ECHO = 24,
            REDIS_CMD_QUIT = 25,
            REDIS_CMD_SHUTDOWN = 26,
            REDIS_CMD_SLAVEOF = 27,
            REDIS_CMD_REPLCONF = 28,
            EWDIS_CMD_SYNC = 29,
            REDIS_CMD_PSYNC = 30,
            REDIS_CMD_SELECT = 31,
            REDIS_CMD_APPEND = 32,
            REDIS_CMD_GET = 33,
            REDIS_CMD_SET = 34,
            REDIS_CMD_DEL = 35,
            REDIS_CMD_EXISTS = 36,
            REDIS_CMD_EXPIRE = 37,
            REDIS_CMD_PEXPIRE = 38,
            REDIS_CMD_EXPIREAT = 39,
            REDIS_CMD_PEXPIREAT = 40,
            REDIS_CMD_RANDOMKEY = 41,
            REDIS_CMD_PERSIST = 42,
            REDIS_CMD_TTL = 43,
            REDIS_CMD_PTTL = 44,
            REDIS_CMD_TYPE = 45,
            REDIS_CMD_BITCOUNT = 46,
            REDIS_CMD_BITOP = 47,
            REDIS_CMD_BITOPCUNT = 48,
            REDIS_CMD_DECR = 49,
            REDIS_CMD_DECRBY = 50,
            REDIS_CMD_GETBIT = 51,
            REDIS_CMD_GETRANGE = 52,
            REDIS_CMD_GETSET = 53,
            REDIS_CMD_INCR = 54,
            REDIS_CMD_INCRBY = 55,
            REDIS_CMD_INCRBYFLOAT = 56,
            REDIS_CMD_MGET = 57,
            REDIS_CMD_MSET = 58,
            REDIS_CMD_MSETNX = 59,
            REDIS_CMD_PSETEX = 60,
            REDIS_CMD_SETBIT = 61,
            REDIS_CMD_SETEX = 62,
            REDIS_CMD_SETNX = 63,
            REDIS_CMD_SETEANGE = 64,
            REDIS_CMD_STRLEN = 65,
            REDIS_CMD_HDEL = 66,
            REDIS_CMD_HEXISTS = 67,
            REDIS_CMD_HGET = 68,
            REDIS_CMD_HGETALL = 69,
            REDIS_CMD_HINCR = 70,
            REDIS_CMD_HMINCRBY = 71,
            REDIS_CMD_HINCRBYFLOAT = 72,
            REDIS_CMD_HKEYS = 73,
            REDIS_CMD_HLEN = 74,
            REDIS_CMD_HVALS = 75,
            REDIS_CMD_HMGET = 76,
            REDIS_CMD_HSET = 77,
            REDIS_CMD_HSETNX = 78,
            REDIS_CMD_HMSET = 79,
            REDIS_CMD_SCARD = 80,
            REDIS_CMD_SDIFF = 81,
            REDIS_CMD_SDIFFCOUNT = 82,
            REDIS_CMD_SDIFFSTORE = 83,
            REDIS_CMD_SINTER = 84,
            REDIS_CMD_SISMEMBER = 85,
            REDIS_CMD_SINTERSTORE = 86,
            REDIS_CMD_SMEMBERS = 87,
            REDIS_CMD_SMOVE = 88,
            REDIS_CMD_SPOP = 89,
            REDIS_CMD_SRANMEMEBER = 90,
            REDIS_CMD_SREM = 91,
            REDIS_CMD_SUNION = 92,
            REDIS_CMD_SUNIONSTORE = 93,
            REDIS_CMD_SUNIONCOUNT = 94,
            REDIS_CMD_SRANGE = 95,
            REDIS_CMD_SREVREANGE = 96,
            REDIS_CMD_ZADD = 97,
            REDIS_CMD_ZCARD = 98,
            REDIS_CMD_ZCOUNT = 99,
            REDIS_CMD_ZINCRBY = 100,
            REDIS_CMD_ZRANGE = 101,
            REDIS_CMD_ZRANGEBYSCORE = 102,
            REDIS_CMD_ZRANK = 103,
            REDIS_CMD_ZREM = 104,
            REDIS_CMD_ZPOP = 105,
            REDIS_CMD_ZRPOP = 106,
            REDIS_CMD_ZREMRANGEBYRANK = 107,
            REDIS_CMD_ZREMRANGEBYSCORE = 108,
            REDIS_CMD_ZREVRANGE = 109,
            REDIS_CMD_ZREVRANGEBYSCORE = 110,
            REDIS_CMD_ZINTERSTORE = 111,
            REDIS_CMD_ZUNIONSTORE = 112,
            REDIS_CMD_ZREVRANK = 113,
            REDIS_CMD_ZSCORE = 114,
            REDIS_CMD_LINDEX = 115,
            REDIS_CMD_LLEN = 116,
            REDIS_CMD_LPOP = 117,
            REDIS_CMD_LPUSH = 118,
            REDIS_CMD_LREM = 119,
            REDIS_CMD_LTRIM = 120,
            REDIS_CMD_RPOP = 121,
            REDIS_CMD_RPUSH = 122,
            REDIS_CMD_RPUSHX = 123,
            REDIS_CMD_RPOPLPUSH = 124,
            REDIS_CMD_HCLEAR = 125,
            REDIS_CMD_ZCLEAR = 126,
            REDIS_CMD_SCLEAR = 127,
            REDIS_CMD_LCLEAR = 128,
            REDIS_CMD_MOVE = 129,
            REDIS_CMD_RENAME = 130,
            REDIS_CMD_RENAMENX = 132,
            REDIS_CMD_SORT = 133,
            REDIS_CMD_KEYS = 134,
            REDIS_CMD_RAWSET = 135,
            REDIS_CMD_RAWDEL = 136,

            REDIS_CMD_BLPOP = 137,
            REDIS_CMD_BRPOP = 138,
            REDIS_CMD_BRPOPLPUSH = 139,
            REDIS_CMD_AUTH = 140,

            REDIS_CMD_DISCARD = 148,
            REDIS_CMD_PUNSUBSCRIBE = 149,
            REDIS_CMD_SADD = 150,
            REDIS_CMD_SINTERCOUNT = 151,
            REDIS_CMD_LINSERT = 152,
            REDIS_CMD_LPUSHX = 153,
            REDIS_CMD_LRANGE = 154,
            REDIS_CMD_LSET = 155,
            REDIS_CMD_EVAL = 156,
            REDIS_CMD_EVALSHA = 157,
            REDIS_CMD_SCRIPT = 158,
            REDIS_CMD_KEYSCOUNT = 159,

            REDIS_CMD_SCAN = 162,
            REDIS_CMD_SSCAN = 163,
            REDIS_CMD_HSCAN = 164,
            REDIS_CMD_ZSCAN = 165,
            REDIS_CMD_GEO_ADD = 166,
            REDIS_CMD_GEO_SEARCH = 167,
            REDIS_CMD_CACHE = 168,

            REDIS_CMD_PFADD = 169,
            REDIS_CMD_PFCOUNT = 170,
            REDIS_CMD_PFMERGE = 171,
            REDIS_CMD_PFMERGECOUNT = 172,

        };

        class RedisCommandDecoder;
        typedef std::deque<std::string> ArgumentArray;
        class RedisCommandFrame
        {
            private:
                RedisCommandType type;
                bool m_is_inline;
                bool m_cmd_seted;
                std::string m_cmd;
                ArgumentArray m_args;
                /*
                 * Used to identify the received protocol data size
                 */
                uint32 m_raw_data_size;
                inline void FillNextArgument(Buffer& buf, size_t len)
                {
                    const char* str = buf.GetRawReadBuffer();
                    buf.AdvanceReadIndex(len);
                    if (m_cmd_seted)
                    {
                        m_args.push_back(std::string(str, len));
                    }
                    else
                    {
                        m_cmd.append(str, len);
                        m_cmd_seted = true;
                    }
                }
                friend class RedisCommandDecoder;
            public:
                RedisCommandFrame() :
                        type(REDIS_CMD_INVALID), m_is_inline(false), m_cmd_seted(false), m_raw_data_size(0)
                {
                }
                RedisCommandFrame(ArgumentArray& cmd) :
                        type(REDIS_CMD_INVALID), m_is_inline(false), m_cmd_seted(false), m_raw_data_size(0)
                {
                    m_cmd = cmd.front();
                    cmd.pop_front();
                    m_args = cmd;
                }
                RedisCommandFrame(const char* fmt, ...) :
                        type(REDIS_CMD_INVALID), m_is_inline(false), m_cmd_seted(false), m_raw_data_size(0)
                {
                    va_list ap;
                    va_start(ap, fmt);
                    char buf[1024];
                    vsnprintf(buf, sizeof(buf) - 1, fmt, ap);
                    va_end(ap);
                    char * pch;
                    pch = strtok(buf, " ");
                    while (pch != NULL)
                    {
                        m_args.push_back(std::string(pch));
                        pch = strtok(NULL, " ");
                    }
                    m_cmd = m_args.front();
                    m_args.pop_front();
                }

                inline uint32 GetRawDataSize()
                {
                    return m_raw_data_size;
                }
                inline void SetType(RedisCommandType type)
                {
                    this->type = type;
                }
                inline RedisCommandType GetType() const
                {
                    return this->type;
                }
                bool IsInLine() const
                {
                    return m_is_inline;
                }
                const ArgumentArray& GetArguments() const
                {
                    return m_args;
                }
                ArgumentArray& GetMutableArguments()
                {
                    return m_args;
                }
                const std::string& GetCommand() const
                {
                    return m_cmd;
                }
                std::string& GetMutableCommand()
                {
                    return m_cmd;
                }
                void SetCommand(const std::string& cmd)
                {
                    m_cmd = cmd;
                }
                const std::string* GetArgument(uint32 index) const
                {
                    if (index >= m_args.size())
                    {
                        return NULL;
                    }
                    return &(m_args[index]);
                }
                std::string ToString() const
                {
                    std::string cmd;
                    cmd.append(m_cmd).append(" ");
                    for (uint32 i = 0; i < m_args.size(); i++)
                    {
                        cmd.append(m_args[i]);
                        if (i != m_args.size() - 1)
                        {
                            cmd.append(" ");
                        }
                    }
                    return cmd;
                }
                void Clear()
                {
                    m_cmd_seted = false;
                    m_cmd.clear();
                    m_args.clear();
                }
                ~RedisCommandFrame()
                {

                }
        };
    }
}

#endif /* REDIS_COMMAND_HPP_ */
