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
            //'db' commands
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
            REDIS_CMD_SYNC = 29,
            REDIS_CMD_PSYNC = 30,
            REDIS_CMD_SELECT = 31,
            REDIS_CMD_AUTH = 32,
            REDIS_CMD_DISCARD = 33,
            REDIS_CMD_PUNSUBSCRIBE = 34,
            REDIS_CMD_EVAL = 35,
            REDIS_CMD_EVALSHA = 36,
            REDIS_CMD_SCRIPT = 37,
            REDIS_CMD_RAWSET = 38,
            REDIS_CMD_PUBSUB = 39,
            REDIS_CMD_MONITOR = 40,
            REDIS_CMD_DEBUG = 41,
            REDIS_CMD_BACKUP = 42,

            //'keys' commands
            REDIS_CMD_DEL = 50,
            REDIS_CMD_EXISTS = 51,
            REDIS_CMD_EXPIRE = 52,
            REDIS_CMD_PEXPIRE = 53,
            REDIS_CMD_EXPIREAT = 54,
            REDIS_CMD_PEXPIREAT = 55,
            REDIS_CMD_RANDOMKEY = 56,
            REDIS_CMD_PERSIST = 57,
            REDIS_CMD_TTL = 58,
            REDIS_CMD_PTTL = 59,
            REDIS_CMD_TYPE = 60,
            REDIS_CMD_MOVE = 61,
            REDIS_CMD_RENAME = 62,
            REDIS_CMD_RENAMENX = 63,
            REDIS_CMD_SORT = 64,
            REDIS_CMD_KEYS = 65,
            REDIS_CMD_SCAN = 66,
            REDIS_CMD_KEYSCOUNT = 67,
            REDIS_CMD_MIGRATE = 68,
            REDIS_CMD_DUMP = 69,
            REDIS_CMD_RESTORE = 70,
            REDIS_CMD_MIGRATEDB = 71,
            REDIS_CMD_RESTOREDB = 72,
            REDIS_CMD_RESTORECHUNK = 73,
            REDIS_CMD_TOUCH = 74,

            //'string' commands
            REDIS_CMD_APPEND = 100,
            REDIS_CMD_GET = 101,
            REDIS_CMD_SET = 102,
            REDIS_CMD_BITCOUNT = 103,
            REDIS_CMD_BITOP = 104,
            REDIS_CMD_BITOPCUNT = 105,
            REDIS_CMD_DECR = 106,
            REDIS_CMD_DECRBY = 107,
            REDIS_CMD_GETBIT = 108,
            REDIS_CMD_GETRANGE = 109,
            REDIS_CMD_GETSET = 110,
            REDIS_CMD_INCR = 111,
            REDIS_CMD_INCRBY = 112,
            REDIS_CMD_INCRBYFLOAT = 113,
            REDIS_CMD_MGET = 114,
            REDIS_CMD_MSET = 115,
            REDIS_CMD_MSETNX = 116,
            REDIS_CMD_PSETEX = 117,
            REDIS_CMD_SETBIT = 118,
            REDIS_CMD_SETEX = 119,
            REDIS_CMD_SETNX = 120,
            REDIS_CMD_SETRANGE = 121,
            REDIS_CMD_STRLEN = 122,
            REDIS_CMD_PFADD = 123,
            REDIS_CMD_PFCOUNT = 124,
            REDIS_CMD_PFMERGE = 125,
            REDIS_CMD_SETXX = 126,

            //'hash' commands
            REDIS_CMD_HDEL = 150,
            REDIS_CMD_HEXISTS = 151,
            REDIS_CMD_HGET = 152,
            REDIS_CMD_HGETALL = 153,
            REDIS_CMD_HINCR = 154,
            REDIS_CMD_HINCRBYFLOAT = 155,
            REDIS_CMD_HKEYS = 156,
            REDIS_CMD_HLEN = 157,
            REDIS_CMD_HVALS = 158,
            REDIS_CMD_HMGET = 159,
            REDIS_CMD_HSET = 160,
            REDIS_CMD_HSETNX = 161,
            REDIS_CMD_HMSET = 162,
            REDIS_CMD_HSCAN = 163,

            //'set' commands
            REDIS_CMD_SCARD = 200,
            REDIS_CMD_SDIFF = 201,
            REDIS_CMD_SDIFFCOUNT = 202,
            REDIS_CMD_SDIFFSTORE = 203,
            REDIS_CMD_SINTER = 204,
            REDIS_CMD_SISMEMBER = 205,
            REDIS_CMD_SINTERSTORE = 206,
            REDIS_CMD_SMEMBERS = 207,
            REDIS_CMD_SMOVE = 208,
            REDIS_CMD_SPOP = 209,
            REDIS_CMD_SRANMEMEBER = 210,
            REDIS_CMD_SREM = 211,
            REDIS_CMD_SUNION = 212,
            REDIS_CMD_SUNIONSTORE = 213,
            REDIS_CMD_SUNIONCOUNT = 214,
            REDIS_CMD_SADD = 215,
            REDIS_CMD_SINTERCOUNT = 216,
            REDIS_CMD_SSCAN = 217,

            //'zset' commands
            REDIS_CMD_ZADD = 250,
            REDIS_CMD_ZCARD = 251,
            REDIS_CMD_ZCOUNT = 252,
            REDIS_CMD_ZINCRBY = 253,
            REDIS_CMD_ZRANGE = 254,
            REDIS_CMD_ZRANGEBYSCORE = 255,
            REDIS_CMD_ZRANK = 256,
            REDIS_CMD_ZREM = 257,
            REDIS_CMD_ZPOP = 258,
            REDIS_CMD_ZRPOP = 259,
            REDIS_CMD_ZREMRANGEBYRANK = 260,
            REDIS_CMD_ZREMRANGEBYSCORE = 261,
            REDIS_CMD_ZREVRANGE = 262,
            REDIS_CMD_ZREVRANGEBYSCORE = 263,
            REDIS_CMD_ZINTERSTORE = 264,
            REDIS_CMD_ZUNIONSTORE = 265,
            REDIS_CMD_ZREVRANK = 266,
            REDIS_CMD_ZSCORE = 267,
            REDIS_CMD_ZSCAN = 268,
            REDIS_CMD_ZLEXCOUNT = 269,
            REDIS_CMD_ZRANGEBYLEX = 270,
            REDIS_CMD_ZREVRANGEBYLEX = 271,
            REDIS_CMD_ZREMRANGEBYLEX = 272,
            REDIS_CMD_GEO_ADD = 273,
            REDIS_CMD_GEO_RADIUS = 274,
            REDIS_CMD_GEO_RADIUSBYMEMBER = 275,
            REDIS_CMD_GEO_DIST = 276,
            REDIS_CMD_GEO_HASH = 277,
            REDIS_CMD_GEO_POS = 278,

            //'list' commands
            REDIS_CMD_LINDEX = 300,
            REDIS_CMD_LLEN = 301,
            REDIS_CMD_LPOP = 303,
            REDIS_CMD_LPUSH = 304,
            REDIS_CMD_LREM = 305,
            REDIS_CMD_LTRIM = 306,
            REDIS_CMD_RPOP = 307,
            REDIS_CMD_RPUSH = 308,
            REDIS_CMD_RPUSHX = 309,
            REDIS_CMD_RPOPLPUSH = 310,
            REDIS_CMD_BLPOP = 311,
            REDIS_CMD_BRPOP = 312,
            REDIS_CMD_BRPOPLPUSH = 313,
            REDIS_CMD_LINSERT = 314,
            REDIS_CMD_LPUSHX = 315,
            REDIS_CMD_LRANGE = 316,
            REDIS_CMD_LSET = 317,

            //cluster commands
            REDIS_CMD_CLUSTER = 500,  //used in cluster mode

            REDIS_CMD_EXTEND_BEGIN = 1000,
            REDIS_CMD_APPEND2 = 1001,
            REDIS_CMD_INCRBY2 = 1002,
            REDIS_CMD_INCRBYFLOAT2 = 1003,
            REDIS_CMD_INCR2 = 1004,
            REDIS_CMD_DECR2 = 1005,
            REDIS_CMD_DECRBY2 = 1006,
            REDIS_CMD_SETNX2 = 1007,
            REDIS_CMD_SET2 = 1008,
            REDIS_CMD_SETRANGE2 = 1009,
            REDIS_CMD_HSET2 = 1010,
            REDIS_CMD_HSETNX2 = 1011,
            REDIS_CMD_HMSET2 = 1012,
            REDIS_CMD_HINCR2 = 1013,
            REDIS_CMD_HINCRBYFLOAT2 = 1014,
            REDIS_CMD_SREM2 = 1019,
            REDIS_CMD_SADD2 = 1020,
            REDIS_CMD_HDEL2 = 1021,
            REDIS_CMD_MSET2 = 1022,
            REDIS_CMD_MSETNX2 = 1023,
            REDIS_CMD_PEXPIREAT2 = 1024,
            REDIS_CMD_EXPIREAT2 = 1025,
            REDIS_CMD_EXPIRE2 = 1026,
            REDIS_CMD_PEXPIRE2 = 1027,
            REDIS_CMD_SETBIT2 = 1028,
            REDIS_CMD_PFADD2 = 1029,

            REDIS_CMD_MAX = 1100,
        };

        class RedisCommandDecoder;
        class FastRedisCommandDecoder;
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
                 * Used to save temp protocol data
                 */
                Buffer m_raw_msg;
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
                friend class FastRedisCommandDecoder;
            public:
                RedisCommandFrame(const std::string& cmd = "") :
                        type(REDIS_CMD_INVALID), m_is_inline(false), m_cmd_seted(false), m_cmd(cmd)
                {
                }
                bool IsEmpty() const
                {
                    return m_cmd.empty();
                }
                RedisCommandFrame(ArgumentArray& cmd) :
                        type(REDIS_CMD_INVALID), m_is_inline(false), m_cmd_seted(false)
                {
                    m_cmd = cmd.front();
                    cmd.pop_front();
                    m_args = cmd;
                }
                void SetFullCommand(const char* fmt, ...)
                {
                    m_args.clear();
                    va_list ap;
                    va_start(ap, fmt);
                    char buf[1024];
                    vsnprintf(buf, sizeof(buf) - 1, fmt, ap);
                    va_end(ap);
                    char * pch;
                    pch = strtok(buf, " ");
                    std::string merge_str;
                    while (pch != NULL)
                    {
                        if (!merge_str.empty())
                        {
                            if (pch[strlen(pch) - 1] == '"')
                            {
                                merge_str.append(" ").append(pch, strlen(pch) - 1);
                                m_args.push_back(merge_str);
                                merge_str.clear();
                            }
                            else
                            {
                                merge_str.append(" ").append(pch);
                            }
                        }
                        else
                        {
                            if (pch[0] == '"')
                            {
                                merge_str = pch + 1;
                            }
                            else
                            {
                                m_args.push_back(std::string(pch));
                            }
                        }
                        pch = strtok(NULL, " ");
                    }
                    m_cmd = m_args.front();
                    m_args.pop_front();
                }
                inline const Buffer& GetRawProtocolData() const
                {
                    return m_raw_msg;
                }
                void ClearRawProtocolData()
                {
                	m_raw_msg.Clear();
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
                void AddArg(const std::string& arg)
                {
                    m_args.push_back(arg);
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
                std::string* GetMutableArgument(uint32 index)
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
                void ReserveArgs(size_t size)
                {
                    m_args.resize(size);
                }
                void Adapt()
                {
                    if (m_cmd.empty() && !m_args.empty())
                    {
                        m_cmd = m_args.front();
                        m_args.pop_front();
                    }
                }
                void Clear()
                {
                    m_cmd_seted = false;
                    m_cmd.clear();
                    m_args.clear();
                    m_raw_msg.Clear();
                }
                ~RedisCommandFrame()
                {

                }
        };

        typedef std::vector<RedisCommandFrame> RedisCommandFrameArray;
    }
}

#endif /* REDIS_COMMAND_HPP_ */
