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

#include "db/db.hpp"

namespace ardb
{
    bool Ardb::AdjustMergeOp(uint16& op, DataArray& args)
    {
        switch (op)
        {
            case REDIS_CMD_HSET:
            case REDIS_CMD_HSET2:
            {
                op = REDIS_CMD_HSET;
                return true;
            }
            case REDIS_CMD_HSETNX:
            case REDIS_CMD_HSETNX2:
            {
                op = REDIS_CMD_HSETNX;
                return true;
            }
            case REDIS_CMD_SET:
            case REDIS_CMD_SET2:
            {
                op = REDIS_CMD_SET;
                return true;
            }
            case REDIS_CMD_SETXX:
            {
                return true;
            }
            case REDIS_CMD_SETNX:
            case REDIS_CMD_SETNX2:
            {
                op = REDIS_CMD_SETNX;
                return true;
            }
            case REDIS_CMD_PSETEX:
            {
                return true;
            }
            case REDIS_CMD_APPEND:
            case REDIS_CMD_APPEND2:
            {
                op = REDIS_CMD_APPEND;
                return true;
            }
            case REDIS_CMD_INCR:
            case REDIS_CMD_INCR2:
            case REDIS_CMD_INCRBY:
            case REDIS_CMD_INCRBY2:
            {
                op = REDIS_CMD_INCRBY;
                return true;
            }
            case REDIS_CMD_HINCR:
            case REDIS_CMD_HINCR2:
            {
                op = REDIS_CMD_HINCR;
                return true;
            }
            case REDIS_CMD_HINCRBYFLOAT:
            case REDIS_CMD_HINCRBYFLOAT2:
            {
                op = REDIS_CMD_HINCRBYFLOAT;
                return true;
            }
            case REDIS_CMD_DECR:
            case REDIS_CMD_DECR2:
            {
                op = REDIS_CMD_INCRBY;
                args[0].SetInt64(-1);
                return true;
            }
            case REDIS_CMD_DECRBY:
            case REDIS_CMD_DECRBY2:
            {
                op = REDIS_CMD_INCRBY;
                args[0].SetInt64(0 - args[0].GetInt64());
                return true;
            }
            case REDIS_CMD_INCRBYFLOAT:
            case REDIS_CMD_INCRBYFLOAT2:
            {
                op = REDIS_CMD_INCRBYFLOAT;
                return true;
            }
            case REDIS_CMD_SETRANGE:
            case REDIS_CMD_SETRANGE2:
            {
                op = REDIS_CMD_SETRANGE;
                return true;
            }
            case REDIS_CMD_SETBIT:
            case REDIS_CMD_SETBIT2:
            {
                op = REDIS_CMD_SETBIT;
                return true;
            }
            case REDIS_CMD_PFADD:
            case REDIS_CMD_PFADD2:
            {
                op = REDIS_CMD_PFADD;
                return true;
            }
            case REDIS_CMD_PEXPIREAT:
            {
                op = REDIS_CMD_PEXPIREAT;
               return true;
            }
            default:
            {
                ERROR_LOG("Not supported merge operation:%u", op);
                return false;
            }
        }
    }
    int Ardb::MergeOperands(uint16_t left, const DataArray& left_args, uint16_t& right, DataArray& right_args)
    {
        if (left != right)
        {
            return -1;
        }
        switch (left)
        {
            case REDIS_CMD_INCRBY:
            case REDIS_CMD_HINCR:
            {
                right_args[0].SetInt64(right_args[0].GetInt64() + left_args[0].GetInt64());
                return 0;
            }
            case REDIS_CMD_INCRBYFLOAT:
            case REDIS_CMD_HINCRBYFLOAT:
            {
                right_args[0].SetFloat64(right_args[0].GetFloat64() + left_args[0].GetFloat64());
                return 0;
            }
            default:
            {
                return -1;
            }
        }
        return -1;
    }

    int Ardb::MergeOperation(const KeyObject& key, ValueObject& val, uint16_t op, DataArray& args)
    {
        Context merge_ctx;
        int err = 0;
        if (!AdjustMergeOp(op, args))
        {
            return -1;
        }
        switch (op)
        {
            case REDIS_CMD_HSET:
            case REDIS_CMD_HSETNX:
            {
                return MergeHSet(merge_ctx, key, val, op, args[0]);
            }
            case REDIS_CMD_HINCR:
            case REDIS_CMD_HINCRBYFLOAT:
            {
                return MergeHIncrby(merge_ctx, key, val, op, args[0]);
            }
            case REDIS_CMD_SET:
            case REDIS_CMD_SETXX:
            case REDIS_CMD_SETNX:
            case REDIS_CMD_PSETEX:
            {
                return MergeSet(merge_ctx, key, val, op, args[0], 0);
            }
            case REDIS_CMD_APPEND:
            {
                std::string ss;
                args[0].ToString(ss);
                return MergeAppend(merge_ctx, key, val, ss);
            }
            case REDIS_CMD_INCRBY:
            {
                return MergeIncrBy(merge_ctx, key, val, args[0].GetInt64());
            }
            case REDIS_CMD_INCRBYFLOAT:
            {
                return MergeIncrByFloat(merge_ctx, key, val, args[0].GetFloat64());
            }
            case REDIS_CMD_SETRANGE:
            {
                std::string ss;
                args[1].ToString(ss);
                return MergeSetRange(merge_ctx, key, val, args[0].GetInt64(), ss);
            }
            case REDIS_CMD_SETBIT:
            {
                return MergeSetBit(merge_ctx, key, val, args[0].GetInt64(), args[1].GetInt64(), NULL);
            }
            case REDIS_CMD_PFADD:
            {
                return MergePFAdd(merge_ctx, key, val, args, NULL);
            }
            case REDIS_CMD_PEXPIREAT:
            {
                return MergeExpire(merge_ctx, key, val, args[0].GetInt64());
            }
            default:
            {
                ERROR_LOG("Not supported merge operation:%u", op);
                return -1;
            }
        }
        return 0;
    }
}

