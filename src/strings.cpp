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

#include "ardb.hpp"

namespace ardb
{
    int Ardb::Append(const DBID& db, const Slice& key, const Slice& value)
    {
        std::string prev_value;
        int ret = Get(db, key, prev_value);
        if (ret == 0 || ret == ERR_NOT_EXIST)
        {
            prev_value.append(value.data(), value.size());
            Set(db, key, prev_value);
            return prev_value.size();
        }
        return ret;
    }

    int Ardb::Incr(const DBID& db, const Slice& key, int64_t& value)
    {
        return Incrby(db, key, 1, value);
    }
    int Ardb::Incrby(const DBID& db, const Slice& key, int64_t increment, int64_t& value)
    {
        CommonMetaValue* meta = GetMeta(db, key, false);
        StringMetaValue* smeta = NULL;
        if (NULL != meta)
        {
            if (meta->header.type != STRING_META)
            {
                DELETE(meta);
                return ERR_INVALID_TYPE;
            }
            smeta = (StringMetaValue*) meta;
            smeta->value.ToNumber();
            if (smeta->value.type == BYTES_VALUE)
            {
                DELETE(smeta);
                return ERR_INVALID_TYPE;
            }
            smeta->value.Incrby(increment);
            value = smeta->value.integer_value;
            SetMeta(db, key, *smeta);
            DELETE(smeta);
        }
        else
        {
            StringMetaValue nsmeta;
            nsmeta.value.SetValue(increment);
            KeyObject k(key, KEY_META, db);
            SetMeta(k, nsmeta);
            value = increment;
        }
        return 0;
    }

    int Ardb::Decr(const DBID& db, const Slice& key, int64_t& value)
    {
        return Decrby(db, key, 1, value);
    }

    int Ardb::Decrby(const DBID& db, const Slice& key, int64_t decrement, int64_t& value)
    {
        return Incrby(db, key, 0 - decrement, value);
    }

    int Ardb::IncrbyFloat(const DBID& db, const Slice& key, double increment, double& value)
    {
        CommonMetaValue* meta = GetMeta(db, key, false);
        StringMetaValue* smeta = NULL;

        if (NULL != meta)
        {
            if (meta->header.type != STRING_META)
            {
                DELETE(meta);
                return ERR_INVALID_TYPE;
            }
            smeta = (StringMetaValue*) meta;
            smeta->value.ToNumber();
            if (smeta->value.type == BYTES_VALUE)
            {
                DELETE(smeta);
                return ERR_INVALID_TYPE;
            }
            smeta->value.IncrbyFloat(increment);
            value = smeta->value.double_value;
            DELETE(smeta);
        }
        else
        {
            StringMetaValue nsmeta;
            nsmeta.value.SetValue(increment);
            KeyObject k(key, KEY_META, db);
            SetMeta(k, nsmeta);
            value = increment;
        }
        return 0;
    }

    int Ardb::GetRange(const DBID& db, const Slice& key, int start, int end, std::string& v)
    {
        std::string value;
        int ret = Get(db, key, value);
        if (ret < 0)
        {
            return ret;
        }
        start = RealPosition(value, start);
        end = RealPosition(value, end);
        if (start > end)
        {
            return ERR_OUTOFRANGE;
        }
        v = value.substr(start, (end - start + 1));
        return ARDB_OK;
    }

    int Ardb::SetRange(const DBID& db, const Slice& key, int start, const Slice& v)
    {
        std::string value;
        int ret = Get(db, key, value);
        if (ret < 0)
        {
            return ret;
        }
        start = RealPosition(value, start);
        value.resize(start);
        value.append(v.data(), v.size());
        return Set(db, key, value) == 0 ? value.size() : 0;
    }

    int Ardb::GetSet(const DBID& db, const Slice& key, const Slice& value, std::string& v)
    {
        if (Get(db, key, v) < 0)
        {
            Set(db, key, value);
            return ERR_NOT_EXIST;
        }
        return Set(db, key, value);
    }

}

