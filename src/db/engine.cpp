/*
 *Copyright (c) 2013-2016, yinqiwen <yinqiwen@gmail.com>
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
#include "engine.hpp"
#include <assert.h>

OP_NAMESPACE_BEGIN

    int compare_keyslices(const Slice& k1, const Slice& k2, bool has_ns)
    {
        return compare_keys(k1.data(), k1.size(), k2.data(), k2.size(), has_ns);
    }

    int compare_keys(const char* k1, size_t k1_len, const char* k2, size_t k2_len, bool has_ns)
    {
        int ret = (NULL != k2) - (NULL != k1);
        if (0 != ret)
        {
            return ret;
        }

        Buffer kbuf1(const_cast<char*>(k1), 0, k1_len);
        Buffer kbuf2(const_cast<char*>(k2), 0, k2_len);

        KeyObject key1, key2;

        if (has_ns)
        {
            /*
             * 1. compare namespace
             */
            if (!key1.DecodeNS(kbuf1, false))
            {
                FATAL_LOG("Decode first key namespace failed in comparator.");
            }
            if (!key2.DecodeNS(kbuf2, false))
            {
                FATAL_LOG("Decode second key namespace failed in comparator.");
            }
            ret = key1.GetNameSpace().Compare(key2.GetNameSpace(), false);
            if (ret != 0)
            {
                return ret;
            }
        }

        /*
         * 2. decode  prefix
         */
        if (!key1.DecodeKey(kbuf1, false))
        {
            FATAL_LOG("Decode first key prefix failed in comparator. ");
        }
        if (!key2.DecodeKey(kbuf2, false))
        {
            FATAL_LOG("Decode second key prefix failed in comparator. ");
        }

        /*
         * 3. compare key & type
         */
        ret = key1.GetKey().Compare(key2.GetKey(), true);
        if (ret != 0)
        {
            return ret;
        }
        ret = (int) key1.DecodeType(kbuf1) - (int) key2.DecodeType(kbuf2);
        if (ret != 0)
        {
            return ret;
        }
        ret = key1.GetType() - key2.GetType();
        if (ret != 0)
        {
            return ret;
        }
//        if(key1.GetType() == KEY_ANY || key2.GetType() == KEY_ANY)
//        {
//        	return 0;
//        }
        /*
         * 4. only meta key has no element in key part
         */
        uint8_t type = key1.GetType();
        if (type != KEY_META)
        {
            int elen1 = key1.DecodeElementLength(kbuf1);
            int elen2 = key2.DecodeElementLength(kbuf2);
            if (elen1 < 0 || elen2 < 0)
            {
                FATAL_LOG("Invalid element length");
            }
            ret = elen1 - elen2;
            if (ret != 0)
            {
                return ret;
            }
            for (int i = 0; i < elen1; i++)
            {
                if (!key1.DecodeElement(kbuf1, false, i))
                {
                    FATAL_LOG("Decode first key element:%u failed.", i);
                }
                if (!key2.DecodeElement(kbuf2, false, i))
                {
                    FATAL_LOG("Decode second key element:%u failed.", i);
                }
                ret = key1.GetElement(i).Compare(key2.GetElement(i), false);
                if (ret != 0)
                {
                    return ret;
                }
            }
        }
        return ret;
    }

    int Engine::FlushAll(Context& ctx)
    {
        DataArray nss;
        ListNameSpaces(ctx, nss);
        for (size_t i = 0; i < nss.size(); i++)
        {
            Flush(ctx, nss[i]);
        }
        return 0;
    }

    int Engine::CompactAll(Context& ctx)
    {
        DataArray nss;
        ListNameSpaces(ctx, nss);
        for (size_t i = 0; i < nss.size(); i++)
        {
            KeyObject start, end;
            start.SetNameSpace(nss[i]);
            Compact(ctx, start, end);
        }
        return 0;
    }

OP_NAMESPACE_END

