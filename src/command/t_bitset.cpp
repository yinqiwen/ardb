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

OP_NAMESPACE_BEGIN
    static const unsigned long BITOP_AND = 0;
    static const unsigned long BITOP_OR = 1;
    static const unsigned long BITOP_XOR = 2;
    static const unsigned long BITOP_NOT = 3;

    static const uint32 BIT_SUBSET_SIZE = 4096;
    static const uint32 BIT_SUBSET_BYTES_SIZE = BIT_SUBSET_SIZE >> 3;
    //copy from redis
    static long popcount(const void *s, long count)
    {
        long bits = 0;
        unsigned char *p;
        const uint32_t* p4 = (const uint32_t*) s;
        static const unsigned char bitsinbyte[256] =
            { 0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2,
                    3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3,
                    2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3,
                    4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3,
                    3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4,
                    5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5,
                    3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4,
                    5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8 };

        /* Count bits 16 bytes at a time */
        while (count >= 16)
        {
            uint32_t aux1, aux2, aux3, aux4;

            aux1 = *p4++;
            aux2 = *p4++;
            aux3 = *p4++;
            aux4 = *p4++;
            count -= 16;

            aux1 = aux1 - ((aux1 >> 1) & 0x55555555);
            aux1 = (aux1 & 0x33333333) + ((aux1 >> 2) & 0x33333333);
            aux2 = aux2 - ((aux2 >> 1) & 0x55555555);
            aux2 = (aux2 & 0x33333333) + ((aux2 >> 2) & 0x33333333);
            aux3 = aux3 - ((aux3 >> 1) & 0x55555555);
            aux3 = (aux3 & 0x33333333) + ((aux3 >> 2) & 0x33333333);
            aux4 = aux4 - ((aux4 >> 1) & 0x55555555);
            aux4 = (aux4 & 0x33333333) + ((aux4 >> 2) & 0x33333333);
            bits += ((((aux1 + (aux1 >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24)
                    + ((((aux2 + (aux2 >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24)
                    + ((((aux3 + (aux3 >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24)
                    + ((((aux4 + (aux4 >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24);
        }
        /* Count the remaining bytes */
        p = (unsigned char*) p4;
        while (count--)
            bits += bitsinbyte[*p++];
        return bits;
    }

    static long popcount_bitval(const std::string& val, int32 offset, int32 limit)
    {
        if (limit < 0)
        {
            limit = val.size() - 1;
        }
        if (val.size() > limit)
        {
            std::string tmp = val.substr(offset, limit - offset + 1);
            return popcount(tmp.data(), tmp.size());
        }
        else if (val.size() <= limit && val.size() <= offset)
        {
            return 0;
        }
        else if (val.size() > offset)
        {
            std::string tmp = val.substr(offset);
            return popcount(tmp.data(), tmp.size());
        }
        return 0;
    }





    int Ardb::SetBit(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }


    int Ardb::GetBit(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    int Ardb::BitClear(Context& ctx, ValueObject& meta)
    {

        return 0;
    }





    int Ardb::BitopCount(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    int Ardb::Bitop(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    int Ardb::Bitcount(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }


OP_NAMESPACE_END

