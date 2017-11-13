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

    //copy from redis
    static long popcount(const void *s, long count)
    {
        long bits = 0;
        unsigned char *p;
        const uint32_t* p4 = (const uint32_t*) s;
        static const unsigned char bitsinbyte[256] =
        { 0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3,
                3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3,
                3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3,
                3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3,
                3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5,
                5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8 };

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
            bits += ((((aux1 + (aux1 >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24) + ((((aux2 + (aux2 >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24)
                    + ((((aux3 + (aux3 >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24) + ((((aux4 + (aux4 >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24);
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

    static bool get_bit_offset(const std::string& str, int64& offset)
    {
        if (!string_toint64(str, offset) || offset < 0 || ((unsigned long long) offset >> 3) >= (512 * 1024 * 1024))
        {
            return false;
        }
        return true;
    }

    /* Return the position of the first bit set to one (if 'bit' is 1) or
     * zero (if 'bit' is 0) in the bitmap starting at 's' and long 'count' bytes.
     *
     * The function is guaranteed to return a value >= 0 if 'bit' is 0 since if
     * no zero bit is found, it returns count*8 assuming the string is zero
     * padded on the right. However if 'bit' is 1 it is possible that there is
     * not a single set bit in the bitmap. In this special case -1 is returned. */
    static long bitpos(void *s, unsigned long count, int bit)
    {
        unsigned long *l;
        unsigned char *c;
        unsigned long skipval, word = 0, one;
        long pos = 0; /* Position of bit, to return to the caller. */
        unsigned long j;

        /* Process whole words first, seeking for first word that is not
         * all ones or all zeros respectively if we are lookig for zeros
         * or ones. This is much faster with large strings having contiguous
         * blocks of 1 or 0 bits compared to the vanilla bit per bit processing.
         *
         * Note that if we start from an address that is not aligned
         * to sizeof(unsigned long) we consume it byte by byte until it is
         * aligned. */

        /* Skip initial bits not aligned to sizeof(unsigned long) byte by byte. */
        skipval = bit ? 0 : UCHAR_MAX;
        c = (unsigned char*) s;
        while ((unsigned long) c & (sizeof(*l) - 1) && count)
        {
            if (*c != skipval)
                break;
            c++;
            count--;
            pos += 8;
        }

        /* Skip bits with full word step. */
        skipval = bit ? 0 : ULONG_MAX;
        l = (unsigned long*) c;
        while (count >= sizeof(*l))
        {
            if (*l != skipval)
                break;
            l++;
            count -= sizeof(*l);
            pos += sizeof(*l) * 8;
        }

        /* Load bytes into "word" considering the first byte as the most significant
         * (we basically consider it as written in big endian, since we consider the
         * string as a set of bits from left to right, with the first bit at position
         * zero.
         *
         * Note that the loading is designed to work even when the bytes left
         * (count) are less than a full word. We pad it with zero on the right. */
        c = (unsigned char*) l;
        for (j = 0; j < sizeof(*l); j++)
        {
            word <<= 8;
            if (count)
            {
                word |= *c;
                c++;
                count--;
            }
        }

        /* Special case:
         * If bits in the string are all zero and we are looking for one,
         * return -1 to signal that there is not a single "1" in the whole
         * string. This can't happen when we are looking for "0" as we assume
         * that the right of the string is zero padded. */
        if (bit == 1 && word == 0)
            return -1;

        /* Last word left, scan bit by bit. The first thing we need is to
         * have a single "1" set in the most significant position in an
         * unsigned long. We don't know the size of the long so we use a
         * simple trick. */
        one = ULONG_MAX; /* All bits set to 1.*/
        one >>= 1; /* All bits set to 1 but the MSB. */
        one = ~one; /* All bits set to 0 but the MSB. */

        while (one)
        {
            if (((one & word) != 0) == bit)
                return pos;
            pos++;
            one >>= 1;
        }

        /* If we reached this point, there is a bug in the algorithm, since
         * the case of no match is handled as a special case before. */
        abort();
        return 0; /* Just to avoid warnings. */
    }

    int Ardb::MergeSetBit(Context& ctx, const KeyObject& key, ValueObject& meta, int64 offset, uint8 on, uint8* oldbit)
    {
        if (meta.GetType() > 0 && meta.GetType() != KEY_STRING)
        {
            return ERR_WRONG_TYPE;
        }
        meta.SetType(KEY_STRING);
        int byte = offset >> 3;
        Data& data = meta.GetStringValue();
        data.ToMutableStr();
        data.ReserveStringSpace(byte + 1);
        char* bytes = data.ToMutableStr();
        int byteval = bytes[byte];
        int bit = 7 - (offset & 0x7);
        int bitval = byteval & (1 << bit);
        if (NULL != oldbit)
        {
            *oldbit = bitval ? 1 : 0;
        }

        /* Update byte with new bit value and return original value */
        byteval &= ~(1 << bit);
        byteval |= ((on & 0x1) << bit);
        bytes[byte] = byteval;
        return 0;
    }

    int Ardb::SetBit(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        ctx.flags.create_if_notexist = 1;
        int64 offset;
        if (!get_bit_offset(cmd.GetArguments()[1], offset))
        {
            reply.SetErrCode(ERR_BIT_OFFSET_OUTRANGE);
            return 0;
        }
        if (cmd.GetArguments()[2] != "1" && cmd.GetArguments()[2] != "0")
        {
            reply.SetErrCode(ERR_BIT_OUTRANGE);
            return 0;
        }
        KeyObject key(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        uint8 bit = cmd.GetArguments()[2] != "0" ? 1 : 0;
        int err = 0;
        /*
         * merge setbit
         */
        if (!ctx.flags.redis_compatible && m_engine->GetFeatureSet().support_merge)
        {
            DataArray args(2);
            args[0].SetInt64(offset);
            args[1].SetInt64(bit);
            err = MergeKeyValue(ctx, key, cmd.GetType(), args);
            if (err < 0)
            {
                reply.SetErrCode(err);
            }
            else
            {
                reply.SetStatusCode(STATUS_OK);
            }
            return 0;
        }
        ValueObject v;
        if (!CheckMeta(ctx, key, KEY_STRING, v))
        {
            return 0;
        }
        uint8 oldbit = 0;
        err = MergeSetBit(ctx, key, v, offset, bit, &oldbit);
        if (0 == err)
        {
            err = SetKeyValue(ctx, key, v);
        }
        if (err < 0)
        {
            reply.SetErrCode(err);
        }
        else
        {

            reply.SetInteger(oldbit);
        }
        return 0;
    }

    int Ardb::GetBit(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        int64 bitoffset;
        if (!get_bit_offset(cmd.GetArguments()[1], bitoffset))
        {
            reply.SetErrCode(ERR_BIT_OFFSET_OUTRANGE);
            return 0;
        }
        size_t byte = bitoffset >> 3;
        size_t bit = 7 - (bitoffset & 0x7);
        reply.SetInteger(0); //default response
        ValueObject v;
        if (!CheckMeta(ctx, cmd.GetArguments()[0], KEY_STRING, v))
        {
            return 0;
        }
        if (v.GetType() == 0) //empty
        {
            return 0;
        }
        Data& str = v.GetStringValue();
        if (str.IsString())
        {
            if (str.StringLength() < byte)
            {
                return 0;
            }
            int bitval = (str.CStr())[byte] & (1 << bit);
            reply.SetInteger(bitval ? 1 : 0);
        }
        else
        {
            std::string ss;
            str.ToString(ss);
            if (ss.size() < byte)
            {
                return 0;
            }
            int bitval = ss[byte] & (1 << bit);
            reply.SetInteger(bitval ? 1 : 0);
        }
        return 0;
    }

    int Ardb::Bitcount(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 start, end, strlen;
        const unsigned char *p = NULL;
        std::string strbuf;
        RedisReply& reply = ctx.GetReply();
        reply.SetInteger(0); //default response
        ValueObject v;
        if (!CheckMeta(ctx, cmd.GetArguments()[0], KEY_STRING, v) || v.GetType() == 0)
        {
            return 0;
        }

        Data& str = v.GetStringValue();

        /* Set the 'p' pointer to the string, that can be just a stack allocated
         * array if our string was integer encoded. */
        if (!str.IsString())
        {
            str.ToString(strbuf);
            p = (const unsigned char*) (&strbuf[0]);
            strlen = strbuf.size();
        }
        else
        {
            p = (const unsigned char*) str.CStr();
            strlen = str.StringLength();
        }
        /* Parse start/end range if any. */
        if (cmd.GetArguments().size() == 3)
        {
            if (!GetLongFromProtocol(ctx, cmd.GetArguments()[1], start) || !GetLongFromProtocol(ctx, cmd.GetArguments()[2], end))
            {
                return 0;
            }
            /* Convert negative indexes */
            if (start < 0)
                start = strlen + start;
            if (end < 0)
                end = strlen + end;
            if (start < 0)
                start = 0;
            if (end < 0)
                end = 0;
            if (end >= strlen)
                end = strlen - 1;
        }
        else if (cmd.GetArguments().size() == 1)
        {
            /* The whole string. */
            start = 0;
            end = strlen - 1;
        }
        else
        {
            /* Syntax error. */
            reply.SetErrCode(ERR_INVALID_SYNTAX);
            return 0;
        }
        /* Precondition: end >= 0 && end < strlen, so the only condition where
         * zero can be returned is: start > end. */
        if (start <= end)
        {
            long bytes = end - start + 1;
            reply.SetInteger(popcount(p + start, bytes));
        }
        return 0;
    }

    int Ardb::Bitpos(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        int64 bit, start, end, strlen;
        const unsigned char *p = NULL;
        int end_given = 0;

        /* Parse the bit argument to understand what we are looking for, set
         * or clear bits. */
        if (!GetLongFromProtocol(ctx, cmd.GetArguments()[1], bit))
            return 0;
        if (bit != 0 && bit != 1)
        {
            reply.SetErrorReason("The bit argument must be 1 or 0.");
            return 0;
        }

        ValueObject v;
        if (!CheckMeta(ctx, cmd.GetArguments()[0], KEY_STRING, v) || v.GetType() == 0)
        {
            return 0;
        }
        if (v.GetType() == 0)
        {
            reply.SetInteger(bit ? -1 : 0);
            return 0;
        }
        std::string strbuf;
        Data& str = v.GetStringValue();
        /* Set the 'p' pointer to the string, that can be just a stack allocated
         * array if our string was integer encoded. */
        if (!str.IsString())
        {
            str.ToString(strbuf);
            p = (const unsigned char *) &strbuf[0];
            strlen = strbuf.size();
        }
        else
        {
            p = (const unsigned char *) str.CStr();
            strlen = str.StringLength();
        }

        /* Parse start/end range if any. */
        if (cmd.GetArguments().size() == 3 || cmd.GetArguments().size() == 4)
        {
            if (!GetLongFromProtocol(ctx, cmd.GetArguments()[2], start))
                return 0;
            if (cmd.GetArguments().size() == 4)
            {
                if (!GetLongFromProtocol(ctx, cmd.GetArguments()[3], end))
                    return 0;
                end_given = 1;
            }
            else
            {
                end = strlen - 1;
            }
            /* Convert negative indexes */
            if (start < 0)
                start = strlen + start;
            if (end < 0)
                end = strlen + end;
            if (start < 0)
                start = 0;
            if (end < 0)
                end = 0;
            if (end >= strlen)
                end = strlen - 1;
        }
        else if (cmd.GetArguments().size() == 2)
        {
            /* The whole string. */
            start = 0;
            end = strlen - 1;
        }
        else
        {
            /* Syntax error. */
            reply.SetErrCode(ERR_INVALID_SYNTAX);
            return 0;
        }

        /* For empty ranges (start > end) we return -1 as an empty range does
         * not contain a 0 nor a 1. */
        if (start > end)
        {
            reply.SetInteger(-1);
        }
        else
        {
            long bytes = end - start + 1;
            long pos = bitpos((void*) (p + start), bytes, bit);

            /* If we are looking for clear bits, and the user specified an exact
             * range with start-end, we can't consider the right of the range as
             * zero padded (as we do when no explicit end is given).
             *
             * So if redisBitpos() returns the first bit outside the range,
             * we return -1 to the caller, to mean, in the specified range there
             * is not a single "0" bit. */
            if (end_given && bit == 0 && pos == bytes * 8)
            {
                reply.SetInteger(-1);
                return 0;
            }
            if (pos != -1)
                pos += start * 8; /* Adjust for the bytes we skipped. */
            reply.SetInteger(pos);
        }
        return 0;
    }

    int Ardb::BitopCount(Context& ctx, RedisCommandFrame& cmd)
    {
        return Bitop(ctx, cmd);
    }

    int Ardb::Bitop(Context& ctx, RedisCommandFrame& cmd)
    {

        RedisReply& reply = ctx.GetReply();
        const std::string& opname = cmd.GetArguments()[0];
        std::string targetkey;
        int destkey_count = 0;
        if (cmd.GetType() == REDIS_CMD_BITOP)
        {
            targetkey = cmd.GetArguments()[1];
            destkey_count = 1;
        }
        unsigned long op;
        unsigned long maxlen = 0; /* Array of length of src strings,
         and max len. */
        unsigned long minlen = 0; /* Min len among the input keys. */
        std::string res; /* Resulting string. */

        /* Parse the operation name. */
        if ((opname[0] == 'a' || opname[0] == 'A') && !strcasecmp(opname.c_str(), "and"))
            op = BITOP_AND;
        else if ((opname[0] == 'o' || opname[0] == 'O') && !strcasecmp(opname.c_str(), "or"))
            op = BITOP_OR;
        else if ((opname[0] == 'x' || opname[0] == 'X') && !strcasecmp(opname.c_str(), "xor"))
            op = BITOP_XOR;
        else if ((opname[0] == 'n' || opname[0] == 'N') && !strcasecmp(opname.c_str(), "not"))
            op = BITOP_NOT;
        else
        {
            reply.SetErrCode(ERR_INVALID_SYNTAX);
            return 0;
        }

        /* Sanity check: NOT accepts only a single key argument. */
        if (op == BITOP_NOT && cmd.GetArguments().size() != (2 + destkey_count))
        {
            reply.SetErrorReason("BITOP NOT must be called with a single source key.");
            return 0;
        }

        /* Lookup keys, and store pointers to the string objects into an array. */
        KeyObjectArray keys;
        ValueObjectArray vals;
        ErrCodeArray errs;

        for (size_t i = 1; i < cmd.GetArguments().size(); i++)
        {
            KeyObject k(ctx.ns, KEY_META, cmd.GetArguments()[i]);
            keys.push_back(k);
        }
        m_engine->MultiGet(ctx, keys, vals, errs);
        if (cmd.GetType() == REDIS_CMD_BITOP)
        {
            if (vals[0].GetType() != 0 && vals[0].GetType() != KEY_STRING)
            {
                reply.SetErrCode(ERR_WRONG_TYPE);
                return 0;
            }
        }
        //printf("####%s %s\n", vals[0].GetStringValue().AsString().c_str(),vals[1].GetStringValue().AsString().c_str());

        size_t numkeys = keys.size() - destkey_count;
        for (size_t j = destkey_count; j < keys.size(); j++)
        {
            /* Handle non-existing keys as empty strings. */
            if (vals[j].GetType() == 0)
            {
                minlen = 0;
                continue;
            }
            /* Return an error if one of the keys is not a string. */
            if (vals[j].GetType() != KEY_STRING)
            {
                reply.SetErrCode(ERR_WRONG_TYPE);
                return 0;
            }
            vals[j].GetStringValue().ToMutableStr();
            size_t slen = vals[j].GetStringValue().StringLength();
            if (slen > maxlen)
                maxlen = slen;
            if (j == 0 || slen < minlen)
                minlen = slen;
        }

        /* Compute the bit operation, if at least one string is not empty. */
        if (maxlen)
        {
            res.resize(maxlen);
            unsigned char output, byte;
            unsigned long i;

            /* Fast path: as far as we have data for all the input bitmaps we
             * can take a fast path that performs much better than the
             * vanilla algorithm. */
            int j = 0;
            if (minlen >= sizeof(unsigned long) * 4 && numkeys <= 16)
            {
                unsigned long *lp[16];
                unsigned long *lres = (unsigned long*) (&res[0]);

                /* Note: sds pointer is always aligned to 8 byte boundary. */
                for (size_t k = 0; k < numkeys; i++)
                {
                    lp[k] = (unsigned long*) (vals[k + destkey_count].GetStringValue().CStr());
                }
                memcpy(&res[0], vals[destkey_count].GetStringValue().CStr(), minlen);

                /* Different branches per different operations for speed (sorry). */
                if (op == BITOP_AND)
                {
                    while (minlen >= sizeof(unsigned long) * 4)
                    {
                        for (i = 1; i < numkeys; i++)
                        {
                            lres[0] &= lp[i][0];
                            lres[1] &= lp[i][1];
                            lres[2] &= lp[i][2];
                            lres[3] &= lp[i][3];
                            lp[i] += 4;
                        }
                        lres += 4;
                        j += sizeof(unsigned long) * 4;
                        minlen -= sizeof(unsigned long) * 4;
                    }
                }
                else if (op == BITOP_OR)
                {
                    while (minlen >= sizeof(unsigned long) * 4)
                    {
                        for (i = 1; i < numkeys; i++)
                        {
                            lres[0] |= lp[i][0];
                            lres[1] |= lp[i][1];
                            lres[2] |= lp[i][2];
                            lres[3] |= lp[i][3];
                            lp[i] += 4;
                        }
                        lres += 4;
                        j += sizeof(unsigned long) * 4;
                        minlen -= sizeof(unsigned long) * 4;
                    }
                }
                else if (op == BITOP_XOR)
                {
                    while (minlen >= sizeof(unsigned long) * 4)
                    {
                        for (i = 1; i < numkeys; i++)
                        {
                            lres[0] ^= lp[i][0];
                            lres[1] ^= lp[i][1];
                            lres[2] ^= lp[i][2];
                            lres[3] ^= lp[i][3];
                            lp[i] += 4;
                        }
                        lres += 4;
                        j += sizeof(unsigned long) * 4;
                        minlen -= sizeof(unsigned long) * 4;
                    }
                }
                else if (op == BITOP_NOT)
                {
                    while (minlen >= sizeof(unsigned long) * 4)
                    {
                        lres[0] = ~lres[0];
                        lres[1] = ~lres[1];
                        lres[2] = ~lres[2];
                        lres[3] = ~lres[3];
                        lres += 4;
                        j += sizeof(unsigned long) * 4;
                        minlen -= sizeof(unsigned long) * 4;
                    }
                }
            }

            /* j is set to the next byte to process by the previous loop. */
            for (; j < maxlen; j++)
            {
                Data& first = vals[destkey_count].GetStringValue();
                output = (first.StringLength() <= j) ? 0 : first.CStr()[j];
                if (op == BITOP_NOT)
                    output = ~output;
                for (i = 1; i < numkeys; i++)
                {
                    byte = (vals[i + destkey_count].GetStringValue().StringLength() <= j) ? 0 : vals[destkey_count + 1].GetStringValue().CStr()[j];
                    switch (op)
                    {
                        case BITOP_AND:
                            output &= byte;
                            break;
                        case BITOP_OR:
                            output |= byte;
                            break;
                        case BITOP_XOR:
                            output ^= byte;
                            break;
                    }
                }
                res[j] = output;
            }
        }

        /* Store the computed value into the target key */
        int err = 0;
        if (maxlen)
        {
            if (cmd.GetType() == REDIS_CMD_BITOP)
            {
                vals[0].SetType(KEY_STRING);
                vals[0].GetStringValue().SetString(res, false);
                err = SetKeyValue(ctx, keys[0], vals[0]);
            }
            else
            {
                maxlen = popcount(res.c_str(), res.size());
            }
        }
        else
        {
            if (vals[0].GetType() > 0)
            {
                err = RemoveKey(ctx, keys[0]);
            }
        }
        if (0 != err)
        {
            reply.SetErrCode(err);
        }
        else
        {
            reply.SetInteger(maxlen);
        }
        return 0;
    }

OP_NAMESPACE_END

