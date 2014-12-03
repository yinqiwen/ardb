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
#include "engine/engine.hpp"
#include "codec.hpp"
#include "ardb.hpp"
#include "buffer/buffer_helper.hpp"

namespace ardb
{
    int CommonComparator::Compare(const char* akbuf, size_t aksiz, const char* bkbuf, size_t bksiz)
    {
        if(aksiz < 4 || bksiz < 4)
        {
            return aksiz - bksiz;
        }
        uint32 aheader, bheader;
//        memcpy(&aheader, akbuf, 4);
//        memcpy(&bheader, bkbuf, 4);
        aheader = *(uint32*) akbuf;
        bheader = *(uint32*) bkbuf;
        if (aheader != bheader)
        {
            return aheader - bheader;
        }
        uint8 type = (uint8) (aheader & 0xFF);
        Buffer abuf(const_cast<char*>(akbuf + sizeof(aheader)), 0, aksiz - sizeof(aheader));
        Buffer bbuf(const_cast<char*>(bkbuf + sizeof(aheader)), 0, bksiz - sizeof(aheader));
        Slice akey, bkey;
        BufferHelper::ReadVarSlice(abuf, akey);
        BufferHelper::ReadVarSlice(bbuf, bkey);
        int cmp = 0;
        if (type != KEY_EXPIRATION_ELEMENT)
        {
            cmp = akey.compare(bkey);
            if (0 != cmp)
            {
                return cmp;
            }
        }
        switch (type)
        {
            case KEY_META:
            case SCRIPT:
            {
                return 0;
            }
            case SET_ELEMENT:
            case ZSET_ELEMENT_VALUE:
            case HASH_FIELD:
            case LIST_ELEMENT:
            case BITSET_ELEMENT:
            {
                Data a, b;
                a.Decode(abuf);
                b.Decode(bbuf);
                return a.Compare(b);
            }
            case ZSET_ELEMENT_SCORE:
            {
                Data score1, score2;
                score1.Decode(abuf);
                score2.Decode(bbuf);
                cmp = score1.Compare(score2);
                if (cmp != 0)
                {
                    return cmp;
                }
                Data element1, element2;
                element1.Decode(abuf);
                element2.Decode(bbuf);
                return element1.Compare(element2);
            }
            case KEY_EXPIRATION_ELEMENT:
            {
                Data score1, score2;
                score1.Decode(abuf);
                score2.Decode(bbuf);
                cmp = score1.Compare(score2);
                if (cmp != 0)
                {
                    return cmp;
                }
                return akey.compare(bkey);
            }
            default:
            {
                abort();
                return -1;
            }
        }
    }
}

