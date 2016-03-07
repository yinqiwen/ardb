/*
 * engine.cpp
 *
 *  Created on: 2016Äê2ÔÂ16ÈÕ
 *      Author: wangqiying
 */
#include "engine.hpp"
#include <assert.h>

OP_NAMESPACE_BEGIN

    int compare_keys(const char* k1, size_t k1_len, const char* k2, size_t k2_len, bool has_ns)
    {
        Buffer kbuf1(const_cast<char*>(k1), 0, k1_len);
        Buffer kbuf2(const_cast<char*>(k2), 0, k2_len);

        KeyObject key1, key2;
        int ret = 0;
        if (has_ns)
        {
            /*
             * 1. compare namespace
             */
            assert(key1.DecodeNS(kbuf1, false));
            assert(key2.DecodeNS(kbuf2, false));
            ret = key1.GetNameSpace().Compare(key2.GetNameSpace(), false);
            if (ret != 0)
            {
                return ret;
            }
        }

        /*
         * 2. compare type
         */
        assert(key1.DecodeType(kbuf1));
        assert(key2.DecodeType(kbuf2));
        ret = key1.GetType() - key2.GetType();
        if (ret != 0)
        {
            return ret;
        }

        /*
         * 3. compare key
         */
        assert(key1.DecodeKey(kbuf1,false));
        assert(key2.DecodeKey(kbuf2, false));
        ret = key1.GetKey().Compare(key2.GetKey(), false);
        if (ret != 0)
        {
            return ret;
        }

        /*
         * 4. only meta/ttl_value key has no element in key part
         */
        uint8_t type = key1.GetType();
        if (type != KEY_META)
        {
            int elen1 = key1.DecodeElementLength(kbuf1);
            int elen2 = key2.DecodeElementLength(kbuf2);
            ret = elen1 - elen2;
            if (ret != 0)
            {
                return ret;
            }
            for (int i = 0; i < elen1; i++)
            {
                assert(key1.DecodeElement(kbuf1, false, i));
                assert(key2.DecodeElement(kbuf2, false, i));
                ret = key1.GetElement(i).Compare(key2.GetElement(i), false);
                if (ret != 0)
                {
                    return ret;
                }
            }
        }
        return ret;
    }

OP_NAMESPACE_END

