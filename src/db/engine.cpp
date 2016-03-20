/*
 * engine.cpp
 *
 *  Created on: 2016��2��16��
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
            if(!key1.DecodeNS(kbuf1, false))
            {
                abort();
            }
            if(!key2.DecodeNS(kbuf2, false))
            {
                abort();
            }
            ret = key1.GetNameSpace().Compare(key2.GetNameSpace(), false);
            if (ret != 0)
            {
                return ret;
            }
        }

        /*
         * 2. compare type
         */
        if(!key1.DecodeType(kbuf1))
        {
            abort();
        }
        if(!key2.DecodeType(kbuf2))
        {
            abort();
        }
        //printf("####%d  %d\n", key1.GetType(), key2.GetType());
        if(key1.GetType() != KEY_ANY && key2.GetType() != KEY_ANY)
        {
            ret = key1.GetType() - key2.GetType();
            if (ret != 0)
            {
                return ret;
            }
        }

        /*
         * 3. compare key
         */
        if(!key1.DecodeKey(kbuf1,false))
        {
            abort();
        }
        if(!key2.DecodeKey(kbuf2, false))
        {
            abort();
        }
        ret = key1.GetKey().Compare(key2.GetKey(), false);
        if (ret != 0)
        {
            return ret;
        }
        if(key1.GetType() == KEY_ANY || key2.GetType() == KEY_ANY)
        {
        	return 0;
        }
        /*
         * 4. only meta key has no element in key part
         */
        uint8_t type = key1.GetType();
        if (type != KEY_META)
        {
            int elen1 = key1.DecodeElementLength(kbuf1);
            int elen2 = key2.DecodeElementLength(kbuf2);
            if(elen1 <= 0 || elen2 <= 0)
            {
            	 abort();
            }
            if(key1.GetElement(0).IsAny() || key2.GetElement(0).IsAny())
            {
            	return 0;
            }
            ret = elen1 - elen2;
            if (ret != 0)
            {
                return ret;
            }
            for (int i = 0; i < elen1; i++)
            {
                if(!key1.DecodeElement(kbuf1, false, i))
                {
                    abort();
                }
                if(!key2.DecodeElement(kbuf2, false, i))
                {
                    abort();
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

OP_NAMESPACE_END

