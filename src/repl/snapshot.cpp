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

#include "snapshot.hpp"
#include "util/helpers.hpp"

extern "C"
{
#include "redis/lzf.h"
#include "redis/ziplist.h"
#include "redis/zipmap.h"
#include "redis/intset.h"
#include "redis/crc64.h"
#include "redis/endianconv.h"
}
#include <float.h>
#include <cmath>
#include <snappy.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "db/db.hpp"
#include "repl.hpp"
#include "thread/lock_guard.hpp"

#define RETURN_NEGATIVE_EXPR(x)  do\
    {                    \
	    int ret = (x);   \
        if(ret < 0) {   \
           return ret; \
        }             \
    }while(0)

#define REDIS_RDB_VERSION 8

#define ARDB_RDB_VERSION 1

/* Defines related to the dump file format. To store 32 bits lengths for short
 * keys requires a lot of space, so we check the most significant 2 bits of
 * the first byte to interpreter the length:
 *
 * 00|000000 => if the two MSB are 00 the len is the 6 bits of this byte
 * 01|000000 00000000 =>  01, the len is 14 byes, 6 bits + 8 bits of next byte
 * 10|000000 [32 bit integer] => if it's 01, a full 32 bit len will follow
 * 11|000000 this means: specially encoded object will follow. The six bits
 *           number specify the kind of object that follows.
 *           See the REDIS_RDB_ENC_* defines.
 *
 * Lengths up to 63 are stored using a single byte, most DB keys, and may
 * values, will fit inside. */
#define REDIS_RDB_6BITLEN 0
#define REDIS_RDB_14BITLEN 1
#define REDIS_RDB_32BITLEN 0x80
#define REDIS_RDB_64BITLEN 0x81
#define REDIS_RDB_ENCVAL 3
#define REDIS_RDB_LENERR UINT_MAX

/* When a length of a string object stored on disk has the first two bits
 * set, the remaining two bits specify a special encoding for the object
 * accordingly to the following defines: */
#define REDIS_RDB_ENC_INT8 0        /* 8 bit signed integer */
#define REDIS_RDB_ENC_INT16 1       /* 16 bit signed integer */
#define REDIS_RDB_ENC_INT32 2       /* 32 bit signed integer */
#define REDIS_RDB_ENC_LZF 3         /* string compressed with FASTLZ */

/* Special RDB opcodes (saved/loaded with rdbSaveType/rdbLoadType). */
#define RDB_OPCODE_AUX        250
#define RDB_OPCODE_RESIZEDB   251
#define REDIS_RDB_OPCODE_EXPIRETIME_MS 252
#define REDIS_RDB_OPCODE_EXPIRETIME 253
#define REDIS_RDB_OPCODE_SELECTDB   254
#define REDIS_RDB_OPCODE_EOF        255

/* Module serialized values sub opcodes */
#define RDB_MODULE_OPCODE_EOF   0   /* End of module value. */
#define RDB_MODULE_OPCODE_SINT  1   /* Signed integer. */
#define RDB_MODULE_OPCODE_UINT  2   /* Unsigned integer. */
#define RDB_MODULE_OPCODE_FLOAT 3   /* Float. */
#define RDB_MODULE_OPCODE_DOUBLE 4  /* Double. */
#define RDB_MODULE_OPCODE_STRING 5  /* String. */

/* Dup object types to RDB object types. Only reason is readability (are we
 * dealing with RDB types or with in-memory object types?). */
#define REDIS_RDB_TYPE_STRING   0
#define REDIS_RDB_TYPE_LIST     1
#define REDIS_RDB_TYPE_SET      2
#define REDIS_RDB_TYPE_ZSET     3
#define REDIS_RDB_TYPE_HASH     4
#define REDIS_RDB_TYPE_ZSET_2   5 /* ZSET version 2 with doubles stored in binary. */
#define REDIS_RDB_TYPE_MODULE   6
#define REDIS_RDB_TYPE_MODULE_2 7 /* Module value with annotations for parsing without
                               the generating module being loaded. */

/* Object types for encoded objects. */
#define REDIS_RDB_TYPE_HASH_ZIPMAP    9
#define REDIS_RDB_TYPE_LIST_ZIPLIST  10
#define REDIS_RDB_TYPE_SET_INTSET    11
#define REDIS_RDB_TYPE_ZSET_ZIPLIST  12
#define REDIS_RDB_TYPE_HASH_ZIPLIST  13
#define REDIS_RDB_TYPE_LIST_QUICKLIST      14

#define ARDB_RDB_TYPE_CHUNK 1
#define ARDB_RDB_TYPE_SNAPPY_CHUNK 2
#define ARDB_RDB_OPCODE_SELECTDB   3
#define ARDB_OPCODE_AUX        250
#define ARDB_RDB_TYPE_EOF 255

namespace ardb
{

    static time_t g_lastsave = 0;
    static int g_saver_num = 0;
    static int g_lastsave_err = 0;
    static time_t g_lastsave_cost = 0;
    static time_t g_lastsave_start = 0;
    static const uint32 kloading_process_events_interval_bytes = 10 * 1024 * 1024;
    static const uint32 kmax_read_buffer_size = 10 * 1024 * 1024;

    static const char* type2str(int type)
    {
        switch (type)
        {
            case ARDB_DUMP:
                return "ardb";
            case REDIS_DUMP:
                return "redis";
            case BACKUP_DUMP:
                return "backup";
            default:
                return "unknown";
        }
    }

    static int EncodeInteger(long long value, unsigned char *enc)
    {
        /* Finally check if it fits in our ranges */
        if (value >= -(1 << 7) && value <= (1 << 7) - 1)
        {
            enc[0] = (REDIS_RDB_ENCVAL << 6) | REDIS_RDB_ENC_INT8;
            enc[1] = value & 0xFF;
            return 2;
        }
        else if (value >= -(1 << 15) && value <= (1 << 15) - 1)
        {
            enc[0] = (REDIS_RDB_ENCVAL << 6) | REDIS_RDB_ENC_INT16;
            enc[1] = value & 0xFF;
            enc[2] = (value >> 8) & 0xFF;
            return 3;
        }
        else if (value >= -((long long) 1 << 31) && value <= ((long long) 1 << 31) - 1)
        {
            enc[0] = (REDIS_RDB_ENCVAL << 6) | REDIS_RDB_ENC_INT32;
            enc[1] = value & 0xFF;
            enc[2] = (value >> 8) & 0xFF;
            enc[3] = (value >> 16) & 0xFF;
            enc[4] = (value >> 24) & 0xFF;
            return 5;
        }
        else
        {
            return 0;
        }
    }

    /* String objects in the form "2391" "-100" without any space and with a
     * range of values that can fit in an 8, 16 or 32 bit signed value can be
     * encoded as integers to save space */
    static int TryIntegerEncoding(char *s, size_t len, unsigned char *enc)
    {
        long long value;
        char *endptr, buf[32];

        /* Check if it's possible to encode this value as a number */
        value = strtoll(s, &endptr, 10);
        if (endptr[0] != '\0') return 0;
        ll2string(buf, 32, value);

        /* If the number converted back into a string is not identical
         * then it's not possible to encode the string as integer */
        if (strlen(buf) != len || memcmp(buf, s, len)) return 0;

        return EncodeInteger(value, enc);
    }

    DBWriter& ObjectIO::GetDBWriter()
    {
        static DBWriter defaultWriter;
        if (NULL == m_dbwriter)
        {
            return defaultWriter;
        }
        return *m_dbwriter;
    }

    int ObjectIO::ReadType()
    {
        unsigned char type;
        if (Read(&type, 1) == 0) return -1;
        return type;
    }

    time_t ObjectIO::ReadTime()
    {
        int32_t t32;
        if (Read(&t32, 4) == 0) return -1;
        return (time_t) t32;
    }
    int64 ObjectIO::ReadMillisecondTime()
    {
        int64_t t64;
        if (Read(&t64, 8) == 0) return -1;
        return (long long) t64;
    }

    uint64_t ObjectIO::ReadLen(int *isencoded)
    {
        unsigned char buf[2];
        uint32_t len;
        int type;

        if (isencoded) *isencoded = 0;
        if (Read(buf, 1) == 0) return REDIS_RDB_LENERR;
        type = (buf[0] & 0xC0) >> 6;
        if (type == REDIS_RDB_ENCVAL)
        {
            /* Read a 6 bit encoding type. */
            if (isencoded) *isencoded = 1;
            return buf[0] & 0x3F;
        }
        else if (type == REDIS_RDB_6BITLEN)
        {
            /* Read a 6 bit len. */
            return buf[0] & 0x3F;
        }
        else if (type == REDIS_RDB_14BITLEN)
        {
            /* Read a 14 bit len. */
            if (Read(buf + 1, 1) == 0) return REDIS_RDB_LENERR;
            return ((buf[0] & 0x3F) << 8) | buf[1];
        }
        else if (buf[0] == REDIS_RDB_32BITLEN)
        {
            /* Read a 32 bit len. */
            uint32_t len;
            if (!Read(&len, 4)) return -1;
            return ntohl(len);
        }
        else if (buf[0] == REDIS_RDB_64BITLEN)
        {
            /* Read a 64 bit len. */
            uint64_t len;
            if (!Read(&len, 8)) return -1;
            return ntohu64(len);
        }
        else
        {
            ERROR_LOG("Unknown length encoding %d in ReadLen()", type);
            return -1; /* Never reached. */
        }
    }

    bool ObjectIO::ReadLzfStringObject(std::string& str)
    {
        unsigned int len, clen;
        unsigned char *c = NULL;

        if ((clen = ReadLen(NULL)) == REDIS_RDB_LENERR) return false;
        if ((len = ReadLen(NULL)) == REDIS_RDB_LENERR) return false;
        str.resize(len);
        NEW(c, unsigned char[clen]);
        if (NULL == c) return false;

        if (!Read(c, clen))
        {
            DELETE_A(c);
            return false;
        }
        str.resize(len);
        char* val = &str[0];
        if (lzf_decompress(c, clen, val, len) == 0)
        {
            DELETE_A(c);
            return false;
        }
        DELETE_A(c);
        return true;
    }

    bool ObjectIO::ReadInteger(int enctype, int64& val)
    {
        unsigned char enc[4];

        if (enctype == REDIS_RDB_ENC_INT8)
        {
            if (Read(enc, 1) == 0) return false;
            val = (signed char) enc[0];
        }
        else if (enctype == REDIS_RDB_ENC_INT16)
        {
            uint16_t v;
            if (Read(enc, 2) == 0) return false;
            v = enc[0] | (enc[1] << 8);
            val = (int16_t) v;
        }
        else if (enctype == REDIS_RDB_ENC_INT32)
        {
            uint32_t v;
            if (Read(enc, 4) == 0) return false;
            v = enc[0] | (enc[1] << 8) | (enc[2] << 16) | (enc[3] << 24);
            val = (int32_t) v;
        }
        else
        {
            val = 0; /* anti-warning */
            FATAL_LOG("Unknown RDB integer encoding type");
            abort();
        }
        return true;
    }

    int ObjectIO::ReadBinaryDoubleValue(double& val)
    {
        if (!Read(&val, sizeof(val)))
        {
            return -1;
        }memrev64ifbe(val);
        return 0;
    }
    int ObjectIO::ReadBinaryFloatValue(float& val)
    {
        if (!Read(&val, sizeof(val)))
        {
            return -1;
        }memrev32ifbe(val);
        return 0;
    }

    /* For information about double serialization check rdbSaveDoubleValue() */
    int ObjectIO::ReadDoubleValue(double&val, bool binary)
    {
        if (binary)
        {
            return ReadBinaryDoubleValue(val);
        }
        static double R_Zero = 0.0;
        static double R_PosInf = 1.0 / R_Zero;
        static double R_NegInf = -1.0 / R_Zero;
        static double R_Nan = R_Zero / R_Zero;
        char buf[128];
        unsigned char len;

        if (Read(&len, 1) == 0) return -1;
        switch (len)
        {
            case 255:
                val = R_NegInf;
                return 0;
            case 254:
                val = R_PosInf;
                return 0;
            case 253:
                val = R_Nan;
                return 0;
            default:
                if (Read(buf, len) == 0) return -1;
                buf[len] = '\0';
                sscanf(buf, "%lg", &val);
                return 0;
        }
    }

    bool ObjectIO::ReadString(std::string& str)
    {
        str.clear();
        int isencoded;
        uint32_t len;
        len = ReadLen(&isencoded);
        if (isencoded)
        {
            switch (len)
            {
                case REDIS_RDB_ENC_INT8:
                case REDIS_RDB_ENC_INT16:
                case REDIS_RDB_ENC_INT32:
                {
                    int64 v;
                    if (ReadInteger(len, v))
                    {
                        str = stringfromll(v);
                        return true;
                    }
                    else
                    {
                        return true;
                    }
                }
                case REDIS_RDB_ENC_LZF:
                {
                    return ReadLzfStringObject(str);
                }
                default:
                {
                    ERROR_LOG("Unknown RDB encoding type:%d", len);
                    abort();
                    break;
                }
            }
        }

        if (len == REDIS_RDB_LENERR) return false;
        str.clear();
        str.resize(len);
        char* val = &str[0];
        if (len && Read(val, len) == 0)
        {
            return false;
        }
        return true;
    }

    int ObjectIO::WriteType(uint8 type)
    {
        return Write(&type, 1);
    }

    /* Like rdbSaveStringObjectRaw() but handle encoded objects */
    int ObjectIO::WriteStringObject(const Data& o)
    {
        /* Avoid to decode the object, then encode it again, if the
         * object is alrady integer encoded. */
        if (o.IsInteger())
        {
            return WriteLongLongAsStringObject(o.GetInt64());
        }
        else
        {
            std::string str;
            o.ToString(str);
            return WriteRawString(str.data(), str.size());
        }
    }

    int ObjectIO::WriteRawString(const std::string& str)
    {
        return WriteRawString(str.data(), str.size());
    }
    /* Save a string objet as [len][data] on disk. If the object is a string
     * representation of an integer value we try to save it in a special form */
    int ObjectIO::WriteRawString(const char *s, size_t len)
    {
        int enclen;
        int n, nwritten = 0;

        /* Try integer encoding */
        if (len > 0 && len <= 11)
        {
            unsigned char buf[5];
            if ((enclen = TryIntegerEncoding((char*) s, len, buf)) > 0)
            {
                if (Write(buf, enclen) == -1) return -1;
                return enclen;
            }
        }

        /* Try LZF compression - under 20 bytes it's unable to compress even
         * aaaaaaaaaaaaaaaaaa so skip it */
        if (len > 20)
        {
            n = WriteLzfStringObject(s, len);
            if (n == -1) return -1;
            if (n > 0) return n;
            /* Return value of 0 means data can't be compressed, save the old way */
        }

        /* Store verbatim */
        if ((n = WriteLen(len)) == -1) return -1;
        nwritten += n;
        if (len > 0)
        {
            if (Write(s, len) == -1) return -1;
            nwritten += len;
        }
        return nwritten;
    }

    int ObjectIO::WriteLzfStringObject(const char *s, size_t len)
    {
        size_t comprlen, outlen;
        unsigned char byte;
        int n, nwritten = 0;
        void *out;

        /* We require at least four bytes compression for this to be worth it */
        if (len <= 4) return 0;
        outlen = len - 4;
        if ((out = malloc(outlen + 1)) == NULL) return 0;
        comprlen = lzf_compress(s, len, out, outlen);
        if (comprlen == 0)
        {
            free(out);
            return 0;
        }
        /* Data compressed! Let's save it on disk */
        byte = (REDIS_RDB_ENCVAL << 6) | REDIS_RDB_ENC_LZF;
        if ((n = Write(&byte, 1)) == -1) goto writeerr;
        nwritten += n;

        if ((n = WriteLen(comprlen)) == -1) goto writeerr;
        nwritten += n;

        if ((n = WriteLen(len)) == -1) goto writeerr;
        nwritten += n;

        if ((n = Write(out, comprlen)) == -1) goto writeerr;
        nwritten += n;

        free(out);
        return nwritten;

        writeerr: free(out);
        return -1;
    }

    /* Save a long long value as either an encoded string or a string. */
    int ObjectIO::WriteLongLongAsStringObject(long long value)
    {
        unsigned char buf[32];
        int n, nwritten = 0;
        int enclen = EncodeInteger(value, buf);
        if (enclen > 0)
        {
            return Write(buf, enclen);
        }
        else
        {
            /* Encode as string */
            enclen = ll2string((char*) buf, 32, value);
            if ((n = WriteLen(enclen)) == -1) return -1;
            nwritten += n;
            if ((n = Write(buf, enclen)) == -1) return -1;
            nwritten += n;
        }
        return nwritten;
    }

    int ObjectIO::WriteLen(uint64 len)
    {
        unsigned char buf[2];
        size_t nwritten;

        if (len < (1 << 6))
        {
            /* Save a 6 bit len */
            buf[0] = (len & 0xFF) | (REDIS_RDB_6BITLEN << 6);
            if (Write(buf, 1) == -1) return -1;
            nwritten = 1;
        }
        else if (len < (1 << 14))
        {
            /* Save a 14 bit len */
            buf[0] = ((len >> 8) & 0xFF) | (REDIS_RDB_14BITLEN << 6);
            buf[1] = len & 0xFF;
            if (Write(buf, 2) == -1) return -1;
            nwritten = 2;
        }
        else if (len <= UINT32_MAX)
        {
            /* Save a 32 bit len */
            buf[0] = (REDIS_RDB_32BITLEN);
            if (Write(buf, 1) == -1) return -1;
            len = htonl(len);
            if (Write(&len, 4) == -1) return -1;
            nwritten = 1 + 4;
        }
        else
        {
            /* Save a 64 bit len */
            buf[0] = REDIS_RDB_64BITLEN;
            if (Write(buf, 1) == -1) return -1;
            len = htonu64(len);
            if (Write(&len, 8) == -1) return -1;
            nwritten = 1 + 8;
        }
        return nwritten;
    }

    int ObjectIO::WriteKeyType(KeyType type)
    {
        switch (type)
        {
            case KEY_STRING:
            {
                return WriteType(REDIS_RDB_TYPE_STRING);
            }
            case KEY_SET:
            case KEY_SET_MEMBER:
            {
                return WriteType(REDIS_RDB_TYPE_SET);
            }
            case KEY_LIST:
            case KEY_LIST_ELEMENT:
            {
                return WriteType(REDIS_RDB_TYPE_LIST);
            }
            case KEY_ZSET:
            case KEY_ZSET_SCORE:
            case KEY_ZSET_SORT:
            {
                return WriteType(REDIS_RDB_TYPE_ZSET);
            }
            case KEY_HASH:
            case KEY_HASH_FIELD:
            {
                return WriteType(REDIS_RDB_TYPE_HASH);
            }
            default:
            {
                return -1;
            }
        }
    }

    int ObjectIO::WriteDouble(double val)
    {
        unsigned char buf[128];
        int len;

        if (std::isnan(val))
        {
            buf[0] = 253;
            len = 1;
        }
        else if (!std::isfinite(val))
        {
            len = 1;
            buf[0] = (val < 0) ? 255 : 254;
        }
        else
        {
#if (DBL_MANT_DIG >= 52) && (LLONG_MAX == 0x7fffffffffffffffLL)
            /* Check if the float is in a safe range to be casted into a
             * long long. We are assuming that long long is 64 bit here.
             * Also we are assuming that there are no implementations around where
             * double has precision < 52 bit.
             *
             * Under this assumptions we test if a double is inside an interval
             * where casting to long long is safe. Then using two castings we
             * make sure the decimal part is zero. If all this is true we use
             * integer printing function that is much faster. */
            double min = -4503599627370495LL; /* (2^52)-1 */
            double max = 4503599627370496LL; /* -(2^52) */
            if (val > min && val < max && val == ((double) ((long long) val)))
            {
                ll2string((char*) buf + 1, sizeof(buf), (long long) val);
            }
            else
#endif
            snprintf((char*) buf + 1, sizeof(buf) - 1, "%.17g", val);
            buf[0] = strlen((char*) buf + 1);
            len = buf[0] + 1;
        }
        return Write(buf, len);

    }

    int ObjectIO::WriteMillisecondTime(uint64 ts)
    {
        return Write(&ts, 8);
    }

    int ObjectIO::WriteTime(time_t t)
    {
        int32_t t32 = (int32_t) t;
        return Write(&t32, 4);
    }

    void ObjectIO::RedisWriteMagicHeader()
    {
        char magic[10];
        snprintf(magic, sizeof(magic), "REDIS%04d", 6);
        Write(magic, 9);
    }

    void ObjectIO::RedisLoadListZipList(Context& ctx, unsigned char* data, const std::string& key,
            ValueObject& listmeta)
    {
        unsigned char* iter = ziplistIndex(data, 0);
        listmeta.SetType(KEY_LIST);
        //BatchWriteGuard guard(g_db->GetKeyValueEngine());
        int64 idx = 0;
        while (iter != NULL)
        {
            unsigned char *vstr;
            unsigned int vlen;
            long long vlong;
            if (ziplistGet(iter, &vstr, &vlen, &vlong))
            {
                std::string value;
                if (vstr)
                {
                    value.assign((char*) vstr, vlen);
                }
                else
                {
                    value = stringfromll(vlong);
                }
                KeyObject lk(ctx.ns, KEY_LIST_ELEMENT, key);
                lk.SetListIndex(idx);
                ValueObject lv;
                lv.SetType(KEY_LIST_ELEMENT);
                lv.SetListElement(value);
                idx++;
                //g_db->SetKeyValue(ctx, lk, lv);
                GetDBWriter().Put(ctx, lk, lv);
            }
            iter = ziplistNext(data, iter);
        }
        listmeta.SetObjectLen(idx);
        listmeta.SetListMinIdx(0);
        listmeta.SetListMaxIdx(idx - 1);
        listmeta.GetMetaObject().list_sequential = true;
    }

    static double zzlGetScore(unsigned char *sptr)
    {
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        char buf[128];
        double score;

        ASSERT(sptr != NULL);
        ASSERT(ziplistGet(sptr, &vstr, &vlen, &vlong));
        if (vstr)
        {
            memcpy(buf, vstr, vlen);
            buf[vlen] = '\0';
            score = strtod(buf, NULL);
        }
        else
        {
            score = vlong;
        }
        return score;
    }
    void ObjectIO::RedisLoadZSetZipList(Context& ctx, unsigned char* data, const std::string& key,
            ValueObject& meta_value)
    {
        meta_value.SetType(KEY_ZSET);
        meta_value.SetObjectLen(ziplistLen(data) / 2);
        unsigned char* iter = ziplistIndex(data, 0);
        while (iter != NULL)
        {
            unsigned char *vstr;
            unsigned int vlen;
            long long vlong;
            std::string value;
            if (ziplistGet(iter, &vstr, &vlen, &vlong))
            {
                if (vstr)
                {
                    value.assign((char*) vstr, vlen);
                }
                else
                {
                    value = stringfromll(vlong);
                }
            }
            iter = ziplistNext(data, iter);
            if (NULL == iter)
            {
                break;
            }
            double score = zzlGetScore(iter);
            KeyObject zsort(ctx.ns, KEY_ZSET_SORT, key);
            zsort.SetZSetMember(value);
            zsort.SetZSetScore(score);
            ValueObject zsort_value;
            zsort_value.SetType(KEY_ZSET_SORT);
            KeyObject zscore(ctx.ns, KEY_ZSET_SCORE, key);
            zscore.SetZSetMember(value);
            ValueObject zscore_value;
            zscore_value.SetType(KEY_ZSET_SCORE);
            zscore_value.SetZSetScore(score);
            //g_db->SetKeyValue(ctx, zsort, zscore_value);
            //g_db->SetKeyValue(ctx, zscore, zscore_value);
            GetDBWriter().Put(ctx, zsort, zsort_value);
            GetDBWriter().Put(ctx, zscore, zscore_value);
            iter = ziplistNext(data, iter);
        }
    }

    void ObjectIO::RedisLoadHashZipList(Context& ctx, unsigned char* data, const std::string& key,
            ValueObject& meta_value)
    {
        meta_value.SetType(KEY_HASH);
        meta_value.SetObjectLen(ziplistLen(data) / 2);
        //BatchWriteGuard guard(g_db->GetKeyValueEngine());
        unsigned char* iter = ziplistIndex(data, 0);
        while (iter != NULL)
        {
            unsigned char *vstr, *fstr;
            unsigned int vlen, flen;
            long long vlong, flong;
            std::string field, value;
            if (ziplistGet(iter, &fstr, &flen, &flong))
            {
                if (fstr)
                {
                    field.assign((char*) fstr, flen);
                }
                else
                {
                    field = stringfromll(flong);
                }
            }
            else
            {
                break;
            }
            iter = ziplistNext(data, iter);
            if (NULL == iter)
            {
                break;
            }
            if (ziplistGet(iter, &vstr, &vlen, &vlong))
            {
                if (vstr)
                {
                    value.assign((char*) vstr, vlen);
                }
                else
                {
                    value = stringfromll(vlong);
                }
                KeyObject fkey(ctx.ns, KEY_HASH_FIELD, key);
                fkey.SetHashField(field);
                ValueObject fvalue;
                fvalue.SetType(KEY_HASH_FIELD);
                fvalue.SetHashValue(value);
                GetDBWriter().Put(ctx, fkey, fvalue);
                //g_db->SetKeyValue(ctx, fkey, fvalue);
            }
            iter = ziplistNext(data, iter);
        }
        //        KeyObject hkey(ctx.ns, KEY_META, key);
        //        g_db->SetKeyValue(ctx, hkey, hmeta);
    }

    void ObjectIO::RedisLoadSetIntSet(Context& ctx, unsigned char* data, const std::string& key,
            ValueObject& meta_value)
    {
        int ii = 0;
        int64_t llele = 0;
        meta_value.SetType(KEY_SET);
        meta_value.SetObjectLen(intsetLen((intset*) data));

        while (intsetGet((intset*) data, ii++, &llele) > 0)
        {
            KeyObject member(ctx.ns, KEY_SET_MEMBER, key);
            member.SetSetMember(stringfromll(llele));
            ValueObject member_value;
            member_value.SetType(KEY_SET_MEMBER);
            //g_db->SetKeyValue(ctx, member, member_value);
            GetDBWriter().Put(ctx, member, member_value);
        }
    }

    bool ObjectIO::RedisLoadObject(Context& ctx, int rdbtype, const std::string& key, int64 expiretime)
    {
        //TransactionGuard guard(ctx);
        KeyObject meta_key(ctx.ns, KEY_META, key);
        ValueObject meta_value;
        switch (rdbtype)
        {
            case REDIS_RDB_TYPE_STRING:
            {
                std::string str;
                if (ReadString(str))
                {
                    ValueObject string_value;
                    meta_value.SetType(KEY_STRING);
                    meta_value.SetStringValue(str);
                }
                else
                {
                    return false;
                }
                break;
            }
            case REDIS_RDB_TYPE_LIST:
            case REDIS_RDB_TYPE_SET:
            {
                uint32 len;
                if ((len = ReadLen(NULL)) == REDIS_RDB_LENERR) return false;

                if (REDIS_RDB_TYPE_SET == rdbtype)
                {
                    meta_value.SetType(KEY_SET);
                    meta_value.SetObjectLen(len);
                }
                else
                {
                    meta_value.SetType(KEY_LIST);
                    meta_value.SetObjectLen(len);
                    meta_value.GetMetaObject().list_sequential = true;
                }
                int64 idx = 0;
                while (len--)
                {
                    std::string str;
                    if (ReadString(str))
                    {
                        //push to list/set
                        if (REDIS_RDB_TYPE_SET == rdbtype)
                        {
                            KeyObject member(ctx.ns, KEY_SET_MEMBER, key);
                            member.SetSetMember(str);
                            ValueObject member_val;
                            member_val.SetType(KEY_SET_MEMBER);
                            //g_db->SetKeyValue(ctx, member, member_val);
                            GetDBWriter().Put(ctx, member, member_val);
                        }
                        else
                        {
                            KeyObject lk(ctx.ns, KEY_LIST_ELEMENT, key);
                            lk.SetListIndex(idx);
                            ValueObject lv;
                            lv.SetType(KEY_LIST_ELEMENT);
                            lv.SetListElement(str);
                            //g_db->SetKeyValue(ctx, lk, lv);
                            GetDBWriter().Put(ctx, lk, lv);
                            idx++;
                        }
                    }
                    else
                    {
                        return false;
                    }
                }
                if (REDIS_RDB_TYPE_LIST == rdbtype)
                {
                    meta_value.GetMetaObject().list_sequential = true;
                    meta_value.SetListMinIdx(0);
                    meta_value.SetListMaxIdx(idx - 1);
                }
                //g_db->SetKeyValue(ctx, meta_key, zmeta);
                break;
            }
            case REDIS_RDB_TYPE_LIST_QUICKLIST:
            {
                uint32 ziplen;
                if ((ziplen = ReadLen(NULL)) == REDIS_RDB_LENERR) return false;
                meta_value.SetType(KEY_LIST);
                meta_value.GetMetaObject().list_sequential = true;
                int64 idx = 0;
                while (ziplen--)
                {
                    std::string zipstr;
                    if (ReadString(zipstr))
                    {
                        unsigned char* data = (unsigned char*) (&(zipstr[0]));
                        unsigned char* iter = ziplistIndex(data, 0);
                        while (iter != NULL)
                        {
                            unsigned char *vstr;
                            unsigned int vlen;
                            long long vlong;
                            if (ziplistGet(iter, &vstr, &vlen, &vlong))
                            {
                                std::string value;
                                if (vstr)
                                {
                                    value.assign((char*) vstr, vlen);
                                }
                                else
                                {
                                    value = stringfromll(vlong);
                                }
                                KeyObject lk(ctx.ns, KEY_LIST_ELEMENT, key);
                                lk.SetListIndex(idx);
                                ValueObject lv;
                                lv.SetType(KEY_LIST_ELEMENT);
                                lv.SetListElement(value);
                                //g_db->Set
                                idx++;
                                //g_db->SetKeyValue(ctx, lk, lv);
                                GetDBWriter().Put(ctx, lk, lv);
                            }
                            iter = ziplistNext(data, iter);
                        }
                    }
                    else
                    {
                        return false;
                    }
                }
                meta_value.GetMetaObject().list_sequential = true;
                meta_value.SetListMinIdx(0);
                meta_value.SetListMaxIdx(idx - 1);
                meta_value.SetObjectLen(idx);
                break;
            }
            case REDIS_RDB_TYPE_ZSET:
            case REDIS_RDB_TYPE_ZSET_2:
            {
                uint32 len;
                if ((len = ReadLen(NULL)) == REDIS_RDB_LENERR) return false;
                meta_value.SetType(KEY_ZSET);
                meta_value.SetObjectLen(len);
                while (len--)
                {
                    std::string str;
                    double score;
                    if (ReadString(str) && 0 == ReadDoubleValue(score, REDIS_RDB_TYPE_ZSET_2 == rdbtype))
                    {
                        //save value score
                        //g_db->ZAdd(m_current_db, key, score, str);
                        KeyObject zsort(ctx.ns, KEY_ZSET_SORT, key);
                        zsort.SetZSetMember(str);
                        zsort.SetZSetScore(score);
                        ValueObject zsort_value;
                        zsort_value.SetType(KEY_ZSET_SORT);
                        KeyObject zscore(ctx.ns, KEY_ZSET_SCORE, key);
                        zscore.SetZSetMember(str);
                        ValueObject zscore_value;
                        zscore_value.SetType(KEY_ZSET_SCORE);
                        zscore_value.SetZSetScore(score);
                        //g_db->SetKeyValue(ctx, zsort, zscore_value);
                        //g_db->SetKeyValue(ctx, zscore, zscore_value);
                        GetDBWriter().Put(ctx, zsort, zsort_value);
                        GetDBWriter().Put(ctx, zscore, zscore_value);
                    }
                    else
                    {
                        return false;
                    }
                }
                //g_db->SetKeyValue(ctx, meta_key, zmeta);
                break;
            }
            case REDIS_RDB_TYPE_HASH:
            {
                uint32 len;
                if ((len = ReadLen(NULL)) == REDIS_RDB_LENERR) return false;
                meta_value.SetType(KEY_HASH);
                meta_value.SetObjectLen(len);
                while (len--)
                {
                    std::string field, str;
                    if (ReadString(field) && ReadString(str))
                    {
                        //save hash value
                        KeyObject fkey(ctx.ns, KEY_HASH_FIELD, key);
                        fkey.SetHashField(field);
                        ValueObject fvalue;
                        fvalue.SetType(KEY_HASH_FIELD);
                        fvalue.SetHashValue(str);
                        //g_db->SetKeyValue(ctx, fkey, fvalue);
                        GetDBWriter().Put(ctx, fkey, fvalue);
                    }
                    else
                    {
                        return false;
                    }
                }
                //g_db->SetKeyValue(ctx, meta_key, hmeta);
                break;
            }
            case REDIS_RDB_TYPE_HASH_ZIPMAP:
            case REDIS_RDB_TYPE_LIST_ZIPLIST:
            case REDIS_RDB_TYPE_SET_INTSET:
            case REDIS_RDB_TYPE_ZSET_ZIPLIST:
            case REDIS_RDB_TYPE_HASH_ZIPLIST:
            {
                std::string aux;
                if (!ReadString(aux))
                {
                    return false;
                }
                unsigned char* data = (unsigned char*) (&(aux[0]));
                switch (rdbtype)
                {
                    case REDIS_RDB_TYPE_HASH_ZIPMAP:
                    {
                        unsigned char *zi = zipmapRewind(data);
                        unsigned char *fstr, *vstr;
                        unsigned int flen, vlen;
                        meta_value.SetType(KEY_HASH);
                        int64 hlen = 0;
                        while ((zi = zipmapNext(zi, &fstr, &flen, &vstr, &vlen)) != NULL)
                        {
                            std::string fstring, fvstring;
                            fstring.assign((char*) fstr, flen);
                            fvstring.assign((char*) vstr, vlen);
                            KeyObject fkey(ctx.ns, KEY_HASH_FIELD, key);
                            fkey.SetHashField(fstring);
                            ValueObject fvalue;
                            fvalue.SetType(KEY_HASH_FIELD);
                            fvalue.SetHashValue(fvstring);
                            //g_db->SetKeyValue(ctx, fkey, fvalue);
                            GetDBWriter().Put(ctx, fkey, fvalue);
                            hlen++;
                        }
                        meta_value.SetObjectLen(hlen);
                        //g_db->SetKeyValue(ctx, meta_key, hmeta);
                        break;
                    }
                    case REDIS_RDB_TYPE_LIST_ZIPLIST:
                    {
                        RedisLoadListZipList(ctx, data, key, meta_value);
                        break;
                    }
                    case REDIS_RDB_TYPE_SET_INTSET:
                    {
                        RedisLoadSetIntSet(ctx, data, key, meta_value);
                        break;
                    }
                    case REDIS_RDB_TYPE_ZSET_ZIPLIST:
                    {
                        RedisLoadZSetZipList(ctx, data, key, meta_value);
                        break;
                    }
                    case REDIS_RDB_TYPE_HASH_ZIPLIST:
                    {
                        RedisLoadHashZipList(ctx, data, key, meta_value);
                        break;
                    }
                    default:
                    {
                        ERROR_LOG("Unknown encoding");
                        abort();
                    }
                }
                break;
            }
            case REDIS_RDB_TYPE_MODULE:
            {
                ERROR_LOG("Not support RDB_TYPE_MODULE");
                return false;
            }
            case REDIS_RDB_TYPE_MODULE_2:
            {
                uint64_t moduleid = ReadLen(NULL);
                char name[10];
                name[9] = '\0';
                if (!RedisLoadCheckModuleValue(name))
                {
                    return false;
                }
                return true;
            }
            default:
            {
                ERROR_LOG("Unknown object type:%d", rdbtype);
                abort();
            }
        }
        if (expiretime > 0)
        {
            meta_value.SetTTL(expiretime);
            if (!g_db->GetEngine()->GetFeatureSet().support_compactfilter)
            {
                g_db->SaveTTL(ctx, meta_key.GetNameSpace(), meta_key.GetKey().AsString(), 0, expiretime);
            }
        }
        //g_db->SetKeyValue(ctx, meta_key, meta_value);
        GetDBWriter().Put(ctx, meta_key, meta_value);
        return true;
    }

    bool ObjectIO::RedisLoadCheckModuleValue(char* modulename)
    {
        uint64_t opcode;
        while ((opcode = ReadLen(NULL)) != RDB_MODULE_OPCODE_EOF)
        {
            if (opcode == RDB_MODULE_OPCODE_SINT || opcode == RDB_MODULE_OPCODE_UINT)
            {
                uint64_t len;
                if (!ReadLen(NULL) == -1)
                {
                    ERROR_LOG("Error reading integer from module %s value", modulename);
                    return false;
                }
            }
            else if (opcode == RDB_MODULE_OPCODE_STRING)
            {
                std::string str;
                if (!ReadString(str))
                {
                    ERROR_LOG("Error reading string from module %s value", modulename);
                    return false;
                }
            }
            else if (opcode == RDB_MODULE_OPCODE_FLOAT)
            {
                float val;
                if (!ReadBinaryFloatValue(val))
                {
                    ERROR_LOG("Error reading float from module %s value", modulename);
                    return false;
                }
            }
            else if (opcode == RDB_MODULE_OPCODE_DOUBLE)
            {
                double val;
                if (!ReadBinaryDoubleValue(val))
                {
                    ERROR_LOG("Error reading double from module %s value", modulename);
                    return false;
                }
            }
        }
        return true;
    }

    int ObjectIO::ArdbWriteMagicHeader()
    {
        char magic[10];
        snprintf(magic, sizeof(magic), "ARDB%04d", ARDB_RDB_VERSION);
        return Write(magic, 8);
    }

    int ObjectIO::ArdbLoadBuffer(Context& ctx, Buffer& buffer)
    {
        while (buffer.Readable())
        {
            int64 ttl = 0;
            if (!BufferHelper::ReadVarInt64(buffer, ttl))
            {
                ERROR_LOG("Failed to read ttl in kv pair.");
                return -1;
            }
            Slice key, value;
            if (!BufferHelper::ReadVarSlice(buffer, key))
            {
                ERROR_LOG("Failed to read key in kv pair.");
                return -1;
            }
            if (!BufferHelper::ReadVarSlice(buffer, value))
            {
                ERROR_LOG("Failed to read value in kv pair.");
                return -1;
            }
            //g_db->GetEngine()->PutRaw(ctx, ctx.ns, key, value);
            GetDBWriter().Put(ctx, ctx.ns, key, value);
            if (ttl > 0 && !g_db->GetEngine()->GetFeatureSet().support_compactfilter)
            {
                Buffer keybuf((char*) key.data(), 0, key.size());
                KeyObject kk;
                if (!kk.Decode(keybuf, false, false))
                {
                    ERROR_LOG("Failed to decode key object.");
                    return -1;
                }
                g_db->SaveTTL(ctx, ctx.ns, kk.GetKey().AsString(), 0, ttl);
            }
        }
        return 0;
    }

    int ObjectIO::ArdbSaveRawKeyValue(const Slice& key, const Slice& value, Buffer& buffer, int64 ttl)
    {
        BufferHelper::WriteVarInt64(buffer, ttl);
        BufferHelper::WriteVarSlice(buffer, key);
        BufferHelper::WriteVarSlice(buffer, value);
        return 0;
    }

    int ObjectIO::ArdbFlushWriteBuffer(Buffer& buffer)
    {
        if (buffer.Readable())
        {
            std::string compressed;
            snappy::Compress(buffer.GetRawReadBuffer(), buffer.ReadableBytes(), &compressed);
            if (compressed.size() > (buffer.ReadableBytes() + 4))
            {
                RETURN_NEGATIVE_EXPR(WriteType(ARDB_RDB_TYPE_CHUNK));
                uint32 len = buffer.ReadableBytes();
                RETURN_NEGATIVE_EXPR(WriteLen(len));
                RETURN_NEGATIVE_EXPR(Write(buffer.GetRawReadBuffer(), buffer.ReadableBytes()));
            }
            else
            {
                RETURN_NEGATIVE_EXPR(WriteType(ARDB_RDB_TYPE_SNAPPY_CHUNK));
                uint32 rawlen = buffer.ReadableBytes();
                RETURN_NEGATIVE_EXPR(WriteLen(rawlen));
                RETURN_NEGATIVE_EXPR(WriteLen(compressed.size()));
                RETURN_NEGATIVE_EXPR(Write(compressed.data(), compressed.size()));
            }
            buffer.Clear();
        }
        return 0;
    }

    int ObjectIO::ArdbLoadChunk(Context& ctx, int type)
    {
        if (type == ARDB_RDB_TYPE_CHUNK)
        {
            Buffer buffer;
            uint32 len = ReadLen(NULL);
            buffer.EnsureWritableBytes(len);
            char* newbuf = const_cast<char*>(buffer.GetRawWriteBuffer());
            if (!Read(newbuf, len, true))
            {
                return -1;
            }
            RETURN_NEGATIVE_EXPR(ArdbLoadBuffer(ctx, buffer));
            return 0;
        }
        else if (type == ARDB_RDB_TYPE_SNAPPY_CHUNK)
        {
            uint32 rawlen = ReadLen(NULL);
            uint32 compressedlen = ReadLen(NULL);
            char* newbuf = NULL;
            NEW(newbuf, char[compressedlen]);
            if (!Read(newbuf, compressedlen, true))
            {
                ERROR_LOG("Failed to read compressed chunk.");
                DELETE_A(newbuf);
                return -1;
            }
            std::string origin;
            origin.reserve(rawlen);
            if (!snappy::Uncompress(newbuf, compressedlen, &origin))
            {
                ERROR_LOG("Failed to decompress snappy chunk.");
                DELETE_A(newbuf);
                return -1;
            }
            DELETE_A(newbuf);
            Buffer readbuf(const_cast<char*>(origin.data()), 0, origin.size());
            RETURN_NEGATIVE_EXPR(ArdbLoadBuffer(ctx, readbuf));
            return 0;
        }
        return -1;
    }

    ObjectBuffer::ObjectBuffer()
    {

    }
    ObjectBuffer::ObjectBuffer(const std::string& content)
    {
        m_buffer.WrapReadableContent(content.data(), content.size());
    }

    bool ObjectBuffer::Read(void* buf, size_t buflen, bool cksm)
    {
        if (m_buffer.ReadableBytes() < buflen)
        {
            return false;
        }
        m_buffer.Read(buf, buflen);
        return true;
    }
    int ObjectBuffer::Write(const void* buf, size_t buflen)
    {
        m_buffer.Write(buf, buflen);
        return 0;
    }

    bool ObjectBuffer::CheckReadPayload()
    {
        const unsigned char *footer;
        uint16_t rdbver;
        uint64_t crc;

        size_t len = m_buffer.ReadableBytes();
        /* At least 2 bytes of RDB version and 8 of CRC64 should be present. */
        if (len < 10) return false;
        footer = (const unsigned char *) (m_buffer.GetRawReadBuffer() + (len - 10));

        /* Verify RDB version */
        rdbver = (footer[1] << 8) | footer[0];
        if (rdbver > REDIS_RDB_VERSION) return false;

        /* Verify CRC64 */
        crc = crc64(0, (const unsigned char*) m_buffer.GetRawReadBuffer(), len - 8);
        memrev64ifbe(&crc);
        return memcmp(&crc, footer + 2, 8) == 0;
    }

    bool ObjectBuffer::RedisLoad(Context& ctx, const std::string& key, int64 ttl)
    {
        if (!CheckReadPayload())
        {
            return false;
        }
        int type;
        if ((type = ReadType()) == -1)
        {
            return false;
        }
        return RedisLoadObject(ctx, type, key, ttl);
    }

    bool ObjectBuffer::ArdbLoad(Context& ctx)
    {
        int type = 0;
        if ((type = ReadType()) == -1) return false;
        if (type == ARDB_RDB_TYPE_CHUNK || type == ARDB_RDB_TYPE_SNAPPY_CHUNK)
        {
            return ArdbLoadChunk(ctx, type) == 0;
        }
        else
        {
            return false;
        }
    }

    bool ObjectBuffer::RedisSave(Context& ctx, const std::string& key, std::string& content, uint64* ttl)
    {
        unsigned char buf[2];
        uint64_t crc;
        KeyObject start(ctx.ns, KEY_META, key);
        Iterator* iter = g_db->GetEngine()->Find(ctx, start);
        int64 objectlen = 0;
        KeyType current_keytype;
        bool iter_continue = true;
        bool success = false;

        while (iter_continue && iter->Valid())
        {
            KeyObject& k = iter->Key();

            if (start.GetKey() != k.GetKey() || k.GetNameSpace() != start.GetNameSpace())
            {
                //printf("####cmp %s %d\n", key.c_str(), k.GetNameSpace() != start.GetNameSpace());
                break;
            }
            ValueObject& v = iter->Value();
            switch (k.GetType())
            {
                case KEY_META:
                {
                    if (NULL != ttl)
                    {
                        *ttl = v.GetTTL();
                    }
//                    int64 ttl = v.GetTTL();
//                    if (ttl > 0)
//                    {
//                        WriteType(REDIS_RDB_OPCODE_EXPIRETIME_MS);
//                        WriteMillisecondTime(ttl);
//                    }
                    //printf("####type %d\n", current_keytype);
                    current_keytype = (KeyType) v.GetType();
                    WriteKeyType(current_keytype);
//                    std::string kstr;
//                    k.GetKey().ToString(kstr);
//                    WriteRawString(kstr.data(), kstr.size());
                    success = true;
                    switch (current_keytype)
                    {
                        case KEY_STRING:
                        {
                            WriteStringObject(v.GetStringValue());
                            iter_continue = false;
                            break;
                        }
                        case KEY_LIST:
                        case KEY_ZSET:
                        case KEY_SET:
                        case KEY_HASH:
                        {
                            g_db->ObjectLen(ctx, current_keytype, k.GetKey().AsString());
                            objectlen = ctx.GetReply().GetInteger();
                            WriteLen(objectlen);
                            //DUMP_CHECK_WRITE(WriteStringObject(v.GetStringValue()));
                            break;
                        }
                        default:
                        {
                            FATAL_LOG("Invalid key type:%d", current_keytype);
                        }
                    }
                    break;
                }
                case KEY_LIST_ELEMENT:
                {
                    if (current_keytype != KEY_LIST || objectlen <= 0)
                    {
                        iter_continue = false;
                        break;
                    }
                    WriteStringObject(v.GetListElement());
                    objectlen--;
                    break;
                }
                case KEY_SET_MEMBER:
                {
                    if (current_keytype != KEY_SET || objectlen <= 0)
                    {
                        iter_continue = false;
                        break;
                    }
                    WriteStringObject(k.GetSetMember());
                    objectlen--;
                    break;
                }
                case KEY_ZSET_SORT:
                {
                    if (current_keytype != KEY_ZSET || objectlen <= 0)
                    {
                        break;
                    }
                    WriteStringObject(k.GetZSetMember());
                    WriteDouble(k.GetZSetScore());
                    objectlen--;
                    break;
                }
                case KEY_HASH_FIELD:
                {
                    if (current_keytype != KEY_HASH || objectlen <= 0)
                    {
                        iter_continue = false;
                        break;
                    }
                    WriteStringObject(k.GetHashField());
                    WriteStringObject(v.GetHashValue());
                    objectlen--;
                    break;
                }
                case KEY_ZSET_SCORE:
                {
                    iter_continue = false;
                    break;
                }
                default:
                {
                    break;
                }
            }
            iter->Next();
        }
        DELETE(iter);

        success = (success && objectlen == 0);
        if (success)
        {
            /* Write the footer, this is how it looks like:
             * ----------------+---------------------+---------------+
             * ... RDB payload | 2 bytes RDB version | 8 bytes CRC64 |
             * ----------------+---------------------+---------------+
             * RDB version and CRC are both in little endian.
             */

            /* RDB version */
            buf[0] = REDIS_RDB_VERSION & 0xff;
            buf[1] = (REDIS_RDB_VERSION >> 8) & 0xff;
            m_buffer.Write(buf, 2);
            //payload->io.buffer.ptr = sdscatlen(payload->io.buffer.ptr,buf,2);

            /* CRC64 */
            crc = crc64(0, (unsigned char*) m_buffer.GetRawReadBuffer(), m_buffer.ReadableBytes());
            memrev64ifbe(&crc);
            m_buffer.Write(&crc, 8);

            content.assign(m_buffer.GetRawBuffer(), m_buffer.ReadableBytes());
        }
        return success;
    }

    Snapshot::Snapshot() :
            m_read_fp(NULL), m_write_fp(NULL), m_cksm(0), m_routine_cb(
            NULL), m_routine_cbdata(
            NULL), m_processed_bytes(0), m_file_size(0), m_state(SNAPSHOT_INVALID), m_routinetime(0), m_read_buf(
            NULL), m_expected_data_size(0), m_writed_data_size(0), m_cached_repl_offset(0), m_cached_repl_cksm(0), m_save_time(
                    0), m_type((SnapshotType) 0), m_engine_snapshot(
            NULL)
    {

    }
    Snapshot::~Snapshot()
    {
        Close();
        DELETE_A(m_read_buf);
    }

    void Snapshot::SetExpectedDataSize(int64 size)
    {
        m_expected_data_size = size;
    }

    int64 Snapshot::DumpLeftDataSize()
    {
        return m_expected_data_size - m_writed_data_size;
    }

    int64 Snapshot::ProcessLeftDataSize()
    {
        return m_expected_data_size - m_processed_bytes;
    }

    void Snapshot::Flush()
    {
        if (NULL != m_write_fp)
        {
            fflush(m_write_fp);
        }
    }

    int Snapshot::Rename(const std::string& default_file)
    {
        if (default_file == GetPath())
        {
            return 0;
        }
        Close();
        int ret = rename(GetPath().c_str(), default_file.c_str());
        if (0 == ret)
        {
            m_file_path = default_file;
        }
        else
        {
            int err = errno;
            ERROR_LOG("Failed to rename %s to %s for reason:%s", GetPath().c_str(), default_file.c_str(),
                    strerror(err));
        }
        return ret;
    }

    void Snapshot::Remove()
    {
        Close();
        if (!m_file_path.empty() && is_file_exist(m_file_path))
        {
            WARN_LOG("Remove snapshot file:%s", m_file_path.c_str());
            file_del(m_file_path);
        }
    }

    void Snapshot::Close()
    {
        if (NULL != m_read_fp)
        {
            fclose(m_read_fp);
            m_read_fp = NULL;
        }
        if (NULL != m_write_fp)
        {
            fclose(m_write_fp);
            m_write_fp = NULL;
        }
        m_write_buffer.Clear();
        m_write_buffer.Compact(256);
        m_cksm = 0;
        m_processed_bytes = 0;
        m_writed_data_size = 0;
        g_engine->ReleaseSnapshot(m_engine_snapshot);
        //delete ((Iterator*) m_snapshot_iter);
        m_engine_snapshot = NULL;
    }

    int Snapshot::SetFilePath(const std::string& path)
    {
        this->m_file_path = path;
        return 0;
    }

    int Snapshot::OpenWriteFile(const std::string& file)
    {
        SetFilePath(file);
        if ((m_write_fp = fopen(m_file_path.c_str(), "w")) == NULL)
        {
            ERROR_LOG("Failed to open ardb dump file:%s to write", m_file_path.c_str());
            return -1;
        }
        m_writed_data_size = 0;
        return 0;
    }

    int Snapshot::OpenReadFile(const std::string& file)
    {
        if (m_file_path != file)
        {
            SetFilePath(file);
        }
        if ((m_read_fp = fopen(m_file_path.c_str(), "r")) == NULL)
        {
            ERROR_LOG("Failed to open ardb dump file:%s to write", m_file_path.c_str());
            return -1;
        }
        struct stat st;
        stat(m_file_path.c_str(), &st);
        m_file_size = st.st_size;
        if (NULL == m_read_buf)
        {
            NEW(m_read_buf, char[kmax_read_buffer_size]);
            setvbuf(m_read_fp, m_read_buf, _IOFBF, kmax_read_buffer_size);
        }
        return 0;
    }

    int Snapshot::Reload(SnapshotRoutine* cb, void *data)
    {
        return Load(m_file_path, cb, data);
    }

    void Snapshot::SetRoutineCallback(SnapshotRoutine* cb, void *data)
    {
        m_routine_cb = cb;
        m_routine_cbdata = data;
    }

    int Snapshot::Load(const std::string& file, SnapshotRoutine* cb, void *data)
    {
        int ret = 0;
        m_routine_cb = cb;
        m_routine_cbdata = data;
        if (NULL != m_routine_cb)
        {
            m_routine_cb(LOAD_START, this, m_routine_cbdata);
        }
        uint64_t start_time = get_current_epoch_millis();
        m_type = GetSnapshotType(file);
        if (m_type == -1)
        {
            ERROR_LOG("Invalid snapshot file:%s", file.c_str());
            return -1;
        }
        if (m_type != BACKUP_DUMP)
        {
            ret = OpenReadFile(file);
            if (0 != ret)
            {
                return ret;
            }
        }
        else
        {
            m_file_path = file;
        }

        if (m_type == REDIS_DUMP)
        {
            ret = RedisLoad();
        }
        else if (m_type == ARDB_DUMP)
        {
            ret = ArdbLoad();
        }
        else if (m_type == BACKUP_DUMP)
        {
            ret = BackupLoad();
        }
        else
        {
            ret = -1;
        }
        if (ret == 0)
        {
            uint64_t cost = get_current_epoch_millis() - start_time;
            INFO_LOG("Cost %.2fs to load snapshot file with type:%s.", cost / 1000.0, type2str(m_type));
        }
        m_state = ret == 0 ? LOAD_SUCCESS : LOAD_FAIL;
        if (NULL != m_routine_cb)
        {
            m_routine_cb(m_state, this, m_routine_cbdata);
        }
        return ret;
    }

    int Snapshot::Write(const void* buf, size_t buflen)
    {

        if (NULL == m_write_fp)
        {
            /*
             * maybe closed while writing
             */
            ERROR_LOG("Snapshot file is closed while writing.");
            return false;
        }
        /*
         * routine callback every 100ms
         */
        if (NULL != m_routine_cb && get_current_epoch_millis() - m_routinetime >= 2)
        {
            int cbret = m_routine_cb(DUMPING, this, m_routine_cbdata);
            if (0 != cbret)
            {
                ERROR_LOG("Routine return error:%d or snapshot file:%s", cbret, m_file_path.c_str());
                return cbret;
            }
            m_routinetime = get_current_epoch_millis();
        }

        if (NULL == m_write_fp)
        {
            ERROR_LOG("Failed to open snapshot file:%s to write", m_file_path.c_str());
            return -1;
        }

        size_t max_write_bytes = 1024 * 1024 * 2;
        const char* data = (const char*) buf;
        while (buflen)
        {
            size_t bytes_to_write = (max_write_bytes < buflen) ? max_write_bytes : buflen;
            if (fwrite(data, bytes_to_write, 1, m_write_fp) == 0)
            {
                ERROR_LOG("Failed to write %u bytes to snapshot file:%s to write", bytes_to_write, m_file_path.c_str());
                return -1;
            }

            m_writed_data_size += bytes_to_write;
            //check sum here
            m_cksm = crc64(m_cksm, (unsigned char *) data, bytes_to_write);
            data += bytes_to_write;
            buflen -= bytes_to_write;
        }
        return 0;
    }

    bool Snapshot::Read(void* buf, size_t buflen, bool cksm)
    {
        if (NULL == m_read_fp)
        {
            /*
             * maybe closed while reading
             */
            ERROR_LOG("Snapshot file is closed while reading.");
            return false;
        }
        if ((m_processed_bytes + buflen) / kloading_process_events_interval_bytes
                > m_processed_bytes / kloading_process_events_interval_bytes)
        {
            INFO_LOG("%llu bytes loaded from dump file.", m_processed_bytes);
        }
        m_processed_bytes += buflen;
        /*
         * routine callback every 2ms(dirty ix), this should be a config,  if it's too long while slave is loading data,
         * the server may close connection.
         */
        if (NULL != m_routine_cb && get_current_epoch_millis() - m_routinetime >= 2)
        {
            int cbret = m_routine_cb(LODING, this, m_routine_cbdata);
            if (0 != cbret)
            {
                return cbret;
            }
            m_routinetime = get_current_epoch_millis();
        }
        size_t max_read_bytes = 1024 * 1024 * 2;
        while (buflen)
        {
            size_t bytes_to_read = (max_read_bytes < buflen) ? max_read_bytes : buflen;
            if (fread(buf, bytes_to_read, 1, m_read_fp) == 0) return false;
            if (cksm)
            {
                //check sum here
                m_cksm = crc64(m_cksm, (unsigned char *) buf, bytes_to_read);
            }
            buf = (char*) buf + bytes_to_read;
            buflen -= bytes_to_read;
        }
        return true;
    }

    void Snapshot::VerifyState()
    {
        if (m_state != SNAPSHOT_INVALID)
        {
            if (!is_file_exist(m_file_path))
            {
                m_state = SNAPSHOT_INVALID;
            }
        }
    }

    int Snapshot::PrepareSave(SnapshotType type, const std::string& file, SnapshotRoutine* cb, void *data)
    {
        int err = 0;
        if (type != BACKUP_DUMP)
        {
            err = OpenWriteFile(file);
        }
        else
        {
            if (!make_dir(file))
            {
                return -1;
            }
            m_file_path = file;
        }
        if (0 != err)
        {
            return err;
        }

        m_state = DUMPING;
        this->m_routine_cb = cb;
        this->m_routine_cbdata = data;
        m_type = type;
        /*
         * todo:
         * We need block all write command for a while to makesure the offset/cksm is represent the snapshot creating
         * 1. block all coming write command
         * 2. wait all wrte command to complete
         * 3. wait all wal log appended
         */
        g_db->CloseWriteLatchBeforeSnapshotPrepare();
        m_cached_repl_offset = g_repl->GetReplLog().WALEndOffset();
        m_cached_repl_cksm = g_repl->GetReplLog().WALCksm();
        m_save_time = time(NULL);
        if (type != BACKUP_DUMP)
        {
            /*
             * Create stable view for later all db iteration, 'm_snapshot_iter' would be close in 'Close' method
             */
            //Context tmpctx;
            //KeyObject start;
            //m_snapshot_iter = g_engine->Find(tmpctx, start);
            m_engine_snapshot = g_engine->CreateSnapshot();
            g_db->OpenWriteLatchAfterSnapshotPrepare();
        }
        return 0;
    }

    int Snapshot::DoSave()
    {
        int ret = 0;
        if (NULL != m_routine_cb)
        {
            m_routine_cb(DUMP_START, this, m_routine_cbdata);
        }
        time_t start = time(NULL);
        g_lastsave_start = start;
        g_saver_num++;
        if (m_type == REDIS_DUMP)
        {
            ret = RedisSave();
        }
        else if (m_type == ARDB_DUMP)
        {
            ret = ArdbSave();
        }
        else if (m_type == BACKUP_DUMP)
        {
            ret = BackupSave();
        }
        else
        {
            return ERR_NOTSUPPORTED;
        }
        Close();
        m_state = ret == 0 ? DUMP_SUCCESS : DUMP_FAIL;
        if (NULL != m_routine_cb)
        {
            m_routine_cb(m_state, this, m_routine_cbdata);
        }
        g_lastsave_err = ret;
        if (0 == ret)
        {
            g_lastsave = time(NULL);
            time_t end = g_lastsave;
            g_lastsave_cost = end - start;
            INFO_LOG("Cost %us to save snapshot file with type:%s.", g_lastsave_cost, type2str(m_type));
        }
        else
        {
            WARN_LOG("Failed to save snapshot file with type:%s", type2str(m_type));
        }
        g_saver_num--;
        if (g_saver_num < 0)
        {
            g_saver_num = 0;
        }
        g_lastsave_start = 0;
        return ret;
    }

    bool Snapshot::IsSaving()
    {
        VerifyState();
        return m_state == DUMPING;
    }

    bool Snapshot::IsReady()
    {
        VerifyState();
        return m_state == DUMP_SUCCESS;
    }

    void Snapshot::MarkDumpComplete()
    {
        m_state = DUMP_SUCCESS;
    }

    int Snapshot::Save(SnapshotType type, const std::string& file, SnapshotRoutine* cb, void *data)
    {
        if (IsSaving())
        {
            return -1;
        }
        int ret = BeforeSave(type, file, cb, data);
        if (0 != ret)
        {
            return ret;
        }

        INFO_LOG("Start to save snapshot file:%s with type:%s.", file.c_str(),
                (m_type == REDIS_DUMP) ? "redis" : "ardb");
        ret = DoSave();
        AfterSave(file, ret);
        return ret;
    }
    int Snapshot::BeforeSave(SnapshotType type, const std::string& file, SnapshotRoutine* cb, void *data)
    {
        char tmpname[1024];
        snprintf(tmpname, sizeof(tmpname) - 1, "%s.%llu.tmp", file.c_str(), get_current_epoch_millis());
        return PrepareSave(type, tmpname, cb, data);
    }
    int Snapshot::AfterSave(const std::string& fname, int err)
    {
        if (0 == err)
        {
            char snapshot_name[1024];
            snprintf(snapshot_name, sizeof(snapshot_name) - 1, "%s.%llu.%llu.%u", fname.c_str(), m_cached_repl_offset,
                    m_cached_repl_cksm, m_save_time);
            Rename(snapshot_name);
        }
        else
        {
            Remove();
        }
        return 0;
    }
    int Snapshot::BGSave(SnapshotType type, const std::string& file, SnapshotRoutine* cb, void *data)
    {
        if (IsSaving())
        {
            ERROR_LOG("There is already a background task saving data.");
            return -1;
        }
        int ret = BeforeSave(type, file, cb, data);
        if (0 != ret)
        {
            return ret;
        }
        struct BGTask: public Thread
        {
                Snapshot* snapshot;
                std::string snapshot_file;
                BGTask(Snapshot* s, const std::string& fname) :
                        snapshot(s), snapshot_file(fname)
                {
                }
                void Run()
                {
                    int ret = snapshot->DoSave();
                    snapshot->AfterSave(snapshot_file, ret);
                    delete this;
                }
        };
        BGTask* task = new BGTask(this, file);
        task->Start();
        return 0;
    }

    std::string Snapshot::GetSyncSnapshotPath(SnapshotType type, uint64 offset, uint64 cksm)
    {
        char tmp[g_db->GetConf().backup_dir.size() + 100];
        uint32 now = time(NULL);
        if (type == BACKUP_DUMP)
        {
            sprintf(tmp, "%s/sync-%s-backup.%llu.%llu.%u", g_db->GetConf().backup_dir.c_str(), g_engine_name, offset,
                    cksm, now);
        }
        else
        {
            sprintf(tmp, "%s/sync-%s-snapshot.%llu.%llu.%u", g_db->GetConf().backup_dir.c_str(), type2str(type), offset,
                    cksm, now);
        }
        return tmp;
    }

    SnapshotType Snapshot::GetSnapshotTypeByName(const std::string& name)
    {
        for (int i = REDIS_DUMP; i <= BACKUP_DUMP; i++)
        {
            if (!strcasecmp(type2str((SnapshotType) i), name.c_str()))
            {
                return (SnapshotType) i;
            }
        }
        return (SnapshotType) -1;
    }

    SnapshotType Snapshot::GetSnapshotType(const std::string& file)
    {
        SnapshotType invalid = (SnapshotType) -1;
        if (is_dir_exist(file))
        {
            return BACKUP_DUMP;
        }
        FILE* fp = NULL;
        if ((fp = fopen(file.c_str(), "r")) == NULL)
        {
            ERROR_LOG("Failed to load redis dump file:%s  %s", file.c_str());
            return invalid;
        }
        char buf[10];
        if (fread(buf, 10, 1, fp) != 1)
        {
            fclose(fp);
            return invalid;
        }
        buf[9] = '\0';
        fclose(fp);
        if (memcmp(buf, "REDIS", 5) == 0)
        {
            return REDIS_DUMP;
        }
        else if (memcmp(buf, "ARDB", 4) == 0)
        {
            return ARDB_DUMP;
        }
        return invalid;
    }

    int Snapshot::RedisLoad()
    {
        DataArray nss;
        char buf[1024];
        int rdbver, type;
        int64 expiretime = -1;
        std::string key;

        Context loadctx;
        loadctx.flags.no_fill_reply = 1;
        loadctx.flags.no_wal = 1;
        loadctx.flags.create_if_notexist = 1;
        loadctx.flags.bulk_loading = 1;
        //BatchWriteGuard guard(tmpctx);
        if (!Read(buf, 9, true))
        {
            ERROR_LOG("Failed to read head.");
            goto eoferr;
        }
        buf[9] = '\0';
        if (memcmp(buf, "REDIS", 5) != 0)
        {
            Close();
            WARN_LOG("Wrong signature:%s trying to load DB from file:%s", buf, m_file_path.c_str());
            return -1;
        }
        rdbver = atoi(buf + 5);
        if (rdbver < 1 || rdbver > REDIS_RDB_VERSION)
        {
            WARN_LOG("Can't handle RDB format version %d", rdbver);
            return -1;
        }
        INFO_LOG("Start loading RDB file with format version:%d", rdbver);
        g_engine->BeginBulkLoad(loadctx);
        while (true)
        {
            expiretime = 0;
            /* Read type. */
            if ((type = ReadType()) == -1) goto eoferr;
            if (type == REDIS_RDB_OPCODE_EXPIRETIME)
            {
                if ((expiretime = ReadTime()) == -1) goto eoferr;
                /* We read the time so we need to read the object type again. */
                if ((type = ReadType()) == -1) goto eoferr;
                /* the EXPIRETIME opcode specifies time in seconds, so convert
                 * into milliseconds. */
                expiretime *= 1000;
            }
            else if (type == REDIS_RDB_OPCODE_EXPIRETIME_MS)
            {
                /* Milliseconds precision expire times introduced with RDB
                 * version 3. */
                if ((expiretime = ReadMillisecondTime()) == -1) goto eoferr;
                /* We read the time so we need to read the object type again. */
                if ((type = ReadType()) == -1) goto eoferr;
            }

            if (type == REDIS_RDB_OPCODE_EOF) break;

            /* Handle SELECT DB opcode as a special case */
            if (type == REDIS_RDB_OPCODE_SELECTDB)
            {
                uint32_t dbid = 0;
                if ((dbid = ReadLen(NULL)) == REDIS_RDB_LENERR)
                {
                    ERROR_LOG("Failed to read current DBID.");
                    goto eoferr;
                }
                loadctx.ns.SetString(stringfromll(dbid), false);
                nss.push_back(loadctx.ns);
                continue;
            }
            else if (type == RDB_OPCODE_RESIZEDB)
            {
                uint32_t db_size, expires_size;
                if ((db_size = ReadLen(NULL)) == REDIS_RDB_LENERR) goto eoferr;
                if ((expires_size = ReadLen(NULL)) == REDIS_RDB_LENERR) goto eoferr;
                //donothing or readed data
                continue;
            }
            else if (type == RDB_OPCODE_AUX)
            {
                std::string auxkey, auxval;

                if (!ReadString(auxkey) || !ReadString(auxval))
                {
                    return false;
                }
                if (auxkey.size() > 0 && auxkey[0] == '%')
                {
                    /* All the fields with a name staring with '%' are considered
                     * information fields and are logged at startup with a log
                     * level of NOTICE. */
                    INFO_LOG("RDB '%s': %s", auxkey.c_str(), auxval.c_str());
                }
                else
                {
                    /* We ignore fields we don't understand, as by AUX field
                     * contract. */
                    DEBUG_LOG("Unrecognized RDB AUX field: '%s' '%s'", key.c_str(), auxval.c_str());
                }
                continue;
            }
            //load key, object

            if (!ReadString(key))
            {
                ERROR_LOG("Failed to read current key.");
                goto eoferr;
            }
            //g_db->RemoveKey(tmpctx, key);
            if (!RedisLoadObject(loadctx, type, key, expiretime))
            {
                ERROR_LOG("Failed to load object:%d", type);
                goto eoferr;
            }
        }

        /* Verify the checksum if RDB version is >= 5 */
        if (rdbver >= 5)
        {
            uint64_t cksum, expected = m_cksm;
            if (!Read(&cksum, 8, true))
            {
                ERROR_LOG("Failed to load cksum.");
                goto eoferr;
            }memrev64ifbe(&cksum);
            if (cksum == 0)
            {
                WARN_LOG("RDB file was saved with checksum disabled: no check performed.");
            }
            else if (cksum != expected)
            {
                ERROR_LOG("Wrong RDB checksum. Aborting now(%llu-%llu)", cksum, expected);
                exit(1);
            }
        }
        Close();
        g_engine->FlushAll(loadctx);
        g_engine->EndBulkLoad(loadctx);
        INFO_LOG("All data load successfully from redis snapshot file.");
        if (g_db->GetConf().compact_after_snapshot_load)
        {
            g_db->CompactAll(loadctx);
        }
        INFO_LOG("Redis snapshot file load finished.");
        return 0;
        eoferr: Close();
        g_engine->EndBulkLoad(loadctx);
        WARN_LOG("Short read or OOM loading DB. Unrecoverable error, aborting now.");
        return -1;
    }
    void* Snapshot::GetIteratorByNamespace(Context& ctx, const Data& ns)
    {
        KeyObject empty;
        empty.SetNameSpace(ns);
        ctx.engine_snapshot = m_engine_snapshot;
        return g_db->GetEngine()->Find(ctx, empty);
    }

    int Snapshot::RedisSave()
    {
        RedisWriteMagicHeader();
        int err = 0;
#define DUMP_CHECK_WRITE(x)  if((err = (x)) < 0) break
        Context dumpctx;
        //dumpctx.flags.iterate_multi_keys = 1;
        DataArray nss;
        g_db->GetEngine()->ListNameSpaces(dumpctx, nss);
        for (size_t i = 0; i < nss.size(); i++)
        {
            /*
             * do not iterate ttl db
             */
            if (nss[i].AsString() == TTL_DB_NSMAESPACE)
            {
                continue;
            }
            dumpctx.ns = nss[i];
            DUMP_CHECK_WRITE(WriteType(REDIS_RDB_OPCODE_SELECTDB));
            int64 dbid = 0;
            string_toint64(nss[i].AsString(), dbid);
            DUMP_CHECK_WRITE(WriteLen(dbid));
            //KeyObject empty;
            //empty.SetNameSpace(nss[i]);
            //Iterator* iter = g_db->GetEngine()->Find(dumpctx, empty);
            Iterator* iter = (Iterator*)GetIteratorByNamespace(dumpctx, nss[i]);
            Data current_key;
            KeyType current_keytype;
            int64 objectlen = 0;
            int64 object_totallen = 0;

            while (iter->Valid())
            {
                KeyObject& k = iter->Key();
                ValueObject& v = iter->Value();
                DEBUG_LOG("Save key/value with type:%u & key:%s", k.GetType(), k.GetKey().AsString().c_str());
                switch (k.GetType())
                {
                    case KEY_META:
                    {
                        if (objectlen != 0)
                        {
                            ERROR_LOG(
                                    "Previous key:%s with type:%u is not complete dump, %lld missing in %lld elements",
                                    current_key.AsString().c_str(), current_keytype, objectlen, object_totallen);
                            err = -2;
                            break;
                        }
                        int64 ttl = v.GetTTL();
                        if (ttl > 0)
                        {
                            DUMP_CHECK_WRITE(WriteType(REDIS_RDB_OPCODE_EXPIRETIME_MS));
                            DUMP_CHECK_WRITE(WriteMillisecondTime(ttl));
                        }
                        current_keytype = (KeyType) v.GetType();
                        DUMP_CHECK_WRITE(WriteKeyType(current_keytype));
                        current_key = k.GetKey();
                        current_key.ToMutableStr();
                        std::string kstr;
                        k.GetKey().ToString(kstr);
                        DUMP_CHECK_WRITE(WriteRawString(kstr.data(), kstr.size()));

                        //printf("###key:%d %s %d \n", current_keytype, k.GetKey().AsString().c_str(), err);
                        switch (current_keytype)
                        {
                            case KEY_STRING:
                            {
                                DUMP_CHECK_WRITE(WriteStringObject(v.GetStringValue()));
                                break;
                            }
                            case KEY_LIST:
                            case KEY_ZSET:
                            case KEY_SET:
                            case KEY_HASH:
                            {
                                g_db->ObjectLen(dumpctx, current_keytype, kstr);
                                objectlen = dumpctx.GetReply().GetInteger();
                                object_totallen = objectlen;
                                DUMP_CHECK_WRITE(WriteLen(objectlen));
                                //DUMP_CHECK_WRITE(WriteStringObject(v.GetStringValue()));
                                break;
                            }
                            default:
                            {
                                FATAL_LOG("Invalid key type:%d", current_keytype);
                            }
                        }
                        break;
                    }
                    case KEY_LIST_ELEMENT:
                    {
                        if (current_key != k.GetKey() || current_keytype != KEY_LIST || objectlen <= 0)
                        {
                            break;
                        }
                        DUMP_CHECK_WRITE(WriteStringObject(v.GetListElement()));
                        objectlen--;
                        break;
                    }
                    case KEY_SET_MEMBER:
                    {
                        if (current_key != k.GetKey() || current_keytype != KEY_SET || objectlen <= 0)
                        {
                            break;
                        }
                        DUMP_CHECK_WRITE(WriteStringObject(k.GetSetMember()));
                        objectlen--;
                        break;
                    }
                    case KEY_ZSET_SORT:
                    {
                        if (current_key != k.GetKey() || current_keytype != KEY_ZSET || objectlen <= 0)
                        {
                            break;
                        }
                        DUMP_CHECK_WRITE(WriteStringObject(k.GetZSetMember()));
                        DUMP_CHECK_WRITE(WriteDouble(k.GetZSetScore()));
                        objectlen--;
                        break;
                    }
                    case KEY_HASH_FIELD:
                    {
                        if (current_key != k.GetKey() || current_keytype != KEY_HASH || objectlen <= 0)
                        {
                            break;
                        }
                        DUMP_CHECK_WRITE(WriteStringObject(k.GetHashField()));
                        DUMP_CHECK_WRITE(WriteStringObject(v.GetHashValue()));
                        objectlen--;
                        break;
                    }
                    case KEY_ZSET_SCORE:
                    {
                        /*
                         * we enabled prefix extractor in rocksdb, which make the jump action not work well if the flag
                         * 'iterate_total_order' not enabled.
                         * If we set  'iterate_total_order' = 1, then whole iteration would be slower than current impl.
                         * Now we just ignore KEY_ZSET_SCORE k/v and move iterator to next.
                         KeyObject next(dumpctx.ns, KEY_END, k.GetKey());
                         iter->Jump(next);
                         continue;
                         */
                        break;
                    }
                    default:
                    {
                        break;
                    }
                }
                if (err < 0)
                {
                    break;
                }
                iter->Next();
            }
            DELETE(iter);
        }
        if (err < 0)
        {
            ERROR_LOG("Failed to write dump file for reason code:%d", err);
            Close();
            return -1;
        }
        WriteType(REDIS_RDB_OPCODE_EOF);
        uint64 cksm = m_cksm;
        memrev64ifbe(&cksm);
        Write(&cksm, sizeof(cksm));
        Flush();
        Close();
        return 0;
    }

    int Snapshot::ArdbSave()
    {
        RETURN_NEGATIVE_EXPR(ArdbWriteMagicHeader());

        Context dumpctx;
        dumpctx.flags.iterate_multi_keys = 1;

        /*
         * aux header
         */
        RETURN_NEGATIVE_EXPR(WriteType(ARDB_OPCODE_AUX));
        RETURN_NEGATIVE_EXPR(WriteRawString("ardb_version"));
        RETURN_NEGATIVE_EXPR(WriteRawString(ARDB_VERSION));
        RETURN_NEGATIVE_EXPR(WriteType(ARDB_OPCODE_AUX));
        RETURN_NEGATIVE_EXPR(WriteRawString("engine"));
        RETURN_NEGATIVE_EXPR(WriteRawString(g_engine_name));
        RETURN_NEGATIVE_EXPR(WriteType(ARDB_OPCODE_AUX));
        RETURN_NEGATIVE_EXPR(WriteRawString("host"));
        char hostname[1024];
        gethostname(hostname, sizeof(hostname));
        RETURN_NEGATIVE_EXPR(WriteRawString(hostname));
        RETURN_NEGATIVE_EXPR(WriteType(ARDB_OPCODE_AUX));
        RETURN_NEGATIVE_EXPR(WriteRawString("create_time"));
        RETURN_NEGATIVE_EXPR(WriteRawString(stringfromll(time(NULL))));

        DataArray nss;
        g_db->GetEngine()->ListNameSpaces(dumpctx, nss);
        for (size_t i = 0; i < nss.size(); i++)
        {
            /*
             * do not iterate ttl db
             */
            if (nss[i].AsString() == TTL_DB_NSMAESPACE)
            {
                continue;
            }
            dumpctx.ns = nss[i];
            RETURN_NEGATIVE_EXPR(WriteType(ARDB_RDB_OPCODE_SELECTDB));
            RETURN_NEGATIVE_EXPR(WriteStringObject(nss[i]));

            //KeyObject empty;
            //empty.SetNameSpace(nss[i]);
            //Iterator* iter = g_db->GetEngine()->Find(dumpctx, empty);
            Iterator* iter = (Iterator*)GetIteratorByNamespace(dumpctx, nss[i]);
            while (iter->Valid())
            {
                int64 ttl = 0;
                if (iter->Key().GetType() == KEY_META)
                {
                    ttl = iter->Value().GetTTL();
                }
                int ret = ArdbSaveRawKeyValue(iter->RawKey(), iter->RawValue(), m_write_buffer, ttl);
                if (0 != ret)
                {
                    DELETE(iter);
                    Close();
                    return ret;
                }
                if (m_write_buffer.ReadableBytes() >= 1024 * 1024)
                {
                    ArdbFlushWriteBuffer(m_write_buffer);
                }
                iter->Next();
            }
            ArdbFlushWriteBuffer(m_write_buffer);
            DELETE(iter);
        }
        WriteType(REDIS_RDB_OPCODE_EOF);
        uint64 cksm = m_cksm;
        memrev64ifbe(&cksm);
        Write(&cksm, sizeof(cksm));
        return 0;
    }

    int Snapshot::ArdbLoad()
    {
        DataArray nss;
        char buf[1024];
        int rdbver, type;
        std::string verstr;
        Context loadctx;
        loadctx.flags.no_fill_reply = 1;
        loadctx.flags.no_wal = 1;
        loadctx.flags.create_if_notexist = 1;
        loadctx.flags.bulk_loading = 1;
        if (!Read(buf, 8, true)) goto eoferr;
        buf[9] = '\0';
        if (memcmp(buf, "ARDB", 4) != 0)
        {
            Close();
            WARN_LOG("Wrong signature:%s trying to load DB from file:%s", buf, m_file_path.c_str());
            return -1;
        }
        verstr.assign(buf + 4, 4);
        rdbver = atoi(verstr.c_str());
        if (rdbver < 1 || rdbver > ARDB_RDB_VERSION)
        {
            WARN_LOG("Can't handle ARDB format version %d", rdbver);
            return -1;
        }
        g_engine->BeginBulkLoad(loadctx);
        while (true)
        {
            /* Read type. */
            if ((type = ReadType()) == -1) goto eoferr;

            if (type == ARDB_RDB_TYPE_EOF) break;

            /* Handle SELECT DB opcode as a special case */
            if (type == ARDB_RDB_OPCODE_SELECTDB)
            {
                std::string ns;
                if (!ReadString(ns))
                {
                    ERROR_LOG("Failed to read selected namespace.");
                    goto eoferr;
                }
                loadctx.ns.SetString(ns, false);
                nss.push_back(loadctx.ns);
            }
            else if (type == ARDB_OPCODE_AUX)
            {
                std::string aux_key, aux_val;
                if (!ReadString(aux_key) || !ReadString(aux_val))
                {
                    ERROR_LOG("Failed to read selected namespace.");
                    goto eoferr;
                }
                INFO_LOG("Snapshot aux info: %s=%s", aux_key.c_str(), aux_val.c_str());
            }
            else if (type == ARDB_RDB_TYPE_CHUNK || type == ARDB_RDB_TYPE_SNAPPY_CHUNK)
            {
                if (0 != ArdbLoadChunk(loadctx, type))
                {
                    ERROR_LOG("Failed to load chunk type:%d.", type);
                    goto eoferr;
                }
            }
            else
            {
                ERROR_LOG("Invalid type:%d.", type);
                goto eoferr;
            }
        }

        if (true)
        {
            /* Verify the checksum if RDB version is >= 5 */
            uint64_t cksum, expected = m_cksm;
            if (!Read(&cksum, 8, true))
            {
                goto eoferr;
            }memrev64ifbe(&cksum);
            if (cksum == 0)
            {
                WARN_LOG("RDB file was saved with checksum disabled: no check performed.");
            }
            else if (cksum != expected)
            {
                ERROR_LOG("Wrong RDB checksum.(%llu-%llu)", cksum, expected);
                //exit(1);
            }
        }

        Close();
        g_engine->FlushAll(loadctx);
        g_engine->EndBulkLoad(loadctx);
        INFO_LOG("All data load successfully from ardb snapshot file.");
        if (g_db->GetConf().compact_after_snapshot_load)
        {
            g_db->CompactAll(loadctx);
        }
        INFO_LOG("Ardb dump file load finished.");
        return 0;
        eoferr: Close();
        g_engine->EndBulkLoad(loadctx);
        WARN_LOG("Short read or OOM loading DB. Unrecoverable error, aborting now.");
        return -1;
    }

    int Snapshot::BackupSave()
    {
        struct BGTask: public Thread
        {
                std::string path;
                int err;
                bool complete;

                BGTask(const std::string& f) :
                        path(f), err(0), complete(false)
                {
                }
                void Run()
                {
                    Context dumpctx;
                    err = g_engine->Backup(dumpctx, path);
                    complete = true;
                }
        };
        BGTask task(m_file_path);
        task.Start();
        /*
         * Wait 1s to make sure the backup operation start,
         * and after backup started, we can open write latch again
         */
        Thread::Sleep(1000);
        g_db->OpenWriteLatchAfterSnapshotPrepare();
        while (!task.complete)
        {

            /*
             * routine callback every 100ms
             */
            if (NULL != m_routine_cb)
            {
                int cbret = m_routine_cb(DUMPING, this, m_routine_cbdata);
                if (0 != cbret)
                {
                    ERROR_LOG("Routine return error:%d or snapshot file:%s", cbret, m_file_path.c_str());
                    break;
                }
                m_routinetime = get_current_epoch_millis();
            }
            Thread::Sleep(100);
        }
        task.Join();
        if (0 == task.err)
        {
            INFO_LOG("Backup dump finished.");
        }
        else
        {
            WARN_LOG("Backup dump failed with err:%d", task.err);
        }
        return task.err;
    }
    int Snapshot::BackupLoad()
    {
        struct BGTask: public Thread
        {
                std::string path;
                int err;
                bool complete;
                BGTask(const std::string& f) :
                        path(f), err(0), complete(false)
                {
                }
                void Run()
                {
                    Context loadctx;
                    err = g_engine->Restore(loadctx, path);
                    complete = true;
                }
        };
        BGTask task(m_file_path);
        task.Start();
        while (!task.complete)
        {
            Thread::Sleep(100);
            /*
             * routine callback every 100ms
             */
            if (NULL != m_routine_cb)
            {
                int cbret = m_routine_cb(LODING, this, m_routine_cbdata);
                if (0 != cbret)
                {
                    ERROR_LOG("Routine return error:%d or snapshot file:%s", cbret, m_file_path.c_str());
                    break;
                }
                m_routinetime = get_current_epoch_millis();
            }
        }
        task.Join();
        if (0 == task.err)
        {
            INFO_LOG("Backup restore finished.");
        }
        else
        {
            WARN_LOG("Backup restore failed with err:%d", task.err);
        }
        return task.err;
    }

    static SnapshotManager g_snapshot_manager_instance;
    SnapshotManager* g_snapshot_manager = &g_snapshot_manager_instance;

    SnapshotManager::SnapshotManager()
    {

    }
    void SnapshotManager::Init()
    {
        std::deque<std::string> fs;
        list_subfiles(g_db->GetConf().backup_dir, fs, true);
        for (size_t i = 0; i < fs.size(); i++)
        {
            AddSnapshot(g_db->GetConf().backup_dir + "/" + fs[i]);
        }
    }

    void SnapshotManager::PrintSnapshotInfo(std::string& str)
    {
        char buffer[1024];
        char tmp[1024];
        LockGuard<ThreadMutexLock> guard(m_snapshots_lock);
        for (size_t i = 0; i < m_snapshots.size(); i++)
        {
            time_t ts = m_snapshots[i]->SaveTime();
            sprintf(buffer, "snapshot%u:type=%s,"
                    "create_time=%s,name=%s\r\n", i, type2str(m_snapshots[i]->GetType()),
                    trim_str(ctime_r(&ts, tmp), " \t\r\n"), get_basename(m_snapshots[i]->GetPath()).c_str());
            str.append(buffer);
        }
    }
    void SnapshotManager::Routine()
    {
        LockGuard<ThreadMutexLock> guard(m_snapshots_lock);
        SnapshotArray::iterator it = m_snapshots.begin();
        while (it != m_snapshots.end())
        {
            Snapshot* s = *it;
            if (s == NULL || (s->CachedReplOffset() < g_repl->GetReplLog().WALStartOffset()))
            {
                if (NULL != s)
                {
                    WARN_LOG("Remove snapshot:%s since it's too old with offset:%llu", s->GetPath().c_str(),
                            s->CachedReplOffset());
                    s->Remove();
                }
                DELETE(s);
                it = m_snapshots.erase(it);
            }
            else
            {
                it++;
            }
        }
        while (m_snapshots.size() > g_db->GetConf().maxsnapshots)
        {
            Snapshot* s = m_snapshots[0];
            if (NULL != s)
            {
                s->Remove();
            }
            WARN_LOG("Remove snapshot:%s since snapshot number exceed limit.", NULL == s? "empty":s->GetPath().c_str());
            m_snapshots.erase(m_snapshots.begin());
        }
    }

    time_t SnapshotManager::LastSave()
    {
        return g_lastsave;
    }
    int SnapshotManager::CurrentSaverNum()
    {
        return g_saver_num;
    }
    time_t SnapshotManager::LastSaveCost()
    {
        return g_lastsave_cost;
    }
    int SnapshotManager::LastSaveErr()
    {
        return g_lastsave_err;
    }
    time_t SnapshotManager::LastSaveStartUnixTime()
    {
        return g_lastsave_start;
    }

    void SnapshotManager::AddSnapshot(const std::string& path)
    {
        std::string fname = get_basename(path);
        std::vector<std::string> ss = split_string(fname, ".");
        if (ss.size() != 4)
        {
            WARN_LOG("Invalid or incomplete snapshot:%s", path.c_str());
            file_del(path);
            return;
        }
        uint64 offset, cksm;
        uint32 create_time = 0;
        if (!string_touint64(ss[1], offset) || !string_touint64(ss[2], cksm) || !string_touint32(ss[3], create_time))
        {
            WARN_LOG("Invalid snapshot:%s", fname.c_str());
            file_del(path);
            return;
        }
        Snapshot* snapshot = NULL;
        NEW(snapshot, Snapshot);
        snapshot->m_cached_repl_offset = offset;
        snapshot->m_cached_repl_cksm = cksm;
        snapshot->m_save_time = create_time;
        snapshot->m_file_path = path;
        snapshot->m_state = DUMP_SUCCESS;
        snapshot->m_type = Snapshot::GetSnapshotType(snapshot->m_file_path);
        INFO_LOG("Add cached snapshot:%s", fname.c_str());
        LockGuard<ThreadMutexLock> guard(m_snapshots_lock);
        m_snapshots.push_back(snapshot);
    }

    Snapshot* SnapshotManager::NewSnapshot(SnapshotType type, bool bgsave, SnapshotRoutine* cb, void *data)
    {
        Snapshot* snapshot = NULL;
        NEW(snapshot, Snapshot);
        char path[1024];
        if (type == BACKUP_DUMP)
        {
            snprintf(path, sizeof(path) - 1, "%s/save-%s-backup", g_db->GetConf().backup_dir.c_str(), g_engine_name);
        }
        else
        {
            snprintf(path, sizeof(path) - 1, "%s/save-%s-snapshot", g_db->GetConf().backup_dir.c_str(), type2str(type));
        }

        if (bgsave)
        {
            if (0 == snapshot->BGSave(type, path, cb, data))
            {
                LockGuard<ThreadMutexLock> guard(m_snapshots_lock);
                m_snapshots.push_back(snapshot);
                return snapshot;
            }
        }
        else
        {
            if (0 == snapshot->Save(type, path, cb, data))
            {
                LockGuard<ThreadMutexLock> guard(m_snapshots_lock);
                m_snapshots.push_back(snapshot);
                return snapshot;
            }
        }
        DELETE(snapshot);
        return NULL;
    }

    Snapshot* SnapshotManager::GetSyncSnapshot(SnapshotType type, SnapshotRoutine* cb, void *data)
    {
        LockGuard<ThreadMutexLock> guard(m_snapshots_lock);
        SnapshotArray::reverse_iterator it = m_snapshots.rbegin();
        while (it != m_snapshots.rend())
        {
            Snapshot* s = *it;
            int64 offset_lag = g_repl->GetReplLog().WALEndOffset() - s->CachedReplOffset();
            if (s->GetType() == type && offset_lag < g_db->GetConf().snapshot_max_lag_offset)
            {
                if (s->IsSaving() || s->IsReady())
                {
                    return s;
                }
            }
            it++;
        }
        Snapshot* snapshot = NULL;
        NEW(snapshot, Snapshot);
        char path[1024];
        if (type == BACKUP_DUMP)
        {
            snprintf(path, sizeof(path) - 1, "%s/master-%s-backup", g_db->GetConf().backup_dir.c_str(), g_engine_name);
        }
        else
        {
            snprintf(path, sizeof(path) - 1, "%s/master-%s-snapshot", g_db->GetConf().backup_dir.c_str(),
                    type2str(type));
        }

        if (0 == snapshot->BGSave(type, path, cb, data))
        {
            m_snapshots.push_back(snapshot);
            return snapshot;
        }
        DELETE(snapshot);
        return NULL;
    }

}

