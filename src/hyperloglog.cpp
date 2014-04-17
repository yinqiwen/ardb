/* hyperloglog.c - Redis HyperLogLog probabilistic cardinality approximation.
 * This file implements the algorithm and the exported Redis commands.
 *
 * Copyright (c) 2014, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "db.hpp"
#include "ardb_server.hpp"

#include <stdint.h>
#include <math.h>

/* The Redis HyperLogLog implementation is based on the following ideas:
 *
 * * The use of a 64 bit hash function as proposed in [1], in order to don't
 *   limited to cardinalities up to 10^9, at the cost of just 1 additional
 *   bit per register.
 * * The use of 16384 6-bit registers for a great level of accuracy, using
 *   a total of 12k per key.
 * * The use of the Redis string data type. No new type is introduced.
 * * No attempt is made to compress the data structure as in [1]. Also the
 *   algorithm used is the original HyperLogLog Algorithm as in [2], with
 *   the only difference that a 64 bit hash function is used, so no correction
 *   is performed for values near 2^32 as in [1].
 *
 * [1] Heule, Nunkesser, Hall: HyperLogLog in Practice: Algorithmic
 *     Engineering of a State of The Art Cardinality Estimation Algorithm.
 *
 * [2] P. Flajolet, Éric Fusy, O. Gandouet, and F. Meunier. Hyperloglog: The
 *     analysis of a near-optimal cardinality estimation algorithm.
 *
 * The representation used by Redis is the following:
 *
 * +--------+--------+--------+------//      //--+----------+------+-----+
 * |11000000|22221111|33333322|55444444 ....     | uint64_t | HYLL | Ver |
 * +--------+--------+--------+------//      //--+----------+------+-----+
 *
 * The 6 bits counters are encoded one after the other starting from the
 * LSB to the MSB, and using the next bytes as needed.
 *
 * At the end of the 16k counters, there is an additional 64 bit integer
 * stored in little endian format with the latest cardinality computed that
 * can be reused if the data structure was not modified since the last
 * computation (this is useful because there are high probabilities that
 * HLLADD operations don't modify the actual data structure and hence the
 * approximated cardinality).
 *
 * After the cached cardinality there are 4 bytes of magic set to the
 * string "HYLL", and a 4 bytes version field that is reserved for
 * future uses and is currently set to 0.
 *
 * When the most significant bit in the most significant byte of the cached
 * cardinality is set, it means that the data structure was modified and
 * we can't reuse the cached value that must be recomputed. */

#define REDIS_HLL_P 14 /* The greater is P, the smaller the error. */
#define REDIS_HLL_REGISTERS (1<<REDIS_HLL_P) /* With P=14, 16384 registers. */
#define REDIS_HLL_P_MASK (REDIS_HLL_REGISTERS-1) /* Mask to index register. */
#define REDIS_HLL_BITS 6 /* Enough to count up to 63 leading zeroes. */
#define REDIS_HLL_REGISTER_MAX ((1<<REDIS_HLL_BITS)-1)
/* Note: REDIS_HLL_SIZE define has a final "+8" since we store a 64 bit
 * integer at the end of the HyperLogLog structure to cache the cardinality. */
#define REDIS_HLL_SIZE ((REDIS_HLL_REGISTERS*REDIS_HLL_BITS+7)/8)+8+8

/* =========================== Low level bit macros ========================= */

/* We need to get and set 6 bit counters in an array of 8 bit bytes.
 * We use macros to make sure the code is inlined since speed is critical
 * especially in order to compute the approximated cardinality in
 * HLLCOUNT where we need to access all the registers at once.
 * For the same reason we also want to avoid conditionals in this code path.
 *
 * +--------+--------+--------+------//
 * |11000000|22221111|33333322|55444444
 * +--------+--------+--------+------//
 *
 * Note: in the above representation the most significant bit (MSB)
 * of every byte is on the left. We start using bits from the LSB to MSB,
 * and so forth passing to the next byte.
 *
 * Example, we want to access to counter at pos = 1 ("111111" in the
 * illustration above).
 *
 * The index of the first byte b0 containing our data is:
 *
 *  b0 = 6 * pos / 8 = 0
 *
 *   +--------+
 *   |11000000|  <- Our byte at b0
 *   +--------+
 *
 * The position of the first bit (counting from the LSB = 0) in the byte
 * is given by:
 *
 *  fb = 6 * pos % 8 -> 6
 *
 * Right shift b0 of 'fb' bits.
 *
 *   +--------+
 *   |11000000|  <- Initial value of b0
 *   |00000011|  <- After right shift of 6 pos.
 *   +--------+
 *
 * Left shift b1 of bits 8-fb bits (2 bits)
 *
 *   +--------+
 *   |22221111|  <- Initial value of b1
 *   |22111100|  <- After left shift of 2 bits.
 *   +--------+
 *
 * OR the two bits, and finally AND with 111111 (63 in decimal) to
 * clean the higher order bits we are not interested in:
 *
 *   +--------+
 *   |00000011|  <- b0 right shifted
 *   |22111100|  <- b1 left shifted
 *   |22111111|  <- b0 OR b1
 *   |  111111|  <- (b0 OR b1) AND 63, our value.
 *   +--------+
 *
 * We can try with a different example, like pos = 0. In this case
 * the 6-bit counter is actually contained in a single byte.
 *
 *  b0 = 6 * pos / 8 = 0
 *
 *   +--------+
 *   |11000000|  <- Our byte at b0
 *   +--------+
 *
 *  fb = 6 * pos % 8 = 0
 *
 *  So we right shift of 0 bits (no shift in practice) and
 *  left shift the next byte of 8 bits, even if we don't use it,
 *  but this has the effect of clearing the bits so the result
 *  will not be affacted after the OR.
 *
 * -------------------------------------------------------------------------
 *
 * Setting the register is a bit more complex, let's assume that 'val'
 * is the value we want to set, already in the right range.
 *
 * We need two steps, in one we need to clear the bits, and in the other
 * we need to bitwise-OR the new bits.
 *
 * Let's try with 'pos' = 1, so our first byte at 'b' is 0,
 *
 * "fb" is 6 in this case.
 *
 *   +--------+
 *   |11000000|  <- Our byte at b0
 *   +--------+
 *
 * To create a AND-mask to clear the bits about this position, we just
 * initialize the mask with the value 63, left shift it of "fs" bits,
 * and finally invert the result.
 *
 *   +--------+
 *   |00111111|  <- "mask" starts at 63
 *   |11000000|  <- "mask" after left shift of "ls" bits.
 *   |00111111|  <- "mask" after invert.
 *   +--------+
 *
 * Now we can bitwise-AND the byte at "b" with the mask, and bitwise-OR
 * it with "val" left-shifted of "ls" bits to set the new bits.
 *
 * Now let's focus on the next byte b1:
 *
 *   +--------+
 *   |22221111|  <- Initial value of b1
 *   +--------+
 *
 * To build the AND mask we start again with the 63 value, right shift
 * it by 8-fb bits, and invert it.
 *
 *   +--------+
 *   |00111111|  <- "mask" set at 2&6-1
 *   |00001111|  <- "mask" after the right shift by 8-fb = 2 bits
 *   |11110000|  <- "mask" after bitwise not.
 *   +--------+
 *
 * Now we can mask it with b+1 to clear the old bits, and bitwise-OR
 * with "val" left-shifted by "rs" bits to set the new value.
 */

/* Note: if we access the last counter, we will also access the b+1 byte
 * that is out of the array, but sds strings always have an implicit null
 * term, so the byte exists, and we can skip the conditional (or the need
 * to allocate 1 byte more explicitly). */

/* Store the value of the register at position 'regnum' into variable 'target'.
 * 'p' is an array of unsigned bytes. */
#define HLL_GET_REGISTER(target,p,regnum) do { \
    uint8_t *_p = (uint8_t*) p; \
    unsigned long _byte = regnum*REDIS_HLL_BITS/8; \
    unsigned long _fb = regnum*REDIS_HLL_BITS&7; \
    unsigned long _fb8 = 8 - _fb; \
    unsigned long b0 = _p[_byte]; \
    unsigned long b1 = _p[_byte+1]; \
    target = ((b0 >> _fb) | (b1 << _fb8)) & REDIS_HLL_REGISTER_MAX; \
} while(0)

/* Set the value of the register at position 'regnum' to 'val'.
 * 'p' is an array of unsigned bytes. */
#define HLL_SET_REGISTER(p,regnum,val) do { \
    uint8_t *_p = (uint8_t*) p; \
    unsigned long _byte = regnum*REDIS_HLL_BITS/8; \
    unsigned long _fb = regnum*REDIS_HLL_BITS&7; \
    unsigned long _fb8 = 8 - _fb; \
    unsigned long _v = val; \
    _p[_byte] &= ~(REDIS_HLL_REGISTER_MAX << _fb); \
    _p[_byte] |= _v << _fb; \
    _p[_byte+1] &= ~(REDIS_HLL_REGISTER_MAX >> _fb8); \
    _p[_byte+1] |= _v >> _fb8; \
} while(0)

namespace ardb
{
    /* ========================= HyperLogLog algorithm  ========================= */

    /* Our hash function is MurmurHash2, 64 bit version.
     * It was modified for Redis in order to provide the same result in
     * big and little endian archs (endian neutral). */
    uint64_t MurmurHash64A(const void * key, int len, unsigned int seed)
    {
        const uint64_t m = 0xc6a4a7935bd1e995;
        const int r = 47;
        uint64_t h = seed ^ (len * m);
        const uint8_t *data = (const uint8_t *) key;
        const uint8_t *end = data + (len - (len & 7));

        while (data != end)
        {
            uint64_t k;

#if (BYTE_ORDER == LITTLE_ENDIAN)
            k = *((uint64_t*) data);
#else
            k = (uint64_t) data[0];
            k |= (uint64_t) data[1] << 8;
            k |= (uint64_t) data[2] << 16;
            k |= (uint64_t) data[3] << 24;
            k |= (uint64_t) data[4] << 32;
            k |= (uint64_t) data[5] << 40;
            k |= (uint64_t) data[6] << 48;
            k |= (uint64_t) data[7] << 56;
#endif

            k *= m;
            k ^= k >> r;
            k *= m;
            h ^= k;
            h *= m;
            data += 8;
        }

        switch (len & 7)
        {
            case 7:
                h ^= (uint64_t) data[6] << 48;
            case 6:
                h ^= (uint64_t) data[5] << 40;
            case 5:
                h ^= (uint64_t) data[4] << 32;
            case 4:
                h ^= (uint64_t) data[3] << 24;
            case 3:
                h ^= (uint64_t) data[2] << 16;
            case 2:
                h ^= (uint64_t) data[1] << 8;
            case 1:
                h ^= (uint64_t) data[0];
                h *= m;
        };

        h ^= h >> r;
        h *= m;
        h ^= h >> r;
        return h;
    }

    /* "Add" the element in the hyperloglog data structure.
     * Actually nothing is added, but the max 0 pattern counter of the subset
     * the element belongs to is incremented if needed.
     *
     * 'registers' is expected to have room for REDIS_HLL_REGISTERS plus an
     * additional byte on the right. This requirement is met by sds strings
     * automatically since they are implicitly null terminated.
     *
     * The function always succeed, however if as a result of the operation
     * the approximated cardinality changed, 1 is returned. Otherwise 0
     * is returned. */
    int hllAdd(uint8_t *registers, unsigned char *ele, size_t elesize)
    {
        uint64_t hash, bit, index;
        uint8_t oldcount, count;

        /* Count the number of zeroes starting from bit REDIS_HLL_REGISTERS
         * (that is a power of two corresponding to the first bit we don't use
         * as index). The max run can be 64-P+1 bits.
         *
         * Note that the final "1" ending the sequence of zeroes must be
         * included in the count, so if we find "001" the count is 3, and
         * the smallest count possible is no zeroes at all, just a 1 bit
         * at the first position, that is a count of 1.
         *
         * This may sound like inefficient, but actually in the average case
         * there are high probabilities to find a 1 after a few iterations. */
        hash = MurmurHash64A(ele, elesize, 0xadc83b19ULL);
        hash |= ((uint64_t) 1 << 63); /* Make sure the loop terminates. */
        bit = REDIS_HLL_REGISTERS; /* First bit not used to address the register. */
        count = 1; /* Initialized to 1 since we count the "00000...1" pattern. */
        while ((hash & bit) == 0)
        {
            count++;
            bit <<= 1;
        }

        /* Update the register if this element produced a longer run of zeroes. */
        index = hash & REDIS_HLL_P_MASK; /* Index a register inside registers. */
        HLL_GET_REGISTER(oldcount, registers, index);
        if (count > oldcount)
        {
            HLL_SET_REGISTER(registers, index, count);
            return 1;
        }
        else
        {
            return 0;
        }
    }

    /* Return the approximated cardinality of the set based on the armonic
     * mean of the registers values. */
    uint64_t hllCount(uint8_t *registers)
    {
        double m = REDIS_HLL_REGISTERS;
        double alpha = 0.7213 / (1 + 1.079 / m);
        double E = 0;
        int ez = 0; /* Number of registers equal to 0. */
        int j;

        /* We precompute 2^(-reg[j]) in a small table in order to
         * speedup the computation of SUM(2^-register[0..i]). */
        static int initialized = 0;
        static double PE[64];
        if (!initialized)
        {
            PE[0] = 1; /* 2^(-reg[j]) is 1 when m is 0. */
            for (j = 1; j < 64; j++)
            {
                /* 2^(-reg[j]) is the same as 1/2^reg[j]. */
                PE[j] = 1.0 / (1ULL << j);
            }
            initialized = 1;
        }

        /* Compute SUM(2^-register[0..i]).
         * Redis default is to use 16384 registers 6 bits each. The code works
         * with other values by modifying the defines, but for our target value
         * we take a faster path with unrolled loops. */
        if (REDIS_HLL_REGISTERS == 16384 && REDIS_HLL_BITS == 6)
        {
            uint8_t *r = registers;
            unsigned long r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15;
            for (j = 0; j < 1024; j++)
            {
                /* Handle 16 registers per iteration. */
                r0 = r[0] & 63;
                if (r0 == 0)
                    ez++;
                r1 = (r[0] >> 6 | r[1] << 2) & 63;
                if (r1 == 0)
                    ez++;
                r2 = (r[1] >> 4 | r[2] << 4) & 63;
                if (r2 == 0)
                    ez++;
                r3 = (r[2] >> 2) & 63;
                if (r3 == 0)
                    ez++;
                r4 = r[3] & 63;
                if (r4 == 0)
                    ez++;
                r5 = (r[3] >> 6 | r[4] << 2) & 63;
                if (r5 == 0)
                    ez++;
                r6 = (r[4] >> 4 | r[5] << 4) & 63;
                if (r6 == 0)
                    ez++;
                r7 = (r[5] >> 2) & 63;
                if (r7 == 0)
                    ez++;
                r8 = r[6] & 63;
                if (r8 == 0)
                    ez++;
                r9 = (r[6] >> 6 | r[7] << 2) & 63;
                if (r9 == 0)
                    ez++;
                r10 = (r[7] >> 4 | r[8] << 4) & 63;
                if (r10 == 0)
                    ez++;
                r11 = (r[8] >> 2) & 63;
                if (r11 == 0)
                    ez++;
                r12 = r[9] & 63;
                if (r12 == 0)
                    ez++;
                r13 = (r[9] >> 6 | r[10] << 2) & 63;
                if (r13 == 0)
                    ez++;
                r14 = (r[10] >> 4 | r[11] << 4) & 63;
                if (r14 == 0)
                    ez++;
                r15 = (r[11] >> 2) & 63;
                if (r15 == 0)
                    ez++;

                /* Additional parens will allow the compiler to optimize the
                 * code more with a loss of precision that is not very relevant
                 * here (floating point math is not commutative!). */
                E += (PE[r0] + PE[r1]) + (PE[r2] + PE[r3]) + (PE[r4] + PE[r5]) + (PE[r6] + PE[r7]) + (PE[r8] + PE[r9])
                        + (PE[r10] + PE[r11]) + (PE[r12] + PE[r13]) + (PE[r14] + PE[r15]);
                r += 12;
            }
        }
        else
        {
            for (j = 0; j < REDIS_HLL_REGISTERS; j++)
            {
                unsigned long reg;

                HLL_GET_REGISTER(reg, registers, j);
                if (reg == 0)
                {
                    ez++;
                    E += 1; /* 2^(-reg[j]) is 1 when m is 0. */
                }
                else
                {
                    E += PE[reg]; /* Precomputed 2^(-reg[j]). */
                }
            }
        }
        /* Muliply the inverse of E for alpha_m * m^2 to have the raw estimate. */
        E = (1 / E) * alpha * m * m;

        /* Use the LINEARCOUNTING algorithm for small cardinalities.
         * For larger values but up to 72000 HyperLogLog raw approximation is
         * used since linear counting error starts to increase. However HyperLogLog
         * shows a strong bias in the range 2.5*16384 - 72000, so we try to
         * compensate for it. */
        if (E < m * 2.5 && ez != 0)
        {
            E = m * log(m / ez); /* LINEARCOUNTING() */
        }
        else if (m == 16384 && E < 72000)
        {
            /* We did polynomial regression of the bias for this range, this
             * way we can compute the bias for a given cardinality and correct
             * according to it. Only apply the correction for P=14 that's what
             * we use and the value the correction was verified with. */
            double bias = 5.9119 * 1.0e-18 * (E * E * E * E) - 1.4253 * 1.0e-12 * (E * E * E)
                    + 1.2940 * 1.0e-7 * (E * E) - 5.2921 * 1.0e-3 * E + 83.3216;
            E -= E * (bias / 100);
        }
        /* We don't apply the correction for E > 1/30 of 2^32 since we use
         * a 64 bit function and 6 bit counters. To apply the correction for
         * 1/30 of 2^64 is not needed since it would require a huge set
         * to approach such a value. */
        return (uint64_t) E;
    }

    /* ========================== HyperLogLog commands ========================== */

    /* An HyperLogLog object is a string with space for 16k 6-bit integers,
     * a cached 64 bit cardinality value, and a 4 byte "magic" and additional
     * 4 bytes for version reserved for future use. */
    void createHLLObject(std::string& str)
    {
        /* Create a string of the right size filled with zero bytes.
         * Note that the cached cardinality is set to 0 as a side effect
         * that is exactly the cardinality of an empty HLL. */
        str.clear();
        str.reserve(REDIS_HLL_SIZE);
        str.resize(REDIS_HLL_SIZE);
        char* ss = &(str[0]);
        memcpy(ss + REDIS_HLL_SIZE - 8, "HYLL", 4);
    }

    /* Check if the object is a String of REDIS_HLL_SIZE bytes.
     * Return REDIS_OK if this is true, otherwise reply to the client
     * with an error and return REDIS_ERR. */
    bool isHLLObjectOrReply(const std::string& str)
    {
        /* If this is a string representing an HLL, the size should match
         * exactly. */
        if (str.size() != REDIS_HLL_SIZE)
        {
            return false;
        }
        return true;
    }

    int Ardb::PFAdd(const DBID& db, const Slice& key, const SliceArray& members)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        std::string value;
        int updated = 0;
        int ret = Get(db, key, value);
        if (0 == ret)
        {
            if (!isHLLObjectOrReply(value))
            {
                return ERR_INVALID_HLL_TYPE;
            }
        }
        else if (ERR_NOT_EXIST == ret)
        {
            createHLLObject(value);
            updated++;
        }
        else
        {
            return ERR_INVALID_TYPE;
        }
        unsigned char* registers = (unsigned char*) (&(value[0]));
        for (uint32 i = 0; i < members.size(); i++)
        {
            if (hllAdd(registers, (unsigned char*) (members[i].data()), members[i].size()))
            {
                updated++;
            }
        }
        if (updated > 0)
        {
            registers[REDIS_HLL_SIZE - 9] |= (1 << 7);
            return Set(db, key, value) == 0 ? 1 : 0;
        }
        return 0;
    }
    static uint64 pfcount_string(std::string& str)
    {
        uint64 card;
        /* Check if the cached cardinality is valid. */
        unsigned char* registers = (unsigned char*) (&(str[0]));
        if ((registers[REDIS_HLL_SIZE - 9] & (1 << 7)) == 0)
        {
            /* Just return the cached value. */
            card = (uint64_t) registers[REDIS_HLL_SIZE - 16];
            card |= (uint64_t) registers[REDIS_HLL_SIZE - 15] << 8;
            card |= (uint64_t) registers[REDIS_HLL_SIZE - 14] << 16;
            card |= (uint64_t) registers[REDIS_HLL_SIZE - 13] << 24;
            card |= (uint64_t) registers[REDIS_HLL_SIZE - 12] << 32;
            card |= (uint64_t) registers[REDIS_HLL_SIZE - 11] << 40;
            card |= (uint64_t) registers[REDIS_HLL_SIZE - 10] << 48;
            card |= (uint64_t) registers[REDIS_HLL_SIZE - 9] << 56;
        }
        else
        {
            /* Recompute it and update the cached value. */
            card = hllCount(registers);
            registers[REDIS_HLL_SIZE - 16] = card & 0xff;
            registers[REDIS_HLL_SIZE - 15] = (card >> 8) & 0xff;
            registers[REDIS_HLL_SIZE - 14] = (card >> 16) & 0xff;
            registers[REDIS_HLL_SIZE - 13] = (card >> 24) & 0xff;
            registers[REDIS_HLL_SIZE - 12] = (card >> 32) & 0xff;
            registers[REDIS_HLL_SIZE - 11] = (card >> 40) & 0xff;
            registers[REDIS_HLL_SIZE - 10] = (card >> 48) & 0xff;
            registers[REDIS_HLL_SIZE - 9] = (card >> 56) & 0xff;
        }
        return card;
    }

    /* PFCOUNT var -> approximated cardinality of set. */
    int Ardb::PFCount(const DBID& db, const Slice& key, uint64& card)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        std::string value;
        int ret = Get(db, key, value);
        if (0 == ret)
        {
            if (!isHLLObjectOrReply(value))
            {
                return ERR_INVALID_HLL_TYPE;
            }
        }
        else if (ERR_NOT_EXIST == ret)
        {
            card = 0;
            return 0;
        }
        else
        {
            return ret;
        }
        /* Check if the cached cardinality is valid. */
        unsigned char* registers = (unsigned char*) (&(value[0]));
        if ((registers[REDIS_HLL_SIZE - 9] & (1 << 7)) == 0)
        {
            /* Just return the cached value. */
            card = (uint64_t) registers[REDIS_HLL_SIZE - 16];
            card |= (uint64_t) registers[REDIS_HLL_SIZE - 15] << 8;
            card |= (uint64_t) registers[REDIS_HLL_SIZE - 14] << 16;
            card |= (uint64_t) registers[REDIS_HLL_SIZE - 13] << 24;
            card |= (uint64_t) registers[REDIS_HLL_SIZE - 12] << 32;
            card |= (uint64_t) registers[REDIS_HLL_SIZE - 11] << 40;
            card |= (uint64_t) registers[REDIS_HLL_SIZE - 10] << 48;
            card |= (uint64_t) registers[REDIS_HLL_SIZE - 9] << 56;
        }
        else
        {
            /* Recompute it and update the cached value. */
            card = hllCount(registers);
            registers[REDIS_HLL_SIZE - 16] = card & 0xff;
            registers[REDIS_HLL_SIZE - 15] = (card >> 8) & 0xff;
            registers[REDIS_HLL_SIZE - 14] = (card >> 16) & 0xff;
            registers[REDIS_HLL_SIZE - 13] = (card >> 24) & 0xff;
            registers[REDIS_HLL_SIZE - 12] = (card >> 32) & 0xff;
            registers[REDIS_HLL_SIZE - 11] = (card >> 40) & 0xff;
            registers[REDIS_HLL_SIZE - 10] = (card >> 48) & 0xff;
            registers[REDIS_HLL_SIZE - 9] = (card >> 56) & 0xff;
            Set(db, key, value);
        }
        return 0;
    }

    int Ardb::PFMerge(const DBID& db, const SliceArray& srckeys, std::string& hllvalue)
    {
        uint8_t max[REDIS_HLL_REGISTERS];
        uint8_t *registers;

        /* Compute an HLL with M[i] = MAX(M[i]_j).
         * We we the maximum into the max array of registers. We'll write
         * it to the target variable later. */
        memset(max, 0, sizeof(max));
        for (uint32 j = 0; j < srckeys.size(); j++)
        {
            uint8_t val;
            std::string value;
            int ret = Get(db, srckeys[j], value);
            /* Check type and size. */
            if (0 == ret)
            {
                if (!isHLLObjectOrReply(value))
                {
                    return ERR_INVALID_HLL_TYPE;
                }
            }
            else if (ERR_INVALID_TYPE == ret)
            {
                return ret;
            }
            else if (ERR_NOT_EXIST == ret)
            {
                continue;
            }
            else
            {
                //no such error
                abort();
            }
            /* Merge with this HLL with our 'max' HHL by setting max[i]
             * to MAX(max[i],hll[i]). */
            registers = (uint8_t*) (&(value[0]));
            for (uint32 i = 0; i < REDIS_HLL_REGISTERS; i++)
            {
                HLL_GET_REGISTER(val, registers, i);
                if (val > max[i])
                    max[i] = val;
            }
        }
        createHLLObject(hllvalue);
        registers = (uint8_t*) (&(hllvalue[0]));
        for (uint32 j = 0; j < REDIS_HLL_REGISTERS; j++)
        {
            HLL_SET_REGISTER(registers, j, max[j]);
        }
        registers[REDIS_HLL_SIZE - 9] |= (1 << 7);
        return 0;
    }

    int Ardb::PFMerge(const DBID& db, const Slice& destkey, const SliceArray& srckeys)
    {
        std::string value, hllvalue;
        int ret = PFMerge(db, srckeys, hllvalue);
        if (0 != ret)
        {
            return ret;
        }
        ret = Get(db, destkey, value);
        if (0 == ret || ERR_NOT_EXIST == ret)
        {
            return Set(db, destkey, hllvalue);
        }
        else
        {
            return ret;
        }
    }

    int Ardb::PFMergeCount(const DBID& db, const SliceArray& srckeys, uint64& v)
    {
        std::string hllvalue;
        int ret = PFMerge(db, srckeys, hllvalue);
        if (0 != ret)
        {
            return ret;
        }
        v = pfcount_string(hllvalue);
        return 0;
    }

    /* PFADD var ele ele ele ... ele => :0 or :1 */
    int ArdbServer::PFAdd(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        std::string value;
        const std::string& key = cmd.GetArguments()[0];
        SliceArray members;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            members.push_back(cmd.GetArguments()[i]);
        }

        int ret = m_db->PFAdd(ctx.currentDB, key, members);
        if (ret >= 0)
        {
            fill_int_reply(ctx.reply, ret);
            return 0;
            if (!isHLLObjectOrReply(value))
            {
                fill_fix_error_reply(ctx.reply, "WRONGTYPE Key is not a valid HyperLogLog string value.");
                return 0;
            }
        }
        else if (ERR_INVALID_HLL_TYPE == ret)
        {
            fill_fix_error_reply(ctx.reply, "WRONGTYPE Key is not a valid HyperLogLog string value.");
        }
        else
        {
            fill_error_reply(ctx.reply, "Operation against a key holding the wrong kind of value");
        }
        return 0;

    }
    /* PFCOUNT var -> approximated cardinality of set. */
    int ArdbServer::PFCount(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        uint64 card;
        int ret = m_db->PFCount(ctx.currentDB, cmd.GetArguments()[0], card);
        if (ERR_INVALID_HLL_TYPE == ret)
        {
            fill_fix_error_reply(ctx.reply, "WRONGTYPE Key is not a valid HyperLogLog string value.");
        }
        else
        {
            fill_int_reply(ctx.reply, (int64) card);
        }
        return 0;
    }

    int ArdbServer::PFMerge(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        SliceArray srckeys;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            srckeys.push_back(cmd.GetArguments()[i]);
        }
        int ret = m_db->PFMerge(ctx.currentDB, cmd.GetArguments()[0], srckeys);
        if (0 == ret)
        {
            fill_status_reply(ctx.reply, "OK");
        }
        else if (ERR_INVALID_HLL_TYPE == ret)
        {
            fill_fix_error_reply(ctx.reply, "WRONGTYPE Key is not a valid HyperLogLog string value.");
        }
        else
        {
            fill_error_reply(ctx.reply, "Operation against a key holding the wrong kind of value");
        }
        return 0;
    }

    int ArdbServer::PFMergeCount(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        SliceArray srckeys;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            srckeys.push_back(cmd.GetArguments()[i]);
        }
        uint64 card;
        int ret = m_db->PFMergeCount(ctx.currentDB, srckeys, card);
        if (0 == ret)
        {
            fill_int_reply(ctx.reply, (int64)card);
        }
        else if (ERR_INVALID_HLL_TYPE == ret)
        {
            fill_fix_error_reply(ctx.reply, "WRONGTYPE Key is not a valid HyperLogLog string value.");
        }
        else
        {
            fill_error_reply(ctx.reply, "Operation against a key holding the wrong kind of value");
        }
        return 0;
    }

}
