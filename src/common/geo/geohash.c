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
#include <stddef.h>
#include <stdio.h>
#include <math.h>
#include "geohash.h"

/**
 * Hashing works like this:
 * Divide the world into 4 buckets.  Label each one as such:
 *  -----------------
 *  |       |       |
 *  |       |       |
 *  | 0,1   | 1,1   |
 *  -----------------
 *  |       |       |
 *  |       |       |
 *  | 0,0   | 1,0   |
 *  -----------------
 */

int geohash_encode(GeoHashRange lat_range, GeoHashRange lon_range, double latitude, double longitude, uint8_t step,
        GeoHashBits* hash)
{
    if (NULL == hash || step > 32 || step == 0)
    {
        return -1;
    }
    hash->bits = 0;
    hash->step = step;
    uint8_t i = 0;
    if (latitude < lat_range.min || latitude > lat_range.max || longitude < lon_range.min || longitude > lon_range.max)
    {
        return -1;
    }

    for (; i < step; i++)
    {
        uint8_t lat_bit, lon_bit;
        if (lat_range.max - latitude >= latitude - lat_range.min)
        {
            lat_bit = 0;
            lat_range.max = (lat_range.max + lat_range.min) / 2;
        }
        else
        {
            lat_bit = 1;
            lat_range.min = (lat_range.max + lat_range.min) / 2;
        }
        if (lon_range.max - longitude >= longitude - lon_range.min)
        {
            lon_bit = 0;
            lon_range.max = (lon_range.max + lon_range.min) / 2;
        }
        else
        {
            lon_bit = 1;
            lon_range.min = (lon_range.max + lon_range.min) / 2;
        }
        hash->bits <<= 1;
        hash->bits += lon_bit;
        hash->bits <<= 1;
        hash->bits += lat_bit;
    }
    return 0;
}

static inline uint8_t get_bit(uint64_t bits, uint8_t pos)
{
    return (bits >> pos) & 0x01;
}

int geohash_decode(GeoHashRange lat_range, GeoHashRange lon_range, GeoHashBits hash, GeoHashArea* area)
{
    if (NULL == area)
    {
        return -1;
    }
    area->hash = hash;
    uint8_t i = 0;
    area->latitude.min = lat_range.min;
    area->latitude.max = lat_range.max;
    area->longitude.min = lon_range.min;
    area->longitude.max = lon_range.max;
    for (; i < hash.step; i++)
    {
        uint8_t lat_bit, lon_bit;
        lon_bit = get_bit(hash.bits, (hash.step - i) * 2 - 1);
        lat_bit = get_bit(hash.bits, (hash.step - i) * 2 - 2);
        if (lat_bit == 0)
        {
            area->latitude.max = (area->latitude.max + area->latitude.min) / 2;
        }
        else
        {
            area->latitude.min = (area->latitude.max + area->latitude.min) / 2;
        }
        if (lon_bit == 0)
        {
            area->longitude.max = (area->longitude.max + area->longitude.min) / 2;
        }
        else
        {
            area->longitude.min = (area->longitude.max + area->longitude.min) / 2;
        }
    }
    return 0;
}

static inline uint64_t interleave64(uint32_t xlo, uint32_t ylo)
{
    static const uint64_t B[] =
        { 0x5555555555555555, 0x3333333333333333, 0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF, 0x0000FFFF0000FFFF };
    static const unsigned int S[] =
        { 1, 2, 4, 8, 16 };

    uint64_t x = xlo; // Interleave lower  bits of x and y, so the bits of x
    uint64_t y = ylo; // are in the even positions and bits from y in the odd; //https://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN

    // x and y must initially be less than 2**32.

    x = (x | (x << S[4])) & B[4];
    y = (y | (y << S[4])) & B[4];

    x = (x | (x << S[3])) & B[3];
    y = (y | (y << S[3])) & B[3];

    x = (x | (x << S[2])) & B[2];
    y = (y | (y << S[2])) & B[2];

    x = (x | (x << S[1])) & B[1];
    y = (y | (y << S[1])) & B[1];

    x = (x | (x << S[0])) & B[0];
    y = (y | (y << S[0])) & B[0];

    return x | (y << 1);
}

static inline uint64_t deinterleave64(uint64_t interleaved)
{
    static const uint64_t B[] =
        { 0x5555555555555555, 0x3333333333333333, 0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF, 0x0000FFFF0000FFFF,
                0x00000000FFFFFFFF };
    static const unsigned int S[] =
        { 0, 1, 2, 4, 8, 16 };

    uint64_t x = interleaved; ///reverse the interleave process (http://stackoverflow.com/questions/4909263/how-to-efficiently-de-interleave-bits-inverse-morton)
    uint64_t y = interleaved >> 1;

    x = (x | (x >> S[0])) & B[0];
    y = (y | (y >> S[0])) & B[0];

    x = (x | (x >> S[1])) & B[1];
    y = (y | (y >> S[1])) & B[1];

    x = (x | (x >> S[2])) & B[2];
    y = (y | (y >> S[2])) & B[2];

    x = (x | (x >> S[3])) & B[3];
    y = (y | (y >> S[3])) & B[3];

    x = (x | (x >> S[4])) & B[4];
    y = (y | (y >> S[4])) & B[4];

    x = (x | (x >> S[5])) & B[5];
    y = (y | (y >> S[5])) & B[5];

    return x | (y << 32);
}

int geohash_fast_encode(GeoHashRange lat_range, GeoHashRange lon_range, double latitude, double longitude, uint8_t step,
        GeoHashBits* hash)
{
    if (NULL == hash || step > 32 || step == 0)
    {
        return -1;
    }
    hash->bits = 0;
    hash->step = step;
    if (latitude < lat_range.min || latitude > lat_range.max || longitude < lon_range.min || longitude > lon_range.max)
    {
        return -1;
    }

    //the algorithm computes the morton code for the geohash location within the range.
    //this can be done MUCH more efficiently using the following code

    //compute the coordinate in the range 0-1
    double lat_offset = (latitude - lat_range.min) / (lat_range.max - lat_range.min);
    double lon_offset = (longitude - lon_range.min) / (lon_range.max - lon_range.min);

    //convert it to fixed point based on the step size
    lat_offset *= (1LL << step);
    lon_offset *= (1LL << step);

    uint32_t ilato = (uint32_t) lat_offset;
    uint32_t ilono = (uint32_t) lon_offset;

    //interleave the bits to create the morton code.  No branching and no bounding
    hash->bits = interleave64(ilato, ilono);
    return 0;
}
int geohash_fast_decode(GeoHashRange lat_range, GeoHashRange lon_range, GeoHashBits hash, GeoHashArea* area)
{
    if (NULL == area)
    {
        return -1;
    }
    area->hash = hash;
    uint8_t step = hash.step;
    uint64_t xyhilo = deinterleave64(hash.bits); //decode morton code

    double lat_scale = lat_range.max - lat_range.min;
    double lon_scale = lon_range.max - lon_range.min;

    uint32_t ilato = xyhilo;        //get back the original integer coordinates
    uint32_t ilono = xyhilo >> 32;

    /*
     * much faster than 'ldexp'
     */
    area->latitude.min = lat_range.min + (ilato * 1.0 / (1ull << step)) * lat_scale;
    area->latitude.max = lat_range.min + ((ilato + 1) * 1.0 / (1ull << step)) * lat_scale;
    area->longitude.min = lon_range.min + (ilono * 1.0 / (1ull << step)) * lon_scale;
    area->longitude.max = lon_range.min + ((ilono + 1) * 1.0 / (1ull << step)) * lon_scale;

    return 0;
}

static int geohash_move_x(GeoHashBits* hash, int8_t d)
{
    if (d == 0)
        return 0;
    uint64_t x = hash->bits & 0xaaaaaaaaaaaaaaaaLL;
    uint64_t y = hash->bits & 0x5555555555555555LL;

    uint64_t zz = 0x5555555555555555LL >> (64 - hash->step * 2);
    if (d > 0)
    {
        x = x + (zz + 1);
    }
    else
    {
        x = x | zz;
        x = x - (zz + 1);
    }
    x &= (0xaaaaaaaaaaaaaaaaLL >> (64 - hash->step * 2));
    hash->bits = (x | y);
    return 0;
}

static int geohash_move_y(GeoHashBits* hash, int8_t d)
{
    if (d == 0)
        return 0;
    uint64_t x = hash->bits & 0xaaaaaaaaaaaaaaaaLL;
    uint64_t y = hash->bits & 0x5555555555555555LL;

    uint64_t zz = 0xaaaaaaaaaaaaaaaaLL >> (64 - hash->step * 2);
    if (d > 0)
    {
        y = y + (zz + 1);
    }
    else
    {
        y = y | zz;
        y = y - (zz + 1);
    }
    y &= (0x5555555555555555LL >> (64 - hash->step * 2));
    hash->bits = (x | y);
    return 0;
}

int geohash_get_neighbors(GeoHashBits hash, GeoHashNeighbors* neighbors)
{
    geohash_get_neighbor(hash, GEOHASH_NORTH, &neighbors->north);
    geohash_get_neighbor(hash, GEOHASH_EAST, &neighbors->east);
    geohash_get_neighbor(hash, GEOHASH_WEST, &neighbors->west);
    geohash_get_neighbor(hash, GEOHASH_SOUTH, &neighbors->south);
    geohash_get_neighbor(hash, GEOHASH_SOUTH_WEST, &neighbors->south_west);
    geohash_get_neighbor(hash, GEOHASH_SOUTH_EAST, &neighbors->south_east);
    geohash_get_neighbor(hash, GEOHASH_NORT_WEST, &neighbors->north_west);
    geohash_get_neighbor(hash, GEOHASH_NORT_EAST, &neighbors->north_east);
    return 0;
}

int geohash_get_neighbor(GeoHashBits hash, GeoDirection direction, GeoHashBits* neighbor)
{
    if (NULL == neighbor)
    {
        return -1;
    }
    *neighbor = hash;
    switch (direction)
    {
        case GEOHASH_NORTH:
        {
            geohash_move_x(neighbor, 0);
            geohash_move_y(neighbor, 1);
            break;
        }
        case GEOHASH_SOUTH:
        {
            geohash_move_x(neighbor, 0);
            geohash_move_y(neighbor, -1);
            break;
        }
        case GEOHASH_EAST:
        {
            geohash_move_x(neighbor, 1);
            geohash_move_y(neighbor, 0);
            break;
        }
        case GEOHASH_WEST:
        {
            geohash_move_x(neighbor, -1);
            geohash_move_y(neighbor, 0);
            break;
        }
        case GEOHASH_SOUTH_WEST:
        {
            geohash_move_x(neighbor, -1);
            geohash_move_y(neighbor, -1);
            break;
        }
        case GEOHASH_SOUTH_EAST:
        {
            geohash_move_x(neighbor, 1);
            geohash_move_y(neighbor, -1);
            break;
        }
        case GEOHASH_NORT_WEST:
        {
            geohash_move_x(neighbor, -1);
            geohash_move_y(neighbor, 1);
            break;
        }
        case GEOHASH_NORT_EAST:
        {
            geohash_move_x(neighbor, 1);
            geohash_move_y(neighbor, 1);
            break;
        }
        default:
        {
            return -1;
        }
    }

    return 0;
}

GeoHashBits geohash_next_leftbottom(GeoHashBits bits)
{
    GeoHashBits newbits = bits;
    newbits.step++;
    newbits.bits <<= 2;
    return newbits;
}
GeoHashBits geohash_next_rightbottom(GeoHashBits bits)
{
    GeoHashBits newbits = bits;
    newbits.step++;
    newbits.bits <<= 2;
    newbits.bits += 2;
    return newbits;
}
GeoHashBits geohash_next_lefttop(GeoHashBits bits)
{
    GeoHashBits newbits = bits;
    newbits.step++;
    newbits.bits <<= 2;
    newbits.bits += 1;
    return newbits;
}
GeoHashBits geohash_next_righttop(GeoHashBits bits)
{
    GeoHashBits newbits = bits;
    newbits.step++;
    newbits.bits <<= 2;
    newbits.bits += 3;
    return newbits;
}


/*
#include <time.h>
#include <sys/time.h>
uint64_t get_current_epoch_millis()
{
    struct timeval timeValue;
    gettimeofday(&timeValue, NULL);
    uint64_t ret = ((uint64_t) timeValue.tv_sec) * 1000;
    ret += ((timeValue.tv_usec) / 1000);
    return ret;
}

int main()
{
    GeoHashBits hash, fast_hash;
    GeoHashNeighbors neighbors;

    GeoHashRange lat_range, lon_range;
    lat_range.max = 20037726.37;
    lat_range.min = -20037726.37;
    lon_range.max = 20037726.37;
    lon_range.min = -20037726.37;
    double radius = 5000;
    double latitude = 9741705.20;
    double longitude = 5417390.90;

    uint32_t loop = 1000000;
    uint32_t i = 0;
    uint64_t start = get_current_epoch_millis();
    for (i = 0; i < loop; i++)
    {
        geohash_encode(lat_range, lon_range, latitude, longitude, 24, &hash);
    }
    uint64_t end = get_current_epoch_millis();
    printf("Cost %llums to encode\n", end - start);

    start = get_current_epoch_millis();
    for (i = 0; i < loop; i++)
    {
        geohash_fast_encode(lat_range, lon_range, latitude, longitude, 24, &fast_hash);
    }
    end = get_current_epoch_millis();
    printf("Cost %llums to fast encode\n", end - start);

    GeoHashArea area, area1;
    start = get_current_epoch_millis();
    for (i = 0; i < loop; i++)
    {
        geohash_decode(lat_range, lon_range, hash, &area);
    }
    end = get_current_epoch_millis();
    printf("Cost %llums to fast decode\n", end - start);

    start = get_current_epoch_millis();
    for (i = 0; i < loop; i++)
    {
        geohash_fast_decode(lat_range, lon_range, hash, &area1);
    }
    end = get_current_epoch_millis();
    printf("Cost %llums to fast decode\n", end - start);

    return 0;
}
*/
/*
 int main()
 {
 GeoHashBits hash, fast_hash;
 GeoHashNeighbors neighbors;

 GeoHashRange lat_range, lon_range;
 lat_range.max = 20037726.37;
 lat_range.min = -20037726.37;
 lon_range.max = 20037726.37;
 lon_range.min = -20037726.37;
 double radius = 5000;
 double latitude = 9741705.20;
 double longitude = 5417390.90;
 GeoHashBits hash_min, hash_max, hash_lt, hash_gr;
 geohash_encode(lat_range, lon_range, latitude, longitude, 24, &hash);
 geohash_fast_encode(lat_range, lon_range, latitude, longitude, 24, &fast_hash);
 geohash_encode(lat_range, lon_range, latitude - radius, longitude - radius, 13, &hash_min);
 geohash_encode(lat_range, lon_range, latitude + radius, longitude + radius, 13, &hash_max);
 geohash_encode(lat_range, lon_range, latitude + radius, longitude - radius, 13, &hash_lt);
 geohash_encode(lat_range, lon_range, latitude - radius, longitude + radius, 13, &hash_gr);

 printf("## %lld\n", hash.bits);
 printf("## %lld\n", fast_hash.bits);
 geohash_get_neighbors(hash, &neighbors);
 printf("%lld\n", hash.bits);
 printf("%lld\n", neighbors.east.bits);
 printf("%lld\n", neighbors.west.bits);
 printf("%lld\n", neighbors.south.bits);
 printf("%lld\n", neighbors.north.bits);
 printf("%lld\n", neighbors.north_west.bits);
 printf("%lld\n", neighbors.north_east.bits);
 printf("%lld\n", neighbors.south_east.bits);
 printf("%lld\n", neighbors.south_west.bits);

 printf("##%lld\n", hash_min.bits);
 printf("##%lld\n", hash_max.bits);
 printf("##%lld\n", hash_lt.bits);
 printf("##%lld\n", hash_gr.bits);

 //    geohash_encode(&lat_range, &lon_range, 9741705.20, 5417390.90, 13, &hash);
 //    printf("from %lld to %lld \n", hash.bits << 2, (hash.bits+1) << 2);

 GeoHashArea area, area1;
 geohash_decode(lat_range, lon_range, hash, &area);
 geohash_fast_decode(lat_range, lon_range, hash, &area1);
 printf("%.10f %.10f\n", area.latitude.min, area.latitude.max);
 printf("%.10f %.10f\n", area.longitude.min, area.longitude.max);
 printf("%.10f %.10f\n", area1.latitude.min, area1.latitude.max);
 printf("%.10f %.10f\n", area1.longitude.min, area1.longitude.max);

 return 0;
 }
 */
