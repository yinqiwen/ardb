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
#include "geohash.h"

static const double EARTH_RADIUS_IN_METERS = 6372797.560856;

static const double MERCATOR_MAX = 20037508.3427892;
static const double MERCATOR_MIN = -20037508.3427892;

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

static inline int _geo_encode(double latitude, double longitude, uint8_t step, GeoHashBits* hash, uint8_t is_mercator)
{
    if (NULL == hash || step > 32 || step == 0)
    {
        return -1;
    }
    hash->bits = 0;
    hash->step = step;
    uint8_t i = 0;
    GeoHashRange lat_range, lon_range;
    lat_range.min = is_mercator ? MERCATOR_MIN : -90;
    lat_range.max = is_mercator ? MERCATOR_MAX : 90;
    lon_range.min = is_mercator ? MERCATOR_MIN : -180;
    lon_range.max = is_mercator ? MERCATOR_MAX : 180;
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

static inline int _geohash_decode(const GeoHashBits* hash, GeoHashArea* area, uint8_t is_mercator)
{
    if (NULL == hash || NULL == area)
    {
        return -1;
    }
    area->hash = *hash;
    uint8_t i = 0;
    GeoHashRange lat_range, lon_range;
    area->latitude.min = is_mercator ? MERCATOR_MIN : -90;
    area->latitude.max = is_mercator ? MERCATOR_MAX : 90;
    area->longitude.min = is_mercator ? MERCATOR_MIN : -180;
    area->longitude.max = is_mercator ? MERCATOR_MAX : 180;
    for (; i < hash->step; i++)
    {
        uint8_t lat_bit, lon_bit;
        lon_bit = get_bit(hash->bits, (hash->step - i) * 2 - 1);
        lat_bit = get_bit(hash->bits, (hash->step - i) * 2 - 2);
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

int geohash_encode(double latitude, double longitude, uint8_t step, GeoHashBits* hash)
{
    return _geo_encode(latitude, longitude, step, hash, 0);
}
int geohash_decode(const GeoHashBits* hash, GeoHashArea* area)
{
    return _geohash_decode(hash, area, 0);
}

int geohash_mercator_encode(double latitude, double longitude, uint8_t step, GeoHashBits* hash)
{
    return _geo_encode(latitude, longitude, step, hash, 1);
}
int geohash_mercator_decode(const GeoHashBits* hash, GeoHashArea* area)
{
    return _geohash_decode(hash, area, 1);
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

int geohash_get_neighbors(const GeoHashBits* hash, GeoHashNeighbors* neighbors)
{
    neighbors->east = *hash;
    neighbors->west = *hash;
    neighbors->north = *hash;
    neighbors->south = *hash;
    neighbors->south_east = *hash;
    neighbors->south_west = *hash;
    neighbors->north_east = *hash;
    neighbors->north_west = *hash;

    geohash_move_x(&neighbors->east, 1);
    geohash_move_y(&neighbors->east, 0);

    geohash_move_x(&neighbors->west, -1);
    geohash_move_y(&neighbors->west, 0);

    geohash_move_x(&neighbors->south, 0);
    geohash_move_y(&neighbors->south, -1);

    geohash_move_x(&neighbors->north, 0);
    geohash_move_y(&neighbors->north, 1);

    geohash_move_x(&neighbors->north_west, -1);
    geohash_move_y(&neighbors->north_west, 1);

    geohash_move_x(&neighbors->north_east, 1);
    geohash_move_y(&neighbors->north_east, 1);

    geohash_move_x(&neighbors->south_east, 1);
    geohash_move_y(&neighbors->south_east, -1);

    geohash_move_x(&neighbors->south_west, -1);
    geohash_move_y(&neighbors->south_west, -1);

    return 0;
}

/*
int main()
{
    GeoHashBits hash;
    hash.bits = 0b0011;
    hash.step = 2;
    GeoHashNeighbors neighbors;
    geohash_get_neighbors(&hash, &neighbors);
    printf("%x\n", hash.bits);
    printf("%x\n", neighbors.east.bits);
    printf("%x\n", neighbors.west.bits);
    printf("%x\n", neighbors.south.bits);
    printf("%x\n", neighbors.north.bits);
    printf("%x\n", neighbors.north_west.bits);
    printf("%x\n", neighbors.north_east.bits);
    printf("%x\n", neighbors.south_east.bits);
    printf("%x\n", neighbors.south_west.bits);


    geohash_encode(-32.1, 120.3, 4, &hash);
    printf("%x\n", hash.bits);
    GeoHashArea area;
    geohash_decode(&hash, &area);
    printf("%.2f %.2f\n", area.latitude.min, area.latitude.max);
    printf("%.2f %.2f\n", area.longitude.min, area.longitude.max);

    uint8_t step = estimate_geohash_steps_by_radius(1000);
    printf("%d\n", step);
    step = estimate_geohash_steps_by_radius(1);
    printf("%d\n", step);
    return 0;
}
*/
