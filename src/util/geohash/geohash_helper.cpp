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
#include "geohash_helper.hpp"
#include <math.h>
#include <assert.h>

namespace ardb
{
    /// @brief The usual PI/180 constant
    static const double DEG_TO_RAD = 0.017453292519943295769236907684886;
    /// @brief Earth's quatratic mean radius for WGS-84
    static const double EARTH_RADIUS_IN_METERS = 6372797.560856;

    static const double MERCATOR_MAX = 20037508.3427892;
    static const double MERCATOR_MIN = -20037508.3427892;

    static inline double mercator_y(double latitude)
    {
        return log(tan(M_PI / 4.0 + (latitude * M_PI / 360))) * EARTH_RADIUS_IN_METERS;
    }

    static inline double mercator_x(double longtitude)
    {
        return longtitude * EARTH_RADIUS_IN_METERS * M_PI / 180;
    }

    static const char BASE32_DECODE_TABLE[44] =
        {
        /* 0 */0, /* 1 */1, /* 2 */2, /* 3 */3, /* 4 */4,
        /* 5 */5, /* 6 */6, /* 7 */7, /* 8 */8, /* 9 */9,
        /* : */-1, /* ; */-1, /* < */-1, /* = */-1, /* > */-1,
        /* ? */-1, /* @ */-1, /* A */-1, /* B */10, /* C */11,
        /* D */12, /* E */13, /* F */14, /* G */15, /* H */16,
        /* I */-1, /* J */17, /* K */18, /* L */-1, /* M */19,
        /* N */20, /* O */-1, /* P */21, /* Q */22, /* R */23,
        /* S */24, /* T */25, /* U */26, /* V */27, /* W */28,
        /* X */29, /* Y */30, /* Z */31 };

    static inline double ArcInRadians(const Position& from, const Position& to)
    {
        double latitudeArc = (from.lat - to.lat) * DEG_TO_RAD;
        double longitudeArc = (from.lon - to.lon) * DEG_TO_RAD;
        double latitudeH = sin(latitudeArc * 0.5);
        latitudeH *= latitudeH;
        double lontitudeH = sin(longitudeArc * 0.5);
        lontitudeH *= lontitudeH;
        double tmp = cos(from.lat * DEG_TO_RAD) * cos(to.lat * DEG_TO_RAD);
        return 2.0 * asin(sqrt(latitudeH + tmp * lontitudeH));
    }

    /** @brief Computes the distance, in meters, between two WGS-84 positions.
     *
     * The result is equal to <code>EARTH_RADIUS_IN_METERS*ArcInRadians(from,to)</code>
     *
     * @sa ArcInRadians
     */
    static inline double DistanceInMeters(const Position& from, const Position& to)
    {
        return EARTH_RADIUS_IN_METERS * ArcInRadians(from, to);
    }

    static int estimate_geohash_length_by_radius(double range_meters)
    {
        /*
         * http://stackoverflow.com/questions/13836416/geohash-and-max-distance
         * Precision, Distance of Adjacent Cell in Meters
         * 1, 5003530
         * 2, 625441
         * 3, 123264
         * 4, 19545
         * 5, 3803
         * 6, 610
         * 7, 118
         * 8, 19
         * 9, 3.71
         * 10, 0.6
         */
        if (range_meters < 0 || range_meters > 5003530)
        {
            return -1;
        }
        if (range_meters > 625441)
        {
            return 1;
        }
        if (range_meters >= 123264)
        {
            return 2;
        }
        if (range_meters >= 19545)
        {
            return 3;
        }
        if (range_meters >= 3803)
        {
            return 4;
        }
        if (range_meters >= 610)
        {
            return 5;
        }
        if (range_meters >= 118)
        {
            return 6;
        }
        if (range_meters >= 19)
        {
            return 7;
        }
        if (range_meters >= 3.71)
        {
            return 8;
        }
        if (range_meters >= 0.6)
        {
            return 9;
        }
        /*
         * max precise length 10
         */
        return 10;
    }

    static inline uint64 get_var_geohash_bits(const char* hash)
    {
        uint64 bits = 0;
        int i = 0;
        for (; hash[i] != 0; i++)
        {
            char c = toupper(hash[i]);
            c -= 0x30;
            uint32 tmp = BASE32_DECODE_TABLE[(uint8) c];
            if (i > 0)
            {
                bits <<= 5;
            }
            bits += tmp;
        }

        return bits;
    }

    double GeoHashHelper::GetMercatorX(double longtitude)
    {
        if (longtitude > 180 || longtitude < -180)
        {
            return longtitude;
        }
        return mercator_x(longtitude);
    }
    double GeoHashHelper::GetMercatorY(double latitude)
    {
        if (latitude > 90 || latitude < -90)
        {
            return latitude;
        }
        return mercator_y(latitude);
    }

    int GeoHashHelper::GetAreasByRadius(double latitude, double longitude, double radius_meters,
            GeoHashBitsArray& results)
    {
        if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
        {
            latitude = mercator_y(latitude);
            longitude = mercator_x(longitude);
        }
        /*
         * compute max delta degree by radius(meters)
         */

//        double delta_longitude = radius_meters / (111320 * cos(latitude));
//        double delta_latitude = radius_meters / 110540.0;
        double delta_longitude = radius_meters;
        double delta_latitude = radius_meters;

        double min_lat = latitude - delta_latitude;
        double max_lat = latitude + delta_latitude;
        double min_lon = longitude - delta_longitude;
        double max_lon = longitude + delta_longitude;

        int hash_len = estimate_geohash_length_by_radius(radius_meters);
        char* hash = GEOHASH_encode_mercator(latitude, longitude, hash_len);
        results.push_back(GetFix50GeoHashBitsByHashString(hash));
        GEOHASH_neighbors* neighbors = GEOHASH_get_neighbors(hash);
        GEOHASH_area* area = GEOHASH_decode_mercator(hash);
        typedef TreeSet<const char*>::Type Neighbors;
        Neighbors neighbor_hashes;
        neighbor_hashes.insert(neighbors->east);
        neighbor_hashes.insert(neighbors->west);
        neighbor_hashes.insert(neighbors->north);
        neighbor_hashes.insert(neighbors->south);
        neighbor_hashes.insert(neighbors->south_east);
        neighbor_hashes.insert(neighbors->south_west);
        neighbor_hashes.insert(neighbors->north_east);
        neighbor_hashes.insert(neighbors->north_west);
        if (area->latitude.min < min_lat)
        {
            neighbor_hashes.erase(neighbors->west);
            neighbor_hashes.erase(neighbors->north_west);
            neighbor_hashes.erase(neighbors->south_west);
        }
        if (area->latitude.max > max_lat)
        {
            neighbor_hashes.erase(neighbors->east);
            neighbor_hashes.erase(neighbors->north_east);
            neighbor_hashes.erase(neighbors->south_east);
        }
        if (area->longitude.min < min_lon)
        {
            neighbor_hashes.erase(neighbors->south);
            neighbor_hashes.erase(neighbors->south_west);
            neighbor_hashes.erase(neighbors->south_east);
        }
        if (area->latitude.max > max_lon)
        {
            neighbor_hashes.erase(neighbors->north);
            neighbor_hashes.erase(neighbors->north_west);
            neighbor_hashes.erase(neighbors->north_east);
        }
        Neighbors::iterator it = neighbor_hashes.begin();
        while (it != neighbor_hashes.end())
        {
            results.push_back(GetFix50GeoHashBitsByHashString(*it));

            it++;
        }
        GEOHASH_free_area(area);
        GEOHASH_free_neighbors(neighbors);
        free(hash);
        return hash_len * 5;
    }

    GeoHashFix50Bits GeoHashHelper::GetFix50MercatorGeoHashBits(double latitude, double longitude)
    {
        if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
        {
            latitude = mercator_y(latitude);
            longitude = mercator_x(longitude);
        }
        char* hash = GEOHASH_encode_mercator(latitude, longitude, 10);
        GeoHashFix50Bits bits = 0;
        for (int i = 0; hash[i] != 0; i++)
        {
            char c = toupper(hash[i]);
            c -= 0x30;
            uint8 tmp = BASE32_DECODE_TABLE[(uint8) c];
            if (i > 0)
            {
                bits <<= 5;
            }
            bits += tmp;
        }
        return bits;
    }
    GeoHashVarBits GeoHashHelper::GetVarGeoHashBitsByHashString(const char* hash)
    {
        return get_var_geohash_bits(hash);
    }
    GeoHashFix50Bits GeoHashHelper::GetFix50GeoHashBitsByHashString(const char* hash)
    {
        uint64 bits = 0;
        int i = 0;
        for (; hash[i] != 0; i++)
        {
            char c = toupper(hash[i]);
            c -= 0x30;
            uint8 tmp = BASE32_DECODE_TABLE[(uint8) c];
            if (i > 0)
            {
                bits <<= 5;
            }
            bits += tmp;
        }
        for (; i < 10; i++)
        {
            bits <<= 5;
        }
        return bits;
    }

    bool GeoHashHelper::GetDistanceIfInRadius(double x1, double y1, double x2, double y2, double radius,
            double& distance)
    {
        double xx = (x1 - x2) * (x1 - x2);
        double yy = (y1 - y2) * (y1 - y2);
        double dd = xx + yy;
        if (dd > radius * radius)
        {
            return false;
        }
        distance = sqrt(dd);
        return true;
    }

}

