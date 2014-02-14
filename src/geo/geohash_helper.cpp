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
#include <set>

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

    static uint8_t estimate_geohash_steps_by_radius(double range_meters)
    {
        uint8_t step = 1;
        double v = range_meters;
        while(v < MERCATOR_MAX)
        {
            v *= 2;
            step++;
        }
        step--;
        return step;
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
        double delta_longitude = radius_meters;
        double delta_latitude = radius_meters;

        double min_lat = latitude - delta_latitude;
        double max_lat = latitude + delta_latitude;
        double min_lon = longitude - delta_longitude;
        double max_lon = longitude + delta_longitude;

        int steps = estimate_geohash_steps_by_radius(radius_meters);
        GeoHashBits hash;
        geohash_mercator_encode(latitude, longitude, steps, &hash);
        GeoHashNeighbors neighbors;
        geohash_get_neighbors(&hash, &neighbors);
        GeoHashArea area;
        geohash_decode(&hash, &area);
        results.push_back(hash);

        typedef std::set<uint64_t> Neighbors;
        Neighbors neighbor_hashes;
        neighbor_hashes.insert(neighbors.east.bits);
        neighbor_hashes.insert(neighbors.west.bits);
        neighbor_hashes.insert(neighbors.north.bits);
        neighbor_hashes.insert(neighbors.south.bits);
        neighbor_hashes.insert(neighbors.south_east.bits);
        neighbor_hashes.insert(neighbors.south_west.bits);
        neighbor_hashes.insert(neighbors.north_east.bits);
        neighbor_hashes.insert(neighbors.north_west.bits);
        if (area.latitude.min < min_lat)
        {
            neighbor_hashes.erase(neighbors.south.bits);
            neighbor_hashes.erase(neighbors.south_west.bits);
            neighbor_hashes.erase(neighbors.south_east.bits);
        }
        if (area.latitude.max > max_lat)
        {
            neighbor_hashes.erase(neighbors.north.bits);
            neighbor_hashes.erase(neighbors.north_east.bits);
            neighbor_hashes.erase(neighbors.north_west.bits);
        }
        if (area.longitude.min < min_lon)
        {
            neighbor_hashes.erase(neighbors.west.bits);
            neighbor_hashes.erase(neighbors.south_west.bits);
            neighbor_hashes.erase(neighbors.north_west.bits);
        }
        if (area.longitude.max > max_lon)
        {
            neighbor_hashes.erase(neighbors.east.bits);
            neighbor_hashes.erase(neighbors.south_east.bits);
            neighbor_hashes.erase(neighbors.north_east.bits);
        }
        Neighbors::iterator it = neighbor_hashes.begin();
        while (it != neighbor_hashes.end())
        {
            GeoHashBits h;
            h.bits = *it;
            h.step = hash.step;
            results.push_back(h);
            it++;
        }
        return 0;
    }

    GeoHashFix50Bits GeoHashHelper::GetFix50MercatorGeoHashBits(double latitude, double longitude)
    {
        if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
        {
            latitude = mercator_y(latitude);
            longitude = mercator_x(longitude);
        }
        GeoHashBits hash;
        geohash_mercator_encode(latitude, longitude, 25, &hash);
        return hash.bits;
    }

    GeoHashFix50Bits GeoHashHelper::Allign50Bits(const GeoHashBits& hash)
    {
        uint64_t bits = hash.bits;
        bits <<= (50 - hash.step * 2);
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

