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

#define D_R (M_PI / 180.0)
#define R_D (180.0 / M_PI)
#define R_MAJOR 6378137.0
#define R_MINOR 6356752.3142
#define RATIO (R_MINOR/R_MAJOR)
#define ECCENT (sqrt(1.0 - (RATIO * RATIO)))
#define COM (0.5 * ECCENT)

namespace ardb
{
    /// @brief The usual PI/180 constant
    static const double DEG_TO_RAD = 0.017453292519943295769236907684886;
    /// @brief Earth's quatratic mean radius for WGS-84
    static const double EARTH_RADIUS_IN_METERS = 6372797.560856;

    static const double MERCATOR_MAX = 20037726.37;
    static const double MERCATOR_MIN = -20037726.37;

    static inline double deg_rad(double ang)
    {
        return ang * D_R;
    }

    static inline double mercator_y(double lat)
    {
        lat = fmin(89.5, fmax(lat, -89.5));
        double phi = deg_rad(lat);
        double sinphi = sin(phi);
        double con = ECCENT * sinphi;
        con = pow((1.0 - con) / (1.0 + con), COM);
        double ts = tan(0.5 * (M_PI * 0.5 - phi)) / con;
        return 0 - R_MAJOR * log(ts);
    }

    static inline double mercator_x(double lon)
    {
        return R_MAJOR * deg_rad(lon);
    }

    static uint8_t estimate_geohash_steps_by_radius(double range_meters)
    {
        uint8_t step = 1;
        double v = range_meters;
        while (v < MERCATOR_MAX)
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

    int GeoHashHelper::GetCoordRange(uint8 coord_type, GeoHashRange& lat_range, GeoHashRange& lon_range)
    {
        switch (coord_type)
        {
            case GEO_WGS84_TYPE:
            {
                lat_range.max = 90.0;
                lat_range.min = -90.0;
                lon_range.max = 180.0;
                lon_range.min = -180.0;
                break;
            }
            case GEO_MERCATOR_TYPE:
            {
                lat_range.max = 20037726.37;
                lat_range.min = -20037726.37;
                lon_range.max = 20037726.37;
                lon_range.min = -20037726.37;
                break;
            }
            default:
            {
                return -1;
            }
        }
        return 0;
    }

    int GeoHashHelper::GetAreasByRadius(uint8 coord_type, double latitude, double longitude, double radius_meters,
            GeoHashBitsSet& results)
    {
        GeoHashRange lat_range, lon_range;
        GeoHashHelper::GetCoordRange(coord_type, lat_range, lon_range);
        double delta_longitude = radius_meters;
        double delta_latitude = radius_meters;
        if (coord_type == GEO_WGS84_TYPE)
        {
            delta_latitude = radius_meters / (111320.0 * cos(latitude));
            delta_longitude = radius_meters / 110540.0;
        }

        double min_lat = latitude - delta_latitude;
        double max_lat = latitude + delta_latitude;
        double min_lon = longitude - delta_longitude;
        double max_lon = longitude + delta_longitude;

        int steps = estimate_geohash_steps_by_radius(radius_meters);
        GeoHashBits hash;
        geohash_encode(&lat_range, &lat_range, latitude, longitude, steps, &hash);
        GeoHashNeighbors neighbors;
        geohash_get_neighbors(&hash, &neighbors);
        GeoHashArea area;
        geohash_decode(&lat_range, &lat_range, &hash, &area);
        results.insert(hash);
        results.insert(neighbors.east);
        results.insert(neighbors.west);
        results.insert(neighbors.north);
        results.insert(neighbors.south);
        results.insert(neighbors.south_east);
        results.insert(neighbors.south_west);
        results.insert(neighbors.north_east);
        results.insert(neighbors.north_west);
        if (area.latitude.min < min_lat)
        {
            results.erase(neighbors.south);
            results.erase(neighbors.south_west);
            results.erase(neighbors.south_east);
        }
        if (area.latitude.max > max_lat)
        {
            results.erase(neighbors.north);
            results.erase(neighbors.north_east);
            results.erase(neighbors.north_west);
        }
        if (area.longitude.min < min_lon)
        {
            results.erase(neighbors.west);
            results.erase(neighbors.south_west);
            results.erase(neighbors.north_west);
        }
        if (area.longitude.max > max_lon)
        {
            results.erase(neighbors.east);
            results.erase(neighbors.south_east);
            results.erase(neighbors.north_east);
        }
        return 0;
    }

    GeoHashFix52Bits GeoHashHelper::Allign52Bits(const GeoHashBits& hash)
    {
        uint64_t bits = hash.bits;
        bits <<= (52 - hash.step * 2);
        return bits;
    }

    static double distanceEarth(double lat1d, double lon1d, double lat2d, double lon2d)
    {
        double lat1r, lon1r, lat2r, lon2r, u, v;
        lat1r = deg_rad(lat1d);
        lon1r = deg_rad(lon1d);
        lat2r = deg_rad(lat2d);
        lon2r = deg_rad(lon2d);
        u = sin(lat2r - lat1r);
        v = sin(lon2r - lon1r);
        return 2.0 * EARTH_RADIUS_IN_METERS * asin(sqrt(u * u + cos(lat1r) * cos(lat2r) * v * v));
    }

    bool GeoHashHelper::GetDistanceSquareIfInRadius(uint8 coord_type, double x1, double y1, double x2, double y2,
            double radius, double& distance)
    {
        if (coord_type == GEO_WGS84_TYPE)
        {
            distance = distanceEarth(y1, x1, y2, x2);
            if (distance > radius)
            {
                return false;
            }
            distance = distance * distance;
        }
        else
        {
            double xx = (x1 - x2) * (x1 - x2);
            double yy = (y1 - y2) * (y1 - y2);
            double dd = xx + yy;
            if (dd > radius * radius)
            {
                return false;
            }
            distance = dd;
        }

        return true;
    }
}

