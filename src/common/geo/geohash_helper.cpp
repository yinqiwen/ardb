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
#include <complex>

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

    static inline double rad_deg(double ang)
    {
        return ang * R_D;
    }

    static inline double merc_lon(double x)
    {
        return rad_deg(x) / R_MAJOR;
    }

    static inline double merc_lat(double y)
    {
        double ts = exp(-y / R_MAJOR);
        double phi = M_PI_2 - 2 * atan(ts);
        double dphi = 1.0;
        int i;
        for (i = 0; fabs(dphi) > 0.000000001 && i < 15; i++)
        {
            double con = ECCENT * sin(phi);
            dphi = M_PI_2 - 2 * atan(ts * pow((1.0 - con) / (1.0 + con), COM)) - phi;
            phi += dphi;
        }
        return rad_deg(phi);
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
    static uint8_t estimate_geohash_steps_by_radius_lat(double range_meters, double lat)
    {
        uint8_t step = 1;
        while (range_meters < MERCATOR_MAX)
        {
            range_meters *= 2;
            step++;
        }
        step -= 2; /* Make sure range is included in the worst case. */
        /* Wider range torwards the poles... Note: it is possible to do better
         * than this approximation by computing the distance between meridians
         * at this latitude, but this does the trick for now. */
        if (lat > 66 || lat < -66)
            step--;
        if (lat > 80 || lat < -80)
            step--;

        /* Frame to valid range. */
        if (step < 1)
            step = 1;
        if (step > 26)
            step = 25;
        return step;
    }

    double GeoHashHelper::GetWGS84X(double x)
    {
        return merc_lon(x);
    }
    double GeoHashHelper::GetWGS84Y(double y)
    {
        return merc_lat(y);
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
                lat_range.max = 85.05113;   //
                lat_range.min = -85.05113;  //
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

    int GeoHashHelper::GetAreasByRadiusV2(uint8 coord_type, double latitude, double longitude, double radius_meters, GeoHashBitsSet& results)
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
        geohash_fast_encode(lat_range, lat_range, latitude, longitude, steps, &hash);

        GeoHashArea area;
        geohash_fast_decode(lat_range, lat_range, hash, &area);
        results.insert(hash);

        double range_lon = (area.longitude.max - area.longitude.min) / 2;
        double range_lat = (area.latitude.max - area.latitude.min) / 2;

        GeoHashNeighbors neighbors;

        bool split_east = false;
        bool split_west = false;
        bool split_north = false;
        bool split_south = false;
        if (max_lon > area.longitude.max)
        {
            geohash_get_neighbor(hash, GEOHASH_EAST, &(neighbors.east));
            if (area.longitude.max + range_lon > max_lon)
            {
                results.insert(geohash_next_leftbottom(neighbors.east));
                results.insert(geohash_next_lefttop(neighbors.east));
                split_east = true;
            }
            else
            {
                results.insert(neighbors.east);
            }
        }
        if (min_lon < area.longitude.min)
        {
            geohash_get_neighbor(hash, GEOHASH_WEST, &neighbors.west);
            if (area.longitude.min - range_lon < min_lon)
            {
                results.insert(geohash_next_rightbottom(neighbors.west));
                results.insert(geohash_next_righttop(neighbors.west));
                split_west = true;
            }
            else
            {
                results.insert(neighbors.west);
            }
        }
        if (max_lat > area.latitude.max)
        {
            geohash_get_neighbor(hash, GEOHASH_NORTH, &neighbors.north);
            if (area.latitude.max + range_lat > max_lat)
            {
                results.insert(geohash_next_rightbottom(neighbors.north));
                results.insert(geohash_next_leftbottom(neighbors.north));
                split_north = true;
            }
            else
            {
                results.insert(neighbors.north);
            }
        }
        if (min_lat < area.latitude.min)
        {
            geohash_get_neighbor(hash, GEOHASH_SOUTH, &neighbors.south);

            if (area.latitude.min - range_lat < min_lat)
            {
                results.insert(geohash_next_righttop(neighbors.south));
                results.insert(geohash_next_lefttop(neighbors.south));
                split_south = true;
            }
            else
            {
                results.insert(neighbors.south);
            }
        }

        if (max_lon > area.longitude.max && max_lat > area.latitude.max)
        {

            geohash_get_neighbor(hash, GEOHASH_NORT_EAST, &neighbors.north_east);
            if (split_north && split_east)
            {
                results.insert(geohash_next_leftbottom(neighbors.north_east));
            }
            else if (split_north)
            {
                results.insert(geohash_next_rightbottom(neighbors.north_east));
                results.insert(geohash_next_leftbottom(neighbors.north_east));
            }
            else if (split_east)
            {
                results.insert(geohash_next_leftbottom(neighbors.north_east));
                results.insert(geohash_next_lefttop(neighbors.north_east));
            }
            else
            {
                results.insert(neighbors.north_east);
            }
        }
        if (max_lon > area.longitude.max && min_lat < area.latitude.min)
        {
            geohash_get_neighbor(hash, GEOHASH_SOUTH_EAST, &neighbors.south_east);
            if (split_south && split_east)
            {
                results.insert(geohash_next_lefttop(neighbors.south_east));
            }
            else if (split_south)
            {
                results.insert(geohash_next_righttop(neighbors.south_east));
                results.insert(geohash_next_lefttop(neighbors.south_east));
            }
            else if (split_east)
            {
                results.insert(geohash_next_leftbottom(neighbors.south_east));
                results.insert(geohash_next_lefttop(neighbors.south_east));
            }
            else
            {
                results.insert(neighbors.south_east);
            }
        }
        if (min_lon < area.longitude.min && max_lat > area.latitude.max)
        {
            geohash_get_neighbor(hash, GEOHASH_NORT_WEST, &neighbors.north_west);
            if (split_north && split_west)
            {
                results.insert(geohash_next_rightbottom(neighbors.north_west));
            }
            else if (split_north)
            {
                results.insert(geohash_next_rightbottom(neighbors.north_west));
                results.insert(geohash_next_leftbottom(neighbors.north_west));
            }
            else if (split_west)
            {
                results.insert(geohash_next_rightbottom(neighbors.north_west));
                results.insert(geohash_next_righttop(neighbors.north_west));
            }
            else
            {
                results.insert(neighbors.north_west);
            }
        }
        if (min_lon < area.longitude.min && min_lat < area.latitude.min)
        {
            geohash_get_neighbor(hash, GEOHASH_SOUTH_WEST, &neighbors.south_west);
            if (split_south && split_west)
            {
                results.insert(geohash_next_righttop(neighbors.south_west));
            }
            else if (split_south)
            {
                results.insert(geohash_next_righttop(neighbors.south_west));
                results.insert(geohash_next_lefttop(neighbors.south_west));
            }
            else if (split_west)
            {
                results.insert(geohash_next_rightbottom(neighbors.south_west));
                results.insert(geohash_next_righttop(neighbors.south_west));
            }
            else
            {
                results.insert(neighbors.south_west);
            }
        }
        return 0;
    }

    int GeoHashHelper::GetAreasByRadius(uint8 coord_type, double latitude, double longitude, double radius_meters, GeoHashBitsSet& results)
    {
        GeoHashRange lat_range, lon_range;
        GeoHashHelper::GetCoordRange(coord_type, lat_range, lon_range);
        double delta_longitude = radius_meters;
        double delta_latitude = radius_meters;

        double min_lat = latitude - delta_latitude;
        double max_lat = latitude + delta_latitude;
        double min_lon = longitude - delta_longitude;
        double max_lon = longitude + delta_longitude;
        int steps;
        if (coord_type == GEO_WGS84_TYPE)
        {
            double lonr, latr;
            lonr = deg_rad(longitude);
            latr = deg_rad(latitude);

            double distance = radius_meters / EARTH_RADIUS_IN_METERS;
            double min_latitude = latr - distance;
            double max_latitude = latr + distance;

            double min_longitude, max_longitude;
            double difference_longitude = asin(sin(distance) / cos(latr));
            min_longitude = lonr - difference_longitude;
            max_longitude = lonr + difference_longitude;

            min_lon = rad_deg(min_longitude);
            min_lat = rad_deg(min_latitude);
            max_lon = rad_deg(max_longitude);
            max_lat = rad_deg(max_latitude);
            steps = estimate_geohash_steps_by_radius_lat(radius_meters, latitude);
        }
        else
        {
            steps = estimate_geohash_steps_by_radius(radius_meters);
        }

        GeoHashBits hash;
        geohash_fast_encode(lat_range, lon_range, latitude, longitude, steps, &hash);

        GeoHashArea area;
        geohash_fast_decode(lat_range, lon_range, hash, &area);
        results.insert(hash);

        GeoHashNeighbors neighbors;
        geohash_get_neighbors(hash, &neighbors);

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

    uint64 GeoHashHelper::AllignHashBits(uint8 step, const GeoHashBits& hash)
    {
        uint64_t bits = hash.bits;
        bits <<= (step * 2 - hash.step * 2);
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

    double GeoHashHelper::GetWGS84Distance(double lon1d, double lat1d, double lon2d, double lat2d)
    {
        double lat1r, lon1r, lat2r, lon2r, u, v;
        lat1r = deg_rad(lat1d);
        lon1r = deg_rad(lon1d);
        lat2r = deg_rad(lat2d);
        lon2r = deg_rad(lon2d);
        u = sin((lat2r - lat1r) / 2);
        v = sin((lon2r - lon1r) / 2);
        return 2.0 * EARTH_RADIUS_IN_METERS * asin(sqrt(u * u + cos(lat1r) * cos(lat2r) * v * v));
    }

    bool GeoHashHelper::GetDistanceSquareIfInRadius(uint8 coord_type, double x1, double y1, double x2, double y2, double radius, double& distance,
            double accurace)
    {
        if (coord_type == GEO_WGS84_TYPE)
        {
            distance = distanceEarth(y1, x1, y2, x2);
            if (distance > radius)
            {
                if (std::abs(distance - radius) > std::abs(accurace))
                {
                    return false;
                }
            }
            distance = distance * distance;
        }
        else
        {
            double xx = (x1 - x2) * (x1 - x2);
            double yy = (y1 - y2) * (y1 - y2);
            double dd = xx + yy;
            double rr = (radius + accurace) * (radius + accurace);
            if (dd > rr)
            {
                return false;
            }
            distance = dd;
        }

        return true;
    }

    bool GeoHashHelper::VerifyCoordinates(uint8 coord_type, double x, double y)
    {
        GeoHashRange lat_range, lon_range;
        GeoHashHelper::GetCoordRange(coord_type, lat_range, lon_range);
        if (x < lon_range.min || x > lon_range.max || y < lat_range.min || y > lat_range.max)
        {
            return false;
        }
        return true;
    }

    bool GeoHashHelper::GetXYByHash(uint8 coord_type, uint8 step, uint64_t hash, double& x, double& y)
    {
        GeoHashRange lat_range, lon_range;
        GeoHashHelper::GetCoordRange(coord_type, lat_range, lon_range);
        GeoHashBits hashbits;
        hashbits.bits = hash;
        hashbits.step = step;
        GeoHashArea area;
        if (0 == geohash_fast_decode(lat_range, lon_range, hashbits, &area))
        {
            y = (area.latitude.min + area.latitude.max) / 2;
            x = (area.longitude.min + area.longitude.max) / 2;
            return true;
        }
        else
        {
            return false;
        }
    }

    bool GeoHashHelper::GetMercatorXYByHash(GeoHashFix60Bits hash, double& x, double& y)
    {
        return GetXYByHash(GEO_MERCATOR_TYPE, 30,hash, x, y);
    }
}

