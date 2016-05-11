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

#ifndef GEOHASH_HELPER_HPP_
#define GEOHASH_HELPER_HPP_

#include "geohash.h"
#include "common.hpp"
#include <string>
#include <vector>

#define GEO_WGS84_TYPE     1
#define GEO_MERCATOR_TYPE  2
namespace ardb
{
    typedef uint64_t GeoHashFix60Bits;
    typedef uint64_t GeoHashVarBits;

    struct GeoHashBitsComparator
    {
            bool operator() (const GeoHashBits& a, const GeoHashBits& b) const
            {
                if(a.step < b.step)
                {
                    return true;
                }
                if(a.step > b.step)
                {
                    return false;
                }
                return a.bits < b.bits;
            }
    };

    typedef TreeSet<GeoHashBits, GeoHashBitsComparator>::Type GeoHashBitsSet;


    class GeoHashHelper
    {
        public:
            static int GetCoordRange(uint8 coord_type, GeoHashRange& lat_range, GeoHashRange& lon_range);
            static int GetAreasByRadius(uint8 coord_type,double latitude, double longitude, double radius_meters, GeoHashBitsSet& results);
            static int GetAreasByRadiusV2(uint8 coord_type,double latitude, double longitude, double radius_meters, GeoHashBitsSet& results);
            static uint64 AllignHashBits(uint8 step, const GeoHashBits& hash);
            static double GetMercatorX(double longtitude);
            static double GetMercatorY(double latitude);
            static double GetWGS84X(double x);
            static double GetWGS84Y(double y);
            static bool VerifyCoordinates(uint8 coord_type, double x, double y);
            static bool GetDistanceSquareIfInRadius(uint8 coord_type, double x1, double y1, double x2, double y2, double radius, double& distance, double accurace);

            static bool GetMercatorXYByHash(GeoHashFix60Bits hash, double& x, double& y);
            static bool GetXYByHash(uint8 coord_type, uint8 step, uint64_t hash, double& x, double& y);
            static double GetWGS84Distance(double lon1d, double lat1d, double lon2d, double lat2d);
    };
}

#endif /* GEOHASH_HELPER_HPP_ */
