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

#include "ardb.hpp"
#include "geo/geohash_helper.hpp"
#include <algorithm>
#include <math.h>

namespace ardb
{
    /*
     *  GEOADD key MERCATOR|WGS84 x y value [x y value....]
     */
    int Ardb::GeoAdd(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }

    static bool less_by_distance(const GeoPoint& v1, const GeoPoint& v2)
    {
        return v1.distance < v2.distance;
    }

    static bool great_by_distance(const GeoPoint& v1, const GeoPoint& v2)
    {
        return v1.distance > v2.distance;
    }

    /*
     *  GEOSEARCH key MERCATOR|WGS84 x y  <GeoOptions>
     *  GEOSEARCH key MEMBER m            <GeoOptions>
     *
     *  <GeoOptions> = [IN N m0 m1 ..] [RADIUS r]
     *                 [ASC|DESC] [WITHCOORDINATES] [WITHDISTANCES]
     *                 [GET pattern [GET pattern ...]]
     *                 [INCLUDE key_pattern value_pattern [INCLUDE key_pattern value_pattern ...]]
     *                 [EXCLUDE key_pattern value_pattern [EXCLUDE key_pattern value_pattern ...]]
     *                 [LIMIT offset count]
     *
     *  For 'GET pattern' in GEOSEARCH:
     *  If 'pattern' is '#.<attr>',  return actual point's attribute stored by 'GeoAdd'
     *  Other pattern would processed the same as 'sort' command (Use same C++ function),
     *  The patterns like '#', "*->field" are valid.
     */

    int Ardb::GeoSearch(Context& ctx, RedisCommandFrame& cmd)
    {

        return 0;
    }
}

