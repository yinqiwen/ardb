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

#include "ardb.hpp"
#include "util/geohash/geohash_helper.hpp"
#include <algorithm>

namespace ardb
{
    int Ardb::GeoAdd(const DBID& db, const Slice& key, const Slice& value, double x, double y)
    {
        GeoHashFix50Bits score = GeoHashHelper::GetFix50MercatorGeoHashBits(y, x);
        ValueData score_value;
        score_value.SetIntValue((int64) score);
        GeoPoint point;
        point.value.assign(value.data(), value.size());
        point.x = x;
        point.y = y;
        point.mercator_x = GeoHashHelper::GetMercatorX(x);
        point.mercator_y = GeoHashHelper::GetMercatorY(y);
        Buffer content;
        point.Encode(content);
        Slice content_value(content.GetRawReadBuffer(), content.ReadableBytes());
        return ZAdd(db, key, score_value, content_value);
    }

    static bool less_by_distance(const GeoPoint& v1, const GeoPoint& v2)
    {
        return v1.distance < v2.distance;
    }
    static bool great_by_distance(const GeoPoint& v1, const GeoPoint& v2)
    {
        return v1.distance > v2.distance;
    }

    int Ardb::GeoSearch(const DBID& db, const Slice& key, double x, double y, const GeoSearchOptions& options,
            GeoPointArray& results)
    {
        GeoHashBitsArray ress;
        double mercator_x = GeoHashHelper::GetMercatorX(x);
        double mercator_y = GeoHashHelper::GetMercatorY(y);
        int hash_bits = GeoHashHelper::GetAreasByRadius(mercator_y, mercator_x, options.radius, ress);
        std::sort(ress.begin(), ress.end());
        std::vector<ZRangeSpec> range_array;
        for (uint32 i = 0; i < ress.size(); i++)
        {
            GeoHashFix50Bits hash = ress[i];
            uint64 next = hash >> (50 - hash_bits);
            next++;
            next <<= (50 - hash_bits);
            while (i < (ress.size() - 1) && next == ress[i + 1])
            {
                next = next >> (50 - hash_bits);
                next++;
                next <<= (50 - hash_bits);
                i++;
            }
            ZRangeSpec range;
            range.contain_min = true;
            range.contain_max = false;
            range.min.SetIntValue(hash);
            range.max.SetIntValue(next);
            range_array.push_back(range);
        }

        std::vector<ZRangeSpec>::iterator hit = range_array.begin();
        Iterator* iter = NULL;
        while (hit != range_array.end())
        {
            //INFO_LOG("Search hash:%lld->%lld", hit->min.integer_value, hit->max.integer_value);
            ZSetQueryOptions z_options;
            z_options.withscores = false;
            ValueDataArray values;
            ZRangeByScoreRange(db, key, *hit, iter, values, z_options);
            ValueDataArray::iterator vit = values.begin();
            while (vit != values.end())
            {
                Buffer content(const_cast<char*>(vit->bytes_value.data()), 0, vit->bytes_value.size());
                GeoPoint point;
                if (point.Decode(content))
                {
                    if (GeoHashHelper::GetDistanceIfInRadius(mercator_x, mercator_y, point.mercator_x, point.mercator_y,
                            options.radius, point.distance))
                    {
                        results.push_back(point);
                    }
                    else
                    {
                        //DEBUG_LOG("%s is not in radius:%.2fm", point.value.c_str(), options.radius);
                    }
                }
                else
                {
                    WARN_LOG("Failed to decode geo point.");
                }
                vit++;
            }
            hit++;
        }
        DELETE(iter);
        if (!options.nosort)
        {
            std::sort(results.begin(), results.end(), options.asc ? less_by_distance : great_by_distance);
        }
        if (options.offset > 0)
        {
            if (options.offset > results.size())
            {
                results.clear();
            }
            else
            {
                GeoPointArray::iterator start = results.begin() + options.offset;
                results.erase(results.begin(), start);
            }
        }
        if (options.limit > 0)
        {
            if (options.limit < results.size())
            {
                GeoPointArray::iterator end = results.begin() + options.limit;
                results.erase(end, results.end());
            }
        }
        return 0;
    }
}

