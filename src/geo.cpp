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
#include "geo/geohash_helper.hpp"
#include <algorithm>

namespace ardb
{
    int Ardb::GeoAdd(const DBID& db, const Slice& key, const Slice& value, double x, double y)
    {
        GeoHashFix50Bits score = GeoHashHelper::GetFix50MercatorGeoHashBits(y, x);
        ValueData score_value;
        score_value.SetIntValue((int64) score);
        GeoPoint point;
        point.x = x;
        point.y = y;
        point.mercator_x = GeoHashHelper::GetMercatorX(x);
        point.mercator_y = GeoHashHelper::GetMercatorY(y);
        Buffer content;
        point.Encode(content);
        Slice content_value(content.GetRawReadBuffer(), content.ReadableBytes());
        return ZAdd(db, key, score_value, value, content_value);
    }

    static bool less_by_bits(const GeoHashBits& v1, const GeoHashBits& v2)
    {
        return v1.bits < v2.bits;
    }

    static bool less_by_distance(const GeoPoint& v1, const GeoPoint& v2)
    {
        return v1.distance < v2.distance;
    }

    static bool great_by_distance(const GeoPoint& v1, const GeoPoint& v2)
    {
        return v1.distance > v2.distance;
    }

    int Ardb::GetGeoPoint(const DBID& db, const Slice& key, const Slice& value, GeoPoint& point)
    {
        return 0;
    }

    int Ardb::GeoSearch(const DBID& db, const Slice& key, const GeoSearchOptions& options, ValueDataDeque& results)
    {
        GeoHashBitsArray ress;
        double mercator_x, mercator_y;
        if (options.by_coodinates)
        {
            mercator_x = GeoHashHelper::GetMercatorX(options.x);
            mercator_y = GeoHashHelper::GetMercatorY(options.y);
        }
        else
        {
            ValueData score, attr;
            int err = ZGetNodeValue(db, key, options.member, score, attr);
            if(0 != err)
            {
                return err;
            }
            Buffer attr_content(const_cast<char*>(attr.bytes_value.data()), 0, attr.bytes_value.size());
            GeoPoint point;
            point.Decode(attr_content);
            mercator_x = point.mercator_x;
            mercator_y = point.mercator_y;
        }
        GeoHashHelper::GetAreasByRadius(mercator_y, mercator_x, options.radius, ress);

        /*
         * Merge areas if possible to avoid disk search
         */
        std::sort(ress.begin(), ress.end(), less_by_bits);
        std::vector<ZRangeSpec> range_array;
        for (uint32 i = 0; i < ress.size(); i++)
        {
            GeoHashBits& hash = ress[i];
            GeoHashBits next = hash;
            next.bits++;
            while (i < (ress.size() - 1) && next.bits == ress[i + 1].bits)
            {
                next.bits++;
                i++;
            }
            ZRangeSpec range;
            range.contain_min = true;
            range.contain_max = false;
            range.min.SetIntValue(GeoHashHelper::Allign50Bits(hash));
            range.max.SetIntValue(GeoHashHelper::Allign50Bits(next));
            range_array.push_back(range);
        }

        GeoPointArray points;
        std::vector<ZRangeSpec>::iterator hit = range_array.begin();
        Iterator* iter = NULL;
        while (hit != range_array.end())
        {
            ZSetQueryOptions z_options;
            z_options.withscores = false;
            z_options.withattr = true;
            ValueDataArray values;
            ZRangeByScoreRange(db, key, *hit, iter, values, z_options, true);
            ValueDataArray::iterator vit = values.begin();
            while (vit != values.end())
            {
                GeoPoint point;
                vit->ToString(point.value);
                vit++;
                Buffer content(const_cast<char*>(vit->bytes_value.data()), 0, vit->bytes_value.size());
                if (point.Decode(content))
                {
                    if (GeoHashHelper::GetDistanceIfInRadius(mercator_x, mercator_y, point.mercator_x, point.mercator_y,
                            options.radius, point.distance))
                    {
                        points.push_back(point);
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
            std::sort(points.begin(), points.end(), options.asc ? less_by_distance : great_by_distance);
        }
        if (options.offset > 0)
        {
            if ((uint32)options.offset > points.size())
            {
                points.clear();
            }
            else
            {
                GeoPointArray::iterator start = points.begin() + options.offset;
                points.erase(points.begin(), start);
            }
        }
        if (options.limit > 0)
        {
            if ((uint32)options.limit < points.size())
            {
                GeoPointArray::iterator end = points.begin() + options.limit;
                points.erase(end, points.end());
            }
        }
        GeoPointArray::iterator pit = points.begin();
        while(pit != points.end())
        {
            ValueData v;
            v.SetValue(pit->value, false);
            results.push_back(v);
            if(options.with_coodinates)
            {
                ValueData x, y;
                x.SetDoubleValue(pit->x);
                y.SetDoubleValue(pit->y);
                results.push_back(x);
                results.push_back(y);
            }
            if(options.with_distance)
            {
                ValueData dis;
                dis.SetDoubleValue(pit->distance);
                results.push_back(dis);
            }
            pit++;
        }
        return 0;
    }
}

