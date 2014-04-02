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

#include "db.hpp"
#include "ardb_server.hpp"
#include "geo/geohash_helper.hpp"
#include <algorithm>

namespace ardb
{
    /*
     *  GEOADD key MERCATOR|WGS84 x y value  [attr_name attr_value ...]
     */
    int ArdbServer::GeoAdd(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        GeoAddOptions options;
        std::string err;
        if (0 != options.Parse(cmd.GetArguments(), err, 1))
        {
            fill_error_reply(ctx.reply, "%s", err.c_str());
            return 0;
        }
        int ret = m_db->GeoAdd(ctx.currentDB, cmd.GetArguments()[0], options);
        if (ret >= 0)
        {
            fill_status_reply(ctx.reply, "OK");
        }
        else
        {
            fill_error_reply(ctx.reply, "Failed to %s", cmd.ToString().c_str());
        }
        return 0;
    }

    /*
     *  GEOSEARCH key MERCATOR|WGS84 x y  <GeoOptions>
     *  GEOSEARCH key MEMBER m            <GeoOptions>
     *
     *  <GeoOptions> = IN N m0 m1 m2 RADIUS r
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

    int ArdbServer::GeoSearch(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        GeoSearchOptions options;
        std::string err;
        if (0 != options.Parse(cmd.GetArguments(), err, 1))
        {
            fill_error_reply(ctx.reply, "%s", err.c_str());
            return 0;
        }
        ValueDataDeque res;
        m_db->GeoSearch(ctx.currentDB, cmd.GetArguments()[0], options, res);
        fill_array_reply(ctx.reply, res);
        return 0;
    }

    int Ardb::GeoAdd(const DBID& db, const Slice& key, const GeoAddOptions& options)
    {
        GeoHashRange lat_range, lon_range;
        GeoHashHelper::GetCoordRange(options.coord_type, lat_range, lon_range);
        if (options.x < lon_range.min || options.x > lon_range.max || options.y < lat_range.min
                || options.y > lat_range.max)
        {
            return ERR_INVALID_ARGS;
        }
        GeoPoint point;
        point.x = options.x;
        point.y = options.y;

        if (options.coord_type == GEO_WGS84_TYPE)
        {
            point.x = GeoHashHelper::GetMercatorX(options.x);
            point.y = GeoHashHelper::GetMercatorY(options.y);
            GeoHashHelper::GetCoordRange(GEO_MERCATOR_TYPE, lat_range, lon_range);
        }
        point.attrs = options.attrs;

        GeoHashBits hash;
        geohash_encode(&lat_range, &lon_range, point.y, point.x, 26, &hash);
        GeoHashFix52Bits score = hash.bits;
        ValueData score_value;
        score_value.SetIntValue((int64) score);

        Buffer content;
        point.Encode(content);
        Slice content_value(content.GetRawReadBuffer(), content.ReadableBytes());
        return ZAdd(db, key, score_value, options.value, content_value);
    }

    static bool less_by_distance(const GeoPoint& v1, const GeoPoint& v2)
    {
        return v1.distance < v2.distance;
    }

    static bool great_by_distance(const GeoPoint& v1, const GeoPoint& v2)
    {
        return v1.distance > v2.distance;
    }

    static int ZSetValueStoreCallback(const ValueData& value, int cursor, void* cb)
    {
        ValueDataArray* s = (ValueDataArray*) cb;
        s->push_back(value);
        return 0;
    }

    int Ardb::GeoSearch(const DBID& db, const Slice& key, const GeoSearchOptions& options, ValueDataDeque& results)
    {
        uint64 start_time = get_current_epoch_micros();
        GeoHashBitsSet ress;
        double x = options.x, y = options.y;
        if (options.coord_type == GEO_WGS84_TYPE)
        {
            x = GeoHashHelper::GetMercatorX(options.x);
            y = GeoHashHelper::GetMercatorY(options.y);
        }
        if (options.by_member)
        {
            ValueData score, attr;
            int err = ZGetNodeValue(db, key, options.member, score, attr);
            if (0 != err)
            {
                return err;
            }
            Buffer attr_content(const_cast<char*>(attr.bytes_value.data()), 0, attr.bytes_value.size());
            GeoPoint point;
            point.Decode(attr_content);
            x = point.x;
            y = point.y;
        }
        ZSetCacheElementSet subset;
        if (options.in_members)
        {
            StringSet::const_iterator sit = options.submembers.begin();
            while (sit != options.submembers.end())
            {
                ZSetCaheElement ele;

                ValueData score, attr;
                if (0 == ZGetNodeValue(db, key, *sit, score, attr))
                {
                    ele.score = score.NumberValue();
                    Buffer buf1, buf2;
                    ValueData vv;
                    vv.SetValue(*sit, true);
                    vv.Encode(buf1);
                    attr.Encode(buf2);
                    ele.value.assign(buf1.GetRawReadBuffer(), buf1.ReadableBytes());
                    ele.attr.assign(buf2.GetRawReadBuffer(), buf2.ReadableBytes());
                    subset.insert(ele);
                }
                sit++;
            }
        }
        GeoHashHelper::GetAreasByRadius(GEO_MERCATOR_TYPE, y, x, options.radius, ress);

        /*
         * Merge areas if possible to avoid disk search
         */
        std::vector<ZRangeSpec> range_array;
        GeoHashBitsSet::iterator rit = ress.begin();
        GeoHashBitsSet::iterator next_it = ress.begin();
        next_it++;
        while (rit != ress.end())
        {
            GeoHashBits& hash = *rit;
            GeoHashBits next = hash;
            next.bits++;
            while (next_it != ress.end() && next.bits == next_it->bits)
            {
                next.bits++;
                next_it++;
                rit++;
            }
            ZRangeSpec range;
            range.contain_min = true;
            range.contain_max = false;
            range.min.SetIntValue(GeoHashHelper::Allign52Bits(hash));
            range.max.SetIntValue(GeoHashHelper::Allign52Bits(next));
            range_array.push_back(range);
            rit++;
            next_it++;
        }
        DEBUG_LOG("After areas merging, reduce searching area size from %u to %u", ress.size(), range_array.size());

        GeoPointArray points;
        std::vector<ZRangeSpec>::iterator hit = range_array.begin();
        Iterator* iter = NULL;
        while (hit != range_array.end())
        {
            ZSetQueryOptions z_options;
            z_options.withscores = false;
            z_options.withattr = true;
            ValueDataArray values;
            if (options.in_members)
            {
                ZSetCache::GetRangeInZSetCache(subset, *hit, z_options.withscores, z_options.withattr,
                        ZSetValueStoreCallback, &values);
            }
            else
            {
                ZRangeByScoreRange(db, key, *hit, iter, z_options, true, ZSetValueStoreCallback, &values);
            }
            ValueDataArray::iterator vit = values.begin();
            while (vit != values.end())
            {
                GeoPoint point;
                vit->ToString(point.value);
                //attributes
                vit++;
                Buffer content(const_cast<char*>(vit->bytes_value.data()), 0, vit->bytes_value.size());
                if (point.Decode(content))
                {
                    if (GeoHashHelper::GetDistanceSquareIfInRadius(GEO_MERCATOR_TYPE, x, y, point.x, point.y,
                            options.radius, point.distance))
                    {
                        /*
                         * filter by exclude/include
                         */
                        if (!options.includes.empty() || !options.excludes.empty())
                        {
                            ValueData subst;
                            subst.SetValue(point.value, false);
                            bool matched = options.includes.empty() ? true : false;
                            if (!options.includes.empty())
                            {
                                StringStringMap::const_iterator sit = options.includes.begin();
                                while (sit != options.includes.end())
                                {
                                    ValueData mv;
                                    if (0 != MatchValueByPattern(db, sit->first, sit->second, subst, mv))
                                    {
                                        matched = false;
                                        break;
                                    }
                                    else
                                    {
                                        matched = true;
                                    }
                                    sit++;
                                }
                            }
                            if (matched && !options.excludes.empty())
                            {
                                StringStringMap::const_iterator sit = options.excludes.begin();
                                while (sit != options.excludes.end())
                                {
                                    ValueData mv;
                                    if (0 == MatchValueByPattern(db, sit->first, sit->second, subst, mv))
                                    {
                                        matched = false;
                                        break;
                                    }
                                    else
                                    {
                                        matched = true;
                                    }
                                    sit++;
                                }
                            }

                            if (matched)
                            {
                                points.push_back(point);
                            }
                        }
                        else
                        {
                            points.push_back(point);
                        }
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
            if ((uint32) options.offset > points.size())
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
            if ((uint32) options.limit < points.size())
            {
                GeoPointArray::iterator end = points.begin() + options.limit;
                points.erase(end, points.end());
            }
        }
        GeoPointArray::iterator pit = points.begin();
        while (pit != points.end())
        {
            ValueData v;
            v.SetValue(pit->value, false);
            results.push_back(v);
            GeoGetOptionDeque::const_iterator ait = options.get_patterns.begin();
            while (ait != options.get_patterns.end())
            {
                ValueData attr;
                if (ait->get_distances)
                {
                    attr.SetDoubleValue(sqrt(pit->distance));
                    results.push_back(attr);
                }
                else if (ait->get_coodinates)
                {
                    if (options.coord_type == GEO_WGS84_TYPE)
                    {
                        pit->x = GeoHashHelper::GetWGS84X(pit->x);
                        pit->y = GeoHashHelper::GetWGS84Y(pit->y);
                    }
                    attr.SetDoubleValue(pit->x);
                    results.push_back(attr);
                    attr.SetDoubleValue(pit->y);
                    results.push_back(attr);
                }
                else if (ait->get_attr)
                {
                    StringStringMap::iterator found = pit->attrs.find(ait->get_pattern);
                    if (found != pit->attrs.end())
                    {
                        attr.SetValue(found->second, false);
                    }
                    results.push_back(attr);
                }
                else
                {
                    GetValueByPattern(db, ait->get_pattern, v, attr);
                    results.push_back(attr);
                }
                ait++;
            }
            pit++;
        }
        uint64 end_time = get_current_epoch_micros();
        DEBUG_LOG("Cost %llu microseconds to search.", end_time - start_time);
        return 0;
    }
}

