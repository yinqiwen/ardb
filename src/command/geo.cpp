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
        GeoAddOptions options;
        std::string err;
        if (0 != options.Parse(cmd.GetArguments(), err, 1))
        {
            fill_error_reply(ctx.reply, "%s", err.c_str());
            return 0;
        }
        GeoHashRange lat_range, lon_range;
        GeoHashHelper::GetCoordRange(options.coord_type, lat_range, lon_range);

        KeyLockerGuard keylock(m_key_lock, ctx.currentDB, cmd.GetArguments()[0]);
        ValueObject meta;
        int ret = GetMetaValue(ctx, cmd.GetArguments()[0], ZSET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);

        RedisCommandFrame zadd("zadd");
        zadd.AddArg(cmd.GetArguments()[0]);
        BatchWriteGuard guard(GetKeyValueEngine());
        GeoPointArray::iterator git = options.points.begin();
        uint32 count = 0;
        while (git != options.points.end())
        {
            GeoPoint& point = *git;
            if (point.x < lon_range.min || point.x > lon_range.max || point.y < lat_range.min
                    || point.y > lat_range.max)
            {
                guard.MarkFailed();
                break;
            }
            GeoHashBits hash;
            geohash_encode(&lat_range, &lon_range, point.y, point.x, 30, &hash);
            GeoHashFix60Bits score = hash.bits;
            Data score_value;
            score_value.SetInt64((int64) score);
            Data element;
            element.SetString(point.value, true);
            count += ZSetAdd(ctx, meta, element, score_value, NULL);

            std::string tmp;
            score_value.GetDecodeString(tmp);
            zadd.AddArg(tmp);
            element.GetDecodeString(tmp);
            zadd.AddArg(tmp);
            git++;
        }
        if (guard.Success())
        {
            SetKeyValue(ctx, meta);
            RewriteClientCommand(ctx, zadd);
            fill_int_reply(ctx.reply, count);
        }
        else
        {
            fill_error_reply(ctx.reply, "Invalid arguments");
        }
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
        GeoSearchOptions options;
        std::string err;
        if (0 != options.Parse(cmd.GetArguments(), err, 1))
        {
            fill_error_reply(ctx.reply, "%s", err.c_str());
            return 0;
        }
        ValueObject meta;
        int ret = GetMetaValue(ctx, cmd.GetArguments()[0], ZSET_META, meta);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        ctx.reply.type = REDIS_REPLY_ARRAY;
        if (0 != ret)
        {
            return 0;
        }
        uint64 t1 = get_current_epoch_millis();
        uint32 min_radius = 75; //magic number
        if (options.asc && options.limit > 0 && options.offset == 0 && options.radius > min_radius)
        {
            uint32 old_radius = options.radius;
            options.radius = min_radius;
            do
            {
                ctx.reply.Clear();
                int point_num = GeoSearchByOptions(ctx, meta, options);
                if(point_num < 0)
                {
                    break;
                }
                if (options.radius == old_radius)
                {
                    break;
                }
                if (point_num > 0)
                {
                    if (point_num >= options.limit)
                    {
                        break;
                    }
                    options.radius *= sqrt((options.limit / point_num) + 1);
                }
                else
                {
                    options.radius *= 8;
                }
                if (options.radius > old_radius)
                {
                    options.radius = old_radius;
                }
            } while (options.radius <= old_radius);
        }
        else
        {
            GeoSearchByOptions(ctx, meta, options);
        }
        ctx.reply.type = REDIS_REPLY_ARRAY;
        uint64 t2 = get_current_epoch_millis();
        DEBUG_LOG("####Cost %llums to range geosearch", t2 - t1);
        return 0;
    }

    int Ardb::GeoSearchByOptions(Context& ctx, ValueObject& meta, GeoSearchOptions& options)
    {
        uint64 start_time = get_current_epoch_micros();
        int ret = 0;
        double x = options.x, y = options.y;
        if (options.by_member)
        {
            Data element, score;
            element.SetString(options.member, true);
            ret = ZSetScore(ctx, meta, element, score);
            if (0 != ret || score.IsNil())
            {
                return -1;
            }
            GeoHashHelper::GetMercatorXYByHash(score.value.iv, x, y);
        }
        else
        {
            if (options.coord_type != GEO_MERCATOR_TYPE)
            {
                x = GeoHashHelper::GetMercatorX(options.x);
                y = GeoHashHelper::GetMercatorY(options.y);
            }
        }
        DEBUG_LOG("####Step1: Cost %lluus", get_current_epoch_micros() - start_time);
        GeoPointArray points;
        ZSetRangeByScoreOptions fetch_options;
        fetch_options.withscores = false;
        fetch_options.op = OP_GET;
        fetch_options.fill_reply = false;
        fetch_options.fetch_geo_location = true;
        if (options.in_members)
        {
            StringSet::iterator it = options.submembers.begin();
            while (it != options.submembers.end())
            {
                //GeoPoint point;
                Data element, score;
                element.SetString(*it, true);
                Location loc;
                ret = ZSetScore(ctx, meta, element, score, &loc);
                if (0 == ret)
                {
                    fetch_options.results.push_back(element);
                    //fetch_options.results.push_back(score);
                    fetch_options.locs.push_back(loc);
                }
                it++;
            }
        }
        else
        {
            GeoHashBitsSet ress;
            GeoHashHelper::GetAreasByRadiusV2(GEO_MERCATOR_TYPE, y, x, options.radius, ress);
            /*
             * Merge areas if possible to avoid disk search
             */
            std::vector<ZRangeSpec> range_array;
            GeoHashBitsSet::iterator rit = ress.begin();
            typedef TreeMap<uint64, uint64>::Type HashRangeMap;
            HashRangeMap tmp;
            while (rit != ress.end())
            {
                GeoHashBits& hash = *rit;
                GeoHashBits next = hash;
                next.bits++;
                tmp[GeoHashHelper::Allign60Bits(hash)] = GeoHashHelper::Allign60Bits(next);
                rit++;
            }
            HashRangeMap::iterator tit = tmp.begin();
            HashRangeMap::iterator nit = tmp.begin();
            nit++;
            while (tit != tmp.end())
            {
                ZRangeSpec range;
                range.contain_min = true;
                range.contain_max = true;
                range.min.SetInt64(tit->first);
                range.max.SetInt64(tit->second);
                while (nit != tmp.end() && nit->first == range.max.value.iv)
                {
                    range.max.SetInt64(nit->second);
                    nit++;
                    tit++;
                }
                range_array.push_back(range);
                nit++;
                tit++;
            }

            DEBUG_LOG("After areas merging, reduce searching area size from %u to %u", ress.size(), range_array.size());
            std::vector<ZRangeSpec>::iterator hit = range_array.begin();
            ZSetIterator* iter = NULL;
            while (hit != range_array.end())
            {
                ZRangeSpec& range = *hit;
                uint64 t1 = get_current_epoch_millis();
                ZSetRangeByScore(ctx, meta, range, fetch_options, iter);
                uint64 t2 = get_current_epoch_millis();
                DEBUG_LOG("####Cost %llums to range fetch", t2 - t1);
                hit++;
            }
            DELETE(iter);
        }
        DEBUG_LOG("####Step2: Cost %lluus", get_current_epoch_micros() - start_time);
        uint32 outrange = 0;
        LocationDeque::iterator lit = fetch_options.locs.begin();
        DataArray::iterator vit = fetch_options.results.begin();
        while (vit != fetch_options.results.end())
        {
            Location& loc = *lit;
            GeoPoint point;
            point.x = loc.x;
            point.y = loc.y;
            /*
             * distance accuracy is 0.2m
             */
            if (GeoHashHelper::GetDistanceSquareIfInRadius(GEO_MERCATOR_TYPE, x, y, point.x, point.y, options.radius,
                    point.distance, 0.2))
            {
                vit->GetDecodeString(point.value);
                /*
                 * filter by exclude/include
                 */
                if (!options.includes.empty() || !options.excludes.empty())
                {
                    Data subst;
                    subst.SetString(point.value, false);
                    bool matched = options.includes.empty() ? true : false;
                    if (!options.includes.empty())
                    {
                        StringStringMap::const_iterator sit = options.includes.begin();
                        while (sit != options.includes.end())
                        {
                            Data mv;
                            if (0 != MatchValueByPattern(ctx, sit->first, sit->second, subst, mv))
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
                            Data mv;
                            if (0 == MatchValueByPattern(ctx, sit->first, sit->second, subst, mv))
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
                outrange++;
            }
            vit++;
            lit++;
        }
        DEBUG_LOG("###Result size:%d,  outrange:%d", points.size(), outrange);
        DEBUG_LOG("####Step3: Cost %lluus", get_current_epoch_micros() - start_time);
        if (!options.nosort)
        {
            std::sort(points.begin(), points.end(), options.asc ? less_by_distance : great_by_distance);
        }
        DEBUG_LOG("####Step3.5: Cost %lluus", get_current_epoch_micros() - start_time);
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
        DEBUG_LOG("####Step4: Cost %lluus", get_current_epoch_micros() - start_time);
        ValueObjectMap meta_cache;
        GeoPointArray::iterator pit = points.begin();
        while (pit != points.end())
        {
            RedisReply& r = ctx.reply.AddMember();
            fill_str_reply(r, pit->value);
            GeoGetOptionArray::const_iterator ait = options.get_patterns.begin();
            while (ait != options.get_patterns.end())
            {
                if (ait->get_distances)
                {
                    RedisReply& rr = ctx.reply.AddMember();
                    rr.type = REDIS_REPLY_STRING;
                    char dbuf[128];
                    int dlen = snprintf(dbuf, sizeof(dbuf), "%.2f", sqrt(pit->distance));
                    rr.str.assign(dbuf, dlen);
                }
                else if (ait->get_coodinates)
                {
                    if (options.coord_type == GEO_WGS84_TYPE)
                    {
                        pit->x = GeoHashHelper::GetWGS84X(pit->x);
                        pit->y = GeoHashHelper::GetWGS84Y(pit->y);
                    }
                    RedisReply& rr1 = ctx.reply.AddMember();
                    RedisReply& rr2 = ctx.reply.AddMember();
                    if (options.coord_type == GEO_WGS84_TYPE)
                    {
                        fill_double_reply(rr1, pit->x);
                        fill_double_reply(rr2, pit->y);
                    }
                    else
                    {
                        char dbuf[128];
                        int dlen = snprintf(dbuf, sizeof(dbuf), "%.2f", pit->x);
                        rr1.type = REDIS_REPLY_STRING;
                        rr1.str.assign(dbuf, dlen);
                        dlen = snprintf(dbuf, sizeof(dbuf), "%.2f", pit->y);
                        rr2.type = REDIS_REPLY_STRING;
                        rr2.str.assign(dbuf, dlen);
                    }
                }
                else if (ait->hgetall)
                {
                    std::string keystr(ait->get_pattern.data(), ait->get_pattern.size());
                    string_replace(keystr, "*", pit->value);
                    RedisReply& rr = ctx.reply.AddMember();
                    rr.type = REDIS_REPLY_ARRAY;
                    HashGetAll(ctx, keystr, rr);
                }
                else
                {
                    Data v, attr;
                    v.SetString(pit->value, false);
                    GetValueByPattern(ctx, ait->get_pattern, v, attr, &meta_cache);
                    RedisReply& rr = ctx.reply.AddMember();
                    fill_value_reply(rr, attr);
                }
                ait++;
            }
            pit++;
        }
        DEBUG_LOG("####Step5: Cost %lluus", get_current_epoch_micros() - start_time);
        uint64 end_time = get_current_epoch_micros();
        DEBUG_LOG("Cost %llu microseconds to search.", end_time - start_time);
        return points.size();
    }
}

