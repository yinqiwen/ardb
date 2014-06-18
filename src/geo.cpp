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
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
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
     *  <GeoOptions> = [IN N m0 m1 ..] RADIUS r
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
        int ret = m_db->GeoSearch(ctx.currentDB, cmd.GetArguments()[0], options, res);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_array_reply(ctx.reply, res);
        return 0;
    }

    /*
     *  AreaAdd key value MERCATOR|WGS84  vertex_x vertex_y [vertex_x vertex_y] ....
     */
    int ArdbServer::AreaAdd(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        AreaAddOptions options;
        std::string err;
        if (0 != options.Parse(cmd.GetArguments(), err, 1))
        {
            fill_error_reply(ctx.reply, "%s", err.c_str());
            return 0;
        }
        int ret = m_db->AreaAdd(ctx.currentDB, cmd.GetArguments()[0], options);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
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
     *  AreaDel key value
     */
    int ArdbServer::AreaDel(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->AreaDel(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1]);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
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
     *  AreaClear key
     */
    int ArdbServer::AreaClear(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        int ret = m_db->AreaClear(ctx.currentDB, cmd.GetArguments()[0]);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
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
     *  AreaLocate key MERCATOR|WGS84 x y [GET pattern [GET pattern ...]]
     */
    int ArdbServer::AreaLocate(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        AreaLocateOptions options;
        std::string err;
        if (0 != options.Parse(cmd.GetArguments(), err, 1))
        {
            fill_error_reply(ctx.reply, "%s", err.c_str());
            return 0;
        }
        ValueDataDeque res;
        int ret = m_db->AreaLocate(ctx.currentDB, cmd.GetArguments()[0], options, res);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_array_reply(ctx.reply, res);
        return 0;
    }

    //=======================================================DB========================================================
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

    int Ardb::AreaIter(const AreaValue& area, OnSubArea* cb, void* data)
    {
        GeoHashRange lat_range, lon_range;
        GeoHashHelper::GetCoordRange(area.coord_type, lat_range, lon_range);

        GeoHashBits geohash_cursor;
        geohash_encode(&lat_range, &lon_range, area.min_x, area.min_y, m_config.area_geohash_step, &geohash_cursor);
        GeoHashArea cursor_area;
        geohash_decode(&lat_range, &lon_range, &geohash_cursor, &cursor_area);
        double delta_x = cursor_area.longitude.max - cursor_area.longitude.min;
        double delta_y = cursor_area.latitude.max - cursor_area.latitude.min;

        double x_cursor = cursor_area.longitude.min;
        while (x_cursor <= area.max_x)
        {
            double y_cursor = cursor_area.latitude.min;
            GeoHashBits x_current = geohash_cursor;
            while (y_cursor <= area.max_y)
            {
                cb(geohash_cursor, data);
                GeoHashBits tmp = geohash_cursor;
                geohash_get_neighbor(&tmp, GEOHASH_NORTH, &geohash_cursor);
                y_cursor += delta_y;
            }
            geohash_get_neighbor(&x_current, GEOHASH_EAST, &geohash_cursor);
            x_cursor += delta_x;
        }
        return 0;
    }

    struct AreaAddIterContext
    {
            Ardb* db;
            DBID dbid;
            std::string key;
            std::string value;
    };

    static void AreaAddIterCallback(const GeoHashBits& hash, void* data)
    {
        AreaAddIterContext* ctx = (AreaAddIterContext*) data;
        std::string key = ctx->key;
        key.append(":").append(stringfromll(hash.bits));
        ctx->db->SAdd(ctx->dbid, key, ctx->value);
    }

    int Ardb::AreaAdd(const DBID& db, const Slice& key, const AreaAddOptions& options)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        std::string value;
        int ret = HGet(db, key, options.value, &value);
        if (ret != 0 && ret != ERR_NOT_EXIST)
        {
            return ret;
        }
        AreaValue area;
        if (0 == ret)
        {
            AreaDel(db, key, options.value);
        }
        area.xx = options.xx;
        area.yy = options.yy;
        area.geohash_step = m_config.area_geohash_step;
        area.coord_type = options.coord_type;
#define GET_MIN(current, value)  \
do{\
       if(DBL_MIN == current || value < current) current = value;\
}while(0)
#define GET_MAX(current, value)  \
do{\
       if(DBL_MIN == current || value > current) current = value;\
}while(0)
        for (uint32 i = 0; i < area.xx.size(); i++)
        {
            GET_MIN(area.min_x, area.xx[i]);
            GET_MIN(area.min_y, area.yy[i]);
            GET_MAX(area.max_x, area.xx[i]);
            GET_MAX(area.max_y, area.yy[i]);
        }
        Buffer buf;
        area.Encode(buf);
        Slice sv(buf.GetRawReadBuffer(), buf.ReadableBytes());
        HSet(db, key, options.value, sv);
        AreaAddIterContext ctx;
        ctx.db = this;
        ctx.dbid = db;
        ctx.key.assign(key.data(), key.size());
        ctx.value.assign(options.value.data(), options.value.size());
        AreaIter(area, AreaAddIterCallback, &ctx);

        return 0;
    }

    int Ardb::AreaLocate(const DBID& db, const Slice& key, const AreaLocateOptions& options, ValueDataDeque& results)
    {
        GeoHashRange lat_range, lon_range;
        GeoHashHelper::GetCoordRange(options.coord_type, lat_range, lon_range);
        GeoHashBits hash;
        geohash_encode(&lat_range, &lon_range, options.x, options.y, m_config.area_geohash_step, &hash);
        std::string sk;
        sk.assign(key.data(), key.size());
        sk.append(":").append(stringfromll(hash.bits));
        ValueDataDeque rs;
        SMembers(db, sk, rs);
        ValueDataDeque::iterator it = rs.begin();
        while (it != rs.end())
        {
            ValueData& field = *it;
            std::string v;
            if (0 == HGet(db, key, field.AsString(), &v))
            {
                AreaValue av;
                Buffer buf(const_cast<char*>(v.data()), 0, v.size());
                if (!av.Decode(buf))
                {
                    WARN_LOG("Invalid content for area %s", field.AsString().c_str());
                    it++;
                    continue;
                }
                if (av.xx.size() > 0)
                {
                    /*
                     * This algorithm is ported from http://alienryderflex.com/polygon/
                     */
                    uint32 i, j = av.xx.size() - 1;
                    bool oddNodes = false;
                    for (i = 0; i < av.xx.size(); i++)
                    {
                        if (((av.yy[i] < options.y && av.yy[j] >= options.y)
                                || (av.yy[j] < options.y && av.yy[i] >= options.y))
                                && (av.xx[i] <= options.x || av.xx[j] <= options.x))
                        {
                            oddNodes ^=
                                    (av.xx[i] + (options.y - av.yy[i]) / (av.yy[j] - av.yy[i]) * (av.xx[j] - av.xx[i])
                                            < options.x);
                        }
                        j = i;
                    }
                    if (oddNodes)
                    {
                        results.push_back(field);
                        StringArray::const_iterator git = options.get_patterns.begin();
                        while (git != options.get_patterns.end())
                        {
                            ValueData join;
                            GetValueByPattern(db, *git, field, join);
                            results.push_back(join);
                            git++;
                        }
                    }
                }
            }
            it++;
        }
        return 0;
    }
    struct AreaDelIterContext
    {
            Ardb* db;
            DBID dbid;
            std::string key;
            std::string value;
    };

    static void AreaDelIterCallback(const GeoHashBits& hash, void* data)
    {
        AreaDelIterContext* ctx = (AreaDelIterContext*) data;
        std::string key = ctx->key;
        key.append(":").append(stringfromll(hash.bits));
        ctx->db->SRem(ctx->dbid, key, ctx->value);
    }
    int Ardb::AreaDel(const DBID& db, const Slice& key, const Slice& value)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        std::string v;
        int ret = HGet(db, key, value, &v);
        if (ret != 0)
        {
            return ret;
        }
        AreaValue area;
        Buffer buf(const_cast<char*>(v.data()), 0, v.size());
        if (!area.Decode(buf))
        {
            WARN_LOG("Invalid content for area %s", value.data());
            return -1;
        }
        AreaDelIterContext ctx;
        ctx.db = this;
        ctx.dbid = db;
        ctx.key.assign(key.data(), key.size());
        ctx.value.assign(value.data(), value.size());
        AreaIter(area, AreaDelIterCallback, &ctx);
        return 0;
    }

    struct AreaClearContext
    {
            Ardb* db;
            DBID dbid;
            std::string key;

    };
    static int AreaClearVisitCallback(const ValueData& value, int cursor, void* cb)
    {
        AreaClearContext* ctx = (AreaClearContext*) cb;
        if (cursor % 2 == 0)
        {
            ctx->db->AreaDel(ctx->dbid, ctx->key, value.AsString());
        }
        return 0;
    }

    int Ardb::AreaClear(const DBID& db, const Slice& key)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        AreaClearContext ctx;
        ctx.db = this;
        ctx.key.assign(key.data(), key.size());
        ctx.dbid = db;
        HIterate(db, key, AreaClearVisitCallback, &ctx);
        HClear(db, key);
        return 0;
    }
}

