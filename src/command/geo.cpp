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

#include "db/db.hpp"
#include "geo/geohash_helper.hpp"
#include <algorithm>
#include <math.h>
#define GEO_STEP_MAX 26
namespace ardb
{

    static double extractUnitOrReply(const std::string& u)
    {
        if (!strcasecmp(u.c_str(), "m"))
        {
            return 1;
        }
        else if (!strcasecmp(u.c_str(), "km"))
        {
            return 1000;
        }
        else if (!strcasecmp(u.c_str(), "ft"))
        {
            return 0.3048;
        }
        else if (!strcasecmp(u.c_str(), "mi"))
        {
            return 1609.34;
        }
        else
        {
            //addReplyError(c, "unsupported unit provided. please use m, km, ft, mi");
            return -1;
        }
    }
    struct GeoSearchGetOption
    {
            bool get_coodinates;
            bool get_distances;
            bool get_hash;
            std::string get_pattern;
            GeoSearchGetOption() :
                    get_coodinates(false), get_distances(false), get_hash(false)
            {
            }
    };
    typedef std::vector<GeoSearchGetOption> GeoGetOptionArray;
    struct GeoSearchOptions
    {
            bool asc;   //sort by asc
            bool nosort;
            int32 offset;
            int32 limit;
            const char* storekey;
            bool storedist;
            GeoGetOptionArray get_patterns;
            GeoSearchOptions() :
                    asc(false), nosort(true), offset(0), limit(0), storekey(NULL), storedist(false)
            {
            }

            int Parse(const std::deque<std::string>& args, std::string& err, uint32 off = 0)
            {
                for (uint32 i = off; i < args.size(); i++)
                {
                    if (!strcasecmp(args[i].c_str(), "asc"))
                    {
                        nosort = false;
                        asc = true;
                    }
                    else if (!strcasecmp(args[i].c_str(), "desc"))
                    {
                        nosort = false;
                        asc = false;
                    }
                    else if (!strcasecmp(args[i].c_str(), "limit") && i < args.size() - 2)
                    {
                        if (!string_toint32(args[i + 1], offset) || !string_toint32(args[i + 2], limit))
                        {
                            err = "Invalid limit/offset value.";
                            return -1;
                        }
                        nosort = false;
                        asc = true;
                        i += 2;
                    }
                    else if (!strcasecmp(args[i].c_str(), "count") && i < args.size() - 1)
                    {
                        if (!string_toint32(args[i + 1], limit) || limit < 0)
                        {
                            err = "COUNT must be > 0";
                            return -1;
                        }
                        nosort = false;
                        asc = true;
                        offset = 0;
                        i++;
                    }
                    else if (!strcasecmp(args[i].c_str(), "store") && i < args.size() - 1)
                    {
                        storekey = args[i + 1].c_str();
                    }
                    else if (!strcasecmp(args[i].c_str(), "storedist") && i < args.size() - 1)
                    {
                        storekey = args[i + 1].c_str();
                        storedist = true;
                    }
                    else if ((!strcasecmp(args[i].c_str(), "GET")) && i < args.size() - 1)
                    {
                        GeoSearchGetOption get;
                        get.get_pattern = args[i + 1];
                        get_patterns.push_back(get);
                        i++;
                    }
                    else if ((!strcasecmp(args[i].c_str(), "withcoord")))
                    {
                        GeoSearchGetOption get;
                        get.get_coodinates = true;
                        get_patterns.push_back(get);
                    }
                    else if ((!strcasecmp(args[i].c_str(), "withhash")))
                    {
                        GeoSearchGetOption get;
                        get.get_hash = true;
                        get_patterns.push_back(get);
                    }
                    else if ((!strcasecmp(args[i].c_str(), "withdist")))
                    {
                        GeoSearchGetOption get;
                        get.get_distances = true;
                        get_patterns.push_back(get);
                    }
                    else
                    {
                        DEBUG_LOG("Invalid geosearch option:%s", args[i].c_str());
                        err = "Invalid geosearch options.";
                        return -1;
                    }
                }
                return 0;
            }

    };

    struct GeoPoint
    {
            Data value;
            int64_t score;
            double x, y;
            double distance;
    };
    typedef std::vector<GeoPoint> GeoPointArray;

    static void setDistance(RedisReply& r, double distance)
    {
        char dbuf[128];
        snprintf(dbuf, sizeof(dbuf), "%.4f", distance);
        r.SetString(dbuf);
    }

    /*
     *  GEOADD key longitude latitude value [longitude latitude value....]
     */
    int Ardb::GeoAdd(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        if ((cmd.GetArguments().size() - 1) % 3 != 0)
        {
            reply.SetErrorReason("syntax error. Try GEOADD key [x1] [y1] [name1] "
                    "[x2] [y2] [name2] ... ");
            return 0;
        }
        GeoHashRange lat_range, lon_range;
        GeoHashHelper::GetCoordRange(GEO_WGS84_TYPE, lat_range, lon_range);
        RedisCommandFrame zadd("zadd");
        zadd.SetType(REDIS_CMD_ZADD);
        zadd.AddArg(cmd.GetArguments()[0]);
        for (size_t i = 1; i < cmd.GetArguments().size(); i += 3)
        {
            double x, y;
            if (!string_todouble(cmd.GetArguments()[i], x) || !string_todouble(cmd.GetArguments()[i + 1], y))
            {
                reply.SetErrCode(ERR_INVALID_FLOAT_ARGS);
                return 0;
            }
            if (x < lon_range.min || x > lon_range.max || y < lat_range.min || y > lat_range.max)
            {
                reply.SetErrorReason("invalid longitude,latitude pair");
                return 0;
            }
            GeoHashBits hash;
            geohash_fast_encode(lat_range, lon_range, y, x, GEO_STEP_MAX, &hash);
            zadd.AddArg(stringfromll(hash.bits));
            zadd.AddArg(cmd.GetArguments()[i + 2]);
            //printf("###add %llu\n", hash.bits);
        }
        return ZAdd(ctx, zadd);
    }

    int Ardb::GeoDist(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        double to_meter = 1;
        if (cmd.GetArguments().size() == 4)
        {
            to_meter = extractUnitOrReply(cmd.GetArguments()[3]);
            if (to_meter < 0)
            {
                reply.SetErrCode(ERR_UNSUPPORT_DIST_UNIT);
                return 0;
            }
        }
        KeyObjectArray members;
        KeyObject member1(ctx.ns, KEY_ZSET_SCORE, cmd.GetArguments()[0]);
        member1.SetZSetMember(cmd.GetArguments()[1]);
        KeyObject member2(ctx.ns, KEY_ZSET_SCORE, cmd.GetArguments()[0]);
        member2.SetZSetMember(cmd.GetArguments()[2]);
        members.push_back(member1);
        members.push_back(member2);
        ValueObjectArray vs;
        ErrCodeArray errs;
        m_engine->MultiGet(ctx, members, vs, errs);
        if (errs[0] != 0 || errs[1] != 0)
        {
            reply.Clear();
            return 0;
        }
        double score1 = vs[0].GetZSetScore();
        double score2 = vs[1].GetZSetScore();
        double x[2], y[2];
        if (!GeoHashHelper::GetXYByHash(GEO_WGS84_TYPE, GEO_STEP_MAX, (uint64) score1, x[0], y[0]) || !GeoHashHelper::GetXYByHash(GEO_WGS84_TYPE, GEO_STEP_MAX, (uint64) score2, x[1], y[1]))
        {
            reply.Clear();
            return 0;
        }
        double distance = GeoHashHelper::GetWGS84Distance(x[0], y[0], x[1], y[1]) / to_meter;
        setDistance(reply, distance);
        return 0;
    }

    int Ardb::GeoHash(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        KeyObjectArray members;
        for (size_t i = 1; i < cmd.GetArguments().size(); i++)
        {
            KeyObject member(ctx.ns, KEY_ZSET_SCORE, cmd.GetArguments()[0]);
            member.SetZSetMember(cmd.GetArguments()[i]);
            members.push_back(member);
        }
        ValueObjectArray vs;
        ErrCodeArray errs;
        m_engine->MultiGet(ctx, members, vs, errs);
        for (size_t i = 0; i < vs.size(); i++)
        {
            RedisReply& r = reply.AddMember();
            if (errs[i] != 0)
            {
                r.Clear();
            }
            else
            {
                const char *geoalphabet = "0123456789bcdefghjkmnpqrstuvwxyz";
                double score = vs[i].GetZSetScore();
                /* The internal format we use for geocoding is a bit different
                 * than the standard, since we use as initial latitude range
                 * -85,85, while the normal geohashing algorithm uses -90,90.
                 * So we have to decode our position and re-encode using the
                 * standard ranges in order to output a valid geohash string. */

                /* Decode... */
                double x, y;
                if (!GeoHashHelper::GetXYByHash(GEO_WGS84_TYPE, GEO_STEP_MAX, (uint64) score, x, y))
                {
                    r.Clear();
                    continue;
                }

                /* Re-encode */
                GeoHashRange lat_range, lon_range;
                GeoHashBits hash;
                lon_range.min = -180;
                lon_range.max = 180;
                lat_range.min = -90;
                lat_range.max = 90;
                geohash_fast_encode(lat_range, lon_range, y, x, GEO_STEP_MAX, &hash);

                char buf[12];
                int i;
                for (i = 0; i < 11; i++)
                {
                    int idx = (hash.bits >> (52 - ((i + 1) * 5))) & 0x1f;
                    buf[i] = geoalphabet[idx];
                }
                buf[11] = '\0';
                r.SetString(buf);
            }
        }
        return 0;
    }

    int Ardb::GeoPos(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        KeyObjectArray members;
        for (size_t i = 1; i < cmd.GetArguments().size(); i++)
        {
            KeyObject member(ctx.ns, KEY_ZSET_SCORE, cmd.GetArguments()[0]);
            member.SetZSetMember(cmd.GetArguments()[i]);
            members.push_back(member);
        }
        ValueObjectArray vs;
        ErrCodeArray errs;
        m_engine->MultiGet(ctx, members, vs, errs);
        for (size_t i = 0; i < vs.size(); i++)
        {
            RedisReply& r = reply.AddMember();
            if (errs[i] != 0)
            {
                r.ReserveMember(-1);
            }
            else
            {
                double score = vs[i].GetZSetScore();
                /* The internal format we use for geocoding is a bit different
                 * than the standard, since we use as initial latitude range
                 * -85,85, while the normal geohashing algorithm uses -90,90.
                 * So we have to decode our position and re-encode using the
                 * standard ranges in order to output a valid geohash string. */

                /* Decode... */
                double x, y;
                if (!GeoHashHelper::GetXYByHash(GEO_WGS84_TYPE, GEO_STEP_MAX, (uint64) score, x, y))
                {
                    r.ReserveMember(-1);
                    continue;
                }
                RedisReply& r1 = r.AddMember();
                RedisReply& r2 = r.AddMember();
                r1.SetDouble(x);
                r2.SetDouble(y);
            }
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
     *  GEORADIUS key x y              <GeoOptions>
     *  GEORADIUSBYMEMBER key member   <GeoOptions>
     *
     *  <GeoOptions> = radius unit
     *                 [ASC|DESC] [WITHDIST] [WITHHASH] [WITHCOORD]
     *                 [GET pattern [GET pattern ...]]
     *                 [STORE key] [STOREDIST key]
     *                 [LIMIT offset count]
     *
     *  For 'GET pattern' in GEORADIUS/GEORADIUSBYMEMBER:
     *  If 'pattern' is '#.<attr>',  return actual point's attribute stored by 'GeoAdd'
     *  Other pattern would processed the same as 'sort' command (Use same C++ function),
     *  The patterns like '#', "*->field" are valid.
     */

    int Ardb::GeoRadius(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisReply& reply = ctx.GetReply();
        GeoSearchOptions options;
        double x, y, radius;

        size_t radius_arg_pos = 3;
        GeoHashRange lat_range, lon_range;
        GeoHashHelper::GetCoordRange(GEO_WGS84_TYPE, lat_range, lon_range);
        if (cmd.GetType() == REDIS_CMD_GEO_RADIUS)
        {
            if (!string_todouble(cmd.GetArguments()[1], x) || !string_todouble(cmd.GetArguments()[2], y))
            {
                reply.SetErrCode(ERR_INVALID_FLOAT_ARGS);
                return 0;
            }
            //printf
            if (x < lon_range.min || x > lon_range.max || y < lat_range.min || y > lat_range.max)
            {
                reply.SetErrorReason("invalid longitude,latitude pair");
                return 0;
            }
            radius_arg_pos = 3;
        }
        else
        {
            KeyObject member(ctx.ns, KEY_ZSET_SCORE, cmd.GetArguments()[0]);
            member.SetZSetMember(cmd.GetArguments()[1]);
            ValueObject score_val;
            int err = m_engine->Get(ctx, member, score_val);
            double score = score_val.GetZSetScore();
            if (0 != err || !GeoHashHelper::GetXYByHash(GEO_WGS84_TYPE, GEO_STEP_MAX, (uint64) score, x, y))
            {
                reply.SetErrorReason("could not decode requested zset member");
                return 0;
            }
            radius_arg_pos = 2;
        }
        if ((radius_arg_pos + 1) >= cmd.GetArguments().size() || !string_todouble(cmd.GetArguments()[radius_arg_pos], radius))
        {
            reply.SetErrorReason("need numeric radius");
            return 0;
        }
        if(radius < 0)
        {
            reply.SetErrorReason("radius cannot be negative");
            return 0;
        }
        double to_meters = extractUnitOrReply(cmd.GetArguments()[radius_arg_pos + 1]);
        if (to_meters < 0)
        {
            reply.SetErrCode(ERR_UNSUPPORT_DIST_UNIT);
            return 0;
        }
        radius *= to_meters;

        std::string err;
        if (0 != options.Parse(cmd.GetArguments(), err, radius_arg_pos + 2))
        {
            reply.SetErrorReason(err);
            return 0;
        }
        /* Trap options not compatible with STORE and STOREDIST. */
        if (options.storekey != NULL && (!options.get_patterns.empty()))
        {
            reply.SetErrorReason("STORE option in GEORADIUS is not compatible with "
                    "WITHDIST, WITHHASH, WITHCOORDS and GET options");
            return 0;
        }

        KeyObject geokey(ctx.ns, KEY_META, cmd.GetArguments()[0]);
        ValueObject geometa;
        if (!CheckMeta(ctx, geokey, KEY_ZSET, geometa))
        {
            return 0;
        }

        /*
         * 1. Get all neighbors area by radius
         */
        GeoHashBitsSet ress;
        GeoHashHelper::GetAreasByRadius(GEO_WGS84_TYPE, y, x, radius, ress);

        /*
         * 2. Merge neighbors areas if possible to avoid more tree search
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
            tmp[GeoHashHelper::AllignHashBits(GEO_STEP_MAX, hash)] = GeoHashHelper::AllignHashBits(GEO_STEP_MAX, next);
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
            while (nit != tmp.end() && nit->first == range.max.GetInt64())
            {
                range.max.SetInt64(nit->second);
                nit++;
                tit++;
            }
            range_array.push_back(range);
            nit++;
            tit++;
        }

        /*
         * 3. Get all data by iterate areas
         */
        GeoPointArray points;
        std::vector<ZRangeSpec>::iterator hit = range_array.begin();
        Iterator* iter = NULL;
        //printf("###%d \n", range_array.size());
        while (hit != range_array.end())
        {
            ZRangeSpec& range = *hit;
            KeyObject zmember(ctx.ns, KEY_ZSET_SORT, cmd.GetArguments()[0]);
            zmember.SetZSetScore(range.min.GetFloat64());
            if (NULL == iter)
            {
                iter = m_engine->Find(ctx, zmember);
            }
            else
            {
                iter->Jump(zmember);
            }
            //printf("###%.2f\n", range.min.GetFloat64());
            while (iter->Valid())
            {
                KeyObject& zkey = iter->Key(true);
                if (zkey.GetType() != KEY_ZSET_SORT || zkey.GetKey() != zmember.GetKey() || zkey.GetNameSpace() != ctx.ns)
                {
                    break;
                }
                int inrange = range.InRange(zkey.GetZSetScore());
                if (0 == inrange)
                {
                    GeoPoint point;
                    point.score = (int64_t) zkey.GetZSetScore();
                    point.value = zkey.GetZSetMember();
                    //printf("###Find %lld %s\n", point.score, point.value.AsString().c_str());
                    if (GeoHashHelper::GetXYByHash(GEO_WGS84_TYPE, GEO_STEP_MAX, (uint64) point.score, point.x, point.y))
                    {
                        point.distance = GeoHashHelper::GetWGS84Distance(x, y, point.x, point.y);
                        if (point.distance < radius)
                        {
                            points.push_back(point);
                        }
                    }
                }
                else if (inrange > 0)
                {
                    break;
                }
                iter->Next();
            }
            if (!iter->Valid())
            {
                break;
            }
            hit++;
        }
        DELETE(iter);

        /*
         * 4. sort & erase results
         */
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

        /*
         * 4. fill reply
         */
        if (NULL == options.storekey)
        {
            reply.ReserveMember(0);
            //printf("###%d\n", points.size());
            GeoPointArray::iterator pit = points.begin();
            while (pit != points.end())
            {
                RedisReply& r = reply.AddMember();
                if (options.get_patterns.empty())
                {
                    r.SetString(pit->value);
                }
                else
                {
                    RedisReply& point_reply = r.AddMember();
                    point_reply.SetString(pit->value);
                    GeoGetOptionArray::const_iterator ait = options.get_patterns.begin();
                    while (ait != options.get_patterns.end())
                    {
                        RedisReply& rr = r.AddMember();
                        if (ait->get_distances)
                        {
                            setDistance(rr, pit->distance/to_meters);
                        }
                        else if (ait->get_coodinates)
                        {
                            RedisReply& rr1 = rr.AddMember();
                            RedisReply& rr2 = rr.AddMember();
                            rr1.SetDouble(pit->x);
                            rr2.SetDouble(pit->y);
                        }
                        else if (ait->get_hash)
                        {
                            rr.SetInteger(pit->score);
                        }
                        else
                        {
                            Data attr;
                            GetValueByPattern(ctx, ait->get_pattern, pit->value, attr);
                            rr.SetString(attr);
                        }
                        ait++;
                    }
                }

                pit++;
            }
        }
        else
        {
            KeyObject dstkey(ctx.ns, KEY_META, options.storekey);
            KeyLockGuard guard(ctx,dstkey);
            DelKey(ctx, dstkey);
            if (points.size() > 0)
            {
                WriteBatchGuard batch(ctx, m_engine);
                ValueObject dstmeta;
                dstmeta.SetType(KEY_ZSET);
                dstmeta.SetObjectLen(points.size());
                GeoPointArray::iterator pit = points.begin();
                while (pit != points.end())
                {
                    KeyObject zsort(ctx.ns, KEY_ZSET_SORT, options.storekey);
                    zsort.SetZSetMember(pit->value);
                    zsort.SetZSetScore(options.storedist ? pit->distance : pit->score);
                    ValueObject zsort_value;
                    zsort_value.SetType(KEY_ZSET_SORT);
                    KeyObject zscore(ctx.ns, KEY_ZSET_SCORE, options.storekey);
                    zscore.SetZSetMember(pit->value);
                    ValueObject zscore_value;
                    zscore_value.SetType(KEY_ZSET_SCORE);
                    zscore_value.SetZSetScore(options.storedist ? pit->distance : pit->score);
                    SetKeyValue(ctx, zsort, zsort_value);
                    SetKeyValue(ctx, zscore, zscore_value);
                    pit++;
                }
                SetKeyValue(ctx, dstkey, dstmeta);
            }
            if (0 == ctx.transc_err)
            {
                reply.SetInteger(points.size());
            }
            else
            {
                reply.SetErrCode(ctx.transc_err);
            }
        }
        return 0;
    }

    int Ardb::GeoRadiusByMember(Context& ctx, RedisCommandFrame& cmd)
    {
        return GeoRadius(ctx, cmd);
    }
}

