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

#ifndef OPTIONS_HPP_
#define OPTIONS_HPP_

#include "common/common.hpp"
#include "codec.hpp"
#include "channel/all_includes.hpp"

using namespace ardb::codec;
OP_NAMESPACE_BEGIN
    enum DataOperation
    {
        OP_GET = 1, OP_SET, OP_DELETE, OP_COUNT
    };
    enum AggregateType
    {
        AGGREGATE_EMPTY = 0,
        AGGREGATE_SUM = 1,
        AGGREGATE_MIN = 2,
        AGGREGATE_MAX = 3,
        AGGREGATE_COUNT = 4,
        AGGREGATE_AVG = 5,
    };



    struct GenericSetOptions
    {
            bool with_expire;
            bool with_nx;
            bool with_xx;
            bool fill_reply;
            int64 expire;
            TimeUnit unit;
            RedisReply abort_reply;
            RedisReply ok_reply;
            GenericSetOptions() :
                    with_expire(false), with_nx(false), with_xx(false), fill_reply(true), expire(0), unit(SECONDS)
            {
            }
    };
    struct GenericGetOptions
    {
            bool fill_reply;
            GenericGetOptions() :
                    fill_reply(true)
            {
            }
    };

    struct KeysOptions
    {
            std::string pattern;
            int32 limit;
            DataOperation op;
            KeysOptions() :
                    limit(-1), op(OP_GET)
            {
            }
    };

    struct ZRangeSpec
    {
            Data min, max;
            bool contain_min, contain_max;
            ZRangeSpec() :
                    contain_min(false), contain_max(false)
            {
                min.SetInt64(0);
                max.SetInt64(0);
            }
            bool Pasrse(const std::string& minstr, const std::string& maxstr);

            bool InRange(const Data& d) const
            {
                bool inrange = d >= min && d <= max;
                if (inrange)
                {
                    if (!contain_min && d == min)
                    {
                        inrange = false;
                    }
                    if (!contain_max && d == max)
                    {
                        inrange = false;
                    }
                }
                return inrange;
            }
            bool InRange(double d) const
            {
                Data data;
                data.SetDouble(d);
                return InRange(data);
            }
    };
    struct LimitOptions
    {
            bool with_limit;
            int32 offset;
            int32 limit;
            LimitOptions() :
                    with_limit(false), offset(0), limit(-1)
            {
            }
            uint32 Parse(const std::deque<std::string>& args, uint32 idx);
    };

    struct LexRangeOptions
    {
            std::string min;
            std::string max;
            bool include_min;
            bool include_max;
            LexRangeOptions() :
                    include_min(false), include_max(false)
            {
            }
            bool Parse(const std::string& minstr, const std::string& maxstr);
            bool InRange(const std::string& str) const
            {
                bool in_range = false;
                if (max.empty())
                {
                    in_range = str >= min;
                }
                else
                {
                    in_range = str >= min && str <= max;
                }

                if (in_range)
                {
                    if (!include_min && str == min)
                    {
                        return false;
                    }
                    if (!include_max && str == max)
                    {
                        return false;
                    }
                }
                return in_range;
            }
    };

    struct ZSetRangeByLexOptions
    {
            LexRangeOptions range_option;
            LimitOptions limit_option;

            bool Parse(const std::deque<std::string>& args, uint32 idx);
    };

    struct ZSetMergeOptions
    {
            StringArray keys;
            DoubleArray weights;
            AggregateType aggregate;
            ZSetMergeOptions() :
                    aggregate(AGGREGATE_SUM)
            {
            }
            bool Parse(const std::deque<std::string>& args, uint32 idx);
    };

    struct SortOptions
    {
            const char* by;
            bool with_limit;
            int32 limit_offset;
            int32 limit_count;
            std::vector<const char*> get_patterns;
            bool is_desc;
            bool with_alpha;
            bool nosort;
            const char* store_dst;
            AggregateType aggregate;
            SortOptions() :
                    by(NULL), with_limit(false), limit_offset(0), limit_count(0), is_desc(false), with_alpha(false), nosort(
                            false), store_dst(
                    NULL), aggregate(AGGREGATE_EMPTY)
            {
            }
            bool Parse(const std::deque<std::string>& args, uint32 idx);
    };


    struct ZSetRangeByScoreOptions
    {
            DataOperation op;
            bool withscores;
            bool reverse;
            bool fill_reply;
            bool fetch_geo_location;

            LimitOptions limit_option;

            DataArray results;
            LocationDeque locs;

            ZSetRangeByScoreOptions() :
                    op(OP_GET), withscores(false), reverse(false), fill_reply(true), fetch_geo_location(false)
            {
            }

            bool Parse(const std::deque<std::string>& args, uint32 idx);
    };

    struct GeoPoint
    {
            double x, y;
            std::string value;

            double distance;
            GeoPoint() :
                    x(0), y(0), distance(0)
            {
            }

    };
    typedef std::deque<GeoPoint> GeoPointArray;
    struct GeoAddOptions
    {
            uint8 coord_type;
            GeoPointArray points;

            GeoAddOptions() :
                    coord_type(0)
            {
            }

            int Parse(const std::deque<std::string>& args, std::string& err, uint32 off = 0);
    };
#define GEO_SEARCH_WITH_DISTANCES "WITHDISTANCES"
#define GEO_SEARCH_WITH_COORDINATES "WITHCOORDINATES"
    struct GeoSearchGetOption
    {
            bool get_coodinates;
            bool get_distances;
            std::string get_pattern;
            GeoSearchGetOption() :
                    get_coodinates(false), get_distances(false)
            {
            }
    };
    typedef std::vector<GeoSearchGetOption> GeoGetOptionArray;
    typedef TreeMap<std::string, std::string>::Type StringStringMap;
    typedef TreeMap<std::string, double>::Type StringDoubleMap;
    struct GeoSearchOptions
    {
            uint8 coord_type;
            bool nosort;
            bool asc;   //sort by asc
            uint32 radius;  //range meters
            int32 offset;
            int32 limit;
            bool by_member;
            bool by_location;
            bool in_members;

            double x, y;
            std::string member;
            StringSet submembers;

            StringStringMap includes;
            StringStringMap excludes;

            GeoGetOptionArray get_patterns;
            GeoSearchOptions() :
                    coord_type(0), nosort(true), asc(false), radius(0), offset(0), limit(0), by_member(false), by_location(
                            false), in_members(false), x(0), y(0)
            {
            }

            int Parse(const std::deque<std::string>& args, std::string& err, uint32 off = 0);

    };
OP_NAMESPACE_END

#endif /* OPTIONS_HPP_ */
