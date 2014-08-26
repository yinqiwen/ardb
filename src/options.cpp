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
#include "options.hpp"
#include "util/string_helper.hpp"
#include "geo/geohash_helper.hpp"
#include <float.h>

OP_NAMESPACE_BEGIN

    static int parse_score(const std::string& score_str, double& score, bool& contain)
    {
        contain = true;
        const char* str = score_str.c_str();
        if (score_str.at(0) == '(')
        {
            contain = false;
            str++;
        }
        if (strcasecmp(str, "-inf") == 0)
        {
            score = -DBL_MAX;
        }
        else if (strcasecmp(str, "+inf") == 0)
        {
            score = DBL_MAX;
        }
        else
        {
            if (!str_todouble(str, score))
            {
                return -1;
            }
        }
        return 0;
    }
    bool ZRangeSpec::Pasrse(const std::string& minstr, const std::string& maxstr)
    {
        double min_d, max_d;
        int ret = parse_score(minstr, min_d, contain_min);
        if (0 == ret)
        {
            ret = parse_score(maxstr, max_d, contain_max);
        }
        if (ret == 0 && min_d > max_d)
        {
            return false;
        }
        min.SetDouble(min_d);
        max.SetDouble(max_d);
        return ret == 0;
    }

    uint32 LimitOptions::Parse(const std::deque<std::string>& args, uint32 idx)
    {
        if (!strcasecmp(args[idx].c_str(), "limit"))
        {
            if (idx + 2 >= args.size())
            {
                return 0;
            }
            if (!string_toint32(args[idx + 1], offset) || !string_toint32(args[idx + 2], limit))
            {
                return 0;
            }
            return 3;
        }
        return 0;
    }

    static bool verify_lexrange_para(const std::string& str)
    {
        if (str == "-" || str == "+")
        {
            return true;
        }
        if (str.empty())
        {
            return false;
        }

        if (str[0] != '(' && str[0] != '[')
        {
            return false;
        }
        return true;
    }

    bool LexRangeOptions::Parse(const std::string& minstr, const std::string& maxstr)
    {
        if (!verify_lexrange_para(minstr) || !verify_lexrange_para(maxstr))
        {
            return false;
        }
        if (minstr[0] == '(')
        {
            include_min = false;
        }
        if (minstr[0] == '[')
        {
            include_min = true;
        }
        if (maxstr[0] == '(')
        {
            include_max = false;
        }
        if (maxstr[0] == '[')
        {
            include_max = true;
        }

        if (minstr == "-")
        {
            min.clear();
            include_min = true;
        }
        else
        {
            min = minstr.substr(1);
        }
        if (maxstr == "+")
        {
            max.clear();
            include_max = true;
        }
        else
        {
            max = maxstr.substr(1);
        }
        if (min > max && !max.empty())
        {
            return false;
        }
        return true;
    }

    bool ZSetRangeByScoreOptions::Parse(const std::deque<std::string>& args, uint32 idx)
    {
        for (uint32 i = idx; i < args.size(); i++)
        {
            int ret = limit_option.Parse(args, i);
            if (ret > 0)
            {
                i += ret;
                if (i >= args.size())
                {
                    break;
                }
            }
            if (!strcasecmp(args[i].c_str(), "withscores"))
            {
                withscores = true;
            }
            else
            {
                return false;
            }
        }
        return true;
    }

    bool ZSetRangeByLexOptions::Parse(const std::deque<std::string>& args, uint32 idx)
    {
        if (!range_option.Parse(args[idx], args[idx + 1]))
        {
            return false;
        }
        for (uint32 i = idx + 2; i < args.size(); i++)
        {
            i += limit_option.Parse(args, i);
        }
        return true;
    }

    bool ZSetMergeOptions::Parse(const std::deque<std::string>& args, uint32 idx)
    {
        int numkeys;
        if (!string_toint32(args[idx], numkeys) || numkeys <= 0)
        {
            return false;
        }
        if (args.size() < (uint32) numkeys + idx + 1)
        {
            return false;
        }
        idx++;
        for (int i = idx; i < numkeys + idx; i++)
        {
            keys.push_back(args[i]);
        }
        idx += numkeys;
        for (uint32 i = idx; i < args.size(); i++)
        {
            if (!strcasecmp(args[i].c_str(), "weights"))
            {
                if (args.size() < i + keys.size())
                {
                    return false;
                }
                uint32 weight;
                i++;
                for (int j = 0; j < keys.size(); j++)
                {
                    if (!string_touint32(args[i + j], weight))
                    {
                        return false;
                    }
                    weights.push_back(weight);
                }
                i += weights.size() - 1;
            }
            else if (!strcasecmp(args[i].c_str(), "aggregate"))
            {
                if (args.size() < i + 1)
                {
                    return false;
                }
                i++;
                if (!strcasecmp(args[i].c_str(), "sum"))
                {
                    aggregate = AGGREGATE_SUM;
                }
                else if (!strcasecmp(args[i].c_str(), "max"))
                {
                    aggregate = AGGREGATE_MAX;
                }
                else if (!strcasecmp(args[i].c_str(), "min"))
                {
                    aggregate = AGGREGATE_MIN;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }
        if (weights.empty())
        {
            for (uint32 i = 0; i < keys.size(); i++)
            {
                weights.push_back(1);
            }
        }
        return true;
    }

    bool SortOptions::Parse(const std::deque<std::string>& args, uint32 idx)
    {
        for (uint32 i = idx; i < args.size(); i++)
        {
            if (!strcasecmp(args[i].c_str(), "asc"))
            {
                is_desc = false;
            }
            else if (!strcasecmp(args[i].c_str(), "desc"))
            {
                is_desc = true;
            }
            else if (!strcasecmp(args[i].c_str(), "alpha"))
            {
                with_alpha = true;
            }
            else if (!strcasecmp(args[i].c_str(), "limit") && (i + 2) < args.size())
            {
                with_limit = true;
                if (!string_toint32(args[i + 1], limit_offset) || !string_toint32(args[i + 2], limit_count))
                {
                    return false;
                }
                i += 2;
            }
            else if (!strcasecmp(args[i].c_str(), "aggregate") && i < args.size() - 1)
            {
                if (!strcasecmp(args[i + 1].c_str(), "sum"))
                {
                    aggregate = AGGREGATE_SUM;
                }
                else if (!strcasecmp(args[i + 1].c_str(), "max"))
                {
                    aggregate = AGGREGATE_MAX;
                }
                else if (!strcasecmp(args[i + 1].c_str(), "min"))
                {
                    aggregate = AGGREGATE_MIN;
                }
                else if (!strcasecmp(args[i + 1].c_str(), "count"))
                {
                    aggregate = AGGREGATE_COUNT;
                }
                else if (!strcasecmp(args[i + 1].c_str(), "avg"))
                {
                    aggregate = AGGREGATE_AVG;
                }
                else
                {
                    return false;
                }
                i++;
            }
            else if (!strcasecmp(args[i].c_str(), "store") && i < args.size() - 1)
            {
                store_dst = args[i + 1].c_str();
                i++;
            }
            else if (!strcasecmp(args[i].c_str(), "by") && i < args.size() - 1)
            {
                by = args[i + 1].c_str();
                if (!strcasecmp(by, "nosort"))
                {
                    by = NULL;
                    nosort = true;
                }
                i++;
            }
            else if (!strcasecmp(args[i].c_str(), "get") && i < args.size() - 1)
            {
                get_patterns.push_back(args[i + 1].c_str());
                i++;
            }
            else
            {
                DEBUG_LOG("Invalid sort option:%s", args[i].c_str());
                return false;
            }
        }
        return true;
    }

    int GeoAddOptions::Parse(const std::deque<std::string>& args, std::string& err, uint32 off)
    {
        if (!strcasecmp(args[off].c_str(), "wgs84"))
        {
            coord_type = GEO_WGS84_TYPE;
        }
        else if (!strcasecmp(args[off].c_str(), "mercator"))
        {
            coord_type = GEO_MERCATOR_TYPE;
        }
        else
        {
            WARN_LOG("Invalid coord-type:%s.", args[off].c_str());
            return -1;
        }
        off++;
        for (; off < args.size(); off += 3)
        {
            if (off + 2 >= args.size())
            {
                WARN_LOG("Invalid args.");
                return -1;
            }
            GeoPoint point;
            point.value = args[off + 2];
            if (!string_todouble(args[off], point.x) || !string_todouble(args[off + 1], point.y))
            {
                err = "Invalid coodinates " + args[off] + "/" + args[off + 1];
                return -1;
            }
            if (!GeoHashHelper::VerifyCoordinates(coord_type, point.x, point.y))
            {
                err = "Invalid coodinates " + args[off] + "/" + args[off + 1];
                return -1;
            }
            points.push_back(point);
        }

        return 0;
    }

    int GeoSearchOptions::Parse(const std::deque<std::string>& args, std::string& err, uint32 off)
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
                asc = false;
                nosort = false;
            }
            else if (!strcasecmp(args[i].c_str(), "limit") && i < args.size() - 2)
            {
                if (!string_toint32(args[i + 1], offset) || !string_toint32(args[i + 2], limit))
                {
                    err = "Invalid limit/offset value.";
                    return -1;
                }
                i += 2;
            }
            else if (!strcasecmp(args[i].c_str(), "RADIUS") && i < args.size() - 1)
            {
                if (!string_touint32(args[i + 1], radius) || radius < 1 || radius >= 10000000)
                {
                    err = "Invalid radius value.";
                    return -1;
                }
                i++;
            }
            else if ((!strcasecmp(args[i].c_str(), "mercator") || !strcasecmp(args[i].c_str(), "wgs84"))
                    && i < args.size() - 2)
            {
                if (!string_todouble(args[i + 1], x) || !string_todouble(args[i + 2], y))
                {
                    err = "Invalid location value.";
                    return -1;
                }
                if (!strcasecmp(args[i].c_str(), "wgs84"))
                {
                    coord_type = GEO_WGS84_TYPE;
                }
                else if (!strcasecmp(args[i].c_str(), "mercator"))
                {
                    coord_type = GEO_MERCATOR_TYPE;
                }
                if (!GeoHashHelper::VerifyCoordinates(coord_type, x, y))
                {
                    err = "Invalid location value.";
                    return -1;
                }
                by_location = true;
                i += 2;
            }
            else if (!strcasecmp(args[i].c_str(), "member") && i < args.size() - 1)
            {
                member = args[i + 1];
                by_member = true;
                i++;
            }
            else if (!strcasecmp(args[i].c_str(), "in") && i < args.size() - 1)
            {
                uint32 len;
                if (!string_touint32(args[i + 1], len) || (i + 1 + len) > args.size() - 1)
                {
                    err = "Invalid member value.";
                    return -1;
                }
                for (uint32 j = 0; j < len; j++)
                {
                    submembers.insert(args[i + 2 + j]);
                }
                in_members = true;
                i = i + len + 1;
            }
            else if ((!strcasecmp(args[i].c_str(), "GET")) && i < args.size() - 1)
            {
                GeoSearchGetOption get;
                get.get_pattern = args[i + 1];
                get_patterns.push_back(get);
                i++;
            }
            else if ((!strcasecmp(args[i].c_str(), "HGETALL")) && i < args.size() - 1)
            {
                GeoSearchGetOption get;
                get.get_pattern = args[i + 1];
                get.hgetall = true;
                get_patterns.push_back(get);
                i++;
            }
            else if ((!strcasecmp(args[i].c_str(), "include")) && i < args.size() - 2)
            {
                if (!includes.insert(StringStringMap::value_type(args[i + 1], args[i + 2])).second)
                {
                    err = "duplicate include key pattern:" + args[i + 1];
                    return -1;
                }
                i += 2;
            }
            else if ((!strcasecmp(args[i].c_str(), "exclude")) && i < args.size() - 2)
            {
                if (!excludes.insert(StringStringMap::value_type(args[i + 1], args[i + 2])).second)
                {
                    err = "duplicate exclude key pattern:" + args[i + 1];
                    return -1;
                }
                i += 2;
            }
            else if ((!strcasecmp(args[i].c_str(), GEO_SEARCH_WITH_COORDINATES)))
            {
                GeoSearchGetOption get;
                get.get_coodinates = true;
                get_patterns.push_back(get);
            }
            else if ((!strcasecmp(args[i].c_str(), GEO_SEARCH_WITH_DISTANCES)))
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

        if (radius < 1)
        {
            err = "no radius specified";
            return -1;
        }
        if (!by_location && !by_member)
        {
            err = "no location/member specified";
            return -1;
        }
        return 0;
    }
OP_NAMESPACE_END

