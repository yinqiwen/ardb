/*
 * geo_test.cpp
 *
 *  Created on: 2014Äê8ÔÂ7ÈÕ
 *      Author: wangqiying
 */

#include "ardb.hpp"
using namespace ardb;

void test_geo_common(Context& ctx, Ardb& db)
{
    db.GetConfig().zset_max_ziplist_entries = 16;
    RedisCommandFrame del;
    del.SetFullCommand("del mygeo");
    db.Call(ctx, del, 0);
    double x = 300.3;
    double y = 300.3;

    double p_x = 1000.0;
    double p_y = 1000.0;
    uint32 raius = 1000;
    uint32 total = 100000;
    GeoPointArray cmp;
    for (uint32 i = 0; i < total; i++)
    {
        char name[100];
        sprintf(name, "p%u", i);
        /*
         * min accuracy is 0.2meters
         */
        double xx = x + i * 0.3;
        double yy = y + i * 0.3;
        if (((xx - p_x) * (xx - p_x) + (yy - p_y) * (yy - p_y)) <= raius * raius)
        {
            GeoPoint p;
            p.x = xx;
            p.y = yy;
            cmp.push_back(p);
        }
        RedisCommandFrame geoadd;
        geoadd.SetFullCommand("geoadd mygeo MERCATOR %.2f %.2f %s", xx, yy, name);
        db.Call(ctx, geoadd, 0);
    }

    RedisCommandFrame zcard;
    zcard.SetFullCommand("zcard mygeo");
    db.Call(ctx, zcard, 0);
    CHECK_FATAL(ctx.reply.integer != total, "geoadd failed");
    RedisCommandFrame geosearch;
    geosearch.SetFullCommand("geosearch mygeo MERCATOR %.2f %.2f radius %d ASC WITHCOORDINATES WITHDISTANCES", p_x, p_y,
            raius);
    db.Call(ctx, geosearch, 0);
    CHECK_FATAL(ctx.reply.MemberSize() != cmp.size() * 4, "geosearch failed");
}

void test_geo(Ardb& db)
{
    Context tmpctx;
    test_geo_common(tmpctx, db);
}
