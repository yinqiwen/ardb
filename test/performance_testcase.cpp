/*
 * performance_testcase.cpp
 *
 *  Created on: Jan 7, 2014
 *      Author: wangqiying
 */

#include "ardb.hpp"
#include "util/math_helper.hpp"
#include <string>

using namespace ardb;

#define PERF_LOOP_COUNT  1000000

void test_string_performace(Ardb& db)
{
    printf("=====================String Performace Test Start=====================\n");
    uint64 start = get_current_epoch_millis();
    DBID dbid = 0;
    for (uint32 i = 0; i < PERF_LOOP_COUNT; i++)
    {
        char field[64], value[128];
        sprintf(field, "string_%u", i);
        sprintf(value, "value_%u", i);
        db.Set(dbid, field, value);
    }
    uint64 end = get_current_epoch_millis();
    printf("Cost %llums to execute set %u times, avg %.2f qps.\n", (end - start), PERF_LOOP_COUNT,
    PERF_LOOP_COUNT * 1000.0 / (end - start));
    start = get_current_epoch_millis();
    for (uint32 i = 0; i < PERF_LOOP_COUNT; i++)
    {
        char field[64];
        sprintf(field, "string_%u", random_between_int32(0, PERF_LOOP_COUNT));
        std::string v;
        db.Get(dbid, field, v);
    }
    end = get_current_epoch_millis();
    printf("Cost %llums to execute get %u times, avg %.2f qps.\n", (end - start), PERF_LOOP_COUNT,
    PERF_LOOP_COUNT * 1000.0 / (end - start));
    printf("=====================String Performace Test End=====================\n");
}

void test_nonzip_hash_performace(Ardb& db)
{
    DBID dbid = 0;
    printf("=====================Hash(NotZiped) Performace Test Start=====================\n");
    db.Del(dbid, "perfhashtest");
    uint64 start = get_current_epoch_millis();

    for (uint32 i = 0; i < PERF_LOOP_COUNT; i++)
    {
        char field[64], value[128];
        sprintf(field, "field_%u", i);
        sprintf(value, "value_%u", i);
        db.HSet(dbid, "perfhashtest", field, value);
    }
    uint64 end = get_current_epoch_millis();
    printf("Cost %llums to execute hset %u times, avg %.2f qps.\n", (end - start), PERF_LOOP_COUNT,
    PERF_LOOP_COUNT * 1000.0 / (end - start));
    start = get_current_epoch_millis();
    for (uint32 i = 0; i < PERF_LOOP_COUNT; i++)
    {
        char field[64];
        sprintf(field, "field_%u", random_between_int32(0, PERF_LOOP_COUNT));
        std::string v;
        db.HGet(dbid, "perfhashtest", field, &v);
    }
    end = get_current_epoch_millis();
    printf("Cost %llums to execute hget %u times, avg %.2f qps.\n", (end - start), PERF_LOOP_COUNT,
    PERF_LOOP_COUNT * 1000.0 / (end - start));
    printf("=====================Hash(NotZiped) Performace Test End=====================\n");
}

void test_zip_hash_performace(Ardb& db)
{
    printf("=====================Hash(Ziped) Performace Test Start=====================\n");
    uint64 start = get_current_epoch_millis();
    DBID dbid = 0;
    for (uint32 i = 0; i < PERF_LOOP_COUNT / 10; i++)
    {
        for (uint32 j = 0; j < 10; j++)
        {
            char key[64], field[64], value[128];
            sprintf(key, "perfhashtest_%u", i);
            sprintf(field, "field_%u", j);
            sprintf(value, "value_%u", j);
            db.HSet(dbid, key, field, value);
        }
    }
    uint64 end = get_current_epoch_millis();
    printf("Cost %llums to execute hset %u times, avg %.2f qps.\n", (end - start), PERF_LOOP_COUNT,
    PERF_LOOP_COUNT * 1000.0 / (end - start));
    start = get_current_epoch_millis();
    for (uint32 i = 0; i < PERF_LOOP_COUNT / 10; i++)
    {
        for (uint32 j = 0; j < 10; j++)
        {
            char key[64], field[64], value[128];
            sprintf(key, "perfhashtest_%u", random_between_int32(0, PERF_LOOP_COUNT / 10));
            sprintf(field, "field_%u", random_between_int32(0, j));
            std::string v;
            db.HGet(dbid, key, field, &v);
        }
    }
    end = get_current_epoch_millis();
    printf("Cost %llums to execute hget %u times, avg %.2f qps.\n", (end - start), PERF_LOOP_COUNT,
    PERF_LOOP_COUNT * 1000.0 / (end - start));
    printf("=====================Hash(Ziped) Performace Test End=====================\n");
}

void test_nonzip_set_performace(Ardb& db)
{
    DBID dbid = 0;
    printf("=====================Set(NotZiped) Performace Test Start=====================\n");
    db.Del(dbid, "perfsettest");
    uint64 start = get_current_epoch_millis();

    for (uint32 i = 0; i < PERF_LOOP_COUNT; i++)
    {
        char value[128];
        sprintf(value, "value_%u", i);
        db.SAdd(dbid, "perfsettest", value);
    }
    uint64 end = get_current_epoch_millis();
    printf("Cost %llums to execute SAdd %u times, avg %.2f qps.\n", (end - start), PERF_LOOP_COUNT,
    PERF_LOOP_COUNT * 1000.0 / (end - start));
    start = get_current_epoch_millis();
    for (uint32 i = 0; i < PERF_LOOP_COUNT; i++)
    {
        char field[64];
        sprintf(field, "value_%u", random_between_int32(0, PERF_LOOP_COUNT));
        db.SIsMember(dbid, "perfsettest", field);
    }
    end = get_current_epoch_millis();
    printf("Cost %llums to execute SIsMember %u times, avg %.2f qps.\n", (end - start), PERF_LOOP_COUNT,
    PERF_LOOP_COUNT * 1000.0 / (end - start));
    printf("=====================Set(NotZiped) Performace Test End=====================\n");
}

void test_zip_set_performace(Ardb& db)
{
    printf("=====================Set(Ziped) Performace Test Start=====================\n");
    uint64 start = get_current_epoch_millis();
    DBID dbid = 0;
    for (uint32 i = 0; i < PERF_LOOP_COUNT / 10; i++)
    {
        for (uint32 j = 0; j < 10; j++)
        {
            char key[64], value[128];
            sprintf(key, "perfsettest_%u", i);
            sprintf(value, "value_%u", j);
            db.SAdd(dbid, key, value);
        }
    }
    uint64 end = get_current_epoch_millis();
    printf("Cost %llums to execute SAdd %u times, avg %.2f qps.\n", (end - start), PERF_LOOP_COUNT,
    PERF_LOOP_COUNT * 1000.0 / (end - start));
    start = get_current_epoch_millis();
    for (uint32 i = 0; i < PERF_LOOP_COUNT / 10; i++)
    {
        for (uint32 j = 0; j < 10; j++)
        {
            char key[64], value[128];
            sprintf(key, "perfsettest_%u", random_between_int32(0, PERF_LOOP_COUNT / 10));
            sprintf(value, "value_%u", j);
            db.SIsMember(dbid, key, value);
        }
    }
    end = get_current_epoch_millis();
    printf("Cost %llums to execute SIsMember %u times, avg %.2f qps.\n", (end - start), PERF_LOOP_COUNT,
    PERF_LOOP_COUNT * 1000.0 / (end - start));
    printf("=====================Set(Ziped) Performace Test End=====================\n");
}

void test_nonzip_list_performace(Ardb& db)
{
    DBID dbid = 0;
    printf("=====================List(NotZiped) Performace Test Start=====================\n");
    db.Del(dbid, "perflisttest");
    uint64 start = get_current_epoch_millis();

    for (uint32 i = 0; i < PERF_LOOP_COUNT; i++)
    {
        char value[128];
        sprintf(value, "value_%u", i);
        db.LPush(dbid, "perflisttest", value);
    }
    uint64 end = get_current_epoch_millis();
    printf("Cost %llums to execute LPush %u times, avg %.2f qps.\n", (end - start), PERF_LOOP_COUNT,
    PERF_LOOP_COUNT * 1000.0 / (end - start));
    start = get_current_epoch_millis();
    for (uint32 i = 0; i < PERF_LOOP_COUNT; i++)
    {
        std::string v;
        db.RPop(dbid, "perflisttest", v);
    }
    end = get_current_epoch_millis();
    printf("Cost %llums to execute RPop %u times, avg %.2f qps.\n", (end - start), PERF_LOOP_COUNT,
    PERF_LOOP_COUNT * 1000.0 / (end - start));
    printf("=====================List(NotZiped) Performace Test End=====================\n");
}

void test_zip_list_performace(Ardb& db)
{
    printf("=====================List(Ziped) Performace Test Start=====================\n");
    uint64 start = get_current_epoch_millis();
    DBID dbid = 0;
    for (uint32 i = 0; i < PERF_LOOP_COUNT / 10; i++)
    {
        for (uint32 j = 0; j < 10; j++)
        {
            char key[64], value[128];
            sprintf(key, "perflisttest_%u", i);
            sprintf(value, "value_%u", j);
            db.LPush(dbid, key, value);
        }
    }
    uint64 end = get_current_epoch_millis();
    printf("Cost %llums to execute LPush %u times, avg %.2f qps.\n", (end - start), PERF_LOOP_COUNT,
    PERF_LOOP_COUNT * 1000.0 / (end - start));
    start = get_current_epoch_millis();
    for (uint32 i = 0; i < PERF_LOOP_COUNT / 10; i++)
    {
        for (uint32 j = 0; j < 10; j++)
        {
            char key[64], value[128];
            sprintf(key, "perfsettest_%u", random_between_int32(0, PERF_LOOP_COUNT / 10));
            std::string v;
            db.RPop(dbid, key, v);
        }
    }
    end = get_current_epoch_millis();
    printf("Cost %llums to execute RPop %u times, avg %.2f qps.\n", (end - start), PERF_LOOP_COUNT,
    PERF_LOOP_COUNT * 1000.0 / (end - start));
    printf("=====================List(Ziped) Performace Test End=====================\n");
}

void test_nonzip_zset_performace(Ardb& db)
{
    DBID dbid = 0;
    printf("=====================ZSet(NotZiped) Performace Test Start=====================\n");
    db.Del(dbid, "perfzsettest");
    uint64 start = get_current_epoch_millis();

    for (uint32 i = 0; i < PERF_LOOP_COUNT; i++)
    {
        char value[128];
        sprintf(value, "value_%u", i);
        ValueData score((int64) i);
        db.ZAdd(dbid, "perfzsettest", score, value);
    }
    uint64 end = get_current_epoch_millis();
    printf("Cost %llums to execute ZAdd %u times, avg %.2f qps.\n", (end - start), PERF_LOOP_COUNT,
    PERF_LOOP_COUNT * 1000.0 / (end - start));
    start = get_current_epoch_millis();
    for (uint32 i = 0; i < PERF_LOOP_COUNT; i++)
    {
        ValueData score;
        char field[64];
        sprintf(field, "value_%u", random_between_int32(0, PERF_LOOP_COUNT));
        db.ZScore(dbid, "perfzsettest", field, score);
    }
    end = get_current_epoch_millis();
    printf("Cost %llums to execute ZScore %u times, avg %.2f qps.\n", (end - start), PERF_LOOP_COUNT,
    PERF_LOOP_COUNT * 1000.0 / (end - start));
    printf("=====================ZSet(NotZiped) Performace Test End=====================\n");
}

void test_zip_zset_performace(Ardb& db)
{
    printf("=====================ZSet(Ziped) Performace Test Start=====================\n");
    uint64 start = get_current_epoch_millis();
    DBID dbid = 0;
    for (uint32 i = 0; i < PERF_LOOP_COUNT / 10; i++)
    {
        for (uint32 j = 0; j < 10; j++)
        {
            char key[64], value[128];
            sprintf(key, "perfzsettest_%u", i);
            ValueData score((int64) j);
            sprintf(value, "value_%u", j);
            db.ZAdd(dbid, key, score, value);
        }
    }
    uint64 end = get_current_epoch_millis();
    printf("Cost %llums to execute ZAdd %u times, avg %.2f qps.\n", (end - start), PERF_LOOP_COUNT,
    PERF_LOOP_COUNT * 1000.0 / (end - start));
    start = get_current_epoch_millis();
    for (uint32 i = 0; i < PERF_LOOP_COUNT / 10; i++)
    {
        for (uint32 j = 0; j < 10; j++)
        {
            char key[64], value[128];
            sprintf(key, "perfzsettest_%u", random_between_int32(0, PERF_LOOP_COUNT / 10));
            sprintf(value, "value_%u", random_between_int32(0, PERF_LOOP_COUNT));
            ValueData score;
            db.ZScore(dbid, key, value, score);
        }
    }
    end = get_current_epoch_millis();
    printf("Cost %llums to execute ZScore %u times, avg %.2f qps.\n", (end - start), PERF_LOOP_COUNT,
    PERF_LOOP_COUNT * 1000.0 / (end - start));
    printf("=====================ZSet(Ziped) Performace Test End=====================\n");
}

void test_performance(Ardb& db)
{
    test_string_performace(db);
    test_nonzip_hash_performace(db);
    test_zip_hash_performace(db);
    test_nonzip_set_performace(db);
    test_zip_set_performace(db);
    test_nonzip_list_performace(db);
    test_zip_list_performace(db);
    test_nonzip_zset_performace(db);
    test_zip_zset_performace(db);
}
