/*
 * test_case.cpp
 *
 *  Created on: 2013-4-4
 *      Author: yinqiwen
 */
#include "test_common.hpp"
#include "util/thread/spin_rwlock.hpp"
#include "util/thread/lock_guard.hpp"
#include "util/thread/thread.hpp"

struct WriteTask: public Runnable
{
        std::map<int, int>* mm;
        SpinRWLock* lock;

        void Run()
        {
            uint64 start = get_current_epoch_millis();
            for (int i = 0; i < 1000000; i++)
            {
                WriteLockGuard<SpinRWLock> guard(*lock);
                (*mm)[i] = 1;
            }
            uint64 end = get_current_epoch_millis();
            INFO_LOG("Cost %llums to write", (end - start));
        }
};

struct ReadTask: public Runnable
{
        std::map<int, int>* mm;
        SpinRWLock* lock;
        void Run()
        {
            uint64 start = get_current_epoch_millis();
            for (int i = 0; i < 1000000; i++)
            {
                ReadLockGuard<SpinRWLock> guard(*lock);
                mm->find(i);
            }
            uint64 end = get_current_epoch_millis();
            INFO_LOG("Cost %llums to read", (end - start));
        }
};

void test_rwlock(Ardb& db)
{
    WriteTask wtask;
    ReadTask rtask;
    std::map<int, int> mm;
    SpinRWLock lock;
    wtask.lock = &lock;
    wtask.mm = &mm;
    rtask.lock = &lock;
    rtask.mm = &mm;
    Thread* w = new Thread(&wtask);
    std::vector<Thread*> ts;
    for(uint32 i = 0; i < 10; i++)
    {
        Thread* r = new Thread(&rtask);
        r->Start();
        ts.push_back(r);
    }
    w->Start();
    w->Join();
    std::vector<Thread*>::iterator tit = ts.begin();
    while(tit != ts.end())
    {
        (*tit)->Join();
        tit++;
    }
    INFO_LOG("mm size=%u", mm.size());
}

void test_type(Ardb& db)
{
    DBID dbid = 0;
    db.SAdd(dbid, "myset", "123");
    db.LPush(dbid, "mylist", "value0");
    db.ZAdd(dbid, "myzset1", ValueData((int64) 1), "one");
    db.HSet(dbid, "myhash", "field1", "value1");
    db.Set(dbid, "skey", "abc");
    db.SetBit(dbid, "mybits", 1, 1);

    CHECK_FATAL(db.Type(dbid, "myset") != SET_META, "type failed.");
    CHECK_FATAL(db.Type(dbid, "mylist") != LIST_META, "type failed.");
    CHECK_FATAL(db.Type(dbid, "myzset1") != ZSET_META, "type failed.");
    CHECK_FATAL(db.Type(dbid, "myhash") != HASH_META, "type failed.");
    CHECK_FATAL(db.Type(dbid, "skey") != STRING_META, "type failed.");
    CHECK_FATAL(db.Type(dbid, "mybits") != BITSET_META, "type failed.");
}

void test_sort_list(Ardb& db)
{
    DBID dbid = 0;
    db.LClear(dbid, "mylist");
    db.RPush(dbid, "mylist", "100");
    db.RPush(dbid, "mylist", "10");
    db.RPush(dbid, "mylist", "9");
    db.RPush(dbid, "mylist", "1000");

    StringArray args;
    ValueDataArray vs;
    db.Sort(dbid, "mylist", args, vs);
    CHECK_FATAL(vs.size() != 4, "sort result size error:%zu", vs.size());
    CHECK_FATAL(vs[0].integer_value != 9, "sort result[0]:%"PRId64, vs[0].integer_value);
    CHECK_FATAL(vs[1].integer_value != 10, "sort result[0]:%"PRId64, vs[1].integer_value);
    CHECK_FATAL(vs[2].integer_value != 100, "sort result[0]:%"PRId64, vs[2].integer_value);
    CHECK_FATAL(vs[3].integer_value != 1000, "sort result[0]:%"PRId64, vs[3].integer_value);

    vs.clear();

    args.clear();
    string_to_string_array("limit 1 2", args);
    db.Sort(dbid, "mylist", args, vs);
    CHECK_FATAL(vs.size() != 2, "sort result size error:%zu", vs.size());
    CHECK_FATAL(vs[0].integer_value != 10, "sort result[0]:%"PRId64, vs[0].integer_value);
    CHECK_FATAL(vs[1].integer_value != 100, "sort result[0]:%"PRId64, vs[1].integer_value);

    vs.clear();
    args.clear();
    string_to_string_array("by weight_*", args);
    db.Set(dbid, "weight_100", "1000");
    db.Set(dbid, "weight_10", "900");
    db.Set(dbid, "weight_9", "800");
    db.Set(dbid, "weight_1000", "700");
    db.Sort(dbid, "mylist", args, vs);
    CHECK_FATAL(vs.size() != 4, "sort result size error:%zu", vs.size());
    CHECK_FATAL(vs[0].integer_value != 1000, "sort result[0]:%"PRId64, vs[0].integer_value);
    CHECK_FATAL(vs[1].integer_value != 9, "sort result[0]:%"PRId64, vs[1].integer_value);
    CHECK_FATAL(vs[2].integer_value != 10, "sort result[0]:%"PRId64, vs[2].integer_value);
    CHECK_FATAL(vs[3].integer_value != 100, "sort result[0]:%"PRId64, vs[3].integer_value);

    db.HSet(dbid, "myhash", "field_100", "hash100");
    db.HSet(dbid, "myhash", "field_10", "hash10");
    db.HSet(dbid, "myhash", "field_9", "hash9");
    db.HSet(dbid, "myhash", "field_1000", "hash1000");

    args.clear();
    string_to_string_array("by weight_* get myhash->field_* get #", args);
    vs.clear();
    db.Sort(dbid, "mylist", args, vs);
    std::string str;
    CHECK_FATAL(vs.size() != 8, "sort result size error:%zu", vs.size());
    CHECK_FATAL(vs[0].ToString(str) != "hash1000", "sort result[0]:%s", str.c_str());
    CHECK_FATAL(vs[2].ToString(str) != "hash9", "sort result[2]:%s", str.c_str());
    CHECK_FATAL(vs[4].ToString(str) != "hash10", "sort result[4]:%s", str.c_str());
    CHECK_FATAL(vs[6].ToString(str) != "hash100", "sort result[6]:%s", str.c_str());
    CHECK_FATAL(vs[1].integer_value != 1000, "sort result[1]:%"PRId64, vs[1].integer_value);
    CHECK_FATAL(vs[3].integer_value != 9, "sort result[3]:%"PRId64, vs[3].integer_value);
    CHECK_FATAL(vs[5].integer_value != 10, "sort result[5]:%"PRId64, vs[5].integer_value);
    CHECK_FATAL(vs[7].integer_value != 100, "sort result[7]:%"PRId64, vs[7].integer_value);
}

void test_sort_set(Ardb& db)
{
    DBID dbid = 0;
    db.SClear(dbid, "myset");
    db.SAdd(dbid, "myset", "ab3");
    db.SAdd(dbid, "myset", "ab2");
    db.SAdd(dbid, "myset", "ab1");
    db.SAdd(dbid, "myset", "ab4");

    StringArray args;
    ValueDataArray vs;
    db.Sort(dbid, "myset", args, vs);
    std::string str;
    CHECK_FATAL(vs.size() != 4, "sort result size error:%zu", vs.size());
    CHECK_FATAL(vs[0].ToString(str) != "ab1", "sort result[0]:%s", str.c_str());
    CHECK_FATAL(vs[1].ToString(str) != "ab2", "sort result[1]:%s", str.c_str());
    CHECK_FATAL(vs[2].ToString(str) != "ab3", "sort result[2]:%s", str.c_str());
    CHECK_FATAL(vs[3].ToString(str) != "ab4", "sort result[3]:%s", str.c_str());

    vs.clear();
    string_to_string_array("limit 1 2", args);
    db.Sort(dbid, "myset", args, vs);
    CHECK_FATAL(vs.size() != 2, "sort result size error:%zu", vs.size());
    CHECK_FATAL(vs[0].ToString(str) != "ab2", "sort result[0]:%s", str.c_str());
    CHECK_FATAL(vs[1].ToString(str) != "ab3", "sort result[1]:%s", str.c_str());

    vs.clear();
    args.clear();
    string_to_string_array("by weight_*", args);
    db.Set(dbid, "weight_ab1", "1000");
    db.Set(dbid, "weight_ab2", "900");
    db.Set(dbid, "weight_ab3", "800");
    db.Set(dbid, "weight_ab4", "700");
    db.Sort(dbid, "myset", args, vs);
    CHECK_FATAL(vs.size() != 4, "sort result size error:%zu", vs.size());
    CHECK_FATAL(vs[0].ToString(str) != "ab4", "sort result[0]:%s", str.c_str());
    CHECK_FATAL(vs[1].ToString(str) != "ab3", "sort result[1]:%s", str.c_str());
    CHECK_FATAL(vs[2].ToString(str) != "ab2", "sort result[2]:%s", str.c_str());
    CHECK_FATAL(vs[3].ToString(str) != "ab1", "sort result[3]:%s", str.c_str());

    db.HSet(dbid, "myhash_ab1", "field", "hash100");
    db.HSet(dbid, "myhash_ab2", "field", "hash10");
    db.HSet(dbid, "myhash_ab3", "field", "hash9");
    db.HSet(dbid, "myhash_ab4", "field", "hash1000");
    args.clear();
    string_to_string_array("by weight_* get myhash_*->field get #", args);

    vs.clear();
    db.Sort(dbid, "myset", args, vs);
    CHECK_FATAL(vs.size() != 8, "sort result size error:%zu", vs.size());
    CHECK_FATAL(vs[0].ToString(str) != "hash1000", "sort result[0]:%s", str.c_str());
    CHECK_FATAL(vs[2].ToString(str) != "hash9", "sort result[2]:%s", str.c_str());
    CHECK_FATAL(vs[4].ToString(str) != "hash10", "sort result[4]:%s", str.c_str());
    CHECK_FATAL(vs[6].ToString(str) != "hash100", "sort result[6]:%s", str.c_str());
    CHECK_FATAL(vs[1].ToString(str) != "ab4", "sort result[1]:%s", str.c_str());
    CHECK_FATAL(vs[3].ToString(str) != "ab3", "sort result[3]:%s", str.c_str());
    CHECK_FATAL(vs[5].ToString(str) != "ab2", "sort result[5]:%s", str.c_str());
    CHECK_FATAL(vs[7].ToString(str) != "ab1", "sort result[7]:%s", str.c_str());
}

void test_sort_zset(Ardb& db)
{
    DBID dbid = 0;
    db.ZClear(dbid, "myzset");
    db.ZAdd(dbid, "myzset", ValueData((int64) 0), "v0");
    db.ZAdd(dbid, "myzset", ValueData((int64) 10), "v10");
    db.ZAdd(dbid, "myzset", ValueData((int64) 3), "v3");
    db.ZAdd(dbid, "myzset", ValueData((int64) 5), "v5");

    StringArray args;
    ValueDataArray vs;
    db.Sort(dbid, "myzset", args, vs);
    std::string str;
    CHECK_FATAL(vs.size() != 4, "sort result size error:%zu", vs.size());
    CHECK_FATAL(vs[0].ToString(str) != "v0", "sort result[0]:%s", str.c_str());
    CHECK_FATAL(vs[1].ToString(str) != "v3", "sort result[1]:%s", str.c_str());
    CHECK_FATAL(vs[2].ToString(str) != "v5", "sort result[2]:%s", str.c_str());
    CHECK_FATAL(vs[3].ToString(str) != "v10", "sort result[3]:%s", str.c_str());

    vs.clear();
    string_to_string_array("limit 1 2", args);
    db.Sort(dbid, "myzset", args, vs);
    CHECK_FATAL(vs.size() != 2, "sort result size error:%zu", vs.size());
    CHECK_FATAL(vs[0].ToString(str) != "v3", "sort result[0]:%s", str.c_str());
    CHECK_FATAL(vs[1].ToString(str) != "v5", "sort result[1]:%s", str.c_str());

    vs.clear();
    args.clear();
    string_to_string_array("by weight_*", args);
    db.Set(dbid, "weight_v0", "1000");
    db.Set(dbid, "weight_v3", "900");
    db.Set(dbid, "weight_v5", "800");
    db.Set(dbid, "weight_v10", "700");
    db.Sort(dbid, "myzset", args, vs);
    CHECK_FATAL(vs.size() != 4, "sort result size error:%zu", vs.size());
    CHECK_FATAL(vs[0].ToString(str) != "v10", "sort result[0]:%s", str.c_str());
    CHECK_FATAL(vs[1].ToString(str) != "v5", "sort result[1]:%s", str.c_str());
    CHECK_FATAL(vs[2].ToString(str) != "v3", "sort result[2]:%s", str.c_str());
    CHECK_FATAL(vs[3].ToString(str) != "v0", "sort result[3]:%s", str.c_str());

    db.HSet(dbid, "myhash_v0", "field", "100");
    db.HSet(dbid, "myhash_v3", "field", "10");
    db.HSet(dbid, "myhash_v5", "field", "9");
    db.HSet(dbid, "myhash_v10", "field", "1000");

    string_to_string_array("by weight_* get myhash_*->field aggregate sum", args);
    db.Sort(dbid, "myzset", args, vs);
    CHECK_FATAL(vs.size() != 1, "sort result size error:%zu", vs.size());
    CHECK_FATAL(vs[0].ToString(str) != "1119", "sort result[0]:%s", str.c_str());

    string_to_string_array("by weight_* get myhash_*->field aggregate min", args);
    db.Sort(dbid, "myzset", args, vs);
    CHECK_FATAL(vs.size() != 1, "sort result size error:%zu", vs.size());
    CHECK_FATAL(vs[0].ToString(str) != "9", "sort result[0]:%s", str.c_str());

    string_to_string_array("by weight_* get myhash_*->field aggregate max", args);
    db.Sort(dbid, "myzset", args, vs);
    CHECK_FATAL(vs.size() != 1, "sort result size error:%zu", vs.size());
    CHECK_FATAL(vs[0].ToString(str) != "1000", "sort result[0]:%s", str.c_str());

    string_to_string_array("by weight_* get myhash_*->field aggregate avg", args);
    db.Sort(dbid, "myzset", args, vs);
    CHECK_FATAL(vs.size() != 1, "sort result size error:%zu", vs.size());
    CHECK_FATAL(vs[0].ToString(str) != "279.75", "sort result[0]:%s", str.c_str());

    string_to_string_array("by weight_* get myhash_*->field aggregate count", args);
    db.Sort(dbid, "myzset", args, vs);
    CHECK_FATAL(vs.size() != 1, "sort result size error:%zu", vs.size());
    CHECK_FATAL(vs[0].ToString(str) != "4", "sort result[0]:%s", str.c_str());
}

void test_keys(Ardb& db)
{
    DBID dbid = 0;
    db.HSet(dbid, "myhash_v0", "field", "100");
    db.SAdd(dbid, "myset_v0", "field");
    db.LPush(dbid, "mylist", "122");
    db.ZAdd(dbid, "myzset", ValueData((int64) 3), "v0");
    db.Set(dbid, "mykey", "12312");

    StringArray ret;
    db.Keys(dbid, "my*", "", 100, ret);
    CHECK_FATAL(ret.size() < 5, "keys my* size error:%zu", ret.size());
    ret.clear();
    db.Keys(dbid, "*set*", "", 100, ret);
    CHECK_FATAL(ret.size() < 2, "keys *set* size error:%zu", ret.size());
}

void test_pthread_create_performance()
{
    uint64 start = get_current_epoch_millis();
    uint32 loop = 1000000;
    for (uint32 i = 0; i < loop; i++)
    {
        pthread_mutex_t mutex;
        pthread_mutexattr_t attr;
        pthread_mutexattr_init(&attr);
        pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_DEFAULT);
        pthread_mutex_init(&mutex, &attr);
        pthread_mutexattr_destroy(&attr);

        pthread_mutex_destroy(&mutex);
    }
    uint64 end = get_current_epoch_millis();
    INFO_LOG("Cost %llums to create/destory %u mutexes.", (end - start), loop);
}

void test_misc(Ardb& db)
{
    test_type(db);
    test_sort_list(db);
    test_sort_set(db);
    test_sort_zset(db);
    test_keys(db);
}
