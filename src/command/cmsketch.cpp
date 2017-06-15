#include <iostream>
#include <cmath>
#include <cstdlib>
#include <ctime>

#include "db/db.hpp"

#define LONG_PRIME 32993
#define CMS_SIGNATURE "CMS:"
#define CMS_DEFAULT_ERROR 0.01
#define CMS_DEFAULT_PROB 0.01
#define MIN(a, b) ((a) < (b) ? (a) : (b))


struct CMSketch
{
    long long c;
    unsigned int w, d;
    int *v;
    unsigned int *ha, *hb;
};

int64_t hashKey(const char *key)
{
    int64_t hash = 5381;
    int c;
    while (c = *key++)
    {
        hash = ((hash << 5) + hash) + c;
    }

    return hash;
}

void *newCMSketch(std::string& value, float error, float prob)
{
    unsigned int width = ceil(exp(1) / error);
    unsigned int depth = ceil(log(1 / prob));
    int slen = sizeof(CMS_SIGNATURE) +
               sizeof(struct CMSketch) +
               sizeof(int) * width * depth +
               sizeof(unsigned int) * 2 * depth;

    value.resize(slen);

    memcpy(&value[0], CMS_SIGNATURE, 4);

    size_t offset = strlen(CMS_SIGNATURE);
    struct CMSketch *s = (struct CMSketch *) (&value[offset]);
    s->c = 0;
    s->w = width;
    s->d = depth;
    offset += sizeof(CMSketch);
    s->v = (int *)&value[offset];
    offset += sizeof(int) * width * depth;
    s->ha = (unsigned int *) (&value[offset]);
    offset += sizeof(unsigned int) * depth;
    s->hb = (unsigned int *) (&value[offset]);

    srand(time(NULL));
    for (unsigned int i = 0; i < s->d; i++)
    {
        s->ha[i] = int(float(rand()) * float(LONG_PRIME) / float(RAND_MAX) + 1);
        s->hb[i] = int(float(rand()) * float(LONG_PRIME) / float(RAND_MAX) + 1);
    }
}

CMSketch *getCMSketch(std::string& value)
{
    size_t offset = strlen(CMS_SIGNATURE);
    struct CMSketch *s = (struct CMSketch *) (&value[offset]);
    offset += sizeof(CMSketch);
    s->v = (int *)&value[offset];
    offset += sizeof(int) * s->w * s->d;
    s->ha = (unsigned int *) (&value[offset]);
    offset += sizeof(unsigned int) * s->d;
    s->hb = (unsigned int *) (&value[offset]);

    return s;
}

unsigned int cmsIncrBy(std::string& value, int64_t item, int incr)
{
    struct CMSketch *s = getCMSketch(value);

    s->c += incr;
    unsigned int h = (s->ha[0] * item + s->hb[0]) % s->w;
    s->v[h] += incr;
    unsigned int min_val = s->v[h];
    long long offset;
    for (unsigned int j = 1; j < s->d; j++)
    {
        h = (s->ha[j] * item + s->hb[j]) % s->w;
        offset = j * s->w + h;
        s->v[offset] += incr;
        min_val = MIN(min_val, s->v[offset]);
    }

    return min_val;
}

unsigned int cmsIncrBy(std::string& value, const char *item, int incr)
{
    int64_t item_val = hashKey(item);
    return cmsIncrBy(value, item_val, incr);
}

unsigned int cmsQuery(std::string& value, int64_t item)
{
    struct CMSketch *s = getCMSketch(value);

    unsigned int h = (s->ha[0] * item + s->hb[0]) % s->w;
    unsigned int min_val = s->v[h];
    for (unsigned int j = 1; j < s->d; j++)
    {
        h = (s->ha[j] * item + s->hb[j]) % s->w;
        min_val = MIN(min_val, s->v[j * s->w + h]);
    }

    return min_val;
}

unsigned int cmsQuery(std::string& value, const char *item)
{
    int64_t item_val = hashKey(item);
    return cmsQuery(value, item_val);
}

bool isCMSObjectOrReply(std::string& value)
{
    struct CMSketch *s;
    if (value.size() < sizeof(*s))
        return false;

    if (value[0] != 'C' || value[1] != 'M' || value[2] != 'S' || value[3] != ':')
        return false;

    return true;
}

namespace ardb
{
    int Ardb::CMSInit(Context &ctx, RedisCommandFrame &cmd)
    {
        RedisReply &reply = ctx.GetReply();
        ctx.flags.create_if_notexist = 1;

        const std::string& key_str = cmd.GetArguments()[0];

        double eps = 0;
        double gamma = 0;
        if (!string_todouble(cmd.GetArguments()[1], eps))
        {
          reply.SetErrCode(ERR_INVALID_FLOAT_ARGS);
          return 0;
        }

        if (!string_todouble(cmd.GetArguments()[2], gamma))
        {
          reply.SetErrCode(ERR_INVALID_FLOAT_ARGS);
          return 0;
        }

        if (eps > 1) eps = 1;
        if (eps < 0.001) eps = 0.001;
        if (gamma < 0) gamma = 0;
        if (gamma > 1) gamma = 1;

        KeyObject meta_key(ctx.ns, KEY_META, key_str);
        KeyLockGuard guard(ctx, meta_key);
        ValueObject meta;
        std::string cms_value;
        if (!CheckMeta(ctx, meta_key, KEY_STRING, meta))
        {
            return 0;
        }

        if (meta.GetType() == 0)
        {
            newCMSketch(cms_value, eps, gamma);
            meta.SetType(KEY_STRING);
        }
        else
        {
            reply.SetErrCode(ERR_KEY_EXIST);
            return 0;
        }

        meta.GetStringValue().SetString(cms_value, false);

        int err = SetKeyValue(ctx, meta_key, meta);
        if (err != 0 && err != ERR_NOTPERFORMED)
        {
            reply.SetErrCode(err);
        }
        else
        {
            reply.SetStatusCode(STATUS_OK);
        }

        return 0;
    }

    int Ardb::CMSIncrBy(Context &ctx, RedisCommandFrame &cmd)
    {
        RedisReply& reply = ctx.GetReply();
        ctx.flags.create_if_notexist = 1;

        const std::string& key_str = cmd.GetArguments()[0];
        int incr = 1;
        if (cmd.GetArguments().size() > 2)
        {
            if (!string_toint32(cmd.GetArguments()[2], incr))
            {
                reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
                return 0;
            }
        }

        int err = 0;
        unsigned int count = 0;
        KeyObject meta_key(ctx.ns, KEY_META, key_str);
        KeyLockGuard guard(ctx, meta_key);
        ValueObject meta;
        std::string cms_value;
        if (!CheckMeta(ctx, meta_key, KEY_STRING, meta))
        {
            return 0;
        }

        if (meta.GetType() == 0)
        {
            newCMSketch(cms_value, CMS_DEFAULT_ERROR, CMS_DEFAULT_PROB);
            meta.SetType(KEY_STRING);
        }
        else
        {
            meta.GetStringValue().ToString(cms_value);
            if (!isCMSObjectOrReply(cms_value))
            {
                reply.SetErrCode(ERR_INVALID_CMS_STRING);
                return 0;
            }
        }

        int64_t item_val;
        if (string_toint64(cmd.GetArguments()[1], item_val))
        {
            count = cmsIncrBy(cms_value, item_val, incr);
        }
        else
        {
            const char* item = cmd.GetArguments()[1].c_str();
            count = cmsIncrBy(cms_value, item, incr);
        }

        meta.GetStringValue().SetString(cms_value, false);

        err = SetKeyValue(ctx, meta_key, meta);
        if (err != 0 && err != ERR_NOTPERFORMED)
        {
            reply.SetErrCode(err);
        }
        else
        {
            reply.SetInteger(count);
        }

        return 0;
    }

    int Ardb::CMSQuery(Context &ctx, RedisCommandFrame &cmd)
    {
        RedisReply& reply = ctx.GetReply();

        const std::string& key_str = cmd.GetArguments()[0];
        KeyObject meta_key(ctx.ns, KEY_META, key_str);
        ValueObject meta;
        if (!CheckMeta(ctx, meta_key, KEY_STRING, meta))
        {
            return 0;
        }

        if (meta.GetType() == 0)
        {
            reply.SetInteger(0);
            return 0;
        }

        std::string cms_value;
        meta.GetStringValue().ToString(cms_value);
        if (!isCMSObjectOrReply(cms_value))
        {
            reply.SetErrCode(ERR_INVALID_CMS_STRING);
            return 0;
        }

        unsigned int count = 0;
        int64_t item_val;
        if (string_toint64(cmd.GetArguments()[1], item_val))
        {
            count = cmsQuery(cms_value, item_val);
        }
        else
        {
            const char* item = cmd.GetArguments()[1].c_str();
            count = cmsQuery(cms_value, item);
        }

        reply.SetInteger(count);

        return 0;
    }
}
