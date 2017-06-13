#include "db/db.hpp"
#include "util/json.hpp"

using json = nlohmann::json;

namespace ardb
{
    int Ardb::JSet(Context &ctx, RedisCommandFrame &cmd)
    {
        RedisReply &reply = ctx.GetReply();
        ctx.flags.create_if_notexist = 1;

        const std::string& key_str = cmd.GetArguments()[0];
        const std::string& field = cmd.GetArguments()[1];
        const std::string& value = cmd.GetArguments()[2];
        int64_t ttl = -1;
        int8_t nx_xx = -1;
        int err = 0;
        if (cmd.GetArguments().size() > 3)
        {
            for (uint32 i = 3; i < cmd.GetArguments().size(); i++)
            {
                const std::string& arg = cmd.GetArguments()[i];
                if (!strcasecmp(arg.c_str(), "px") || !strcasecmp(arg.c_str(), "ex"))
                {
                    int64_t iv;
                    if (!raw_toint64(cmd.GetArguments()[i + 1].c_str(), cmd.GetArguments()[i + 1].size(), iv) || iv < 0)
                    {
                        reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
                        return 0;
                    }
                    if (!strcasecmp(arg.c_str(), "px"))
                    {
                        ttl = iv;
                    }
                    else
                    {
                        ttl = iv;
                        ttl *= 1000;
                    }
                    ttl += get_current_epoch_millis();
                    i++;
                }
                else if (!strcasecmp(arg.c_str(), "xx"))
                {
                    nx_xx = 1;
                }
                else if (!strcasecmp(arg.c_str(), "nx"))
                {
                    nx_xx = 0;
                }
                else
                {
                    reply.SetErrCode(ERR_INVALID_SYNTAX);
                    return 0;
                }
            }
        }

        KeyObject meta_key(ctx.ns, KEY_META, key_str);
        KeyLockGuard guard(ctx, meta_key);
        ValueObject meta;
        if (!CheckMeta(ctx, meta_key, KEY_JSON, meta))
        {
            reply.SetErrCode(ERR_WRONG_TYPE);
            return 0;
        }

        if (field == ".")
        {
            if (meta.GetType() == 0)
            {
                if (nx_xx == 1)
                {
                    reply.Clear();
                    return 0;
                }
            }
            else
            {
                if (nx_xx == 0)
                {
                    reply.Clear();
                    return 0;
                }
                else
                {
                    reply.SetErrCode(ERR_KEY_EXIST);
                    return 0;
                }
            }

            ValueObject valueobj;
            meta.SetType(KEY_JSON);
            meta.SetTTL(ttl);
            meta.GetStringValue().SetString(value, true, false);
            err = SetKeyValue(ctx, meta_key, meta);
        }
        else
        {
            std::string json_value;
            meta.GetStringValue().ToString(json_value);
            json j = json::parse(json_value);
            j[field] = value;
            json_value = j.dump();
            meta.GetStringValue().SetString(json_value, true, false);
            err = SetKeyValue(ctx, meta_key, meta);
        }

        if (err != 0)
        {
            reply.SetErrCode(err);
        }
        else
        {
            reply.SetStatusCode(STATUS_OK);
        }

        return 0;
    }

    int Ardb::JGet(Context &ctx, RedisCommandFrame &cmd)
    {
        RedisReply &reply = ctx.GetReply();
        const std::string& key_str = cmd.GetArguments()[0];
        const std::string& field = cmd.GetArguments()[1];
        KeyObject meta_key(ctx.ns, KEY_META, key_str);
        ValueObject meta;
        if (!CheckMeta(ctx, meta_key, KEY_JSON, meta))
        {
            reply.SetErrCode(ERR_WRONG_TYPE);
            return 0;
        }

        if (meta.GetType() == 0)
        {
            reply.Clear();
            return 0;
        }

        if (field == ".")
        {
            reply.SetString(meta.GetStringValue());
        }
        else
        {
            std::string json_value;
            meta.GetStringValue().ToString(json_value);
            json j = json::parse(json_value);
            if (j.find(field) == j.end())
            {
                reply.Clear();
            }
            else
            {
                reply.SetString(j.at(field).get<std::string>());
            }

        }

        return 0;
    }
}