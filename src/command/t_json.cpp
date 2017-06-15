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
        if (field.find("/") != 0)
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
            return 0;
        }

        const std::string& value = cmd.GetArguments()[2];
        int8_t nx = 0;
        if (field != "/" && cmd.GetArguments().size() > 3)
        {
            const std::string& arg = cmd.GetArguments()[3];
            if (!strcasecmp(arg.c_str(), "nx"))
            {
                nx = 1;
            }
            else
            {
                reply.SetErrCode(ERR_INVALID_SYNTAX);
                return 0;
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

        int err= 0;
        if (field == "/")
        {
            if (meta.GetType() != 0)
            {
                reply.SetErrCode(ERR_KEY_EXIST);
                return 0;
            }

            ValueObject valueobj;
            meta.SetType(KEY_JSON);
            meta.SetTTL(0);
            meta.GetStringValue().SetString(value, true, false);
            err = SetKeyValue(ctx, meta_key, meta);
        }
        else
        {
            std::string json_value;
            json j;
            json::json_pointer jp(field);
            if (meta.GetType() == 0)
            {
                j[jp] = value;
                meta.SetType(KEY_JSON);
            }
            else{
                meta.GetStringValue().ToString(json_value);
                j = json::parse(json_value);
                if (!j[jp].is_null() && nx)
                {
                    reply.Clear();
                    return 0;
                }
                j[jp] = value;
            }
            json_value = j.dump();
            meta.SetTTL(0);
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

    int Ardb::JSetBool(Context &ctx, RedisCommandFrame &cmd)
    {
        RedisReply &reply = ctx.GetReply();
        ctx.flags.create_if_notexist = 1;

        const std::string& key_str = cmd.GetArguments()[0];
        const std::string& field = cmd.GetArguments()[1];
        if (field.find("/") != 0)
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
            return 0;
        }

        if (field == "/")
        {
            reply.Clear();
            return 0;
        }
        
        int8_t nx = 0;
        if (cmd.GetArguments().size() > 3)
        {
            const std::string& arg = cmd.GetArguments()[3];
            if (!strcasecmp(arg.c_str(), "nx"))
            {
                nx = 1;
            }
            else
            {
                reply.SetErrCode(ERR_INVALID_SYNTAX);
                return 0;
            }
        }

        int value = cmd.GetArguments()[2] == "true" ? 1 : 0;
        KeyObject meta_key(ctx.ns, KEY_META, key_str);
        KeyLockGuard guard(ctx, meta_key);
        ValueObject meta;
        if (!CheckMeta(ctx, meta_key, KEY_JSON, meta))
        {
            reply.SetErrCode(ERR_WRONG_TYPE);
            return 0;
        }

        std::string json_value;
        json j;
        json::json_pointer jp(field);
        if (meta.GetType() == 0)
        {
            j[jp] = value;
            meta.SetType(KEY_JSON);
        }
        else
        {
            meta.GetStringValue().ToString(json_value);
            j = json::parse(json_value);
            if (!j[jp].is_null() && nx)
            {
                reply.Clear();
                return 0;
            }

            j[jp] = value;
        }
        
        json_value = j.dump();
        meta.SetTTL(0);
        meta.GetStringValue().SetString(json_value, true, false);
        int err = SetKeyValue(ctx, meta_key, meta);
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

    int Ardb::JSetInt(Context &ctx, RedisCommandFrame &cmd)
    {
        RedisReply &reply = ctx.GetReply();
        ctx.flags.create_if_notexist = 1;

        const std::string& key_str = cmd.GetArguments()[0];
        const std::string& field = cmd.GetArguments()[1];
        if (field.find("/") != 0)
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
            return 0;
        }

        if (field == "/")
        {
            reply.Clear();
            return 0;
        }
        
        int8_t nx = 0;
        if (cmd.GetArguments().size() > 3)
        {
            const std::string& arg = cmd.GetArguments()[3];
            if (!strcasecmp(arg.c_str(), "nx"))
            {
                nx = 1;
            }
            else
            {
                reply.SetErrCode(ERR_INVALID_SYNTAX);
                return 0;
            }
        }

        int64_t value;
        if (!string_toint64(cmd.GetArguments()[2], value))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }

        KeyObject meta_key(ctx.ns, KEY_META, key_str);
        KeyLockGuard guard(ctx, meta_key);
        ValueObject meta;
        if (!CheckMeta(ctx, meta_key, KEY_JSON, meta))
        {
            reply.SetErrCode(ERR_WRONG_TYPE);
            return 0;
        }

        std::string json_value;
        json j;
        json::json_pointer jp(field);
        if (meta.GetType() == 0)
        {
            j[jp] = value;
            meta.SetType(KEY_JSON);
        }
        else{
            meta.GetStringValue().ToString(json_value);
            j = json::parse(json_value);
            if (!j[jp].is_null() && nx)
            {
                reply.Clear();
                return 0;
            }

            j[jp] = value;
        }

        json_value = j.dump();
        meta.SetTTL(0);
        meta.GetStringValue().SetString(json_value, true, false);
        int err = SetKeyValue(ctx, meta_key, meta);
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

    int Ardb::JSetFloat(Context &ctx, RedisCommandFrame &cmd)
    {
        RedisReply &reply = ctx.GetReply();
        ctx.flags.create_if_notexist = 1;

        const std::string& key_str = cmd.GetArguments()[0];
        const std::string& field = cmd.GetArguments()[1];
        if (field.find("/") != 0)
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
            return 0;
        }

        if (field == "/")
        {
            reply.Clear();
            return 0;
        }
        
        int8_t nx = 0;
        if (cmd.GetArguments().size() > 3)
        {
            const std::string& arg = cmd.GetArguments()[3];
            if (!strcasecmp(arg.c_str(), "nx"))
            {
                nx = 1;
            }
            else
            {
                reply.SetErrCode(ERR_INVALID_SYNTAX);
                return 0;
            }
        }

        double_t value;
        if (!string_todouble(cmd.GetArguments()[2], value))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return 0;
        }

        KeyObject meta_key(ctx.ns, KEY_META, key_str);
        KeyLockGuard guard(ctx, meta_key);
        ValueObject meta;
        if (!CheckMeta(ctx, meta_key, KEY_JSON, meta))
        {
            reply.SetErrCode(ERR_WRONG_TYPE);
            return 0;
        }

        std::string json_value;
        json j;
        json::json_pointer jp(field);
        if (meta.GetType() == 0)
        {
            j[jp] = value;
            meta.SetType(KEY_JSON);
        }
        else{
            meta.GetStringValue().ToString(json_value);
            j = json::parse(json_value);
            if (!j[jp].is_null() && nx)
            {
                reply.Clear();
                return 0;
            }

            j[jp] = value;
        }

        json_value = j.dump();
        meta.SetTTL(0);
        meta.GetStringValue().SetString(json_value, true, false);
        int err = SetKeyValue(ctx, meta_key, meta);
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

    int Ardb::JSetObj(Context &ctx, RedisCommandFrame &cmd)
    {
        RedisReply &reply = ctx.GetReply();
        ctx.flags.create_if_notexist = 1;

        const std::string& key_str = cmd.GetArguments()[0];
        const std::string& field = cmd.GetArguments()[1];
        if (field.find("/") != 0)
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
            return 0;
        }

        if (field == "/")
        {
            reply.Clear();
            return 0;
        }
        
        int8_t nx = 0;
        if (cmd.GetArguments().size() > 3)
        {
            const std::string& arg = cmd.GetArguments()[3];
            if (!strcasecmp(arg.c_str(), "nx"))
            {
                nx = 1;
            }
            else
            {
                reply.SetErrCode(ERR_INVALID_SYNTAX);
                return 0;
            }
        }

        const std::string& value = cmd.GetArguments()[2];
        KeyObject meta_key(ctx.ns, KEY_META, key_str);
        KeyLockGuard guard(ctx, meta_key);
        ValueObject meta;
        if (!CheckMeta(ctx, meta_key, KEY_JSON, meta))
        {
            reply.SetErrCode(ERR_WRONG_JSON_TYPE);
            return 0;
        }

        std::string json_value;
        json j;
        json::json_pointer jp(field);
        if (meta.GetType() == 0)
        {
            j[jp] = json::parse(value);
            meta.SetType(KEY_JSON);
        }
        else
        {
            meta.GetStringValue().ToString(json_value);
            j = json::parse(json_value);
            if (!j[jp].is_null() && nx)
            {
                reply.Clear();
                return 0;
            }

            j[jp] = json::parse(value);
        }

        json_value = j.dump();
        meta.SetTTL(0);
        meta.GetStringValue().SetString(json_value, true, false);
        int err = SetKeyValue(ctx, meta_key, meta);
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

    int Ardb::JIncrby(Context &ctx, RedisCommandFrame &cmd)
    {
        RedisReply &reply = ctx.GetReply();
        ctx.flags.create_if_notexist = 1;

        const std::string& key_str = cmd.GetArguments()[0];
        const std::string& field = cmd.GetArguments()[1];
        if (field.find("/") != 0)
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
            return 0;
        }

        if (field == "/")
        {
            reply.Clear();
            return 0;
        }

        int64_t value = 1;
        if (cmd.GetArguments().size() == 3)
        {
            if (!string_toint64(cmd.GetArguments()[2], value))
            {
                reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
                return 0;
            }
        }

        KeyObject meta_key(ctx.ns, KEY_META, key_str);
        KeyLockGuard guard(ctx, meta_key);
        ValueObject meta;
        if (!CheckMeta(ctx, meta_key, KEY_JSON, meta))
        {
            reply.SetErrCode(ERR_WRONG_JSON_TYPE);
            return 0;
        }

        std::string json_value;
        json j;
        json::json_pointer jp(field);
        if (meta.GetType() == 0)
        {
            j[jp] = value;
            meta.SetType(KEY_JSON);
        }
        else{
            meta.GetStringValue().ToString(json_value);
            j = json::parse(json_value);

            if (j[jp].is_null())
            {
                j[jp] = value;
            }
            else if (j[jp].is_number_integer())
            {
                j[jp] = j[jp].get<int64_t>() + value;
            }
            else
            {
                reply.SetErrCode(ERR_WRONG_JSON_TYPE);
                return 0;
            }
        }

        json_value = j.dump();
        meta.SetTTL(0);
        meta.GetStringValue().SetString(json_value, true, false);
        int err = SetKeyValue(ctx, meta_key, meta);
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

int Ardb::JIncrbyFloat(Context &ctx, RedisCommandFrame &cmd)
    {
        RedisReply &reply = ctx.GetReply();
        ctx.flags.create_if_notexist = 1;

        const std::string& key_str = cmd.GetArguments()[0];
        const std::string& field = cmd.GetArguments()[1];
        if (field.find("/") != 0)
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
            return 0;
        }

        if (field.find("/") != 0)
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
            return 0;
        }

        if (field == "/")
        {
            reply.Clear();
            return 0;
        }

        double_t value = 1.0;
        if (cmd.GetArguments().size() == 3)
        {
            if (!string_todouble(cmd.GetArguments()[2], value))
            {
                reply.SetErrCode(ERR_INVALID_FLOAT_ARGS);
                return 0;
            }
        }

        KeyObject meta_key(ctx.ns, KEY_META, key_str);
        KeyLockGuard guard(ctx, meta_key);
        ValueObject meta;
        if (!CheckMeta(ctx, meta_key, KEY_JSON, meta))
        {
            reply.SetErrCode(ERR_WRONG_JSON_TYPE);
            return 0;
        }

        std::string json_value;
        json j;
        json::json_pointer jp(field);
        if (meta.GetType() == 0)
        {
            j[jp] = value;
            meta.SetType(KEY_JSON);
        }
        else{
            meta.GetStringValue().ToString(json_value);
            j = json::parse(json_value);

            if (j[jp].is_null())
            {
                j[jp] = value;
            }
            else if (j[jp].is_number_float())
            {
                j[jp] = j[jp].get<int64_t>() + value;
            }
            else
            {
                reply.SetErrCode(ERR_WRONG_JSON_TYPE);
                return 0;
            }
        }

        json_value = j.dump();
        meta.SetTTL(0);
        meta.GetStringValue().SetString(json_value, true, false);
        int err = SetKeyValue(ctx, meta_key, meta);
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

    int Ardb::JPush(Context &ctx, RedisCommandFrame &cmd)
    {
        RedisReply &reply = ctx.GetReply();
        ctx.flags.create_if_notexist = 1;

        const std::string& key_str = cmd.GetArguments()[0];
        const std::string& field = cmd.GetArguments()[1];
        if (field.find("/") != 0)
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
            return 0;
        }

        if (field == "/")
        {
            reply.Clear();
            return 0;
        }

        const std::string& value = cmd.GetArguments()[2];
        KeyObject meta_key(ctx.ns, KEY_META, key_str);
        KeyLockGuard guard(ctx, meta_key);
        ValueObject meta;
        if (!CheckMeta(ctx, meta_key, KEY_JSON, meta))
        {
            reply.SetErrCode(ERR_WRONG_JSON_TYPE);
            return 0;
        }

        std::string json_value;
        json j;
        json::json_pointer jp(field);
        if (meta.GetType() == 0)
        {
            json j2 = json::parse(value);
            if (!j2.is_array())
            {
                reply.Clear();
                return 0;
            } 

            j[jp] = j2;
            meta.SetType(KEY_JSON);
        }
        else{
            meta.GetStringValue().ToString(json_value);
            j = json::parse(json_value);
            if (!j[jp].is_null() && !j[jp].is_array())
            {
                reply.SetErrCode(ERR_WRONG_JSON_TYPE);
                return 0;
            }

            json j2 = json::parse(value);
            if (!j2.is_array())
            {
                reply.SetErrCode(ERR_INVALID_JSON_ARRAY_ARGS);
                return 0;
            }

            if (j[jp].is_null())
            {
                j[jp] = j2;
            }
            else
            {
                j[jp].insert(j[jp].end(), j2.begin(), j2.end());
            }
        }

        json_value = j.dump();
        meta.SetTTL(0);
        meta.GetStringValue().SetString(json_value, true, false);
        int err = SetKeyValue(ctx, meta_key, meta);
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

    int Ardb::JPop(Context &ctx, RedisCommandFrame &cmd)
    {
        RedisReply &reply = ctx.GetReply();
        ctx.flags.create_if_notexist = 1;

        const std::string& key_str = cmd.GetArguments()[0];
        const std::string& field = cmd.GetArguments()[1];
        if (field.find("/") != 0)
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
            return 0;
        }

        if (field == "/")
        {
            reply.Clear();
            return 0;
        }

        KeyObject meta_key(ctx.ns, KEY_META, key_str);
        KeyLockGuard guard(ctx, meta_key);
        ValueObject meta;
        if (!CheckMeta(ctx, meta_key, KEY_JSON, meta))
        {
            reply.SetErrCode(ERR_WRONG_JSON_TYPE);
            return 0;
        }

        std::string json_value;
        json j;
        json v;
        json::json_pointer jp(field);
        if (meta.GetType() == 0)
        {
            reply.Clear();
            return 0;
        }
        else{
            meta.GetStringValue().ToString(json_value);
            j = json::parse(json_value);
            if (j[jp].is_null())
            {
                reply.Clear();
                return 0;
            }

            if (!j[jp].is_array())
            {
                reply.SetErrCode(ERR_WRONG_JSON_TYPE);
                return 0;
            }

            v = j[jp][j[jp].size() - 1];
            j[jp].erase(j[jp].end());
        }

        json_value = j.dump();
        meta.SetTTL(0);
        meta.GetStringValue().SetString(json_value, true, false);
        int err = SetKeyValue(ctx, meta_key, meta);
        if (err != 0)
        {
            reply.SetErrCode(err);
        }
        else
        {
            if (v.is_primitive())
            {
                if (v.is_string())
                {
                    reply.SetString(v.get<std::string>());
                }
                else if (v.is_number_integer())
                {
                    reply.SetInteger(v.get<int64_t>());
                }
                else if (v.is_number_float())
                {
                    reply.SetString(std::to_string(v.get<double_t>()));
                }
                else if (v.is_boolean())
                {
                    if (v)
                    {
                        reply.SetInteger(1);
                    }
                    else
                    {
                        reply.SetInteger(0);
                    }
                }
                else
                {
                    reply.Clear();
                }
            }
            else
            {
                reply.SetString(v.dump());
            }
        }

        return 0;
    }

    int Ardb::JDel(Context &ctx, RedisCommandFrame &cmd)
    {
        RedisReply &reply = ctx.GetReply();
        ctx.flags.create_if_notexist = 1;

        const std::string& key_str = cmd.GetArguments()[0];
        const std::string& field = cmd.GetArguments()[1];
        if (field.find("/") != 0)
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
            return 0;
        }

        if (field == "/")
        {
            reply.Clear();
            return 0;
        }

        KeyObject meta_key(ctx.ns, KEY_META, key_str);
        KeyLockGuard guard(ctx, meta_key);
        ValueObject meta;
        if (!CheckMeta(ctx, meta_key, KEY_JSON, meta))
        {
            reply.SetErrCode(ERR_WRONG_JSON_TYPE);
            return 0;
        }

        std::string json_value;
        json j;
        json::json_pointer jp(field);
        if (meta.GetType() == 0)
        {
            reply.Clear();
            return 0;
        }
        else{
            meta.GetStringValue().ToString(json_value);
            j = json::parse(json_value);
            if (j[jp].is_null())
            {
                reply.Clear();
                return 0;
            }

            json patch = json::array({{{"op", "remove"}, {"path", field}}});
            j = j.patch(patch);
        }

        if (j.empty())
        {
            DelKey(ctx, meta_key);
            reply.SetStatusCode(STATUS_OK);
            return 0;
        }

        json_value = j.dump();
        meta.SetTTL(0);
        meta.GetStringValue().SetString(json_value, true, false);
        int err = SetKeyValue(ctx, meta_key, meta);
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

    int Ardb::JPatch(Context &ctx, RedisCommandFrame &cmd)
    {
        RedisReply &reply = ctx.GetReply();
        ctx.flags.create_if_notexist = 1;

        const std::string& key_str = cmd.GetArguments()[0];
        const std::string& patch_str = cmd.GetArguments()[1];

        KeyObject meta_key(ctx.ns, KEY_META, key_str);
        KeyLockGuard guard(ctx, meta_key);
        ValueObject meta;
        if (!CheckMeta(ctx, meta_key, KEY_JSON, meta))
        {
            reply.SetErrCode(ERR_WRONG_TYPE);
            return 0;
        }

        std::string json_value;
        json j;
        if (meta.GetType() == 0)
        {
            reply.Clear();
            return 0;
        }
        else{
            meta.GetStringValue().ToString(json_value);
            j = json::parse(json_value);
            json patch = json::parse(patch_str);
            j = j.patch(patch);
        }

        json_value = j.dump();
        meta.SetTTL(0);
        meta.GetStringValue().SetString(json_value, true, false);
        int err = SetKeyValue(ctx, meta_key, meta);
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
        if (field.find("/") != 0)
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
            return 0;
        }

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

        if (field == "/")
        {
            reply.SetString(meta.GetStringValue());
        }
        else
        {
            std::string json_value;
            meta.GetStringValue().ToString(json_value);
            json j = json::parse(json_value);
            json::json_pointer jp(field);
            if (j[jp].is_null())
            {
                reply.Clear();
            }
            else
            {
                if (j[jp].is_primitive())
                {
                    if (j[jp].is_string())
                    {
                        reply.SetString(j.at(jp).get<std::string>());
                    }
                    else if (j[jp].is_number_integer())
                    {
                        reply.SetInteger(j[jp].get<int64_t>());
                    }
                    else if (j[jp].is_number_float())
                    {
                        reply.SetString(std::to_string(j[jp].get<double_t>()));
                    }
                    else if (j[jp].is_boolean())
                    {
                        if (j.at(jp))
                        {
                            reply.SetInteger(1);
                        }
                        else
                        {
                            reply.SetInteger(0);
                        }
                    }
                    else
                    {
                        reply.Clear();
                    }
                }
                else
                {
                    reply.SetString(j[jp].dump());
                }
            }

        }

        return 0;
    }
}