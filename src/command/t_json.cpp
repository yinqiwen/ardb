#include "db/db.hpp"
#include "util/json.hpp"

using json = nlohmann::json;

bool IsInvalidJsonPath(const std::string& path, json::json_pointer& jp)
{
    if (path == "/")
    {
        return true;
    }

    if (path.find("/") != 0)
    {
        return true;
    }

    if (path.rfind("/") == (path.length() - 1))
    {
        return true;
    }

    if (path.rfind("~") != std::string::npos)
    {
        return true;
    }

    try
    {
        jp = json::json_pointer(path);
    }
    catch (const json::parse_error&)
    {
        return true;
    }

    return false;
}

namespace ardb
{
    int Ardb::JNew(Context &ctx, RedisCommandFrame &cmd)
    {
        RedisReply &reply = ctx.GetReply();
        ctx.flags.create_if_notexist = 1;

        const std::string& key_str = cmd.GetArguments()[0];
        const std::string& value = cmd.GetArguments()[1];

        KeyObject meta_key(ctx.ns, KEY_META, key_str);
        KeyLockGuard guard(ctx, meta_key);
        ValueObject meta;
        if (!CheckMeta(ctx, meta_key, KEY_JSON, meta))
        {
            reply.SetErrCode(ERR_WRONG_TYPE);
            return 0;
        }

        json j;
        if (meta.GetType() != 0)
        {
            reply.SetErrCode(ERR_KEY_EXIST);
            return 0;
        }

        try
        {
            j = json::parse(value);
        }
        catch (const json::parse_error&)
        {
            reply.SetErrCode(ERR_INVALID_JSON_OBJECT_ARGS);
            return 0;
        }

        if (!j.is_object())
        {
            reply.SetErrCode(ERR_INVALID_JSON_OBJECT_ARGS);
            return 0;
        }

        meta.SetType(KEY_JSON);
        meta.GetStringValue().SetString(value, false);
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

    int Ardb::JSet(Context &ctx, RedisCommandFrame &cmd)
    {
        RedisReply &reply = ctx.GetReply();
        ctx.flags.create_if_notexist = 1;

        const std::string& key_str = cmd.GetArguments()[0];
        const std::string& path = cmd.GetArguments()[1];
        json::json_pointer jp;
        if (IsInvalidJsonPath(path, jp))
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
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
            reply.SetErrCode(ERR_WRONG_TYPE);
            return 0;
        }

        json j;
        std::string json_value;
        if (meta.GetType() == 0)
        {
            meta.SetType(KEY_JSON);
        }
        else
        {
            meta.GetStringValue().ToString(json_value);
            try
            {
                j = json::parse(json_value);
                if (!j[jp].is_null() && nx)
                {
                    reply.Clear();
                    return 0;
                }
            }
            catch(const json::parse_error&)
            {
                reply.Clear();
                return 0;
            }
            catch (const json::out_of_range&)
            {
                reply.Clear();
                return 0;
            }
            catch (const std::length_error&)
            {
                reply.Clear();
                return 0;
            }

            meta.SetTTL(0);
        }

        j[jp] = value;
        json_value = j.dump();
        meta.GetStringValue().SetString(json_value, false);
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

    int Ardb::JSetBool(Context &ctx, RedisCommandFrame &cmd)
    {
        RedisReply &reply = ctx.GetReply();
        ctx.flags.create_if_notexist = 1;

        const std::string& key_str = cmd.GetArguments()[0];
        const std::string& path = cmd.GetArguments()[1];
        json::json_pointer jp;
        if (IsInvalidJsonPath(path, jp))
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
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

        int value = (cmd.GetArguments()[2] == "false" || cmd.GetArguments()[2] == "0") ? 0 : 1;
        KeyObject meta_key(ctx.ns, KEY_META, key_str);
        KeyLockGuard guard(ctx, meta_key);
        ValueObject meta;
        if (!CheckMeta(ctx, meta_key, KEY_JSON, meta))
        {
            reply.SetErrCode(ERR_WRONG_TYPE);
            return 0;
        }

        json j;
        std::string json_value;
        if (meta.GetType() == 0)
        {
            meta.SetType(KEY_JSON);
        }
        else
        {
            meta.GetStringValue().ToString(json_value);
            try
            {
                j = json::parse(json_value);
                if (!j[jp].is_null() && nx)
                {
                    reply.Clear();
                    return 0;
                }
            }
            catch(const json::parse_error&)
            {
                reply.Clear();
                return 0;
            }
            catch (const json::out_of_range&)
            {
                reply.Clear();
                return 0;
            }
            catch (const std::length_error&)
            {
                reply.Clear();
                return 0;
            }

            meta.SetTTL(0);
        }

        j[jp] = value;
        json_value = j.dump();
        meta.GetStringValue().SetString(json_value, false);
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
        const std::string& path = cmd.GetArguments()[1];
        json::json_pointer jp;
        if (IsInvalidJsonPath(path, jp))
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
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

        json j;
        std::string json_value;
        if (meta.GetType() == 0)
        {
            meta.SetType(KEY_JSON);
        }
        else
        {
            meta.GetStringValue().ToString(json_value);
            try
            {
                j = json::parse(json_value);
                if (!j[jp].is_null() && nx)
                {
                    reply.Clear();
                    return 0;
                }
            }
            catch(const json::parse_error&)
            {
                reply.Clear();
                return 0;
            }
            catch (const json::out_of_range&)
            {
                reply.Clear();
                return 0;
            }
            catch (const std::length_error&)
            {
                reply.Clear();
                return 0;
            }

            meta.SetTTL(0);
        }

        j[jp] = value;
        json_value = j.dump();
        meta.GetStringValue().SetString(json_value, false);
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
        const std::string& path = cmd.GetArguments()[1];
        json::json_pointer jp;
        if (IsInvalidJsonPath(path, jp))
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
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

        json j;
        std::string json_value;
        if (meta.GetType() == 0)
        {
            meta.SetType(KEY_JSON);
        }
        else
        {
            meta.GetStringValue().ToString(json_value);
            try
            {
                j = json::parse(json_value);
                if (!j[jp].is_null() && nx)
                {
                    reply.Clear();
                    return 0;
                }
            }
            catch(const json::parse_error&)
            {
                reply.Clear();
                return 0;
            }
            catch (const json::out_of_range&)
            {
                reply.Clear();
                return 0;
            }
            catch (const std::length_error&)
            {
                reply.Clear();
                return 0;
            }

            meta.SetTTL(0);
        }

        j[jp] = value;
        json_value = j.dump();
        meta.GetStringValue().SetString(json_value, false);
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
        const std::string& path = cmd.GetArguments()[1];
        json::json_pointer jp;
        if (IsInvalidJsonPath(path, jp))
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
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

        json j, j2;
        try
        {
            j2 = json::parse(value);
        }
        catch (const json::parse_error&)
        {
            reply.SetErrCode(ERR_INVALID_JSON_OBJECT_ARGS);
            return 0;
        }

        if (j2.is_primitive())
        {
            reply.SetErrCode(ERR_INVALID_JSON_OBJECT_ARGS);
            return 0;
        }

        std::string json_value;
        if (meta.GetType() == 0)
        {
            meta.SetType(KEY_JSON);
        }
        else
        {
            meta.GetStringValue().ToString(json_value);
            try
            {
                j = json::parse(json_value);
                if (!j[jp].is_null() && nx)
                {
                    reply.Clear();
                    return 0;
                }
            }
            catch(const json::parse_error&)
            {
                reply.Clear();
                return 0;
            }
            catch (const json::out_of_range&)
            {
                reply.Clear();
                return 0;
            }
            catch (const std::length_error&)
            {
                reply.Clear();
                return 0;
            }

            meta.SetTTL(0);
        }

        j[jp] = j2;
        json_value = j.dump();
        meta.GetStringValue().SetString(json_value, false);
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
        const std::string& path = cmd.GetArguments()[1];
        json::json_pointer jp;
        if (IsInvalidJsonPath(path, jp))
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
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

        json j;
        std::string json_value;
        if (meta.GetType() == 0)
        {
            meta.SetType(KEY_JSON);
        }
        else
        {
            meta.GetStringValue().ToString(json_value);
            try
            {
                j = json::parse(json_value);
                if (!j[jp].is_null() && !j[jp].is_number_integer())
                {
                    reply.SetErrCode(ERR_WRONG_JSON_TYPE);
                    return 0;
                }
            }
            catch(const json::parse_error&)
            {
                reply.Clear();
                return 0;
            }
            catch (const json::out_of_range&)
            {
                reply.Clear();
                return 0;
            }
            catch (const std::length_error&)
            {
                reply.Clear();
                return 0;
            }

            meta.SetTTL(0);
        }

        if (j[jp].is_null())
        {
            j[jp] = value;
        }
        else
        {
            j[jp] = j[jp].get<int64_t>() + value;
        }

        json_value = j.dump();
        meta.GetStringValue().SetString(json_value, false);
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
        const std::string& path = cmd.GetArguments()[1];
        json::json_pointer jp;
        if (IsInvalidJsonPath(path, jp))
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
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

        json j;
        std::string json_value;
        if (meta.GetType() == 0)
        {
            meta.SetType(KEY_JSON);
        }
        else
        {
            meta.GetStringValue().ToString(json_value);
            try
            {
                j = json::parse(json_value);
                if (!j[jp].is_null() && !j[jp].is_number_float())
                {
                    reply.SetErrCode(ERR_WRONG_JSON_TYPE);
                    return 0;
                }
            }
            catch(const json::parse_error&)
            {
                reply.Clear();
                return 0;
            }
            catch (const json::out_of_range&)
            {
                reply.Clear();
                return 0;
            }
            catch (const std::length_error&)
            {
                reply.Clear();
                return 0;
            }

            meta.SetTTL(0);
        }

        if (j[jp].is_null())
        {
            j[jp] = value;
        }
        else if (j[jp].is_number_float())
        {
            j[jp] = j[jp].get<double_t>() + value;
        }

        json_value = j.dump();
        meta.GetStringValue().SetString(json_value, false);
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
        const std::string& path = cmd.GetArguments()[1];
        json::json_pointer jp;
        if (IsInvalidJsonPath(path, jp))
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
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

        json j, j2;
        try
        {
            j2 = json::parse(value);
        }
        catch (const json::parse_error&)
        {
            reply.SetErrCode(ERR_INVALID_JSON_ARRAY_ARGS);
            return 0;
        }

        if (!j2.is_array() || j2.empty())
        {
            reply.SetErrCode(ERR_INVALID_JSON_ARRAY_ARGS);
            return 0;
        }

        std::string json_value;
        if (meta.GetType() == 0)
        {
            meta.SetType(KEY_JSON);
        }
        else
        {
            meta.GetStringValue().ToString(json_value);
            try
            {
                j = json::parse(json_value);
                if (!j[jp].is_null() && !j[jp].is_array())
                {
                    reply.SetErrCode(ERR_WRONG_JSON_TYPE);
                    return 0;
                }
            }
            catch(const json::parse_error&)
            {
                reply.Clear();
                return 0;
            }
            catch (const json::out_of_range&)
            {
                reply.Clear();
                return 0;
            }
            catch (const std::length_error&)
            {
                reply.Clear();
                return 0;
            }

            meta.SetTTL(0);
        }

        if (j[jp].is_null())
        {
            j[jp] = j2;
        }
        else
        {
            j[jp].insert(j[jp].end(), j2.begin(), j2.end());
        }

        json_value = j.dump();
        meta.GetStringValue().SetString(json_value, false);
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
        const std::string& path = cmd.GetArguments()[1];
        json::json_pointer jp;
        if (IsInvalidJsonPath(path, jp))
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
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

        json j;
        json v;
        std::string json_value;
        if (meta.GetType() == 0)
        {
            reply.Clear();
            return 0;
        }
        else
        {
            meta.GetStringValue().ToString(json_value);
            try
            {
                j = json::parse(json_value);
                if (j[jp].is_null() || j[jp].empty())
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
            catch(const json::parse_error&)
            {
                reply.Clear();
                return 0;
            }
            catch (const json::out_of_range&)
            {
                reply.Clear();
                return 0;
            }
            catch (const std::length_error&)
            {
                reply.Clear();
                return 0;
            }

            meta.SetTTL(0);
        }

        json_value = j.dump();
        meta.GetStringValue().SetString(json_value, false);
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
        const std::string& path = cmd.GetArguments()[1];
        json::json_pointer jp;
        if (IsInvalidJsonPath(path, jp))
        {
            reply.SetErrCode(ERR_INVALID_JSON_PATH_ARGS);
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

        json j;
        std::string json_value;
        if (meta.GetType() == 0)
        {
            reply.Clear();
            return 0;
        }
        else
        {
            meta.GetStringValue().ToString(json_value);
            try
            {
                j = json::parse(json_value);
                json patch = json::array({{{"op", "remove"}, {"path", path}}});
                j = j.patch(patch);
            }
            catch(const json::parse_error&)
            {
                reply.Clear();
                return 0;
            }
            catch (const json::out_of_range&)
            {
                reply.Clear();
                return 0;
            }
            catch (const std::length_error&)
            {
                reply.Clear();
                return 0;
            }

            if (j.empty())
            {
                DelKey(ctx, meta_key);
                reply.SetStatusCode(STATUS_OK);
                return 0;
            }

            meta.SetTTL(0);
        }

        json_value = j.dump();
        meta.GetStringValue().SetString(json_value, false);
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

        json j;
        std::string json_value;
        if (meta.GetType() == 0)
        {
            reply.Clear();
            return 0;
        }
        else
        {
            meta.GetStringValue().ToString(json_value);
            json patch;
            try
            {
                j = json::parse(json_value);
                patch = json::parse(patch_str);
                j = j.patch(patch);
            }
            catch (const json::parse_error&)
            {
                reply.SetErrCode(ERR_INVALID_JSON_OBJECT_ARGS);
                return 0;
            }

            if (j.empty())
            {
                DelKey(ctx, meta_key);
                reply.SetStatusCode(STATUS_OK);
                return 0;
            }

            meta.SetTTL(0);
        }

        json_value = j.dump();
        meta.GetStringValue().SetString(json_value, false);
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
        const std::string& path = cmd.GetArguments()[1];
        json::json_pointer jp;
        if (path != "/" && IsInvalidJsonPath(path, jp))
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

        std::string json_value;
        if (path == "/")
        {
            meta.GetStringValue().ToString(json_value);
            reply.SetString(json_value);
        }
        else
        {
            meta.GetStringValue().ToString(json_value);
            json j, v;
            try
            {
                j = json::parse(json_value);
                v = j[jp];
            }
            catch(const json::parse_error&)
            {
                reply.Clear();
                return 0;
            }
            catch (const json::out_of_range&)
            {
                reply.Clear();
                return 0;
            }
            catch (const std::length_error&)
            {
                reply.Clear();
                return 0;
            }


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
                    std::string rst = j[jp].dump();
                    reply.SetString(rst);
                }
            }
        }

        return 0;
    }
}