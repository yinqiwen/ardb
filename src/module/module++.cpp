/*
 * module++.cpp
 *
 *  Created on: 2017Äê11ÔÂ13ÈÕ
 *      Author: qiyingwang
 */
#include "module++.hpp"
#include "module_data.h"
#include <stdlib.h>
#include <string.h>
#include "util/string_helper.hpp"
#include "db/db.hpp"

#define REDISMODULE_CORE 1

extern "C"
{
#include "redismodule.h"
    /* --------------------------------------------------------------------------
     * Heap allocation raw functions
     * -------------------------------------------------------------------------- */

    /* Use like malloc(). Memory allocated with this function is reported in
     * Redis INFO memory, used for keys eviction according to maxmemory settings
     * and in general is taken into account as memory allocated by Redis.
     * You should avoid using malloc(). */
    void *RM_Alloc(size_t bytes)
    {
        return malloc(bytes);
    }

    /* Use like calloc(). Memory allocated with this function is reported in
     * Redis INFO memory, used for keys eviction according to maxmemory settings
     * and in general is taken into account as memory allocated by Redis.
     * You should avoid using calloc() directly. */
    void *RM_Calloc(size_t nmemb, size_t size)
    {
        return calloc(nmemb, size);
    }

    /* Use like realloc() for memory obtained with RedisModule_Alloc(). */
    void* RM_Realloc(void *ptr, size_t bytes)
    {
        return realloc(ptr, bytes);
    }

    /* Use like free() for memory obtained by RedisModule_Alloc() and
     * RedisModule_Realloc(). However you should never try to free with
     * RedisModule_Free() memory allocated with malloc() inside your module. */
    void RM_Free(void *ptr)
    {
        free(ptr);
    }

    /* Like strdup() but returns memory allocated with RedisModule_Alloc(). */
    char *RM_Strdup(const char *str)
    {
        return strdup(str);
    }

    /* Helper for RM_CreateCommand(). Truns a string representing command
     * flags into the command flags used by the Redis core.
     *
     * It returns the set of flags, or -1 if unknown flags are found. */
    int commandFlagsFromString(const std::string& s)
    {
        int count, j;
        int flags = 0;
        std::vector<std::string> ss = PROJ_NS::split_string(s, " ");
        for (j = 0; j < ss.size(); j++)
        {
            const char *t = ss[j].c_str();
            if (!strcasecmp(t, "write")) flags |= CMD_WRITE;
            else if (!strcasecmp(t, "readonly")) flags |= CMD_READONLY;
            else if (!strcasecmp(t, "admin")) flags |= CMD_ADMIN;
            else if (!strcasecmp(t, "deny-oom")) flags |= CMD_DENYOOM;
            else if (!strcasecmp(t, "deny-script")) flags |= CMD_NOSCRIPT;
            else if (!strcasecmp(t, "allow-loading")) flags |= CMD_LOADING;
            else if (!strcasecmp(t, "pubsub")) flags |= CMD_PUBSUB;
            else if (!strcasecmp(t, "random")) flags |= CMD_RANDOM;
            else if (!strcasecmp(t, "allow-stale")) flags |= CMD_STALE;
            else if (!strcasecmp(t, "no-monitor")) flags |= CMD_SKIP_MONITOR;
            else if (!strcasecmp(t, "fast")) flags |= CMD_FAST;
            else if (!strcasecmp(t, "getkeys-api")) flags |= CMD_MODULE_GETKEYS;
            else if (!strcasecmp(t, "no-cluster")) flags |= CMD_MODULE_NO_CLUSTER;
            else break;
        }
        //sdsfreesplitres(tokens, count);
        if (j != count) return -1; /* Some token not processed correctly. */
        return flags;
    }

    /* Register a new command in the Redis server, that will be handled by
     * calling the function pointer 'func' using the RedisModule calling
     * convention. The function returns REDISMODULE_ERR if the specified command
     * name is already busy or a set of invalid flags were passed, otherwise
     * REDISMODULE_OK is returned and the new command is registered.
     *
     * This function must be called during the initialization of the module
     * inside the RedisModule_OnLoad() function. Calling this function outside
     * of the initialization function is not defined.
     *
     * The command function type is the following:
     *
     *      int MyCommand_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
     *
     * And is supposed to always return REDISMODULE_OK.
     *
     * The set of flags 'strflags' specify the behavior of the command, and should
     * be passed as a C string compoesd of space separated words, like for
     * example "write deny-oom". The set of flags are:
     *
     * * **"write"**:     The command may modify the data set (it may also read
     *                    from it).
     * * **"readonly"**:  The command returns data from keys but never writes.
     * * **"admin"**:     The command is an administrative command (may change
     *                    replication or perform similar tasks).
     * * **"deny-oom"**:  The command may use additional memory and should be
     *                    denied during out of memory conditions.
     * * **"deny-script"**:   Don't allow this command in Lua scripts.
     * * **"allow-loading"**: Allow this command while the server is loading data.
     *                        Only commands not interacting with the data set
     *                        should be allowed to run in this mode. If not sure
     *                        don't use this flag.
     * * **"pubsub"**:    The command publishes things on Pub/Sub channels.
     * * **"random"**:    The command may have different outputs even starting
     *                    from the same input arguments and key values.
     * * **"allow-stale"**: The command is allowed to run on slaves that don't
     *                      serve stale data. Don't use if you don't know what
     *                      this means.
     * * **"no-monitor"**: Don't propoagate the command on monitor. Use this if
     *                     the command has sensible data among the arguments.
     * * **"fast"**:      The command time complexity is not greater
     *                    than O(log(N)) where N is the size of the collection or
     *                    anything else representing the normal scalability
     *                    issue with the command.
     * * **"getkeys-api"**: The command implements the interface to return
     *                      the arguments that are keys. Used when start/stop/step
     *                      is not enough because of the command syntax.
     * * **"no-cluster"**: The command should not register in Redis Cluster
     *                     since is not designed to work with it because, for
     *                     example, is unable to report the position of the
     *                     keys, programmatically creates key names, or any
     *                     other reason.
     */
    int RM_CreateCommand(RedisModuleCtx *ctx, const char *name, RedisModuleCmdFunc cmdfunc, const char *strflags,
            int firstkey, int lastkey, int keystep)
    {
        int flags = strflags ? commandFlagsFromString((char*) strflags) : 0;
        if (flags == -1) return REDISMODULE_ERR;
        //if ((flags & CMD_MODULE_NO_CLUSTER) && server.cluster_enabled) return REDISMODULE_ERR;

        //struct redisCommand *rediscmd;
        RedisModuleCommandProxy cp;
        //sds cmdname = sdsnew(name);
        /* Check if the command name is busy. */
        if (g_db->ContainsCommand(name))
        {
            return REDISMODULE_ERR;
        }

        /* Create a command "proxy", which is a structure that is referenced
         * in the command table, so that the generic command that works as
         * binding between modules and Redis, can know what function to call
         * and what the module is.
         *
         * Note that we use the Redis command table 'getkeys_proc' in order to
         * pass a reference to the command proxy structure. */
        //cp = zmalloc(sizeof(*cp));
        cp.module = ctx->module;
        cp.func = cmdfunc;
        cp.rediscmd = zmalloc(sizeof(*rediscmd));
        cp.rediscmd->name = cmdname;
        cp.rediscmd->proc = RedisModuleCommandDispatcher;
        cp.rediscmd->arity = -1;
        cp.rediscmd->flags = flags | CMD_MODULE;
        cp.rediscmd->getkeys_proc = (redisGetKeysProc*) (unsigned long) ctx->module->handle;
        cp.rediscmd->firstkey = firstkey;
        cp.rediscmd->lastkey = lastkey;
        cp.rediscmd->keystep = keystep;
        cp.rediscmd->microseconds = 0;
        cp.rediscmd->calls = 0;
        //dictAdd(server.commands, sdsdup(cmdname), cp->rediscmd);
        //dictAdd(server.orig_commands, sdsdup(cmdname), cp->rediscmd);
        RedisCommandHandlerSetting setting;
        setting.name = name;
        //setting.handler =
        g_db->AddCommand(name, setting);
        return REDISMODULE_OK;
    }

    /* Called by RM_Init() to setup the `ctx->module` structure.
     *
     * This is an internal function, Redis modules developers don't need
     * to use it. */
    void RM_SetModuleAttribs(RedisModuleCtx *ctx, const char *name, int ver, int apiver)
    {
        RedisModule *module;

        if (ctx->module != NULL) return;
        module = malloc(sizeof(*module));
        module->name = sdsnew((char*) name);
        module->ver = ver;
        module->apiver = apiver;
        //module->types = listCreate();
        ctx->module = module;
    }

    /* Send an error about the number of arguments given to the command,
     * citing the command name in the error message.
     *
     * Example:
     *
     *     if (argc != 3) return RedisModule_WrongArity(ctx);
     */
    int RM_WrongArity(RedisModuleCtx *ctx)
    {
        RedisReply& reply = ctx->gctx.GetReply();
        reply.SetErrorReason("wrong number of arguments for '" + ctx->gctx.current_cmd->GetCommand() + "' command");
        return REDISMODULE_OK;
    }

    /* Send an integer reply to the client, with the specified long long value.
     * The function always returns REDISMODULE_OK. */
    int RM_ReplyWithLongLong(RedisModuleCtx *ctx, long long ll)
    {
        RedisReply& reply = ctx->gctx.GetReply();
        reply.SetInteger(ll);
        return REDISMODULE_OK;
    }

    /* Reply with the error 'err'.
     *
     * Note that 'err' must contain all the error, including
     * the initial error code. The function only provides the initial "-", so
     * the usage is, for example:
     *
     *     RedisModule_ReplyWithError(ctx,"ERR Wrong Type");
     *
     * and not just:
     *
     *     RedisModule_ReplyWithError(ctx,"Wrong Type");
     *
     * The function always returns REDISMODULE_OK.
     */
    int RM_ReplyWithError(RedisModuleCtx *ctx, const char *err)
    {
        RedisReply& reply = ctx->gctx.GetReply();
        reply.SetErrorReason(err);
        return REDISMODULE_OK;
    }

    /* Reply with a simple string (+... \r\n in RESP protocol). This replies
     * are suitable only when sending a small non-binary string with small
     * overhead, like "OK" or similar replies.
     *
     * The function always returns REDISMODULE_OK. */
    int RM_ReplyWithSimpleString(RedisModuleCtx *ctx, const char *msg)
    {
        RedisReply& reply = ctx->gctx.GetReply();
        reply.SetString(msg);
        return REDISMODULE_OK;
    }
    /* Reply with an array type of 'len' elements. However 'len' other calls
     * to `ReplyWith*` style functions must follow in order to emit the elements
     * of the array.
     *
     * When producing arrays with a number of element that is not known beforehand
     * the function can be called with the special count
     * REDISMODULE_POSTPONED_ARRAY_LEN, and the actual number of elements can be
     * later set with RedisModule_ReplySetArrayLength() (which will set the
     * latest "open" count if there are multiple ones).
     *
     * The function always returns REDISMODULE_OK. */
    int RM_ReplyWithArray(RedisModuleCtx *ctx, long len)
    {
        RedisReply& reply = ctx->gctx.GetReply();
        reply.ReserveMember(len);
        return REDISMODULE_OK;
    }

    /* When RedisModule_ReplyWithArray() is used with the argument
     * REDISMODULE_POSTPONED_ARRAY_LEN, because we don't know beforehand the number
     * of items we are going to output as elements of the array, this function
     * will take care to set the array length.
     *
     * Since it is possible to have multiple array replies pending with unknown
     * length, this function guarantees to always set the latest array length
     * that was created in a postponed way.
     *
     * For example in order to output an array like [1,[10,20,30]] we
     * could write:
     *
     *      RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
     *      RedisModule_ReplyWithLongLong(ctx,1);
     *      RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
     *      RedisModule_ReplyWithLongLong(ctx,10);
     *      RedisModule_ReplyWithLongLong(ctx,20);
     *      RedisModule_ReplyWithLongLong(ctx,30);
     *      RedisModule_ReplySetArrayLength(ctx,3); // Set len of 10,20,30 array.
     *      RedisModule_ReplySetArrayLength(ctx,2); // Set len of top array
     *
     * Note that in the above example there is no reason to postpone the array
     * length, since we produce a fixed number of elements, but in the practice
     * the code may use an interator or other ways of creating the output so
     * that is not easy to calculate in advance the number of elements.
     */
    void RM_ReplySetArrayLength(RedisModuleCtx *ctx, long len)
    {
        RedisReply& reply = ctx->gctx.GetReply();
        reply.ReserveMember(len);
    }

    /* Reply with a bulk string, taking in input a C buffer pointer and length.
     *
     * The function always returns REDISMODULE_OK. */
    int RM_ReplyWithStringBuffer(RedisModuleCtx *ctx, const char *buf, size_t len)
    {
        RedisReply& reply = ctx->gctx.GetReply();
        reply.SetString(std::string(buf, len));
        return REDISMODULE_OK;
    }
    /* Reply with a bulk string, taking in input a RedisModuleString object.
     *
     * The function always returns REDISMODULE_OK. */
    int RM_ReplyWithString(RedisModuleCtx *ctx, const Data& str)
    {
        RedisReply& reply = ctx->gctx.GetReply();
        reply.SetString(str);
        return REDISMODULE_OK;
    }
    /* Reply to the client with a NULL. In the RESP protocol a NULL is encoded
     * as the string "$-1\r\n".
     *
     * The function always returns REDISMODULE_OK. */
    int RM_ReplyWithNull(RedisModuleCtx *ctx)
    {
        RedisReply& reply = ctx->gctx.GetReply();
        reply.Clear();
        return REDISMODULE_OK;
    }

    /* Send a string reply obtained converting the double 'd' into a bulk string.
     * This function is basically equivalent to converting a double into
     * a string into a C buffer, and then calling the function
     * RedisModule_ReplyWithStringBuffer() with the buffer and length.
     *
     * The function always returns REDISMODULE_OK. */
    int RM_ReplyWithDouble(RedisModuleCtx *ctx, double d)
    {
        RedisReply& reply = ctx->gctx.GetReply();
        reply.SetDouble(d);
        return REDISMODULE_OK;
    }

    /* Reply exactly what a Redis command returned us with RedisModule_Call().
     * This function is useful when we use RedisModule_Call() in order to
     * execute some command, as we want to reply to the client exactly the
     * same reply we obtained by the command.
     *
     * The function always returns REDISMODULE_OK. */
    int RM_ReplyWithCallReply(RedisModuleCtx *ctx, void *reply) {
        RedisReply& reply = ctx->gctx.GetReply();

//        sds proto = sdsnewlen(reply->proto, reply->protolen);
//        addReplySds(c,proto);
        return REDISMODULE_OK;
    }
}

OP_NAMESPACE_BEGIN

#define REGISTER_API(name) \
    ModuleRegisterApi("RedisModule_" #name, (void *)(unsigned long)RM_ ## name)

    int ModuleManager::ModuleRegisterApi(const char *funcname, void *funcptr)
    {
        m_moduleapi[funcname] = funcptr;
        return 0;
    }

    void ModuleManager::RegisterCoreAPI()
    {
        REGISTER_API(Alloc);
        REGISTER_API(Calloc);
        REGISTER_API(Realloc);
        REGISTER_API(Free);
        REGISTER_API(Strdup);
        REGISTER_API(CreateCommand);
        REGISTER_API(SetModuleAttribs);
        REGISTER_API(WrongArity);
        REGISTER_API(ReplyWithLongLong);
        REGISTER_API(ReplyWithError);
        REGISTER_API(ReplyWithSimpleString);
        REGISTER_API(ReplyWithArray);
        REGISTER_API(ReplySetArrayLength);
        REGISTER_API(ReplyWithString);
        REGISTER_API(ReplyWithStringBuffer);
        REGISTER_API(ReplyWithNull);
        REGISTER_API(ReplyWithCallReply);
        REGISTER_API(ReplyWithDouble);
//        REGISTER_API(GetSelectedDb);
//        REGISTER_API(SelectDb);
//        REGISTER_API(OpenKey);
//        REGISTER_API(CloseKey);
//        REGISTER_API(KeyType);
//        REGISTER_API(ValueLength);
//        REGISTER_API(ListPush);
//        REGISTER_API(ListPop);
//        REGISTER_API(StringToLongLong);
//        REGISTER_API(StringToDouble);
//        REGISTER_API(Call);
//        REGISTER_API(CallReplyProto);
//        REGISTER_API(FreeCallReply);
//        REGISTER_API(CallReplyInteger);
//        REGISTER_API(CallReplyType);
//        REGISTER_API(CallReplyLength);
//        REGISTER_API(CallReplyArrayElement);
//        REGISTER_API(CallReplyStringPtr);
//        REGISTER_API(CreateStringFromCallReply);
//        REGISTER_API(CreateString);
//        REGISTER_API(CreateStringFromLongLong);
//        REGISTER_API(CreateStringFromString);
//        REGISTER_API(CreateStringPrintf);
//        REGISTER_API(FreeString);
//        REGISTER_API(StringPtrLen);
//        REGISTER_API(AutoMemory);
//        REGISTER_API(Replicate);
//        REGISTER_API(ReplicateVerbatim);
//        REGISTER_API(DeleteKey);
//        REGISTER_API(StringSet);
//        REGISTER_API(StringDMA);
//        REGISTER_API(StringTruncate);
//        REGISTER_API(SetExpire);
//        REGISTER_API(GetExpire);
//        REGISTER_API(ZsetAdd);
//        REGISTER_API(ZsetIncrby);
//        REGISTER_API(ZsetScore);
//        REGISTER_API(ZsetRem);
//        REGISTER_API(ZsetRangeStop);
//        REGISTER_API(ZsetFirstInScoreRange);
//        REGISTER_API(ZsetLastInScoreRange);
//        REGISTER_API(ZsetFirstInLexRange);
//        REGISTER_API(ZsetLastInLexRange);
//        REGISTER_API(ZsetRangeCurrentElement);
//        REGISTER_API(ZsetRangeNext);
//        REGISTER_API(ZsetRangePrev);
//        REGISTER_API(ZsetRangeEndReached);
//        REGISTER_API(HashSet);
//        REGISTER_API(HashGet);
//        REGISTER_API(IsKeysPositionRequest);
//        REGISTER_API(KeyAtPos);
//        REGISTER_API(GetClientId);
//        REGISTER_API(PoolAlloc);
//        REGISTER_API(CreateDataType);
//        REGISTER_API(ModuleTypeSetValue);
//        REGISTER_API(ModuleTypeGetType);
//        REGISTER_API(ModuleTypeGetValue);
//        REGISTER_API(SaveUnsigned);
//        REGISTER_API(LoadUnsigned);
//        REGISTER_API(SaveSigned);
//        REGISTER_API(LoadSigned);
//        REGISTER_API(SaveString);
//        REGISTER_API(SaveStringBuffer);
//        REGISTER_API(LoadString);
//        REGISTER_API(LoadStringBuffer);
//        REGISTER_API(SaveDouble);
//        REGISTER_API(LoadDouble);
//        REGISTER_API(SaveFloat);
//        REGISTER_API(LoadFloat);
//        REGISTER_API(EmitAOF);
//        REGISTER_API(Log);
//        REGISTER_API(LogIOError);
//        REGISTER_API(StringAppendBuffer);
//        REGISTER_API(RetainString);
//        REGISTER_API(StringCompare);
//        REGISTER_API(GetContextFromIO);
//        REGISTER_API(BlockClient);
//        REGISTER_API(UnblockClient);
//        REGISTER_API(IsBlockedReplyRequest);
//        REGISTER_API(IsBlockedTimeoutRequest);
//        REGISTER_API(GetBlockedClientPrivateData);
//        REGISTER_API(AbortBlock);
//        REGISTER_API(Milliseconds);
//        REGISTER_API(GetThreadSafeContext);
//        REGISTER_API(FreeThreadSafeContext);
//        REGISTER_API(ThreadSafeContextLock);
//        REGISTER_API(ThreadSafeContextUnlock);
//        REGISTER_API(DigestAddStringBuffer);
//        REGISTER_API(DigestAddLongLong);
//        REGISTER_API(DigestEndSequence);
    }

    int ModuleManager::Init()
    {
        RegisterCoreAPI();
        return 0;
    }

OP_NAMESPACE_END
