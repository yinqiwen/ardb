/*
 *Copyright (c) 2013-2017, yinqiwen <yinqiwen@gmail.com>
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
#include "module++.hpp"

#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>
#include "util/string_helper.hpp"
#include "db/db.hpp"

#define REDISMODULE_CORE 1

extern "C"
{
#include "redismodule.h"

    struct RedisModuleCtx: public PROJ_NS::RedisModuleCtxImpl
    {
    };
    struct RedisModuleString: public std::string
    {

    };
    struct RedisModuleCallReply
    {
            RedisModuleCtx *ctx;
            int type; /* REDISMODULE_REPLY_... */
            int flags; /* REDISMODULE_REPLYFLAG_...  */
            size_t len; /* Len of strings or num of elements of arrays. */
            char *proto; /* Raw reply protocol. An SDS string at top-level object. */
            size_t protolen;/* Length of protocol. */
            union
            {
                    const char *str; /* String pointer for string and error replies. This
                     does not need to be freed, always points inside
                     a reply->proto buffer of the reply object or, in
                     case of array elements, of parent reply objects. */
                    long long ll; /* Reply value for integer reply. */
                    struct RedisModuleCallReply *array; /* Array of sub-reply elements. */
            } val;
    };
    /* This represents a Redis key opened with RM_OpenKey(). */
    struct RedisModuleKey
    {
            RedisModuleCtx *ctx;
            Data db;
            std::string key; /* Key name object. */
            KeyType value_type;
            //robj *value; /* Value object, or NULL if the key was not found. */
            //void *iter; /* Iterator. */
            int mode; /* Opening mode. */

            /* Zset iterator. */
            uint32_t ztype; /* REDISMODULE_ZSET_RANGE_* */
            //zrangespec zrs; /* Score range. */
            //zlexrangespec zlrs; /* Lex range. */
            uint32_t zstart; /* Start pos for positional ranges. */
            uint32_t zend; /* End pos for positional ranges. */
            void *zcurrent; /* Zset iterator current node. */
            int zer; /* Zset iterator end reached flag
             (true if end was reached). */
    };

    /* Function pointer type of a function representing a command inside
     * a Redis module. */
    typedef int (*RedisModuleCmdFunc)(RedisModuleCtx *ctx, void **argv, int argc);

    /* This struct holds the information about a command registered by a module.*/
    struct RedisModuleCommandProxy
    {
            RedisModule* module;
            RedisModuleCmdFunc func;
    };
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
        //RedisModuleCommandProxy cp;
        //sds cmdname = sdsnew(name);
        /* Check if the command name is busy. */
        if (g_db->ContainsCommand(name))
        {
            return REDISMODULE_ERR;
        }
        RedisModuleCommandProxy* proxy = new RedisModuleCommandProxy;
        proxy->module = ctx->module;
        proxy->func = cmdfunc;
        /* Create a command "proxy", which is a structure that is referenced
         * in the command table, so that the generic command that works as
         * binding between modules and Redis, can know what function to call
         * and what the module is.
         *
         * Note that we use the Redis command table 'getkeys_proc' in order to
         * pass a reference to the command proxy structure. */
        //cp = zmalloc(sizeof(*cp));
//        cp.module = ctx->module;
//        cp.func = cmdfunc;
//        cp.rediscmd = zmalloc(sizeof(*rediscmd));
//        cp.rediscmd->name = cmdname;
//        cp.rediscmd->proc = RedisModuleCommandDispatcher;
//        cp.rediscmd->arity = -1;
//        cp.rediscmd->flags = flags | CMD_MODULE;
//        cp.rediscmd->getkeys_proc = (redisGetKeysProc*) (unsigned long) ctx->module->handle;
//        cp.rediscmd->firstkey = firstkey;
//        cp.rediscmd->lastkey = lastkey;
//        cp.rediscmd->keystep = keystep;
//        cp.rediscmd->microseconds = 0;
//        cp.rediscmd->calls = 0;
        //dictAdd(server.commands, sdsdup(cmdname), cp->rediscmd);
        //dictAdd(server.orig_commands, sdsdup(cmdname), cp->rediscmd);
        RedisCommandHandlerSetting setting;
        setting.name = name;
        setting.flags = flags | CMD_MODULE;
        setting.min_arity = setting.max_arity = -1;
        setting.handler = &Ardb::RedisModuleCommandDispatcher;
        setting.command_proxy = proxy;
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
        if (NULL != ctx->module)
        {
            return;
        }
        NEW(ctx->module, RedisModule);
        ctx->module->name = name;
        ctx->module->ver = ver;
        ctx->module->apiver = apiver;
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
        RedisReply& reply = ctx->GetReply();
        reply.SetErrorReason("wrong number of arguments for '" + ctx->gctx->current_cmd->GetCommand() + "' command");
        return REDISMODULE_OK;
    }

    /* Send an integer reply to the client, with the specified long long value.
     * The function always returns REDISMODULE_OK. */
    int RM_ReplyWithLongLong(RedisModuleCtx *ctx, long long ll)
    {
        RedisReply& reply = ctx->GetReply();
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
        RedisReply& reply = ctx->GetReply();
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
        RedisReply& reply = ctx->GetReply();
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
        RedisReply& reply = ctx->GetReply();
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
        RedisReply& reply = ctx->gctx->GetReply();
        reply.ReserveMember(len);
    }

    /* Reply with a bulk string, taking in input a C buffer pointer and length.
     *
     * The function always returns REDISMODULE_OK. */
    int RM_ReplyWithStringBuffer(RedisModuleCtx *ctx, const char *buf, size_t len)
    {
        RedisReply& reply = ctx->gctx->GetReply();
        reply.SetString(std::string(buf, len));
        return REDISMODULE_OK;
    }
    /* Reply with a bulk string, taking in input a RedisModuleString object.
     *
     * The function always returns REDISMODULE_OK. */
    int RM_ReplyWithString(RedisModuleCtx *ctx, const Data& str)
    {
        RedisReply& reply = ctx->GetReply();
        reply.SetString(str);
        return REDISMODULE_OK;
    }
    /* Reply to the client with a NULL. In the RESP protocol a NULL is encoded
     * as the string "$-1\r\n".
     *
     * The function always returns REDISMODULE_OK. */
    int RM_ReplyWithNull(RedisModuleCtx *ctx)
    {
        RedisReply& reply = ctx->GetReply();
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
        RedisReply& reply = ctx->GetReply();
        reply.SetDouble(d);
        return REDISMODULE_OK;
    }

    /* Reply exactly what a Redis command returned us with RedisModule_Call().
     * This function is useful when we use RedisModule_Call() in order to
     * execute some command, as we want to reply to the client exactly the
     * same reply we obtained by the command.
     *
     * The function always returns REDISMODULE_OK. */
    int RM_ReplyWithCallReply(RedisModuleCtx *ctx, void *reply)
    {
        RedisReply & rr = ctx->GetReply();

//        sds proto = sdsnewlen(reply->proto, reply->protolen);
//        addReplySds(c,proto);
        return REDISMODULE_OK;
    }

    /* --------------------------------------------------------------------------
     * Service API exported to modules
     *
     * Note that all the exported APIs are called RM_<funcname> in the core
     * and RedisModule_<funcname> in the module side (defined as function
     * pointers in redismodule.h). In this way the dynamic linker does not
     * mess with our global function pointers, overriding it with the symbols
     * defined in the main executable having the same names.
     * -------------------------------------------------------------------------- */

    /* Lookup the requested module API and store the function pointer into the
     * target pointer. The function returns REDISMODULE_ERR if there is no such
     * named API, otherwise REDISMODULE_OK.
     *
     * This function is not meant to be used by modules developer, it is only
     * used implicitly by including redismodule.h. */
    int RM_GetApi(const char *funcname, void **targetPtrPtr)
    {
        if (0 != g_module_manager->GetApi(funcname, targetPtrPtr))
        {
            return REDISMODULE_ERR;
        }
        return REDISMODULE_OK;
    }

    /* Return the currently selected DB. */
    int RM_GetSelectedDb(RedisModuleCtx *ctx)
    {
        return ctx->gctx->ns.GetInt64();
    }
    /* Change the currently selected DB. Returns an error if the id
     * is out of range.
     *
     * Note that the client will retain the currently selected DB even after
     * the Redis command implemented by the module calling this function
     * returns.
     *
     * If the module command wishes to change something in a different DB and
     * returns back to the original one, it should call RedisModule_GetSelectedDb()
     * before in order to restore the old DB number before returning. */
    int RM_SelectDb(RedisModuleCtx *ctx, int newid)
    {
        ctx->gctx->ns.SetInt64(newid);
        return REDISMODULE_OK;
    }

    /* Return an handle representing a Redis key, so that it is possible
     * to call other APIs with the key handle as argument to perform
     * operations on the key.
     *
     * The return value is the handle repesenting the key, that must be
     * closed with RM_CloseKey().
     *
     * If the key does not exist and WRITE mode is requested, the handle
     * is still returned, since it is possible to perform operations on
     * a yet not existing key (that will be created, for example, after
     * a list push operation). If the mode is just READ instead, and the
     * key does not exist, NULL is returned. However it is still safe to
     * call RedisModule_CloseKey() and RedisModule_KeyType() on a NULL
     * value. */
    void *RM_OpenKey(RedisModuleCtx *ctx, std::string *keyname, int mode)
    {
        RedisModuleKey *kp = new RedisModuleKey;
//        robj *value;
//
//        if (mode & REDISMODULE_WRITE)
//        {
//            value = lookupKeyWrite(ctx->client->db, keyname);
//        }
//        else
//        {
//            value = lookupKeyRead(ctx->client->db, keyname);
//            if (value == NULL)
//            {
//                return NULL;
//            }
//        }

        kp->ctx = ctx;
        kp->db = ctx->gctx->ns;
        kp->key = *keyname;
        //incrRefCount(keyname);
        //kp->value = value;
        //kp->iter = NULL;
        kp->mode = mode;
        //zsetKeyReset(kp);
        //autoMemoryAdd(ctx, REDISMODULE_AM_KEY, kp);
        return (void*) kp;
    }
    /* Close a key handle. */
    void RM_CloseKey(RedisModuleKey *key)
    {
        if (key == NULL) return;
        if (key->mode & REDISMODULE_WRITE)
        {
            //TODO
            //signalModifiedKey(key->db, key->key);
        }
        /* TODO: if (key->iter) RM_KeyIteratorStop(kp); */
        //RM_ZsetRangeStop(key);
        //decrRefCount(key->key);
        //autoMemoryFreed(key->ctx, REDISMODULE_AM_KEY, key);
        //zfree(key);
        DELETE(key);
    }
    /* Return the type of the key. If the key pointer is NULL then
     * REDISMODULE_KEYTYPE_EMPTY is returned. */
    int RM_KeyType(RedisModuleKey *key)
    {
        if (key == NULL) return REDISMODULE_KEYTYPE_EMPTY;
        /* We map between defines so that we are free to change the internal
         * defines as desired. */
        switch (key->value_type)
        {
            case KEY_STRING:
                return REDISMODULE_KEYTYPE_STRING;
            case KEY_LIST:
                return REDISMODULE_KEYTYPE_LIST;
            case KEY_SET:
                return REDISMODULE_KEYTYPE_SET;
            case KEY_ZSET:
                return REDISMODULE_KEYTYPE_ZSET;
            case KEY_HASH:
                return REDISMODULE_KEYTYPE_HASH;
            case KEY_MODULE:
                return REDISMODULE_KEYTYPE_MODULE;
            default:
                return 0;
        }
    }
    /* Return the length of the value associated with the key.
     * For strings this is the length of the string. For all the other types
     * is the number of elements (just counting keys for hashes).
     *
     * If the key pointer is NULL or the key is empty, zero is returned. */
    size_t RM_ValueLength(RedisModuleKey *key)
    {
        if (key == NULL) return 0;
        Context tmpctx;
        tmpctx.ns = key->db;
        RedisCommandFrame cmd;
        cmd.AddArg(key->key);
        switch (key->value_type)
        {
            case KEY_STRING:
            {
                g_db->Strlen(tmpctx, cmd);
                break;
            }
            case KEY_LIST:
            {
                g_db->LLen(tmpctx, cmd);
                break;
            }
            case KEY_SET:
            {
                g_db->SCard(tmpctx, cmd);
                break;
            }
            case KEY_ZSET:
            {
                g_db->ZCard(tmpctx, cmd);
                break;
            }
            case KEY_HASH:
            {
                g_db->HLen(tmpctx, cmd);
                break;
            }
            default:
                return 0;
        }
        return tmpctx.GetReply().GetInteger();
    }

    /* --------------------------------------------------------------------------
     * Key API for List type
     * -------------------------------------------------------------------------- */

    /* Push an element into a list, on head or tail depending on 'where' argumnet.
     * If the key pointer is about an empty key opened for writing, the key
     * is created. On error (key opened for read-only operations or of the wrong
     * type) REDISMODULE_ERR is returned, otherwise REDISMODULE_OK is returned. */
    int RM_ListPush(RedisModuleKey *key, int where, RedisModuleString *ele)
    {
        if (!(key->mode & REDISMODULE_WRITE)) return REDISMODULE_ERR;
        Context tmpctx;
        tmpctx.ns = key->db;
        RedisCommandFrame cmd;
        cmd.AddArg(key->key);
        cmd.AddArg(ele->c_str());
        if (where == REDISMODULE_LIST_HEAD)
        {
            cmd.SetType(REDIS_CMD_LPUSH);
        }
        else
        {
            cmd.SetType(REDIS_CMD_RPUSH);
        }
        g_db->ListPush(tmpctx, cmd, true);
        //if (key->value && key->value->type != OBJ_LIST) return REDISMODULE_ERR;
        //if (key->value == NULL) moduleCreateEmptyKey(key, REDISMODULE_KEYTYPE_LIST);
        //listTypePush(key->value, ele, (where == REDISMODULE_LIST_HEAD) ? QUICKLIST_HEAD : QUICKLIST_TAIL);
        if (tmpctx.GetReply().IsErr())
        {
            return REDISMODULE_ERR;
        }
        return REDISMODULE_OK;
    }

    /* Pop an element from the list, and returns it as a module string object
     * that the user should be free with RM_FreeString() or by enabling
     * automatic memory. 'where' specifies if the element should be popped from
     * head or tail. The command returns NULL if:
     * 1) The list is empty.
     * 2) The key was not open for writing.
     * 3) The key is not a list. */
    RedisModuleString *RM_ListPop(RedisModuleKey *key, int where)
    {
        if (!(key->mode & REDISMODULE_WRITE)) return NULL;
        Context tmpctx;
        tmpctx.ns = key->db;
        RedisCommandFrame cmd;
        cmd.AddArg(key->key);
        if (where == REDISMODULE_LIST_HEAD)
        {
            cmd.SetType(REDIS_CMD_LPOP);
        }
        else
        {
            cmd.SetType(REDIS_CMD_RPOP);
        }
        g_db->ListPop(tmpctx, cmd, true);
        //if (key->value && key->value->type != OBJ_LIST) return REDISMODULE_ERR;
        //if (key->value == NULL) moduleCreateEmptyKey(key, REDISMODULE_KEYTYPE_LIST);
        //listTypePush(key->value, ele, (where == REDISMODULE_LIST_HEAD) ? QUICKLIST_HEAD : QUICKLIST_TAIL);
        if (tmpctx.GetReply().IsErr())
        {
            return NULL;
        }
        RedisModuleString* decoded = new RedisModuleString;
        decoded->assign(tmpctx.GetReply().GetString());
        return decoded;
    }

    /* --------------------------------------------------------------------------
     * Higher level string operations
     * ------------------------------------------------------------------------- */

    /* Convert the string into a long long integer, storing it at `*ll`.
     * Returns REDISMODULE_OK on success. If the string can't be parsed
     * as a valid, strict long long (no spaces before/after), REDISMODULE_ERR
     * is returned. */
    int RM_StringToLongLong(const RedisModuleString *str, int64_t *ll)
    {
        return string2ll(str->c_str(), str->size(), ll) ? REDISMODULE_OK :
        REDISMODULE_ERR;
    }

    /* Convert the string into a double, storing it at `*d`.
     * Returns REDISMODULE_OK on success or REDISMODULE_ERR if the string is
     * not a valid string representation of a double value. */
    int RM_StringToDouble(const RedisModuleString *str, double *d)
    {
        if (!string_todouble(*str, *d))
        {
            return REDISMODULE_ERR;
        }
        return REDISMODULE_OK;
    }

    /* Compare two string objects, returning -1, 0 or 1 respectively if
     * a < b, a == b, a > b. Strings are compared byte by byte as two
     * binary blobs without any encoding care / collation attempt. */
    int RM_StringCompare(RedisModuleString *a, RedisModuleString *b)
    {
        return a->compare(*b);
    }

    /* Return the (possibly modified in encoding) input 'str' object if
     * the string is unshared, otherwise NULL is returned. */
    RedisModuleString *moduleAssertUnsharedString(RedisModuleString *str)
    {
        return str;
    }

    /* Append the specified buffere to the string 'str'. The string must be a
     * string created by the user that is referenced only a single time, otherwise
     * REDISMODULE_ERR is returend and the operation is not performed. */
    int RM_StringAppendBuffer(RedisModuleCtx *ctx, RedisModuleString *str, const char *buf, size_t len)
    {
        //UNUSED(ctx);
        str = moduleAssertUnsharedString(str);
        if (str == NULL) return REDISMODULE_ERR;
        str->append(buf, len);
        //str->ptr = sdscatlen(str->ptr, buf, len);
        return REDISMODULE_OK;
    }

    /* Exported API to call any Redis command from modules.
     * On success a RedisModuleCallReply object is returned, otherwise
     * NULL is returned and errno is set to the following values:
     *
     * EINVAL: command non existing, wrong arity, wrong format specifier.
     * EPERM:  operation in Cluster instance with key in non local slot. */
    RedisModuleCallReply *RM_Call(RedisModuleCtx *ctx, const char *cmdname, const char *fmt, ...)
    {
        RedisCommandFrame cmd;
        cmd.SetCommand(cmdname);
        RedisCommandHandlerSetting* settting = g_db->FindRedisCommandHandlerSetting(cmd);

        robj **argv = NULL;
        int argc = 0, flags = 0;
        va_list ap;
        RedisModuleCallReply *reply = NULL;
        int replicate = 0; /* Replicate this command? */

        if (NULL == settting)
        {
            errno = EINVAL;
            return NULL;
        }

        /* Create the client and dispatch the command. */
        va_start(ap, fmt);
        //c = createClient(-1);
        //argv = moduleCreateArgvFromUserFormat(cmdname, fmt, &argc, &flags, ap);
        //replicate = flags & REDISMODULE_ARGV_REPLICATE;
        va_end(ap);

        /* Setup our fake client for command execution. */
        //c->flags |= CLIENT_MODULE;
        //c->db = ctx->client->db;
        //c->argv = argv;
        //c->argc = argc;
        //c->cmd = c->lastcmd = cmd;
        /* We handle the above format error only when the client is setup so that
         * we can free it normally. */
        if (argv == NULL) goto cleanup;

        /* Basic arity checks. */
        //if ((cmd->arity > 0 && cmd->arity != argc) || (argc < -cmd->arity))
        //{
        //    errno = EINVAL;
        //    goto cleanup;
        //}

        /* If this is a Redis Cluster node, we need to make sure the module is not
         * trying to access non-local keys, with the exception of commands
         * received from our master. */
        //if (server.cluster_enabled && !(ctx->client->flags & CLIENT_MASTER))
        //{
        //    /* Duplicate relevant flags in the module client. */
        //     c->flags &= ~(CLIENT_READONLY | CLIENT_ASKING);
        //    c->flags |= ctx->client->flags & (CLIENT_READONLY | CLIENT_ASKING);
        //    if (getNodeByQuery(c, c->cmd, c->argv, c->argc, NULL, NULL) != server.cluster->myself)
        //    {
        //        errno = EPERM;
        //        goto cleanup;
        //    }
        //}

        /* If we are using single commands replication, we need to wrap what
         * we propagate into a MULTI/EXEC block, so that it will be atomic like
         * a Lua script in the context of AOF and slaves. */
        //if (replicate) moduleReplicateMultiIfNeeded(ctx);

        /* Run the command */
        //int call_flags = CMD_CALL_SLOWLOG | CMD_CALL_STATS;
        //if (replicate)
        //{
        //    call_flags |= CMD_CALL_PROPAGATE_AOF;
        //    call_flags |= CMD_CALL_PROPAGATE_REPL;
        //}
        g_db->Call(*(ctx->gctx), cmd);

        /* Convert the result of the Redis command into a suitable Lua type.
         * The first thing we need is to create a single string from the client
         * output buffers. */
        //sds proto = sdsnewlen(c->buf, c->bufpos);
        //c->bufpos = 0;
        //while (listLength(c->reply))
        //{
        //    sds o = listNodeValue(listFirst(c->reply));

        //    proto = sdscatsds(proto, o);
        //    listDelNode(c->reply, listFirst(c->reply));
        //}
        //reply = moduleCreateCallReplyFromProto(ctx, proto);
        //autoMemoryAdd(ctx, REDISMODULE_AM_REPLY, reply);

        //cleanup: freeClient(c);
        return reply;
    }

    /* Return a pointer, and a length, to the protocol returned by the command
     * that returned the reply object. */
    const char *RM_CallReplyProto(RedisModuleCallReply *reply, size_t *len)
    {
        if (reply->proto) *len = sdslen(reply->proto);
        return reply->proto;
    }
}

OP_NAMESPACE_BEGIN
    ModuleManager* g_module_manager = NULL;
#define REGISTER_API(name) \
    ModuleRegisterApi("RedisModule_" #name, (void *)(unsigned long)RM_ ## name)

    int ModuleManager::GetApi(const std::string& funcname, void **targetPtrPtr)
    {
        ModuleAPITable::iterator found = m_moduleapi.find(funcname);
        if (found == m_moduleapi.end())
        {
            return -1;
        }
        *targetPtrPtr = found->second;
        return 0;
    }
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
        REGISTER_API(GetSelectedDb);
        REGISTER_API(SelectDb);
        REGISTER_API(OpenKey);
        REGISTER_API(CloseKey);
        REGISTER_API(KeyType);
        REGISTER_API(ValueLength);
        REGISTER_API(ListPush);
        REGISTER_API(ListPop);
        REGISTER_API(StringToLongLong);
        REGISTER_API(StringToDouble);
        REGISTER_API(Call);
        REGISTER_API(CallReplyProto);
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

    int ModuleManager::LoadModule(const std::string& path, void **module_argv, int module_argc)
    {
        int (*onload)(void *, void **, int);
        void *handle;
        RedisModuleCtx ctx;

        handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (handle == NULL)
        {
            ERROR_LOG("Module %s failed to load: %s", path.c_str(), dlerror());
            return -1;
        }
        onload = (int (*)(void *, void **, int))(unsigned long) dlsym(handle,"RedisModule_OnLoad");if
(        onload == NULL)
        {
            ERROR_LOG("Module %s does not export RedisModule_OnLoad() symbol. Module not loaded.",path.c_str());
            return -1;
        }
        if (onload((void*) &ctx, module_argv, module_argc) == REDISMODULE_ERR)
        {
            //if (ctx.module) moduleFreeModuleStructure(ctx.module);
            dlclose(handle);
            ERROR_LOG("Module %s initialization failed. Module not loaded", path.c_str());
            return -1;
        }
        m_modules[ctx.module->name] = ctx.module;
        RedisModule* module = ctx.module;
        /* Redis module loaded! Register it. */
        module->handle = handle;
        INFO_LOG("Module '%s' loaded from %s", module->name.c_str(), path.c_str());
        //moduleFreeContext(&ctx);
        return 0;
    }
    int ModuleManager::UnloadModule(const std::string& name)
    {
        return -1;
    }

    int Ardb::RedisModuleCommandDispatcher(Context& ctx, RedisCommandFrame& cmd)
    {
        RedisModuleCommandProxy* proxy = (RedisModuleCommandProxy*) (ctx.cmd_proxy);
        RedisModuleCtx rctx;
        rctx.gctx = &ctx;
        rctx.module = proxy->module;
        RedisModuleCmdFunc func = proxy->func;
        const char* args[cmd.GetArguments().size() + 1];
        args[0] = cmd.GetCommand().c_str();
        for (size_t i = 0; i < cmd.GetArguments().size(); i++)
        {
            args[i + 1] = cmd.GetArguments()[i].c_str();
        }
        func(&rctx, (void**) args, cmd.GetArguments().size() + 1);
        return 0;
    }

OP_NAMESPACE_END
