/*
 * module_data.h
 *
 *  Created on: 2017Äê4ÔÂ27ÈÕ
 *      Author: qiyingwang
 */

#ifndef SRC_MODULE_MODULE_DATA_H_
#define SRC_MODULE_MODULE_DATA_H_
#include "common/common.hpp"
#include <vector>

OP_NAMESPACE_BEGIN

/* This structure represents a module inside the system. */
    struct RedisModule
    {
            void *handle; /* Module dlopen() handle. */
            char *name; /* Module name. */
            int ver; /* Module version. We use just progressive integers. */
            int apiver; /* Module API version as requested during initialization.*/
            //list *types;    /* Module data types. */
    };
    struct RedisModuleCtx
    {
            void *getapifuncptr; /* NOTE: Must be the first field. */
            struct RedisModule *module; /* Module reference. */
            //client *client;                 /* Client calling a command. */
            struct RedisModuleBlockedClient *blocked_client; /* Blocked client for
             thread safe context. */
            //struct AutoMemEntry *amqueue;   /* Auto memory queue of objects to free. */
            int amqueue_len; /* Number of slots in amqueue. */
            int amqueue_used; /* Number of used slots in amqueue. */
            int flags; /* REDISMODULE_CTX_... flags. */
            void **postponed_arrays; /* To set with RM_ReplySetArrayLength(). */
            int postponed_arrays_count; /* Number of entries in postponed_arrays. */
            void *blocked_privdata; /* Privdata set when unblocking a client. */

            /* Used if there is the REDISMODULE_CTX_KEYS_POS_REQUEST flag set. */
            int *keys_pos;
            int keys_count;

            //struct RedisModulePoolAllocBlock *pa_head;
    };

OP_NAMESPACE_END

extern "C"
{

    typedef PROJ_NS::RedisModule RedisModule;
    typedef PROJ_NS::RedisModuleCtx RedisModuleCtx;
    typedef int (*RedisModuleCmdFunc)(RedisModuleCtx *ctx, void **argv, int argc);

    typedef void redisCommandProc(void *c);
    typedef int *redisGetKeysProc(struct redisCommand *cmd, void **argv, int argc, int *numkeys);
    struct redisCommand
    {
            char *name;
            redisCommandProc *proc;
            int arity;
            char *sflags; /* Flags as string representation, one char per flag. */
            int flags; /* The actual flags, obtained from the 'sflags' field. */
            /* Use a function to determine keys arguments in a command line.
             * Used for Redis Cluster redirect. */
            redisGetKeysProc *getkeys_proc;
            /* What keys should be loaded in background when calling this command? */
            int firstkey; /* The first argument that's a key (0 = no keys) */
            int lastkey; /* The last argument that's a key */
            int keystep; /* The step between first and last key */
            long long microseconds, calls;
    };
    /* This struct holds the information about a command registered by a module.*/
    typedef struct RedisModuleCommandProxy
    {
            RedisModule *module;
            RedisModuleCmdFunc func;
            struct redisCommand *rediscmd;
    } RedisModuleCommandProxy;
}

#endif /* SRC_MODULE_MODULE_DATA_H_ */
