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

#ifndef SRC_MODULE_MODULE___HPP_
#define SRC_MODULE_MODULE___HPP_

#include "common/common.hpp"
#include "context.hpp"
OP_NAMESPACE_BEGIN
/* This structure represents a module inside the system. */
    struct RedisModule
    {
            void *handle; /* Module dlopen() handle. */
            std::string name; /* Module name. */
            int ver; /* Module version. We use just progressive integers. */
            int apiver; /* Module API version as requested during initialization.*/
            //list *types;    /* Module data types. */
    };
    struct RedisModuleCtxImpl
    {
            void *getapifuncptr; /* NOTE: Must be the first field. */
            RedisModule* module; /* Module reference. */
            //client *client;                 /* Client calling a command. */
            //struct RedisModuleBlockedClient *blocked_client; /* Blocked client for thread safe context. */
            //struct AutoMemEntry *amqueue;   /* Auto memory queue of objects to free. */
            //int amqueue_len; /* Number of slots in amqueue. */
            //int amqueue_used; /* Number of used slots in amqueue. */
            int flags; /* REDISMODULE_CTX_... flags. */
            //void **postponed_arrays; /* To set with RM_ReplySetArrayLength(). */
            //int postponed_arrays_count; /* Number of entries in postponed_arrays. */
            //void *blocked_privdata; /* Privdata set when unblocking a client. */

            /* Used if there is the REDISMODULE_CTX_KEYS_POS_REQUEST flag set. */
            //int *keys_pos;
            //int keys_count;
            Context* gctx;
            //struct RedisModulePoolAllocBlock *pa_head;
            RedisModuleCtxImpl()
                    : getapifuncptr(NULL), module(NULL), flags(0), gctx(NULL)
            {
            }
            RedisReply& GetReply()
            {
                return gctx->GetReply();
            }
            ~RedisModuleCtxImpl()
            {

            }
    };

    class ModuleManager
    {
        private:
            typedef TreeMap<std::string, void*>::Type ModuleAPITable;
            typedef TreeMap<std::string, RedisModule*>::Type ModuleTable;
            ModuleAPITable m_moduleapi;
            ModuleTable m_modules;
            void RegisterCoreAPI();
            int ModuleRegisterApi(const char *funcname, void *funcptr);
        public:
            int Init();
            int GetApi(const std::string& funcname, void **targetPtrPtr);
            int LoadModule(const std::string& path, void **module_argv, int module_argc);
            int UnloadModule(const std::string& name);
    };
    extern ModuleManager* g_module_manager;

OP_NAMESPACE_END

#endif /* SRC_MODULE_MODULE___HPP_ */
