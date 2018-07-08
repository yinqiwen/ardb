/*
 *Copyright (c) 2013-2018, yinqiwen <yinqiwen@gmail.com>
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
#include "engine_factory.hpp"
#include "db.hpp"

#if defined __USE_LMDB__
#include "lmdb/lmdb_engine.hpp"
const char* ardb::g_engine_name = "lmdb";
#elif defined __USE_ROCKSDB__
#include "rocksdb/rocksdb_engine.hpp"
const char* ardb::g_engine_name ="rocksdb";
#elif defined __USE_FORESTDB__
#include "forestdb/forestdb_engine.hpp"
const char* ardb::g_engine_name ="forestdb";
#elif defined __USE_LEVELDB__
#include "leveldb/leveldb_engine.hpp"
const char* ardb::g_engine_name = "leveldb";
#elif defined __USE_WIREDTIGER__
#include "wiredtiger/wiredtiger_engine.hpp"
const char* ardb::g_engine_name ="wiredtiger";
#elif defined __USE_PERCONAFT__
#include "perconaft/perconaft_engine.hpp"
const char* ardb::g_engine_name ="perconaft";
#else
const char* ardb::g_engine_name = "unknown";
#endif

OP_NAMESPACE_BEGIN

Engine* create_engine()
{
    Engine* engine = NULL;
#if defined __USE_LMDB__
    NEW(engine, LMDBEngine);
#elif defined __USE_ROCKSDB__
    NEW(engine, RocksDBEngine);
#elif defined __USE_LEVELDB__
    NEW(engine, LevelDBEngine);
#elif defined __USE_FORESTDB__
    NEW(engine, ForestDBEngine);
#elif defined __USE_WIREDTIGER__
    NEW(engine, WiredTigerEngine);
#elif defined __USE_PERCONAFT__
    NEW(engine, PerconaFTEngine);
#else
    ERROR_LOG("Unsupported storage engine specified at compile time.");
    return NULL;
#endif
    return engine;
}

OP_NAMESPACE_END




