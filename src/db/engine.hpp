/*
 * engine.hpp
 *
 *  Created on: 2015Äê12ÔÂ11ÈÕ
 *      Author: wangqiying
 */

#ifndef SRC_DB_ENGINE_HPP_
#define SRC_DB_ENGINE_HPP_
#include "common/common.hpp"
#include "codec.hpp"

OP_NAMESPACE_BEGIN

    class Engine
    {
        public:
            //int Init() = 0;
            int Put(Context& ctx, const KeyObject& key, const ValueObject& value);
            int Get(Context& ctx, const KeyObject& key, ValueObject& value);
            int Del(Context& ctx, const KeyObject& key);
            int MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs);
            int Merge(Context& ctx, const KeyObject& key, MergeOperation& op);
            bool Exists(Context& ctx, const KeyObject& key);
            int BeginTransaction() = 0;
            int CommitTransaction() = 0;
            int DiscardTransaction() = 0;
            virtual ~Engine()
            {
            }
    };

    struct TransactionGuard
    {
            Context& ctx;
            Engine* engine;
            int err;
            TransactionGuard(Context& c, Engine* e) :
                    ctx(c), engine(NULL), err(0)
            {
                int err = e->BeginTransaction();
                if (0 == err)
                {
                    engine = e;
                }
            }
            void MarkFailed(int errcode)
            {
                err = errcode;
            }
            ~TransactionGuard()
            {
                if (NULL != engine)
                {
                    if (0 == err)
                    {
                        err = engine->CommitTransaction();
                    }
                    else
                    {
                        engine->DiscardTransaction();
                    }
                }
            }
    };

OP_NAMESPACE_END

#endif /* SRC_DB_ENGINE_HPP_ */
