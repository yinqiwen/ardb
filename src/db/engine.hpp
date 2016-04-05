/*
 * engine.hpp
 *
 *  Created on: 2015��12��11��
 *      Author: wangqiying
 */

#ifndef SRC_DB_ENGINE_HPP_
#define SRC_DB_ENGINE_HPP_
#include "common/common.hpp"
#include "codec.hpp"
#include "context.hpp"

OP_NAMESPACE_BEGIN

    struct Iterator
    {
            virtual bool Valid() = 0;
            virtual void Next() = 0;
            virtual void Prev() = 0;
            virtual void Jump(const KeyObject& next) = 0;
            virtual void JumpToFirst() = 0;
            virtual void JumpToLast() = 0;
            virtual KeyObject& Key() = 0;
            virtual ValueObject& Value() = 0;
            virtual ~Iterator()
            {
            }
    };

    class Engine
    {
        public:
            //int Init() = 0;
            virtual int Put(Context& ctx, const KeyObject& key, const ValueObject& value) = 0;
            virtual int Get(Context& ctx, const KeyObject& key, ValueObject& value) = 0;
            virtual int Del(Context& ctx, const KeyObject& key) = 0;
            virtual int MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs) = 0;
            virtual int Merge(Context& ctx, const KeyObject& key, uint16_t op, const DataArray& values) = 0;
            int Merge(Context& ctx, const KeyObject& key, uint16_t op, const Data& value)
            {
                return Merge(ctx, key, op, DataArray(1, value));
            }
            virtual bool Exists(Context& ctx, const KeyObject& key) = 0;

            virtual Iterator* Find(Context& ctx, const KeyObject& key) = 0;

            virtual int Compact(Context& ctx, const KeyObject& start, const KeyObject& end) = 0;

            virtual int BeginTransaction() = 0;
            virtual int CommitTransaction() = 0;
            virtual int DiscardTransaction() = 0;
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
                        ctx.transc_err = err;
                    }
                }
            }
    };

    int compare_keys(const char* k1, size_t k1_len, const char* k2, size_t k2_len, bool has_ns);

OP_NAMESPACE_END

#endif /* SRC_DB_ENGINE_HPP_ */
