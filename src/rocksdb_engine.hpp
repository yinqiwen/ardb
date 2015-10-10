/*
 * rocksdb.hpp
 *
 *  Created on: 2015Äê8ÔÂ21ÈÕ
 *      Author: wangqiying
 */

#ifndef ROCKSDB_HPP_
#define ROCKSDB_HPP_

#include "common/common.hpp"
#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/comparator.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/statistics.h"
#include "rocksdb/merge_operator.h"
#include "codec.hpp"
#include "context.hpp"
#include <vector>

OP_NAMESPACE_BEGIN
    class RocksDBEngine
    {
        private:
            struct ColumnFamilyHandleKey
            {
                    const char* ns;
                    uint32 id;
            };
            typedef std::vector<rocksdb::ColumnFamilyHandle*> DBArray;
            rocksdb::DB* m_db;
            rocksdb::Options m_options;
            DBArray m_columns;
            rocksdb::ColumnFamilyHandle* GetDB(Context& ctx, const KeyObject& key, bool create_if_notexist);
        public:
            int Init(const std::string& dir, const std::string& conf);
            int Put(Context& ctx, const KeyObject& key, const ValueObject& value);
            int Get(Context& ctx, const KeyObject& key, ValueObject& value);
            int Del(Context& ctx, const KeyObject& key);
            int MultiGet(Context& ctx, const KeyObjectArray& key, ValueObjectArray& value, std::vector<int>* errs = NULL);
            int Merge(Context& ctx, const KeyObject& key, uint32_t op, const ValueObject& value);
            bool Exists(Context& ctx,const KeyObject& key, bool exact_check = false);
            int BeginBatchWrite();
            int CommitBatchWrite();
            int DiscardBatchWrite();
    };
    class BatchWriteGuard
    {
        private:
            Context& m_ctx;
        public:
            BatchWriteGuard(Context& ctx);
            void MarkFailed(int err)
            {
                m_ctx.write_err = err;
            }
            bool Success()
            {
                return m_ctx.write_err == 0;
            }
            ~BatchWriteGuard();
    };

OP_NAMESPACE_END
#endif /* SRC_ROCKSDB_HPP_ */
