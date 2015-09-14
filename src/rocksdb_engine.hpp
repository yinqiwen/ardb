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

#include <vector>

OP_NAMESPACE_BEGIN

    struct Operation
    {

    };

    class RocksDBEngine
    {
        private:
            typedef std::vector<rocksdb::ColumnFamilyHandle*> DBArray;
            rocksdb::DB* m_db;
            DBArray m_columns;
            rocksdb::ColumnFamilyHandle* GetDB(DBID db,
                    bool create_if_notexist);
        public:
            int Init();
            int Put(const KeyObject& key, const Data& value);
            int Get(const KeyObject& key, std::string& value);
            int MultiGet(const KeyObjectArray& key, const std::string& value);
            int Merge(const KeyObject& key, const MergeOp& op);
    };

OP_NAMESPACE_END
#endif /* SRC_ROCKSDB_HPP_ */
