/*
 * transactions.cpp
 *
 *  Created on: 2013-4-8
 *      Author: wqy
 */

#include "rddb.hpp"

namespace rddb
{
	int RDDB::Discard(DBID db)
	{
		GetDB(db)->DiscardBatchWrite();
		return 0;
	}

	int RDDB::Exec(DBID db)
	{
		GetDB(db)->BeginBatchWrite();
		return 0;
	}
	int RDDB::Multi(DBID db)
	{
		GetDB(db)->CommitBatchWrite();
	}
}

