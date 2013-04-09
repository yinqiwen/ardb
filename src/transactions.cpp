/*
 * transactions.cpp
 *
 *  Created on: 2013-4-8
 *      Author: wqy
 */

#include "ardb.hpp"

namespace ardb
{
	int Ardb::Discard(DBID db)
	{
		GetDB(db)->DiscardBatchWrite();
		return 0;
	}

	int Ardb::Exec(DBID db)
	{
		GetDB(db)->BeginBatchWrite();
		return 0;
	}
	int Ardb::Multi(DBID db)
	{
		GetDB(db)->CommitBatchWrite();
		return 0;
	}
}

