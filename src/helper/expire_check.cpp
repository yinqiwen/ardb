/*
 * expire_check.cpp
 *
 *  Created on: 2013-12-10
 *      Author: wqy
 */
#include "db_helpers.hpp"
#include "ardb.hpp"

namespace ardb
{
    ExpireCheck::ExpireCheck(Ardb* db) :
            m_checking_db(0), m_db(db)
    {
    }

    void ExpireCheck::Run()
    {
        uint64 start = get_current_epoch_millis();
        while (m_checking_db < ARDB_GLOBAL_DB)
        {
            uint64 now = get_current_epoch_millis();
            if (now - start >= 500)
            {
                return;
            }
            DBID nexdb = 0;
            if (!m_db->DBExist(m_checking_db, nexdb))
            {
                if (nexdb == m_checking_db || nexdb == ARDB_GLOBAL_DB)
                {
                    m_checking_db = 0;
                    return;
                }
                m_checking_db = nexdb;
            }
            m_db->CheckExpireKey(m_checking_db);
            m_checking_db++;
        }
        m_checking_db = 0;
    }
}

