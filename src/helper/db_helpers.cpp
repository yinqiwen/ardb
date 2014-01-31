/*
 * db_helpers.cpp
 *
 *  Created on: 2013-12-10
 *      Author: wqy
 */
#include "db_helpers.hpp"

namespace ardb
{
    DBHelper::DBHelper(Ardb* db) :
            m_db(db), m_expire_check(db)
    {
    }

    void DBHelper::Run()
    {
        m_serv.GetTimer().Schedule(&m_expire_check, 100, 100, MILLIS);
        m_serv.Start();
    }

    void DBHelper::StopSelf()
    {
        m_serv.Stop();
        m_serv.Wakeup();
    }
}

