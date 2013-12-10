/*
 * db_helpers.cpp
 *
 *  Created on: 2013Äê12ÔÂ10ÈÕ
 *      Author: wqy
 */
#include "db_helpers.hpp"


namespace ardb
{
    DBHelper::DBHelper(Ardb* db):m_db(db), m_expire_check(db)
    {
    }

    void DBHelper::Run()
    {
        m_serv.GetTimer().Schedule(&m_expire_check, 1, 1, SECONDS);
        m_serv.Start();
    }

    void DBHelper::StopSelf()
    {
        m_serv.Stop();
        m_serv.Wakeup();
    }
}



