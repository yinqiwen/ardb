/*
 * db_helpers.hpp
 *
 *  Created on: 2013Äê12ÔÂ10ÈÕ
 *      Author: wqy
 */

#ifndef DB_HELPERS_HPP_
#define DB_HELPERS_HPP_
#include <stdint.h>
#include <float.h>
#include <map>
#include <vector>
#include <list>
#include <string>
#include <deque>
#include "common.hpp"
#include "util/thread/thread.hpp"
#include "util/thread/thread_mutex.hpp"
#include "util/thread/lock_guard.hpp"
#include "util/concurrent_queue.hpp"
#include "ardb.hpp"
namespace ardb
{
    class Ardb;
    class ExpireCheck: public Runnable
    {
        private:
            DBID m_checking_db;
            Ardb* m_db;
            void Run();
        public:
            ExpireCheck(Ardb* db);
    };

    class DBHelper: public Thread
    {
        private:
            ChannelService m_serv;
            Ardb* m_db;
            ExpireCheck m_expire_check;
            void Run();
        public:
            DBHelper(Ardb* db);
            void StopSelf();
    };
}

#endif /* DB_HELPERS_HPP_ */
