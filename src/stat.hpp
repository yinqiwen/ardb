/*
 * stat.hpp
 *
 *  Created on: 2013?7?3?
 *      Author: wqy
 */

#ifndef STAT_HPP_
#define STAT_HPP_

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include "common.hpp"
#include "util/thread/thread_mutex_lock.hpp"

namespace ardb
{
    class ServerStat
    {
        private:
#ifdef HAVE_ATOMIC
            //do nothing
#else
            ThreadMutexLock m_lock;
#endif
            void IncValue(size_t& v)
            {
#ifdef HAVE_ATOMIC
                __sync_add_and_fetch(&v, 1);
#else
                m_lock.Lock();
                v++;
                m_lock.Unlock();
#endif
            }
            void DecValue(size_t& v)
            {
#ifdef HAVE_ATOMIC
                __sync_add_and_fetch(&v, -1);
#else
                m_lock.Lock();
                v--;
                m_lock.Unlock();
#endif
            }
        public:
            size_t m_stat_numcommands;
            size_t m_connected_clients;
            size_t m_stat_numconnections;
        public:
            ServerStat() :
                    m_stat_numcommands(0), m_connected_clients(0), m_stat_numconnections(0)
            {
            }
            void IncRecvCommands()
            {
                IncValue(m_stat_numcommands);
            }
            void IncAcceptedClient()
            {
                IncValue(m_connected_clients);
                IncValue(m_stat_numconnections);
            }
            void DecAcceptedClient()
            {
                DecValue(m_connected_clients);
            }
            size_t GetNumOfRecvCommands()
            {
                return m_stat_numcommands;
            }
    };
}

#endif /* STAT_HPP_ */
