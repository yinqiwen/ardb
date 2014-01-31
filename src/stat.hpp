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
#include "util/atomic_counter.hpp"

namespace ardb
{
    struct ServerStat:public Runnable
    {
        public:
            AtomicInt64 stat_numcommands;
            AtomicInt64 stat_period_numcommands;
            AtomicInt64 connected_clients;
            AtomicInt64 stat_numconnections;
            ServerStat() :
                    stat_numcommands(0), connected_clients(0), stat_numconnections(0)
            {
            }
            void Run()
            {
                stat_period_numcommands.Set(0);
            }
            void IncRecvCommands()
            {
                stat_numcommands.Add(1);
                stat_period_numcommands.Add(1);
            }
            void IncAcceptedClient()
            {
                connected_clients.Add(1);
                stat_numconnections.Add(1);
            }
            void DecAcceptedClient()
            {
                connected_clients.Sub(1);
            }
    };
}

#endif /* STAT_HPP_ */
