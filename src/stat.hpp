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
            atomic_counter_t stat_numcommands;
            atomic_counter_t stat_period_numcommands;
            atomic_counter_t connected_clients;
            atomic_counter_t stat_numconnections;
            ServerStat() :
                    stat_numcommands(0), connected_clients(0), stat_numconnections(0)
            {
            }
            void Run()
            {
                stat_period_numcommands.set(0);
            }
            void IncRecvCommands()
            {
                stat_numcommands.add(1);
                stat_period_numcommands.add(1);
            }
            void IncAcceptedClient()
            {
                connected_clients.add(1);
                stat_numconnections.add(1);
            }
            void DecAcceptedClient()
            {
                connected_clients.sub(1);
            }
    };
}

#endif /* STAT_HPP_ */
