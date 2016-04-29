/*
 * scheduler.hpp
 *
 *  Created on: May 31, 2014
 *      Author: wangqiying
 */

#ifndef SCHEDULER_HPP_
#define SCHEDULER_HPP_

#include "common.hpp"
#include "coro.h"
#include <stack>

OP_NAMESPACE_BEGIN

    typedef void CoroutineFunc(void* data);

    typedef uint64_t coro_id;
    struct Coroutine
    {
            coro_id id;   //coro id
            CoroutineFunc* func;  //coro func
            coro_context ctx;
            coro_stack stack;
            Coroutine(CoroutineFunc* func = NULL, void* data = NULL, uint32 stack_size = 0);
            ~Coroutine();
    };
    /*
     * A simple coroutine scheduler
     */
    class Scheduler
    {
        private:
            typedef std::stack<Coroutine*> CoroutineStack;
            typedef TreeMap<coro_id, Coroutine*>::Type CoroutineTable;
            CoroutineStack m_pending_coros;
            CoroutineStack m_deleteing_coros;
            CoroutineTable m_join_table;
            CoroutineTable m_exec_table;
            void Clean();
        public:
            /*
             * Wakeup waiting coroutine
             */
            void Wakeup(Coroutine* coro);
            /*
             * Yield & wait
             */
            void Wait(Coroutine* coro);

            void DestroyCoro(Coroutine* coro);

            coro_id StartCoro(uint32 stack_size, CoroutineFunc* func, void* data);

            int Join(coro_id cid);

            static bool IsInMainCoro();

            uint32 GetCoroNum()
            {
            	return m_exec_table.size();
            }
    };

    extern Coroutine* g_current_coro;

OP_NAMESPACE_END

#endif /* SCHEDULER_HPP_ */
