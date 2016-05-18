/*
 *Copyright (c) 2013-2016, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 *
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 *THE POSSIBILITY OF SUCH DAMAGE.
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
            Coroutine* m_current_coro;
            void Clean();

            void SetCurrentCoroutine(Coroutine* coro)
            {
                m_current_coro = coro;
            }
        public:
            Scheduler();
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

            bool IsInMainCoro();

            uint32 GetCoroNum()
            {
                return m_exec_table.size();
            }

            Coroutine* GetCurrentCoroutine()
            {
                return m_current_coro;
            }

            static Scheduler& CurrentScheduler();

    };

OP_NAMESPACE_END

#endif /* SCHEDULER_HPP_ */
