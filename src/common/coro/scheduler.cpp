/*
 * scheduler.cpp
 *
 *  Created on: May 31, 2014
 *      Author: wangqiying
 */

#include "scheduler.hpp"

OP_NAMESPACE_BEGIN

    Coroutine* g_current_coro = NULL;
    static uint64 g_coro_id_seed = 0;
    Coroutine::Coroutine(CoroutineFunc* f, void* data, uint32 stack_size) :
            id(0), func(f)
    {
        id = g_coro_id_seed++;
        if (NULL != func)
        {
            coro_stack_alloc(&stack, stack_size);
            coro_create(&ctx, func, data, stack.sptr, stack.ssze);
        }
        else
        {
            coro_create(&ctx, NULL, NULL, NULL, 0);
        }
    }

    Coroutine::~Coroutine()
    {
        coro_stack_free(&stack);
        coro_destroy(&ctx);
    }

    void Scheduler::Clean()
    {
        while (!m_deleteing_coros.empty())
        {
            Coroutine* top = m_deleteing_coros.top();
            DELETE(top);
            m_deleteing_coros.pop();
        }
    }

    void Scheduler::Wakeup(Coroutine* coro)
    {
        Clean();
        Coroutine* current = g_current_coro;
        //bool create_coro = false;
        if (NULL == current)
        {
            current = new Coroutine;
            //create_coro = true;
        }
        m_pending_coros.push(current);
        g_current_coro = coro;
        coro_transfer(&(current->ctx), &(coro->ctx));
        /*
         * resume global coroutine
         */
        g_current_coro = current;
    }

    void Scheduler::Wait(Coroutine* coro)
    {
        Clean();
        if (!m_pending_coros.empty())
        {
            Coroutine* top = m_pending_coros.top();
            m_pending_coros.pop();
            g_current_coro = top;
            coro_transfer(&(coro->ctx), &(top->ctx));
        }
        else
        {
            abort();
        }
    }

    struct CoroutineContext
    {
            Scheduler* scheduler;
            CoroutineFunc* func;
            void* data;
    };

    static void MyCoroutineFunc(void* data)
    {
        CoroutineContext* ctx = (CoroutineContext*) data;
        Scheduler* scheduler = ctx->scheduler;
        Coroutine* coro = g_current_coro;
        ctx->func(ctx->data);
        DELETE(ctx);
        scheduler->DestroyCoro(coro);
    }

    void Scheduler::DestroyCoro(Coroutine* coro)
    {
        Clean();
        m_exec_table.erase(coro->id);
        m_deleteing_coros.push(coro);
        /*
         * Wake join coroutine first
         */
        CoroutineTable::iterator jit = m_join_table.find(coro->id);
        if (jit != m_join_table.end())
        {
            Coroutine* waiting = jit->second;
            m_join_table.erase(jit);
            g_current_coro = waiting;
            coro_transfer(&(coro->ctx), &(waiting->ctx));
        }
        else
        {
            if (!m_pending_coros.empty())
            {
                Coroutine* top = m_pending_coros.top();
                m_pending_coros.pop();
                g_current_coro = top;
                coro_transfer(&(coro->ctx), &(top->ctx));
            }
            else
            {
                abort();
            }
        }
    }

    coro_id Scheduler::StartCoro(uint32 stack_size, CoroutineFunc* func, void* data)
    {
        Clean();
        CoroutineContext* ctx = new CoroutineContext;
        ctx->func = func;
        ctx->scheduler = this;
        ctx->data = data;

        Coroutine* task = new Coroutine(MyCoroutineFunc, ctx, stack_size);
        m_exec_table[task->id] = task;
        Wakeup(task);
        return task->id;
    }

    int Scheduler::Join(coro_id cid)
    {
        if (IsInMainCoro())
        {
            //can not wait on main context
            return -1;
        }
        CoroutineTable::iterator it = m_exec_table.find(cid);
        if (it == m_exec_table.end())
        {
            return 0;
        }
        it = m_join_table.find(cid);
        if (it != m_join_table.end())
        {
            return -2;
        }

        m_join_table[cid] = g_current_coro;
        Wait(g_current_coro);
        return 0;
    }

    bool Scheduler::IsInMainCoro()
    {
        return NULL == g_current_coro || NULL == g_current_coro->func;
    }

OP_NAMESPACE_END

