/*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
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
/*
 *
 *  Created on: 2013-5-16
 *      Author: yinqiwen
 *
 * Multi-producer/single-consumer queues (MPSC)
 * Single-producer/single-consumer queues (SPSC)
 *
 * This is lock-free MPSC/SPSC queues implementation based on Dmitry Vyukov's algorithm
 *
 *  http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue
 */

#ifndef CONCURRENT_QUEUE_HPP_
#define CONCURRENT_QUEUE_HPP_

#if defined __GNUC__ &&  __GNUC__ < 3 && __GNUC_MINOR__ < 9
/* gcc version < 2.9 */
#define ATOMIC_FUNC_XCHG(NAME, OP, TYPE) \
	inline static TYPE atomic_##NAME##_##TYPE(volatile TYPE* var, TYPE v) \
{ \
	asm volatile( \
			OP " \n\t" \
			: "=q"(v), "=m"(*var) :"m"(*var), "0"(v) : "memory" \
			); \
	return v; \
}
#else
#define ATOMIC_FUNC_XCHG(NAME, OP, TYPE) \
	inline static TYPE atomic_##NAME##_##TYPE(volatile TYPE* var, TYPE v) \
{ \
	asm volatile( \
			OP " \n\t" \
			: "+q"(v), "=m"(*var) : "m"(*var) : "memory" \
			); \
	return v; \
}
#endif /* gcc & gcc version < 2.9 */

typedef void* vptr_t;
#if defined(HAVE_SYNC_OP)
static vptr_t atomic_get_and_set_vptr_t(volatile vptr_t* var, vptr_t v)
{
    bool res;
    vptr_t expected;
    res = false;
    while (!res)
    {
        expected = *var;
        res = __sync_bool_compare_and_swap(var, expected, v);
    }
    return expected;
}
#else
#ifdef __x86_64__
ATOMIC_FUNC_XCHG(get_and_set, "xchgq %1, %0", vptr_t)
#else
ATOMIC_FUNC_XCHG(get_and_set, "xchgl %1, %0", vptr_t)
#endif
#endif

#if(defined(__x86_64__) || defined(__i386__))
#if ( (__GNUC__ == 4) && (__GNUC_MINOR__ >= 1) || __GNUC__ > 4)
#define __memory_barrier() __sync_synchronize()

#elif defined  __x86_64__
#define __memory_barrier() asm volatile( " mfence \n\t " : : : "memory" )
#else
#define __memory_barrier() asm volatile(" lock; addl $0, 0(%%esp) \n\t " : : : "memory" )
#endif
#elif defined(HAVE_SYNC_OP)
#define __memory_barrier() __sync_synchronize()
#endif

namespace ardb
{
//static inline void* atomic_addr_compare_exchange(void* volatile * addr,
//		void* cmp, void* xchg)
//{
//	return __sync_val_compare_and_swap(addr, cmp, xchg);
//}
//
//static inline uint64_t atomic_uint64_compare_exchange(uint64_t volatile* addr,
//		uint64_t cmp, uint64_t xchg)
//{
//	return __sync_val_compare_and_swap(addr, cmp, xchg);
//}
//
//static inline void atomic_signal_fence()
//{
//	__asm __volatile ("" ::: "memory");
//}
//
//static inline void* atomic_ptr_exchange(void* volatile * addr, void* val)
//{
//	void* cmp = *addr;
//	for (;;)
//	{
//		void* prev = __sync_val_compare_and_swap(addr, cmp, val);
//		if (cmp == prev)
//			return cmp;
//		cmp = prev;
//	}
//	return 0;
//}
//
//static inline unsigned atomic_uint_exchange(unsigned volatile* addr,
//		unsigned val)
//{
//	unsigned cmp = *addr;
//	for (;;)
//	{
//		unsigned prev = __sync_val_compare_and_swap(addr, cmp, val);
//		if (cmp == prev)
//			return cmp;
//		cmp = prev;
//	}
//	return 0;
//}
//
//enum memory_order
//{
//	memory_order_relaxed,
//	memory_order_consume,
//	memory_order_acquire,
//	memory_order_release,
//	memory_order_acq_rel,
//	memory_order_seq_cst,
//};
//
//template<typename T>
//class atomic_ptr
//{
//public:
//	unsigned load(memory_order mo) const volatile
//	{
//		(void) mo;
//		assert(
//				mo == memory_order_relaxed || mo == memory_order_consume
//						|| mo == memory_order_acquire
//						|| mo == memory_order_seq_cst);
//		unsigned v = val_;
//		atomic_signal_fence();
//		return v;
//	}
//
//	void store(void* v, memory_order mo) volatile
//	{
//		assert(
//				mo == memory_order_relaxed || mo == memory_order_release
//						|| mo == memory_order_seq_cst);
//		if (mo == memory_order_seq_cst)
//		{
//			atomic_ptr_exchange((void* volatile *) &val_, v);
//		}
//		else
//		{
//			atomic_signal_fence();
//			val_ = v;
//		}
//	}
//
//	bool compare_exchange_weak(void*& cmp, void* xchg, memory_order mo) volatile
//	{
//		T* prev = (T*) atomic_addr_compare_exchange((void* volatile *) &val_,
//				xchg, cmp);
//		if (prev == cmp)
//			return true;
//		cmp = prev;
//		return false;
//	}
//
//private:
//	T* volatile val_;
//};
//
//template<typename T>
//struct StackNode
//{
//	atomic_ptr<StackNode<T> > next;
//	T data;
//};
//
//template<typename T>
//class Stack
//{
//private:
//	atomic_ptr<StackNode<T> > m_head;
//public:
//	void Push(StackNode<T>* node)
//	{
//		StackNode<T>* cmp = m_head.load(memory_order_relaxed);
//		do
//			node->next.store(cmp, memory_order_relaxed);
//		while (!m_head.compare_exchange_weak(cmp, node, memory_order_release));
//	}
//	StackNode<T>* Pop()
//	{
//		StackNode<T>* node = m_head.load(memory_order_consume);
//		for (;;)
//		{
//			if (node == 0)
//				break;
//			StackNode<T>* next = node->next.load(memory_order_relaxed);
//			if (m_head.compare_exchange_weak(node, next, memory_order_release))
//				break;
//		}
//		return node;
//	}
//};

    template<typename T>
    class MPSCQueue
    {
        private:
            struct Node
            {
                    Node* volatile next;
                    T value;
                    Node() :
                            next(0)
                    {
                    }
            };
            typedef Node* node_ptr;

            Node* volatile m_head;
            Node* m_tail;
        public:
            MPSCQueue() :
                    m_head(0), m_tail(0)
            {
                m_head = new Node;
                m_tail = m_head;
            }
            void Push(const T& value)
            {
                /*
                 * consider another way to avoid expensive new operation
                 */
                Node* node = new Node;
                node->value = value;
                Node* prev = (Node*) atomic_get_and_set_vptr_t((vptr_t*) (&m_head), node);
                prev->next = node;
            }
            bool Pop(T& value)
            {
                Node* tail = m_tail;
                Node* next = tail->next;
                if (next)
                {
                    m_tail = next;
                    value = next->value;
                    delete tail;
                    return true;
                }
                return false;
            }
            ~MPSCQueue()
            {
                delete m_tail;
            }
    };

// load with 'consume' (data-dependent) memory ordering
    template<typename T>
    static T load_consume(T const* addr)
    {
        // hardware fence is implicit on x86
        T v = *const_cast<T const volatile*>(addr);
        __memory_barrier(); // compiler fence
        return v;
    }

// store with 'release' memory ordering
    template<typename T>
    static void store_release(T* addr, T v)
    {
        // hardware fence is implicit on x86
        __memory_barrier(); // compiler fence
        *const_cast<T volatile*>(addr) = v;
    }

// single-producer/single-consumer queue
    template<typename T>
    class SPSCQueue
    {
        public:
            SPSCQueue()
            {
                node* n = new node;
                n->next_ = 0;
                tail_ = head_ = first_ = tail_copy_ = n;
            }

            ~SPSCQueue()
            {
                node* n = first_;
                do
                {
                    node* next = n->next_;
                    delete n;
                    n = next;
                } while (n);
            }

            void Push(const T& v)
            {
                node* n = alloc_node();
                n->next_ = 0;
                n->value_ = v;
                store_release(&head_->next_, n);
                head_ = n;
            }

            // returns 'false' if queue is empty
            bool Pop(T& v)
            {
                if (load_consume(&tail_->next_))
                {
                    v = tail_->next_->value_;
                    store_release(&tail_, tail_->next_);
                    return true;
                }
                else
                {
                    return false;
                }
            }

        private:
            // internal node structure
            // cache line size on modern x86 processors (in bytes)
            static const uint8 CACHE_LINE_SIZE = 64;
            struct node
            {
                    node* next_;
                    T value_;
            };

            // consumer part
            // accessed mainly by consumer, infrequently be producer
            node* tail_; // tail of the queue

            // delimiter between consumer part and producer part,
            // so that they situated on different cache lines
            char cache_line_pad_[CACHE_LINE_SIZE];

            // producer part
            // accessed only by producer
            node* head_; // head of the queue
            node* first_; // last unused node (tail of node cache)
            node* tail_copy_; // helper (points somewhere between first_ and tail_)

            node* alloc_node()
            {
                // first tries to allocate node from internal node cache,
                // if attempt fails, allocates node via ::operator new()

                if (first_ != tail_copy_)
                {
                    node* n = first_;
                    first_ = first_->next_;
                    return n;
                }
                tail_copy_ = load_consume(&tail_);
                if (first_ != tail_copy_)
                {
                    node* n = first_;
                    first_ = first_->next_;
                    return n;
                }
                node* n = new node;
                return n;
            }

            SPSCQueue(SPSCQueue const&);
            SPSCQueue& operator =(SPSCQueue const&);
    };
}

#endif /* X_HPP_ */
