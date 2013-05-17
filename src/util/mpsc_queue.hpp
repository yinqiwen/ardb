/*
 * mpsc_queue.hpp
 *
 *  Created on: 2013-5-16
 *      Author: wqy
 *
 * Multi-producer/single-consumer queues (MPSC)
 *
 * This is a lock-free MPSC queue implementation based on Dmitry Vyukov's algorithm
 *
 *  http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue
 */

#ifndef MPSC_QUEUE_HPP_
#define MPSC_QUEUE_HPP_

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
#ifdef __x86_64__
ATOMIC_FUNC_XCHG(get_and_set, "xchgq %1, %0", vptr_t)
#else
ATOMIC_FUNC_XCHG(get_and_set, "xchgl %1, %0", vptr_t)
#endif

namespace ardb
{
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
			void Push(T value)
			{
				Node* node = new Node;
				node->value = value;
				Node* prev = (Node*)atomic_get_and_set_vptr_t((vptr_t*)(&m_head),node);
				prev->next = node;
			}
			bool Pop(T& value)
			{
				Node* tail = m_tail;
				Node* next = tail->next;
				if (next)
				{
					m_tail = next;
					delete tail;
					value = next->value;
					return true;
				}
				return false;
			}
	};
}

#endif /* X_HPP_ */
