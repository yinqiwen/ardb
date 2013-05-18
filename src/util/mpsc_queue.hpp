/*
 * mpsc_queue.hpp
 *
 *  Created on: 2013-5-16
 *      Author: wqy
 *
 * Multi-producer/single-consumer queues (MPSC)
 * Single-producer/single-consumer queues (SPSC)
 *
 * This is lock-free MPSC/SPSC queues implementation based on Dmitry Vyukov's algorithm
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

#if ( (__GNUC__ == 4) && (__GNUC_MINOR__ >= 1) || __GNUC__ > 4) && \
(defined(__x86_64__) || defined(__i386__))
#define __memory_barrier __sync_synchronize
#elif defined  __x86_64__
#define __memory_barrier() asm volatile( " mfence \n\t " : : : "memory" )
#else
#define __memory_barrier() asm volatile(" lock; addl $0, 0(%%esp) \n\t " : : : "memory" )
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
		Node* prev = (Node*) atomic_get_and_set_vptr_t((vptr_t*) (&m_head),
				node);
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

	void Push(T v)
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
	static const uint8 CACHE_LINE_SIZE  = 64;
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
