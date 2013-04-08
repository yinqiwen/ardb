/*
 * Runnable.hpp
 *
 *  Created on: 2011-1-10
 *      Author: qiyingwang
 */

#ifndef NOVA_RUNNABLE_HPP_
#define NOVA_RUNNABLE_HPP_
namespace rddb
{
	class Runnable
	{
		public:
			virtual void Run() = 0;
			virtual ~Runnable()
			{
			}
	};
	typedef void RunnableDestructor(Runnable* run);

	template<typename FUN>
	class RunnableFunctor: public Runnable
	{
		private:
			FUN* m_func;
			void Run()
			{
				m_func();
			}
		public:
			RunnableFunctor(FUN* fun) :
					m_func(fun)
			{
			}
	};

	template<typename FUN, typename Param>
	class RunnableFunctor1: public Runnable
	{
		private:
			const FUN m_func;
			const Param m_para;
			virtual void Run()
			{
				m_func(m_para);
			}
		public:
			RunnableFunctor1(FUN fun, Param param) :
					m_func(fun), m_para(param)
			{
			}
	};

	template<typename FUN, typename Param1, typename Param2>
	class RunnableFunctor2: public Runnable
	{
		private:
			FUN m_func;
			const Param1 m_para1;
			const Param2 m_para2;
			void Run()
			{
				m_func(m_para1, m_para2);
			}
		public:
			RunnableFunctor2(FUN fun, Param1 param1, Param2 param2) :
					m_func(fun), m_para1(param1), m_para2(param2)
			{
			}
	};

	template<typename FUN, typename Param1, typename Param2, typename Param3>
	class RunnableFunctor3: public Runnable
	{
		private:
			FUN m_func;
			const Param1 m_para1;
			const Param2 m_para2;
			const Param3 m_para3;

			void Run()
			{
				m_func(m_para1, m_para2, m_para3);
			}
		public:
			RunnableFunctor3(FUN fun, Param1 param1, Param2 param2,
					Param3 param3) :
					m_func(fun), m_para1(param1), m_para2(param2), m_para3(
							param3)
			{
			}
	};

	template<typename FUN>
	inline Runnable* make_fun_runnable(FUN fun)
	{
		return new RunnableFunctor<FUN>(fun);
	}

	template<typename FUN, typename PARA>
	inline Runnable* make_fun_runnable(FUN fun, PARA para)
	{
		return new RunnableFunctor1<FUN, PARA>(fun, para);
	}

	template<typename FUN, typename PARA1, typename PARA2>
	inline Runnable* make_fun_runnable(FUN fun, PARA1 para1, PARA2 para2)
	{
		return new RunnableFunctor2<FUN, PARA1, PARA2>(fun, para1, para2);
	}

	template<typename FUN, typename PARA1, typename PARA2, typename PARA3>
	inline Runnable* make_fun_runnable(FUN fun, PARA1 para1, PARA2 para2,
			PARA3 para3)
	{
		return new RunnableFunctor3<FUN, PARA1, PARA2, PARA3>(fun, para1, para2,
				para3);
	}
}

#endif /* RUNNABLE_HPP_ */
