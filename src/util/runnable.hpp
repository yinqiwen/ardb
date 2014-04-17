/*
 *Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
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

#ifndef NOVA_RUNNABLE_HPP_
#define NOVA_RUNNABLE_HPP_
namespace ardb
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
            RunnableFunctor3(FUN fun, Param1 param1, Param2 param2, Param3 param3) :
                    m_func(fun), m_para1(param1), m_para2(param2), m_para3(param3)
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
    inline Runnable* make_fun_runnable(FUN fun, PARA1 para1, PARA2 para2, PARA3 para3)
    {
        return new RunnableFunctor3<FUN, PARA1, PARA2, PARA3>(fun, para1, para2, para3);
    }
}

#endif /* RUNNABLE_HPP_ */
