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

#ifndef SRC_ENGINE_OPERATION_HPP_
#define SRC_ENGINE_OPERATION_HPP_

#include "common.hpp"
#include "thread/event_condition.hpp"

namespace ardb
{
    enum WriteOperationType
    {
        PUT_OP = 1, DEL_OP, CKP_OP
    };

    struct WriteOperation
    {
            WriteOperationType type;
            WriteOperation(WriteOperationType t) :
                    type(t)
            {
            }
            virtual ~WriteOperation()
            {
            }
    };

    struct PutOperation: public WriteOperation
    {
            std::string key;
            std::string value;
            PutOperation() :
                    WriteOperation(PUT_OP)
            {
            }
    };

    struct DelOperation: public WriteOperation
    {
            std::string key;
            DelOperation() :
                    WriteOperation(DEL_OP)
            {
            }
    };

    struct CheckPointOperation: public WriteOperation
    {
            EventCondition& cond;
            bool execed;
            bool success;
            CheckPointOperation(EventCondition& c) :
                    WriteOperation(CKP_OP), cond(c), execed(false),success(true)
            {
            }
            void Notify()
            {
                execed = true;
                cond.Notify();
            }
            void Wait()
            {
                cond.Wait();
            }
    };

}

#endif /* SRC_ENGINE_OPERATION_HPP_ */
