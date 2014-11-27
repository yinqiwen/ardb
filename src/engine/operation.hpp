/*
 * operation.hpp
 *
 *  Created on: 2014Äê11ÔÂ27ÈÕ
 *      Author: wangqiying
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
            CheckPointOperation(EventCondition& c) :
                    WriteOperation(CKP_OP), cond(c), execed(false)
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
