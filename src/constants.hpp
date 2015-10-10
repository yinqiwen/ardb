/*
 * constants.hpp
 *
 *  Created on: 2015Äê9ÔÂ7ÈÕ
 *      Author: wangqiying
 */

#ifndef SRC_CONSTANTS_HPP_
#define SRC_CONSTANTS_HPP_

#include "common/common.hpp"

#define ARDB_MAX_DBID 0xFFFFFF
OP_NAMESPACE_BEGIN
    enum ErrCode
    {
        ERR_OK = 0,
        ERR_ENTRY_NOT_EXIST = -1000,
        ERR_INVALID_ARGS = -3,
        ERR_INVALID_OPERATION = -4,
        ERR_INVALID_STR = -5,
        ERR_DB_NOT_EXIST = -6,
        ERR_KEY_EXIST = -7,
        ERR_INVALID_TYPE = -8,
        ERR_OUTOFRANGE = -9,
        ERR_TOO_LARGE_RESPONSE = -11,
        ERR_INVALID_HLL_TYPE = -12,
        ERR_CORRUPTED_HLL_VALUE = -13,
        ERR_STORAGE_ENGINE_INTERNAL = -14,
        ERR_OVERLOAD = -15,
        ERR_NOT_EXIST_IN_CACHE = -16,

    };

OP_NAMESPACE_END

#endif /* SRC_CONSTANTS_HPP_ */
