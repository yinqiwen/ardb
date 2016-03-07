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
    enum ErrorCode
    {
        //STATUS_OK = 0,
        ERR_ENTRY_NOT_EXIST = -1000,
        ERR_INVALID_INTEGER_ARGS = -1001,
        ERR_INVALID_FLOAT_ARGS = -1002,
        ERR_INVALID_SYNTAX = -1003,
        ERR_AUTH_FAILED = -1004,
        ERR_NOTPERFORMED = -1005,
        ERR_STRING_EXCEED_LIMIT = -1006,
        ERR_NOSCRIPT = -1007,
        ERR_INVALID_ARGS = -3,
        ERR_INVALID_OPERATION = -4,
        ERR_INVALID_STR = -5,
        ERR_KEY_EXIST = -7,
        ERR_INVALID_TYPE = -8,
        ERR_OUTOFRANGE = -9,

        ERR_ROCKS_kNotFound = -2001,
        ERR_ROCKS_kCorruption = -2002,
        ERR_ROCKS_kNotSupported = -2003,
        ERR_ROCKS_kInvalidArgument = -2004,
        ERR_ROCKS_kIOError = -2005,
        ERR_ROCKS_kMergeInProgress = -2006,
        ERR_ROCKS_kIncomplete = -2007,
        ERR_ROCKS_kShutdownInProgress = -2008,
        ERR_ROCKS_kTimedOut = -2009,
        ERR_ROCKS_kAborted = -2010,
        ERR_ROCKS_kBusy = -2011,
        ERR_ROCKS_kExpired = -2012,
        ERR_ROCKS_kTryAgain = -2013,
    };

    enum StatusCode
    {
        STATUS_OK = 1000,
        STATUS_PONG = 1001,
        STATUS_QUEUED = 1002,
    };

OP_NAMESPACE_END

#endif /* SRC_CONSTANTS_HPP_ */
