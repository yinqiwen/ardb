/*
 * APIException.cpp
 *
 *  Created on: 2011-1-11
 *      Author: wqy
 */
#include "exception/api_exception.hpp"
#include <string.h>
#include <errno.h>

using namespace rddb;

APIException::APIException() :
	Exception()
{
	m_errorno = errno;
	m_cause = strerror(m_errorno);
}
APIException::APIException(int err) :
	Exception(), m_errorno(err)
{
	m_cause = strerror(m_errorno);
}

APIException::APIException(const string& cause) :
	Exception(cause), m_errorno(errno)
{
}

APIException::APIException(const string& cause, int err) :
	Exception(cause), m_errorno(err)
{
}

