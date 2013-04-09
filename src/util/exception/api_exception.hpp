/*
 * APIException.hpp
 *
 *  Created on: 2011-1-11
 *      Author: wqy
 */

#ifndef NOVA_APIEXCEPTION_HPP_
#define NOVA_APIEXCEPTION_HPP_
#include "exception.hpp"

namespace ardb
{
	class APIException: public Exception
	{
		protected:
			int m_errorno;
		public:
			APIException();
			APIException(int err);
			APIException(const string& cause);
			APIException(const string& cause, int err);
			int GetErrorNO()
			{
				return m_errorno;
			}
			virtual ~APIException() throw ()
			{
			}
	};
}

#endif /* APIEXCEPTION_HPP_ */
