/*
 * Exception.hpp
 *
 *  Created on: 2011-1-11
 *      Author: wqy
 */

#ifndef NOVA_EXCEPTION_HPP_
#define NOVA_EXCEPTION_HPP_
#include <string>
#include <exception>

using std::string;

namespace rddb
{
	class Exception: public std::exception
	{
		protected:
			string m_cause;
		public:
			Exception()
			{
			}
			Exception(const string& cause) :
					m_cause(cause)
			{
			}
			const string& GetCause() const
			{
				return m_cause;
			}
			virtual ~Exception() throw ()
			{
			}
	};
}

#endif /* EXCEPTION_HPP_ */
