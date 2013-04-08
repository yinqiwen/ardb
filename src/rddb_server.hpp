/*
 * rddb_server.hpp
 *
 *  Created on: 2013-3-27
 *      Author: wqy
 */

#ifndef RDDB_SERVER_HPP_
#define RDDB_SERVER_HPP_
#include <string>
#include "rddb_data.hpp"
namespace rddb
{
	class RDDBServer
	{
		private:

		public:
			int Ping();
			int Echo(const std::string& message);
			int Select(DBID id);
	};
}


#endif /* RDDB_SERVER_HPP_ */
