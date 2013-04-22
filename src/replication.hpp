/*
 * replication.hpp
 *
 *  Created on: 2013-4-22
 *      Author: wqy
 */

#ifndef REPLICATION_HPP_
#define REPLICATION_HPP_

namespace ardb
{
	class ReplicationService{
		public:
			int CheckPoint();
			int WriteBinLog();
	};
}

#endif /* REPLICATION_HPP_ */
