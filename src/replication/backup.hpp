/*
 * backup.hpp
 *
 *  Created on: 2013-8-31
 *      Author: yinqiwen@gmail.com
 */

#ifndef BACKUP_HPP_
#define BACKUP_HPP_

#include "util/thread/thread.hpp"
#include "db.hpp"
#include <string>
#include <time.h>

namespace ardb
{
	class ArdbServer;
	class Backup
	{
		private:
			bool m_is_saving;
			ArdbServer* m_server;
			uint32 m_last_save;
		public:
			Backup(ArdbServer* server);
			int Save();
			int BGSave();
			uint32 LastSave()
			{
				return m_last_save;
			}
	};
}

#endif /* BACKUP_HPP_ */
