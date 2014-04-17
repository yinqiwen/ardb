/*
 * backup.cpp
 *
 *  Created on: 2013-8-31
 *      Author: yinqiwen@gmail.com
 */
#include "backup.hpp"
#include "ardb_server.hpp"

namespace ardb
{

	Backup::Backup(ArdbServer* server) :
			m_is_saving(false), m_server(server), m_last_save(0)
	{

	}
	int Backup::Save()
	{
		if (m_is_saving)
		{
			return 1;
		}
		if (m_server->m_cfg.backup_dir.empty())
		{
			ERROR_LOG("Empty bakup dir for backup.");
			return -1;
		}
		//m_server->m_db->CloseAll();
		m_is_saving = true;
		int ret = 0;
		char cmd[m_server->m_cfg.data_base_path.size() + 256];
		make_dir(m_server->m_cfg.backup_dir);

		char dest[m_server->m_cfg.backup_dir.size() + 256];
		char shasumfile[m_server->m_cfg.backup_dir.size() + 256];
		uint32 now = time(NULL);
		sprintf(dest, "%s/dbsave-%u.tar", m_server->m_cfg.backup_dir.c_str(), now);
		sprintf(shasumfile, "%s/dbsave-%u.sha", m_server->m_cfg.backup_dir.c_str(), now);
		std::string source = m_server->m_cfg.data_base_path;
		if (0 == chdir(m_server->m_cfg.data_base_path.c_str()))
		{
			source = "*";
		}
		sprintf(cmd, "tar cf %s %s;", dest, source.c_str());
		ret = system(cmd);
		if (0 != chdir(m_server->m_cfg.home.c_str()))
		{
			WARN_LOG("Failed to change dir to home:%s", m_server->m_cfg.home.c_str());
		}
		if (-1 == ret)
		{
			ERROR_LOG( "Failed to create backup data archive:%s", dest);
		}
		else
		{
			std::string sha1sum_str;
			ret = sha1sum_file(dest, sha1sum_str);
			if (-1 == ret)
			{
				ERROR_LOG( "Failed to compute sha1sum for data archive:%s", dest);
			}
			else
			{
				INFO_LOG("Save file SHA1sum is %s", sha1sum_str.c_str());
				file_write_content(shasumfile, sha1sum_str);
				m_last_save = now;
			}
		}
		m_is_saving = false;
		return ret;
	}
	int Backup::BGSave()
	{
		struct BGTask: public Thread
		{
				Backup* serv;
				BGTask(Backup* s) :
						serv(s)
				{
				}
				void Run()
				{
					serv->Save();
					delete this;
				}
		};
		BGTask* task = new BGTask(this);
		task->Start();
		return 0;
	}
}

