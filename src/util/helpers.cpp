/*
 * helpers.cpp
 *
 *  Created on: 2013-4-4
 *      Author: wqy
 */
#include "helpers.hpp"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <stdio.h>

namespace rddb
{
	bool is_file_exist(const std::string& path)
	{
		struct stat buf;
		int ret = stat(path.c_str(), &buf);
		if (0 == ret)
		{
			return S_ISREG(buf.st_mode);
		}
		return false;
	}

	bool is_dir_exist(const std::string& path)
	{
		struct stat buf;
		int ret = stat(path.c_str(), &buf);
		if (0 == ret)
		{
			return S_ISDIR(buf.st_mode);
		}
		return false;
	}

	bool make_dir(const std::string& para_path)
	{
		if (is_dir_exist(para_path))
		{
			return true;
		}
		if (is_file_exist(para_path))
		{
			return false;
		}
		std::string path = para_path;
		size_t found = path.rfind("/");
		if (found == path.size() - 1)
		{
			path = path.substr(0, path.size() - 1);
			found = path.rfind("/");
		}
		if (found != std::string::npos)
		{
			std::string base_dir = path.substr(0, found);
			if (make_dir(base_dir))
			{
				//mode is 0755
				return mkdir(path.c_str(),
						S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) == 0;
			}
		} else
		{
			return mkdir(path.c_str(),
					S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) == 0;
		}
		return false;
	}
}

