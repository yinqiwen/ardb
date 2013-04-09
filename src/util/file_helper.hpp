/*
 * FileHelper.hpp
 *
 *  Created on: 2011-1-29
 *      Author: wqy
 */

#ifndef NOVA_FILEHELPER_HPP_
#define NOVA_FILEHELPER_HPP_
#include "common.hpp"
#include "buffer.hpp"
#include <string>

namespace ardb
{
	bool is_file_exist(const std::string& path);
	bool is_dir_exist(const std::string& path);
	bool make_dir(const std::string& path);
	bool make_file(const std::string& path);

	int make_fd_nonblocking(int fd);
	int make_fd_blocking(int fd);
	int make_tcp_nodelay(int fd);

	int get_n_bytes_readable_on_socket(int fd);

	int file_read_full(const std::string& path, Buffer& content);
}

#endif /* FILEHELPER_HPP_ */
