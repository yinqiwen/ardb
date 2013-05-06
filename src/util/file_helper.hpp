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
#include <deque>

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
	int file_write_content(const std::string& path, std::string& content);

	int list_subdirs(const std::string& path, std::deque<std::string>& dirs);
	int list_subfiles(const std::string& path, std::deque<std::string>& fs);

	int64 file_size(const std::string& path);

	int sha1sum_file(const std::string& file, std::string& hash);
}

#endif /* FILEHELPER_HPP_ */
