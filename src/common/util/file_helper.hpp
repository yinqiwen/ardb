/*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 * 
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 * 
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 * 
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS 
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
 *THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef NOVA_FILEHELPER_HPP_
#define NOVA_FILEHELPER_HPP_
#include "common.hpp"
#include "buffer/buffer.hpp"
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

    int file_read_full(const std::string& path, std::string& content);
    int file_write_content(const std::string& path, const std::string& content);
    int file_append_content(const std::string& path, const std::string& content);

    int list_subdirs(const std::string& path, std::deque<std::string>& dirs);
    int list_subfiles(const std::string& path, std::deque<std::string>& fs, bool include_dir = false);
    int list_allfiles(const std::string& path, std::deque<std::string>& fs);

    int64 file_size(const std::string& path);

    int sha1sum_file(const std::string& file, std::string& hash);

    int real_path(const std::string& path, std::string& real_path);

    bool is_valid_fd(int fd);

    int file_copy(const std::string& src, const std::string& dst);
    int dir_copy(const std::string& src, const std::string& dst);

    int file_del(const std::string& path);
}

#endif /* FILEHELPER_HPP_ */
