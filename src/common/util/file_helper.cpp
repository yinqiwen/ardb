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

#include "util/file_helper.hpp"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <stdio.h>
#include <dirent.h>
#include <errno.h>
#include "sha1.h"

namespace ardb
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
            ERROR_LOG("Exist file '%s' is not a dir.", para_path.c_str());
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
        }
        else
        {
            return mkdir(path.c_str(),
            S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) == 0;
        }
        return false;
    }

    bool make_file(const std::string& para_path)
    {
        if (is_file_exist(para_path))
        {
            return true;
        }
        if (is_dir_exist(para_path))
        {
            ERROR_LOG("Exist file '%s' is not a regular file.", para_path.c_str());
            return false;
        }
        std::string path = para_path;
        size_t found = path.rfind("/");
        if (found != std::string::npos)
        {
            std::string base_dir = path.substr(0, found);
            if (make_dir(base_dir))
            {
                //mode is 0755
                return open(path.c_str(), O_CREAT,
                S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) == 0;
            }
        }
        else
        {
            return open(path.c_str(), O_CREAT,
            S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) == 0;
        }
        return false;
    }

    int make_fd_blocking(int fd)
    {
        int flags;
        if ((flags = fcntl(fd, F_GETFL, 0)) < 0)
        {
            return -1;
        }
        if (fcntl(fd, F_SETFL, flags | ~O_NONBLOCK) == -1)
        {
            return -1;
        }
        return 0;
    }

    int make_fd_nonblocking(int fd)
    {
        int flags;
        if ((flags = fcntl(fd, F_GETFL, 0)) < 0)
        {
            return -1;
        }
        if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1)
        {
            return -1;
        }
        return 0;
    }

    int make_tcp_nodelay(int fd)
    {
        int yes = 1;
        //return 1;
        return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
    }

    int file_write_content(const std::string& path, const std::string& content)
    {
        make_file(path);
        FILE *fp;
        if ((fp = fopen(path.c_str(), "wb")) == NULL)
        {
            return -1;
        }
        fwrite(content.c_str(), content.size(), 1, fp);
        fclose(fp);
        return 0;
    }

    int file_read_full(const std::string& path, Buffer& content)
    {
        FILE *fp;
        if ((fp = fopen(path.c_str(), "rb")) == NULL)
        {
            return -1;
        }

        fseek(fp, 0, SEEK_END);
        long int fsize = ftell(fp);
        rewind(fp);
        char* buffer = (char*) malloc(fsize);
        size_t len = fread(buffer, 1, fsize, fp);
        content.Write(buffer, len);
        free(buffer);
        fclose(fp);
        return 0;
    }

    int list_subdirs(const std::string& path, std::deque<std::string>& dirs)
    {
        struct stat buf;
        int ret = stat(path.c_str(), &buf);
        if (0 == ret)
        {
            if (S_ISDIR(buf.st_mode))
            {
                DIR* dir = opendir(path.c_str());
                if (NULL != dir)
                {
                    struct dirent * ptr;
                    while ((ptr = readdir(dir)) != NULL)
                    {
                        if (!strcmp(ptr->d_name, ".") || !strcmp(ptr->d_name, ".."))
                        {
                            continue;
                        }
                        std::string file_path = path;
                        file_path.append("/").append(ptr->d_name);
                        memset(&buf, 0, sizeof(buf));
                        ret = stat(path.c_str(), &buf);
                        if (ret == 0)
                        {
                            if (S_ISDIR(buf.st_mode))
                            {
                                dirs.push_back(ptr->d_name);
                            }
                        }
                    }
                    closedir(dir);
                    return 0;
                }
            }
        }
        return -1;
    }

    int list_subfiles(const std::string& path, std::deque<std::string>& fs)
    {
        struct stat buf;
        int ret = stat(path.c_str(), &buf);
        if (0 == ret)
        {
            if (S_ISDIR(buf.st_mode))
            {
                DIR* dir = opendir(path.c_str());
                if (NULL != dir)
                {
                    struct dirent * ptr;
                    while ((ptr = readdir(dir)) != NULL)
                    {
                        if (!strcmp(ptr->d_name, ".") || !strcmp(ptr->d_name, ".."))
                        {
                            continue;
                        }
                        std::string file_path = path;
                        file_path.append("/").append(ptr->d_name);
                        memset(&buf, 0, sizeof(buf));
                        ret = stat(path.c_str(), &buf);
                        if (ret == 0)
                        {
                            if (S_ISREG(buf.st_mode))
                            {
                                fs.push_back(ptr->d_name);
                            }
                        }
                    }
                    closedir(dir);
                    return 0;
                }
            }
        }
        return -1;
    }

    int64 file_size(const std::string& path)
    {
        struct stat buf;
        int ret = stat(path.c_str(), &buf);
        int64 filesize = 0;
        if (0 == ret)
        {
            if (S_ISREG(buf.st_mode))
            {
                return buf.st_size;
            }
            else if (S_ISDIR(buf.st_mode))
            {
                DIR* dir = opendir(path.c_str());
                if (NULL != dir)
                {
                    struct dirent * ptr;
                    while ((ptr = readdir(dir)) != NULL)
                    {
                        if (!strcmp(ptr->d_name, ".") || !strcmp(ptr->d_name, ".."))
                        {
                            continue;
                        }
                        std::string file_path = path;
                        file_path.append("/").append(ptr->d_name);
                        filesize += file_size(file_path);
                    }
                    closedir(dir);
                }
            }
        }
        return filesize;
    }

    int sha1sum_file(const std::string& file, std::string& hash)
    {
        FILE *fp;
        if ((fp = fopen(file.c_str(), "rb")) == NULL)
        {
            return -1;
        }
        SHA1_CTX ctx;
        SHA1Init(&ctx);
        const uint32 buf_size = 65536;
        unsigned char buf[buf_size];
        while (1)
        {
            int ret = fread(buf, 1, buf_size, fp);
            if (ret > 0)
            {
                SHA1Update(&ctx, buf, ret);
            }
            if (ret < (int) buf_size)
            {
                break;
            }
        }
        fclose(fp);
        unsigned char hashstr[20];
        SHA1Final(hashstr, &ctx);

        char result[256];
        uint32 offset = 0;
        for (uint32 i = 0; i < 20; i++)
        {
            int ret = sprintf(result + offset, "%02x", hashstr[i]);
            offset += ret;
        }
        hash = result;
        return 0;
    }

    int real_path(const std::string& path, std::string& real_path)
    {
        char* tmp = realpath(path.c_str(), NULL);
        if(NULL != tmp)
        {
            real_path.assign(tmp);
            free(tmp);
            return 0;
        }
        return errno;
    }

    bool is_valid_fd(int fd)
    {
        return fcntl(fd, F_GETFD) != -1 || errno != EBADF;
    }
}
