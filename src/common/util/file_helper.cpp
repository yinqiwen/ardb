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
#include <limits.h>
#include "sha1.h"

namespace ardb
{
    bool is_file_exist(const std::string& path)
    {
        struct stat buf;
        int ret = stat(path.c_str(), &buf);
        if (0 == ret)
        {
            return S_ISREG(buf.st_mode) || S_ISDIR(buf.st_mode);
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
        int fd = -1;
        if (found != std::string::npos)
        {
            std::string base_dir = path.substr(0, found);
            if (make_dir(base_dir))
            {
                //mode is 0755
                fd = open(path.c_str(), O_CREAT,
                S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
            }
        }
        else
        {
            fd = open(path.c_str(), O_CREAT,
            S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
        }
        if (fd != -1)
        {
            close(fd);
        }
        return fd == -1;
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

    int file_append_content(const std::string& path, const std::string& content)
    {
        make_file(path);
        FILE *fp;
        if ((fp = fopen(path.c_str(), "ab")) == NULL)
        {
            return -1;
        }
        fwrite(content.c_str(), content.size(), 1, fp);
        fclose(fp);
        return 0;
    }

    int file_read_full(const std::string& path, std::string& content)
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
        content.assign(buffer, len);
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

    static int recursive_list_allfiles(const std::string& path, const std::string& base, std::deque<std::string>& fs)
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
                        ret = stat(file_path.c_str(), &buf);
                        if (ret == 0)
                        {
                            if (S_ISREG(buf.st_mode))
                            {
                                fs.push_back(base + ptr->d_name);
                            }
                            else if (S_ISDIR(buf.st_mode))
                            {
                                recursive_list_allfiles(file_path, base + ptr->d_name + "/", fs);
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

    int list_allfiles(const std::string& path, std::deque<std::string>& fs)
    {
        return recursive_list_allfiles(path, "", fs);;
    }

    int list_subfiles(const std::string& path, std::deque<std::string>& fs, bool include_dir)
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
                        ret = stat(file_path.c_str(), &buf);
                        if (ret == 0)
                        {
                            if (S_ISREG(buf.st_mode) || (include_dir && S_ISDIR(buf.st_mode)))
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
        char buf[PATH_MAX + 1];
        char* tmp = realpath(path.c_str(), buf);
        if (NULL != tmp)
        {
            real_path.assign(tmp);
            //free(tmp);
            return 0;
        }
        return errno;
    }

    bool is_valid_fd(int fd)
    {
        return fcntl(fd, F_GETFD) != -1 || errno != EBADF;
    }

    int file_copy(const std::string& src, const std::string& dst)
    {
        int fd_to, fd_from;
        char buf[4096];
        ssize_t nread;
        int saved_errno;

        fd_from = open(src.c_str(), O_RDONLY);
        if (fd_from < 0) return -1;

        remove(dst.c_str());
        fd_to = open(dst.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0666);
        if (fd_to < 0)
        {
            goto out_error;
        }

        while (nread = read(fd_from, buf, sizeof buf), nread > 0)
        {
            char *out_ptr = buf;
            ssize_t nwritten;

            do
            {
                nwritten = write(fd_to, out_ptr, nread);

                if (nwritten >= 0)
                {
                    nread -= nwritten;
                    out_ptr += nwritten;
                }
                else if (errno != EINTR)
                {
                    goto out_error;
                }
            }
            while (nread > 0);
        }

        if (nread == 0)
        {
            if (close(fd_to) < 0)
            {
                fd_to = -1;
                goto out_error;
            }
            close(fd_from);

            /* Success! */
            return 0;
        }

        out_error: saved_errno = errno;

        close(fd_from);
        if (fd_to >= 0) close(fd_to);

        errno = saved_errno;
        return -1;
    }

    int dir_copy(const std::string& src, const std::string& dst)
    {
        std::deque<std::string> fs;
        list_allfiles(src, fs);
        for (size_t i = 0; i < fs.size(); i++)
        {
            make_file(dst + "/" + fs[i]);
            std::string src_file = src + "/" + fs[i];
            std::string dst_file = dst + "/" + fs[i];
            if (0 != file_copy(src_file, dst_file))
            {
                //printf("#####Failed to copy %s to %s\n", src_file.c_str(), dst_file.c_str());
                return -1;
            }
        }
        return 0;
    }

    int file_del(const std::string& path)
    {
        if (is_dir_exist(path))
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
                    struct stat buf;
                    memset(&buf, 0, sizeof(buf));
                    int ret = stat(file_path.c_str(), &buf);
                    if (ret == 0)
                    {
                        if (S_ISREG(buf.st_mode))
                        {
                            ret = unlink(file_path.c_str());
                        }
                        else if (S_ISDIR(buf.st_mode))
                        {
                            ret = file_del(file_path);
                        }
                        if (0 != ret)
                        {
                            return ret;
                        }
                    }
                }
                closedir(dir);
                return remove(path.c_str());
            }
            return 0;
        }
        else
        {
            return unlink(path.c_str());
        }
    }
}
