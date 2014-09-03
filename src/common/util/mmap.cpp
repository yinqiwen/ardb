/*
 * mmap.cpp
 *
 *  Created on: Author: wqy
 */

#include "mmap.hpp"
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>

namespace ardb
{
    int MMapBuf::Init(const std::string& path, uint64 size, int advice_flag)
    {
        int fd = open(path.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
        if (fd < 0)
        {
            const char* err = strerror(errno);
            ERROR_LOG("Failed to open replication state file:%s for reason:%s", path.c_str(), err);
            return -1;
        }
        if (-1 == ftruncate(fd, size))
        {
            const char* err = strerror(errno);
            ERROR_LOG("Failed to truncate mmap file:%s for reason:%s", path.c_str(), err);
            close(fd);
            return false;
        }
        char* mbuf = (char*) mmap(NULL, size,
        PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        close(fd);
        if (mbuf == MAP_FAILED)
        {
            const char* err = strerror(errno);
            ERROR_LOG("Failed to mmap file:%s for reason:%s", path.c_str(), err);
            return -1;
        }
        else
        {
            m_buf = mbuf;
            m_size = size;
            madvise(m_buf, m_size, advice_flag);
        }
        return 0;
    }

    MMapBuf::~MMapBuf()
    {
        if (NULL != m_buf)
        {
            munmap(m_buf, m_size);
        }
    }

}

