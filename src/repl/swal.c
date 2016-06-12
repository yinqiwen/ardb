/*
 *Copyright (c) 2015-2015, yinqiwen <yinqiwen@gmail.com>
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
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stddef.h>
#include <stdlib.h>
#include "swal.h"

#define SWAL_META_SIZE 1024

typedef struct swal_meta_t
{
    size_t log_start_offset;
    size_t log_end_offset;
    size_t log_file_pos;
    uint64_t cksm;
} swal_meta_t;

struct swal_t
{
    char* dir;
    swal_meta_t* meta;
    swal_options_t options;
    int fd;
    void* mmap_buf;
    char* ring_cache;
    size_t ring_cache_start_offset;
    //size_t ring_cache_end_offset;
    size_t ring_cache_idx;
    time_t last_replay_time;
};

swal_options_t* swal_options_create()
{
    swal_options_t* options = (swal_options_t*) malloc(sizeof(swal_options_t));
    memset(options, 0, sizeof(swal_options_t));
    options->create_ifnotexist = 1;
    options->max_file_size = 1 * 1024 * 1024 * 1024LL;  //default 1G
    options->user_meta_size = 0;
    options->ring_cache_size = 0;
    return options;
}
void swal_options_destroy(swal_options_t* options)
{
    if (NULL != options)
    {
        free(options);
    }
}

static int open_wal_logfile(swal_t* wal)
{
    if (wal->fd > 0)
    {
        return 0;
    }
    char file_path[strlen(wal->dir) + 1024];
    if (NULL != wal->options.log_prefix)
    {
        sprintf(file_path, "%s/%s.swal.log", wal->dir, wal->options.log_prefix);
    }
    else
    {
        sprintf(file_path, "%s/swal.log", wal->dir);
    }

    int mode = O_CREAT | O_RDWR, permission = S_IRUSR | S_IWUSR;
    int fd = open(file_path, mode, permission);
    if (fd < 0)
    {
        return SWAL_ERR_LOG_OPEN_FAIL;
    }
    struct stat file_st;
    memset(&file_st, 0, sizeof(file_st));
    fstat(fd, &file_st);
    if (file_st.st_size < wal->options.max_file_size)
    {
        if (-1 == ftruncate(fd, wal->options.max_file_size))
        {
            close(fd);
            return SWAL_ERR_LOG_OPEN_FAIL;
        }
    }
    lseek(fd, wal->meta->log_file_pos, SEEK_SET);
    wal->fd = fd;
    return 0;
}

int swal_open(const char* dir, const swal_options_t* options, swal_t** wal)
{
    swal_t* wal_log = (swal_t*) malloc(sizeof(swal_t));
    if (NULL == wal_log)
    {
        return SWAL_ERR_MALLOC_FAIL;
    }
    memset(wal_log, 0, sizeof(swal_t));
    wal_log->fd = -1;

    wal_log->dir = strdup(dir);
    if (NULL == wal_log->dir)
    {
        swal_close(wal_log);
        return SWAL_ERR_MALLOC_FAIL;
    }

    /*
     * open/create meta file
     */
    char meta_path[strlen(dir) + 1024];
    if (NULL != options->log_prefix)
    {
        sprintf(meta_path, "%s/%s.swal.meta", dir, options->log_prefix);
    }
    else
    {
        sprintf(meta_path, "%s/swal.meta", dir);
    }

    int mode = O_RDWR, permission = S_IRUSR | S_IWUSR;
    int mmap_mode = PROT_READ | PROT_WRITE;
    if (options->create_ifnotexist)
    {
        mode |= O_CREAT;
    }
    int meta_fd = open(meta_path, mode, permission);
    if (meta_fd < 0)
    {
        swal_close(wal_log);
        return SWAL_ERR_META_OPEN_FAIL;
    }
    if (-1 == ftruncate(meta_fd, options->user_meta_size + SWAL_META_SIZE))
    {
        swal_close(wal_log);
        return SWAL_ERR_META_OPEN_FAIL;
    }
    swal_meta_t* meta = (swal_meta_t*) mmap(NULL, options->user_meta_size + SWAL_META_SIZE, mmap_mode, MAP_SHARED, meta_fd, 0);
    wal_log->meta = meta;
    wal_log->options = *options;
    int err = open_wal_logfile(wal_log);
    if (0 != err)
    {
        swal_close(wal_log);
        return err;
    }
    if (options->ring_cache_size > 0)
    {
        wal_log->ring_cache = (char*) malloc(options->ring_cache_size);
        if (NULL == wal_log->ring_cache)
        {
            swal_close(wal_log);
            return SWAL_ERR_MALLOC_FAIL;
        }
        wal_log->ring_cache_idx = 0;
        wal_log->ring_cache_start_offset = meta->log_end_offset;
    }
    *wal = wal_log;
    return 0;
}

void* swal_user_meta(swal_t* wal)
{
    if (NULL == wal)
    {
        return -1;
    }
    char* meta = (char*) wal->meta;
    return meta + SWAL_META_SIZE;
}

int swal_append(swal_t* wal, const void* log, size_t loglen)
{
    if (NULL == wal)
    {
        return -1;
    }
    const char* p = (const char*) log;
    size_t write_len = loglen;
    while (write_len)
    {
        size_t thislen = wal->options.max_file_size - wal->meta->log_file_pos;
        if (thislen > write_len)
            thislen = write_len;
        int err = write(wal->fd, p, thislen);
        if (err < 0)
        {
            return err;
        }
        wal->meta->log_file_pos += thislen;
        if (wal->meta->log_file_pos == wal->options.max_file_size)
        {
            wal->meta->log_file_pos = 0;
            lseek(wal->fd, 0, SEEK_SET);
        }
        write_len -= thislen;
        p += thislen;
    }
    wal->meta->log_end_offset += loglen;
    if (wal->meta->log_end_offset - wal->meta->log_start_offset >= (wal->options.max_file_size))
    {
        wal->meta->log_start_offset = wal->meta->log_end_offset - wal->options.max_file_size;
    }
    if (NULL != wal->options.cksm_func)
    {
        wal->meta->cksm = wal->options.cksm_func(wal->meta->cksm, (const unsigned char*) log, loglen);
    }
    if (NULL != wal->ring_cache)
    {
        size_t len = loglen;
        p = (const char*) log;
        while (len)
        {
            size_t thislen = wal->options.ring_cache_size - wal->ring_cache_idx;
            if (thislen > len)
                thislen = len;
            memcpy(wal->ring_cache + wal->ring_cache_idx, p, thislen);
            wal->ring_cache_idx += thislen;
            if (wal->ring_cache_idx == wal->options.ring_cache_size)
            {
                wal->ring_cache_idx = 0;
            }
            len -= thislen;
            p += thislen;
        }
        //wal->ring_cache_end_offset = wal->meta->log_end_offset;
        if (wal->meta->log_end_offset - wal->ring_cache_start_offset >= (wal->options.ring_cache_size))
        {
            wal->ring_cache_start_offset = wal->meta->log_end_offset - wal->options.ring_cache_size;
        }
    }
    return 0;
}
int swal_sync(swal_t* wal)
{
    if (NULL == wal)
    {
        return -1;
    }
    fdatasync(wal->fd);
    return 0;
}
int swal_sync_meta(swal_t* wal)
{
    if (NULL == wal)
    {
        return -1;
    }
    msync(wal->meta, wal->options.user_meta_size + SWAL_META_SIZE, 0);
    return 0;
}

int swal_replay(swal_t* wal, size_t offset, int64_t limit_len, swal_replay_logfunc func, void* data)
{
    if(offset < wal->meta->log_start_offset || offset > wal->meta->log_end_offset)
    {
        return -1;
    }

    size_t total = wal->meta->log_end_offset - offset;
    wal->last_replay_time = time(NULL);
    if (limit_len > 0 && limit_len < total)
    {
        total = limit_len;
    }
    size_t data_len = wal->meta->log_end_offset - offset;
    if (NULL != wal->ring_cache)
    {
        if (offset >= wal->ring_cache_start_offset)
        {
            size_t cache_end_idx = wal->ring_cache_idx;
            if (wal->ring_cache_idx >= data_len)
            {
                func(wal->ring_cache + wal->ring_cache_idx - data_len, total, data);
            }
            else
            {
                size_t start_pos = wal->options.ring_cache_size - data_len + wal->ring_cache_idx;
                if (total <= wal->options.ring_cache_size - start_pos)
                {
                    func(wal->ring_cache + start_pos, total, data);
                }
                else
                {
                    size_t tlen = wal->options.ring_cache_size - start_pos;
                    size_t hlen = start_pos + total - wal->options.ring_cache_size;
                    size_t consumed = func(wal->ring_cache + start_pos, tlen, data);
                    if(tlen == consumed)
                    {
                        func(wal->ring_cache, hlen, data);
                    }
                    else
                    {
                        char* tmp = (char*)malloc(tlen - consumed + hlen);
                        memcpy(tmp, wal->ring_cache + start_pos + consumed, tlen - consumed);
                        memcpy(tmp + tlen - consumed, wal->ring_cache, hlen);
                        func(tmp, tlen - consumed + hlen, data);
                        free(tmp);
                    }
                }
            }
            return 0;
        }
    }
    if (NULL == wal->mmap_buf)
    {
        wal->mmap_buf = (char*) mmap(NULL, wal->options.max_file_size, PROT_READ, MAP_SHARED, wal->fd, 0);
    }
    size_t start_pos = wal->meta->log_file_pos;
    if(wal->meta->log_file_pos >= data_len)
    {
        start_pos = wal->meta->log_file_pos - data_len;
    }
    else
    {
        start_pos = wal->options.max_file_size - data_len + wal->meta->log_file_pos;
    }
    if ((start_pos + total) < wal->options.max_file_size)
    {
        func(wal->mmap_buf + start_pos, total, data);
    }
    else
    {
        size_t tlen = wal->options.max_file_size - start_pos;
        size_t consumed = func(wal->mmap_buf + start_pos, tlen, data);
        size_t hlen = total + start_pos - wal->options.max_file_size;
        if(consumed == tlen)
        {
            func(wal->mmap_buf, hlen, data);
        }
        else
        {
            char* tmp = (char*)malloc(tlen - consumed + hlen);
            memcpy(tmp, wal->mmap_buf + start_pos + consumed, tlen - consumed);
            memcpy(tmp + tlen - consumed, wal->mmap_buf, hlen);
            func(tmp, tlen - consumed + hlen, data);
            free(tmp);
        }
    }
    return 0;
}
int swal_clear_replay_cache(swal_t* wal)
{
    if (NULL != wal->mmap_buf)
    {
        munmap(wal->mmap_buf, wal->options.max_file_size);
        wal->mmap_buf = NULL;
        return 1;
    }
    return 0;
}
int swal_reset(swal_t* wal, size_t offset, uint64_t cksm)
{
    swal_clear_replay_cache(wal);
    wal->meta->log_start_offset = offset;
    wal->meta->log_end_offset = offset;
    wal->meta->log_file_pos = 0;
    wal->meta->cksm = cksm;
    if (NULL != wal->ring_cache)
    {
        wal->ring_cache_start_offset = offset;
        wal->ring_cache_idx = 0;
    }
    if (-1 != wal->fd)
    {
        lseek(wal->fd, wal->meta->log_file_pos, SEEK_SET);
    }
    return 0;
}
uint64_t swal_cksm(swal_t* wal)
{
    return wal->meta->cksm;
}
size_t swal_start_offset(swal_t* wal)
{
    return wal->meta->log_start_offset;
}
size_t swal_end_offset(swal_t* wal)
{
    return wal->meta->log_end_offset;
}
int swal_close(swal_t* wal)
{
    if (NULL == wal)
    {
        return -1;
    }
    if (-1 != wal->fd)
    {
        close(wal->fd);
        wal->fd = -1;
    }
    if (NULL != wal->ring_cache)
    {
        free(wal->ring_cache);
    }
    if (NULL != wal->meta)
    {
        munmap(wal->meta, wal->options.user_meta_size + SWAL_META_SIZE);
    }
    swal_clear_replay_cache(wal);
    free(wal->dir);
    free(wal);
    return 0;
}

int swal_dump_ring_cache(swal_t* wal, const char* file)
{
    if(NULL == wal->ring_cache)
    {
        return -1;
    }
    int mode = O_CREAT | O_RDWR, permission = S_IRUSR | S_IWUSR;
    int fd = open(file, mode, permission);
    if(-1 == fd)
    {
        return -1;
    }
    write(fd, wal->ring_cache, wal->options.ring_cache_size);
    close(fd);
    return 0;
}

