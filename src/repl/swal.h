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

#ifndef _SWAL_H_
#define _SWAL_H_

#include <stdint.h>
#include <stdio.h>

#define SWAL_ERR_MALLOC_FAIL  -100
#define SWAL_ERR_META_OPEN_FAIL  -101
#define SWAL_ERR_LOG_OPEN_FAIL  -102
#define SWAL_ERR_MISMATCH_LOG_SIZE  -103
#define SWAL_ERR_INVALID_OFFSET  -104


#if defined(__cplusplus)
extern "C"
{
#endif
    typedef uint64_t swal_cksm_func(uint64_t cksm, const unsigned char* log, uint64_t len);
    typedef struct swal_options_t
    {
            int create_ifnotexist;
            size_t max_file_size;
            size_t user_meta_size;
            size_t ring_cache_size;
            swal_cksm_func* cksm_func;
            const char* log_prefix;
    } swal_options_t;
    swal_options_t* swal_options_create();
    void swal_options_destroy(swal_options_t* options);

    typedef struct swal_t swal_t;
    int swal_open(const char* dir, const swal_options_t* options, swal_t** wal);
    void* swal_user_meta(swal_t* wal);
    int swal_append(swal_t* wal, const void* log, size_t loglen);
    int swal_sync(swal_t* wal);
    int swal_sync_meta(swal_t* wal);

    typedef size_t swal_replay_logfunc(const void* log, size_t loglen, void* data);
    int swal_replay(swal_t* wal, size_t offset, int64_t limit_len, swal_replay_logfunc func, void* data);
    int swal_clear_replay_cache(swal_t* wal);
    int swal_reset(swal_t* wal, size_t offset, uint64_t cksm);
    uint64_t swal_cksm(swal_t* wal);
    size_t swal_start_offset(swal_t* wal);
    size_t swal_end_offset(swal_t* wal);
    int swal_close(swal_t* wal);

    int swal_dump_ring_cache(swal_t* wal, const char* file);

#if defined(__cplusplus)
}
#endif
#endif /* _SWAL_H_ */
