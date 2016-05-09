/*
 *Copyright (c) 2013-2016, yinqiwen <yinqiwen@gmail.com>
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
#include <snappy-c.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <wiredtiger.h>
#include "db/engine.hpp"

/* Local compressor structure. */
typedef struct
{
        WT_COMPRESSOR compressor; /* Must come first */

        WT_EXTENSION_API *wt_api; /* Extension API */
} SNAPPY_COMPRESSOR;

/*
 * wt_snappy_error --
 *  Output an error message, and return a standard error code.
 */
static int wt_snappy_error(WT_COMPRESSOR *compressor, WT_SESSION *session, const char *call, snappy_status snret)
{
    WT_EXTENSION_API *wt_api;
    const char *msg;

    wt_api = ((SNAPPY_COMPRESSOR *) compressor)->wt_api;

    switch (snret)
    {
        case SNAPPY_BUFFER_TOO_SMALL:
            msg = "SNAPPY_BUFFER_TOO_SMALL";
            break;
        case SNAPPY_INVALID_INPUT:
            msg = "SNAPPY_INVALID_INPUT";
            break;
        default:
            msg = "unknown error";
            break;
    }

    //(void) wt_api->err_printf(wt_api, session, "snappy error: %s: %s: %d", call, msg, snret);
    return (WT_ERROR);
}

/*
 * wt_snappy_compress --
 *  WiredTiger snappy compression.
 */
static int wt_snappy_compress(WT_COMPRESSOR *compressor, WT_SESSION *session, uint8_t *src, size_t src_len, uint8_t *dst, size_t dst_len, size_t *result_lenp,
        int *compression_failed)
{
    snappy_status snret;
    size_t snaplen;
    char *snapbuf;

    /*
     * dst_len was computed in wt_snappy_pre_size, so we know it's big
     * enough.  Skip past the space we'll use to store the final count
     * of compressed bytes.
     */
    snaplen = dst_len - sizeof(size_t);
    snapbuf = (char *) dst + sizeof(size_t);

    /* snaplen is an input and an output arg. */
    snret = snappy_compress((char *) src, src_len, snapbuf, &snaplen);

    if (snret == SNAPPY_OK)
    {
        /*
         * On decompression, snappy requires the exact compressed byte
         * count (the current value of snaplen).  WiredTiger does not
         * preserve that value, so save snaplen at the beginning of the
         * destination buffer.
         */
        if (snaplen + sizeof(size_t) < src_len)
        {
            *(size_t *) dst = snaplen;
            *result_lenp = snaplen + sizeof(size_t);
            *compression_failed = 0;
        }
        else
            /* The compressor failed to produce a smaller result. */
            *compression_failed = 1;
        return (0);
    }
    return (wt_snappy_error(compressor, session, "snappy_compress", snret));
}

/*
 * wt_snappy_decompress --
 *  WiredTiger snappy decompression.
 */
static int wt_snappy_decompress(WT_COMPRESSOR *compressor, WT_SESSION *session, uint8_t *src, size_t src_len, uint8_t *dst, size_t dst_len, size_t *result_lenp)
{
    WT_EXTENSION_API *wt_api;
    snappy_status snret;
    size_t snaplen;

    wt_api = ((SNAPPY_COMPRESSOR *) compressor)->wt_api;

    /* retrieve the saved length */
    snaplen = *(size_t *) src;
    if (snaplen + sizeof(size_t) > src_len)
    {
        //(void) wt_api->err_printf(wt_api, session, "wt_snappy_decompress: stored size exceeds buffer size");
        return (WT_ERROR);
    }

    /* dst_len is an input and an output arg. */
    snret = snappy_uncompress((char *) src + sizeof(size_t), snaplen, (char *) dst, &dst_len);

    if (snret == SNAPPY_OK)
    {
        *result_lenp = dst_len;
        return (0);
    }

    return (wt_snappy_error(compressor, session, "snappy_decompress", snret));
}

/*
 * wt_snappy_pre_size --
 *  WiredTiger snappy destination buffer sizing.
 */
static int wt_snappy_pre_size(WT_COMPRESSOR *compressor, WT_SESSION *session, uint8_t *src, size_t src_len, size_t *result_lenp)
{
    (void) compressor; /* Unused parameters */
    (void) session;
    (void) src;

    /*
     * Snappy requires the dest buffer be somewhat larger than the source.
     * Fortunately, this is fast to compute, and will give us a dest buffer
     * in wt_snappy_compress that we can compress to directly.  We add space
     * in the dest buffer to store the accurate compressed size.
     */
    *result_lenp = snappy_max_compressed_length(src_len) + sizeof(size_t);
    return (0);
}

/*
 * wt_snappy_terminate --
 *  WiredTiger snappy compression termination.
 */
static int wt_snappy_terminate(WT_COMPRESSOR *compressor, WT_SESSION *session)
{
    (void) session; /* Unused parameters */

    free(compressor);
    return (0);
}

/*
 * snappy_extension_init --
 *  WiredTiger snappy compression extension - called directly when
 *  Snappy support is built in, or via wiredtiger_extension_init when
 *  snappy support is included via extension loading.
 */
static int snappy_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config)
{
    SNAPPY_COMPRESSOR *snappy_compressor;

    (void) config; /* Unused parameters */

    if ((snappy_compressor = (SNAPPY_COMPRESSOR *)calloc(1, sizeof(SNAPPY_COMPRESSOR))) == NULL)
        return (errno);

    snappy_compressor->compressor.compress = wt_snappy_compress;
    snappy_compressor->compressor.compress_raw = NULL;
    snappy_compressor->compressor.decompress = wt_snappy_decompress;
    snappy_compressor->compressor.pre_size = wt_snappy_pre_size;
    snappy_compressor->compressor.terminate = wt_snappy_terminate;

    snappy_compressor->wt_api = connection->get_extension_api(connection);

    return (connection->add_compressor(connection, "snappy", (WT_COMPRESSOR *) snappy_compressor, NULL));
}
static int __ardb_compare_keys(WT_COLLATOR *collator, WT_SESSION *session, const WT_ITEM *v1, const WT_ITEM *v2, int *cmp)
{
    const char *s1 = (const char *) v1->data;
    const char *s2 = (const char *) v2->data;

    (void) session; /* unused */
    (void) collator; /* unused */

    *cmp = compare_keys(s1, v1->size, s2, v2->size, false);
    return (0);
}
static WT_COLLATOR ardb_comparator =
{ __ardb_compare_keys, NULL, NULL };
extern "C"
{
    int ardb_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config)
    {
        snappy_extension_init(connection, config);
        return connection->add_collator(connection, "ardb_comparator", &ardb_comparator, NULL);
    }
}

