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

#include "buffer.hpp"
#include <sys/uio.h>
#include <errno.h>
#include <string.h>
using namespace ardb;

int Buffer::VPrintf(const char *fmt, va_list ap)
{
    char *buffer;
    size_t space;
    int sz, result = -1;
    va_list aq;

    /* make sure that at least some space is available */
    if (!EnsureWritableBytes(64))
        goto done;

    for (;;)
    {
        buffer = m_buffer + m_write_idx;
        space = WriteableBytes();

#ifndef va_copy
#define	va_copy(dst, src)	memcpy(&(dst), &(src), sizeof(va_list))
#endif
        va_copy(aq, ap);

        sz = vsnprintf(buffer, space, fmt, aq);

        va_end(aq);

        if (sz < 0)
            goto done;
        if ((size_t) sz < space)
        {
            AdvanceWriteIndex(sz);
            result = sz;
            goto done;
        }
        if (!EnsureWritableBytes(sz << 1))
            goto done;
    }
    /* NOTREACHED */

    done: ;
    return result;
}

int Buffer::PrintString(const std::string& str)
{
    const char* p = str.data();
    size_t len = str.size();
    Write("\"", 1);
    while (len--)
    {
        switch (*p)
        {
            case '\\':
            case '"':
                Printf("\\%c", *p);
                break;
            case '\n':
                Write("\\n", 2);
                break;
            case '\r':
                Write("\\r", 2);
                break;
            case '\t':
                Write("\\t", 2);
                break;
            case '\a':
                Write("\\a", 2);
                break;
            case '\b':
                Write("\\b", 2);
                break;
            default:
                if (isprint(*p))
                    Printf("%c", *p);
                else
                    Printf("\\x%02x", (unsigned char) *p);
                break;
        }
        p++;
    }
    Write("\"", 1);
    return (int) ReadableBytes();
}

int Buffer::Printf(const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    int ret = VPrintf(fmt, args);
    va_end(args);
    return ret;
}

int Buffer::WriteFD(int fd, int& err)
{
    int n = -1;
    int readable = ReadableBytes();
    if (readable == 0)
    {
        return 0;
    }
    n = ::write(fd, m_buffer + m_read_idx, readable);
    if (n < 0)
    {
        err = errno;
    }
    else
    {
        m_read_idx += n;
    }
    return (n);
}

int Buffer::ReadFD(int fd, int& err)
{
    char extrabuf[65536];
    struct iovec vec[2];
    size_t writable = WriteableBytes();
    vec[0].iov_base = m_buffer + m_write_idx;
    vec[0].iov_len = writable;
    vec[1].iov_base = extrabuf;
    vec[1].iov_len = sizeof(extrabuf);
    int n = readv(fd, vec, writable > sizeof(extrabuf) ? 1 : 2);
    if (n < 0)
    {
        err = errno;
    }
    else if ((size_t) n <= writable)
    {
        m_write_idx += n;
    }
    else
    {
        m_write_idx = m_buffer_len;
        Write(extrabuf, n - writable);
    }
    return n;
}

int Buffer::IndexOf(const void* data, size_t len, size_t start, size_t end)
{
    if (NULL == data || len == 0)
    {
        return -1;
    }
    if (start + len > end || end > m_write_idx)
    {
        return -1;
    }

    char first = ((const char*) data)[0];
    size_t start_idx = start;
    while (start_idx < end)
    {
        char *start_at = m_buffer + start_idx;
        char* p = (char *) memchr(start_at, first, end - start_idx);
        if (p && (p + len - 1) < (m_buffer + end))
        {
            //start_idx.pos += p - start_at;
            start_idx += (p - start_at);
            if (len == 1)
            {
                return start_idx;
            }
            if (0 == memcmp(p, (const char*) data, len))
            {
                if (start_idx + len > end)
                {
                    return -1;
                }
                else
                {
                    return start_idx;
                }

            }
            start_idx += len;
        }
        else
        {
            return -1;
        }
    }
    return -1;
}

int Buffer::IndexOf(const void* data, size_t len)
{
    return IndexOf(data, len, m_read_idx, m_write_idx);
}

