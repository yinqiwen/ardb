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

#ifndef BUFFER__HPP_
#define BUFFER__HPP_
#include <sys/types.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <string>

namespace ardb
{
	/**
	 * A dynamic buffer for read/write.
	 *
	 *       +-------------------+------------------+------------------+
	 *       | readed bytes      |  readable bytes  |  writable bytes  |
	 *       +-------------------+------------------+------------------+
	 *       |                   |                  |                  |
	 *       0      <=      readerIndex   <=   writerIndex    <=    capacity
	 *
	 */
	class Buffer
	{
		private:
			char *m_buffer; //raw buffer
			/** total allocation available in the buffer field. */
			size_t m_buffer_len; //raw buffer length

			size_t m_write_idx;
			size_t m_read_idx;
			bool m_in_heap;
		public:
			static const int BUFFER_MAX_READ = 8192;
			static const size_t DEFAULT_BUFFER_SIZE = 32;
			inline Buffer() :
					m_buffer(0), m_buffer_len(0), m_write_idx(0), m_read_idx(0), m_in_heap(
							true)
			{
			}
			inline Buffer(char* value, int off, int len) :
					m_buffer(value), m_buffer_len(len), m_write_idx(len), m_read_idx(
							off), m_in_heap(false)
			{
			}
			inline Buffer(size_t size) :
					m_buffer(0), m_buffer_len(0), m_write_idx(0), m_read_idx(0), m_in_heap(
							true)
			{
				EnsureWritableBytes(size);
			}
            inline bool WrapReadableContent(const void* data, size_t len)
            {
                Clear();
                if (m_in_heap && m_buffer_len > 0)
                {
                    return false;
                }
                m_in_heap = false;
                m_buffer = (char*) data;
                m_buffer_len = len;
                m_read_idx = 0;
                m_write_idx = len;
                return true;
            }
			inline size_t GetReadIndex() const
			{
				return m_read_idx;
			}
			inline size_t GetWriteIndex() const
			{
				return m_write_idx;
			}
			inline void SetReadIndex(size_t idx)
			{
				m_read_idx = idx;
			}
			inline void AdvanceReadIndex(int step)
			{
				m_read_idx += step;
			}
			inline void SetWriteIndex(size_t idx)
			{
				m_write_idx = idx;
			}
			inline void AdvanceWriteIndex(int step)
			{
				m_write_idx += step;
			}
			inline bool Readable() const
			{
				return m_write_idx > m_read_idx;
			}
			inline bool Writeable() const
			{
				return m_buffer_len > m_write_idx;
			}
			inline size_t ReadableBytes() const
			{
				return Readable() ? m_write_idx - m_read_idx : 0;
			}
			inline size_t WriteableBytes() const
			{
				return Writeable() ? m_buffer_len - m_write_idx : 0;
			}

			inline size_t Compact(size_t leastLength)
			{
				if (!Readable())
				{
					Clear();
				}
				uint32_t writableBytes = WriteableBytes();
				if (writableBytes < leastLength)
				{
					return 0;
				}
				uint32_t readableBytes = ReadableBytes();
				uint32_t total = Capacity();
				char* newSpace = NULL;
				if (readableBytes > 0)
				{
					newSpace = (char*) malloc(readableBytes);
					if (NULL == newSpace)
					{
						return 0;
					}
					memcpy(newSpace, m_buffer + m_read_idx, readableBytes);
				} else
				{
					return 0;
				}
				if (NULL != m_buffer && m_in_heap)
				{
					free(m_buffer);
				}
				m_read_idx = 0;
				m_write_idx = readableBytes;
				m_buffer_len = readableBytes;
				m_buffer = newSpace;
				m_in_heap = true;
				return total - readableBytes;
			}

			inline bool EnsureWritableBytes(size_t minWritableBytes,
					bool growzero = false)
			{
				if (WriteableBytes() >= minWritableBytes)
				{
					return true;
				} else
				{
					size_t newCapacity = Capacity();
					if (0 == newCapacity)
					{
						newCapacity = DEFAULT_BUFFER_SIZE;
					}
					size_t minNewCapacity = GetWriteIndex() + minWritableBytes;
					while (newCapacity < minNewCapacity)
					{
						newCapacity <<= 1;
					}
					char* tmp = NULL;

					//tmp = (char*) realloc(m_buffer, newCapacity);
					tmp = (char*) malloc(newCapacity);
					if (NULL != tmp)
					{
						if (growzero)
						{
							memset(tmp, 0, newCapacity);
						}
						if (NULL != m_buffer)
						{
							memcpy(tmp, m_buffer, Capacity());
						}
						if (m_in_heap && NULL != m_buffer)
						{
							free(m_buffer);
						}
						m_in_heap = true;
						m_buffer = tmp;
						m_buffer_len = newCapacity;
						return true;
					}
					return false;
				}
			}

			inline bool Reserve(size_t len)
			{
				return EnsureWritableBytes(len);
			}
			inline const char* GetRawBuffer() const
			{
				return m_buffer;
			}
			inline const char* GetRawWriteBuffer() const
			{
				return m_buffer + m_write_idx;
			}
			inline const char* GetRawReadBuffer() const
			{
				return m_buffer + m_read_idx;
			}
			inline size_t Capacity() const
			{
				return m_buffer_len;
			}
			inline void Limit()
			{
				m_buffer_len = m_write_idx;
			}
			inline void Clear()
			{
				m_write_idx = m_read_idx = 0;
			}
			inline int Read(void *data_out, size_t datlen)
			{
				if (datlen > ReadableBytes())
				{
					return -1;
				}
				memcpy(data_out, m_buffer + m_read_idx, datlen);
				m_read_idx += datlen;
				return datlen;
			}
			inline int Write(const void *data_in, size_t datlen)
			{
				if (!EnsureWritableBytes(datlen))
				{
					return -1;
				}
				memcpy(m_buffer + m_write_idx, data_in, datlen);
				m_write_idx += datlen;
				return datlen;
			}

			inline int Write(Buffer* unit, size_t datlen)
			{
				if (NULL == unit)
				{
					return -1;
				}
				if (datlen > unit->ReadableBytes())
				{
					datlen = unit->ReadableBytes();
				}
				int ret = Write(unit->m_buffer + unit->m_read_idx, datlen);
				if (ret > 0)
				{
					unit->m_read_idx += ret;
				}
				return ret;
			}

			inline int WriteByte(char ch)
			{
				return Write(&ch, 1);
			}

			inline int Read(Buffer* unit, size_t datlen)
			{
				if (NULL == unit)
				{
					return -1;
				}
				return unit->Write(this, datlen);
			}

			inline int SetBytes(void* data, size_t datlen, size_t index)
			{
				if (NULL == data)
				{
					return -1;
				}
				if (index + datlen > m_write_idx)
				{
					return -1;
				}
				memcpy(m_buffer + index, data, datlen);
				return datlen;
			}

			inline bool ReadByte(char& ch)
			{
				return Read(&ch, 1) == 1;
			}

			inline int Copyout(void *data_out, size_t datlen)
			{
				if (datlen == 0)
				{
					return 0;
				}
				if (datlen > ReadableBytes())
				{
					datlen = ReadableBytes();
				}
				memcpy(data_out, m_buffer + m_read_idx, datlen);
				return datlen;
			}

			inline int Copyout(Buffer* unit, size_t datlen)
			{
				if (NULL == unit || !unit->EnsureWritableBytes(datlen))
				{
					return -1;
				}
				int ret = Copyout(unit->m_buffer + +unit->m_write_idx, datlen);
				if (ret > 0)
				{
					unit->m_write_idx += ret;
				}
				return ret;
				//return Copyout();
			}

			inline void SkipBytes(size_t len)
			{
				AdvanceReadIndex(len);
			}

			inline void DiscardReadedBytes()
			{
				if (m_read_idx > 0)
				{
					if (Readable())
					{
						size_t tmp = ReadableBytes();
						memmove(m_buffer, m_buffer + m_read_idx, tmp);
						m_read_idx = 0;
						m_write_idx = tmp;
					} else
					{
						m_read_idx = m_write_idx = 0;
					}
				}
			}
			int IndexOf(const void* data, size_t len, size_t start, size_t end);
			int IndexOf(const void* data, size_t len);
			int Printf(const char *fmt, ...);
			int VPrintf(const char *fmt, va_list ap);
			int PrintString(const std::string& str);
			int ReadFD(int fd, int& err);
			int WriteFD(int fd, int& err);

			inline std::string AsString() const
			{
				return std::string(m_buffer + m_read_idx, ReadableBytes());
			}
			inline ~Buffer()
			{
				if (m_buffer != NULL && m_in_heap)
				{
					free(m_buffer);
				}
			}

	};
}

#endif /* BUFFER__HPP_ */
