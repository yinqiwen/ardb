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

#include <arpa/inet.h>
#include "buffer_helper.hpp"
#include "util/network_helper.hpp"
#include "util/system_helper.hpp"
#include "util/math_helper.hpp"

namespace ardb
{
	static const int kMaxVarintBytes = 10;
	static const int kMaxVarint32Bytes = 5;

	static inline uint32_t ZigZagEncode32(int32_t n)
	{
		// Note:  the right-shift must be arithmetic
		return (n << 1) ^ (n >> 31);
	}

	static inline int32_t ZigZagDecode32(uint32_t n)
	{
		return (n >> 1) ^ -static_cast<int32_t>(n & 1);
	}

	inline uint64_t ZigZagEncode64(int64_t n)
	{
		// Note:  the right-shift must be arithmetic
		return (n << 1) ^ (n >> 63);
	}

	static inline int64_t ZigZagDecode64(uint64_t n)
	{
		return (n >> 1) ^ -static_cast<int64_t>(n & 1);
	}

	bool BufferHelper::ReadBool(Buffer& buffer, bool& value)
	{
		char temp;
		if (!buffer.ReadByte(temp))
		{
			return false;
		}
		value = (bool) temp;
		return true;
	}

	bool BufferHelper::ReadFixUInt64(Buffer& buffer, uint64_t& i,
			bool bigendian)
	{
		uint64_t u;
		if (buffer.Read(&u, sizeof(uint64_t)) != sizeof(uint64_t))
		{
			return false;
		}
		if(bigendian == is_bigendian())
		{
		    i = u;
		}
		else
		{
	        i = swap_uint64(u);
		}
		return true;
	}

	bool BufferHelper::ReadFixInt64(Buffer& buffer, int64_t& i,
			bool bigendian)
	{
        int64_t u;
        if (buffer.Read(&u, sizeof(int64_t)) != sizeof(int64_t))
        {
            return false;
        }
        if(bigendian == is_bigendian())
        {
            i = u;
        }
        else
        {
            i = swap_int64(u);
        }
        return true;
	}

	bool BufferHelper::ReadFixUInt32(Buffer& buffer, uint32_t& i,
			bool bigendian)
	{
        uint32_t u;
        if (buffer.Read(&u, sizeof(uint32_t)) != sizeof(uint32_t))
        {
            return false;
        }
        if(bigendian == is_bigendian())
        {
            i = u;
        }
        else
        {
            i = swap_uint32(u);
        }
        return true;
	}

	bool BufferHelper::ReadFixInt32(Buffer& buffer, int32_t& i,
			bool bigendian)
	{
        int32_t u;
        if (buffer.Read(&u, sizeof(int32_t)) != sizeof(int32_t))
        {
            return false;
        }
        if(bigendian == is_bigendian())
        {
            i = u;
        }
        else
        {
            i = swap_int32(u);
        }
        return true;
	}

	bool BufferHelper::ReadFixFloat(Buffer& buffer, float& i, bool bigendian)
	{
		union
		{
				float d;
				uint32_t ull;
		} u;

		if (!ReadFixUInt32(buffer, u.ull, bigendian))
		{
			return false;
		}
		i = u.d;
		return true;
	}

	bool BufferHelper::ReadFixDouble(Buffer& buffer, double& i,
			bool bigendian)
	{
		union
		{
				double d;
				uint64_t ull;
		} u;

		if (!ReadFixUInt64(buffer, u.ull, bigendian))
		{
			return false;
		}
		i = u.d;
		return true;
	}

	bool BufferHelper::ReadFixUInt16(Buffer& buffer, uint16_t& i,
			bool bigendian)
	{
	    uint16_t u;
        if (buffer.Read(&u, sizeof(uint16_t)) != sizeof(uint16_t))
        {
            return false;
        }
        if(bigendian == is_bigendian())
        {
            i = u;
        }
        else
        {
            i = swap_uint16(u);
        }
        return true;
	}

	bool BufferHelper::ReadFixInt16(Buffer& buffer, int16_t& i,
			bool bigendian)
	{
        int16_t u;
        if (buffer.Read(&u, sizeof(int16_t)) != sizeof(int16_t))
        {
            return false;
        }
        if(bigendian == is_bigendian())
        {
            i = u;
        }
        else
        {
            i = swap_int16(u);
        }
        return true;
	}

	bool BufferHelper::ReadFixUInt8(Buffer& buffer, uint8_t& i)
	{
		return buffer.Read(&i, sizeof(uint8_t)) == sizeof(uint8_t);
	}
	bool BufferHelper::ReadFixInt8(Buffer& buffer, int8_t& i)
	{
		return buffer.Read(&i, sizeof(int8_t)) == sizeof(int8_t);
	}

	bool BufferHelper::ReadFixString(Buffer& buffer, string& str,
			bool bigendian)
	{
		uint32_t len;
		if (!ReadFixUInt32(buffer, len, bigendian))
		{
			return false;
		}
		if (0 == len)
		{
			return true;
		}
		char temp[len];
		if (buffer.Read(temp, len) != (int) len)
		{
			return false;
		}
		str = string(temp, len);
		return true;
	}

	bool BufferHelper::ReadFixString(Buffer& buffer, char*& str,
			bool bigendian)
	{
		uint32_t len;
		if (!ReadFixUInt32(buffer, len, bigendian))
		{
			return false;
		}
		if (0 == len)
		{
			str = NULL;
			return true;
		}
		char* temp = new char[len + 1];
		//NEW(temp, char[len+1]);
		if (buffer.Read(temp, len) != (int) len)
		{
			//DELETE_A(temp);
			delete[] temp;
			return false;
		}
		temp[len] = 0;
		str = temp;
		return true;
	}

	bool BufferHelper::ReadVarDouble(Buffer& buffer, double& i)
	{
		union
		{
				double d;
				uint64_t ull;
		} u;

		if (!ReadVarUInt64(buffer, u.ull))
		{
			return false;
		}
		i = u.d;
		return true;
	}

	bool BufferHelper::ReadVarUInt64(Buffer& buffer, uint64_t& i)
	{
		uint64_t result = 0;
		int count = 0;
		uint32_t b;

		do
		{
			if (count == kMaxVarintBytes)
				return false;
			unsigned char ch;
			if (1 != buffer.Read(&ch, 1))
			{
				return false;
			}
			b = (uint32_t) ch;
			result |= static_cast<uint64_t>(b & 0x7F) << (7 * count);
			++count;
		} while (b & 0x80);
		i = result;
		return true;
	}

	bool BufferHelper::ReadVarInt64(Buffer& buffer, int64_t& i)
	{
		uint64_t result;
		if (!ReadVarUInt64(buffer, result))
		{
			return false;
		}
		i = ZigZagDecode64(result);
		return true;
	}

	bool BufferHelper::ReadVarUInt32(Buffer& buffer, uint32_t& i)
	{
		uint64_t result;
		if (!ReadVarUInt64(buffer, result))
		{
			return false;
		}
		i = (uint32_t) result;
		return true;
	}

	bool BufferHelper::ReadVarUInt32IfEqual(Buffer& buffer, uint32_t v)
	{
	    size_t idx = buffer.GetReadIndex();
	    uint32_t cmp = 0;
	    if(ReadVarUInt32(buffer, cmp) && cmp == v)
	    {
	        return true;
	    }
	    buffer.SetReadIndex(idx);
	    return false;
	}

	bool BufferHelper::ReadVarInt32(Buffer& buffer, int32_t& i)
	{
		uint64_t result;
		if (!ReadVarUInt64(buffer, result))
		{
			return false;
		}
		i = ZigZagDecode32(result);
		return true;
	}

	bool BufferHelper::ReadVarUInt16(Buffer& buffer, uint16_t& i)
	{
		uint64_t result;
		if (!ReadVarUInt64(buffer, result))
		{
			return false;
		}
		i = ZigZagDecode32(result);
		return true;
	}
	bool BufferHelper::ReadVarInt16(Buffer& buffer, int16_t& i)
	{
		uint64_t result;
		if (!ReadVarUInt64(buffer, result))
		{
			return false;
		}
		i = (int16_t) result;
		return true;
	}

	bool BufferHelper::ReadVarString(Buffer& buffer, string& str)
	{
		uint32_t len;
		if (!ReadVarUInt32(buffer, len))
		{
			return false;
		}
		if (0 == len)
		{
			return true;
		}
		char temp[len];
		if (buffer.Read(temp, len) != (int) len)
		{
			return false;
		}
		str = string(temp, len);
		return true;
	}

	bool BufferHelper::ReadVarString(Buffer& buffer, char*& str)
	{
		uint32_t len;
		if (!ReadVarUInt32(buffer, len))
		{
			return false;
		}
		if (0 == len)
		{
			str = NULL;
			return true;
		}
		char* temp = new char[len + 1];
		//NEW(temp, char[len+1]);
		if (buffer.Read(temp, len) != (int) len)
		{
			//DELETE_A(temp);
			delete[] temp;
			return false;
		}
		temp[len] = 0;
		str = temp;
		return true;
	}

	bool BufferHelper::ReadVarSlice(Buffer& buffer, Slice& str)
	{
		uint32_t len;
		if (!ReadVarUInt32(buffer, len))
		{
			return false;
		}
		if (0 == len)
		{
			str.clear();
			return true;
		}
		if (buffer.ReadableBytes() < len)
		{
			return false;
		}
		Slice s(buffer.GetRawReadBuffer(), len);
		buffer.SkipBytes(len);
		str = s;
		return true;
	}

	bool BufferHelper::WriteBool(Buffer& buffer, bool value)
	{
		buffer.WriteByte((char) value);
		return true;
	}

	bool BufferHelper::WriteFixUInt64(Buffer& buffer, uint64_t i,
			bool bigendian)
	{
		if (bigendian != is_bigendian())
		{
			 i = swap_uint64(i);
		}
		return sizeof(uint64_t) == buffer.Write(&i, sizeof(uint64_t));
	}

	bool BufferHelper::WriteFixInt64(Buffer& buffer, int64_t i, bool bigendian)
	{
        if (bigendian != is_bigendian())
        {
             i = swap_int64(i);
        }
        return sizeof(uint64_t) == buffer.Write(&i, sizeof(int64_t));
	}

	bool BufferHelper::WriteFixUInt32(Buffer& buffer, uint32_t i,
			bool bigendian)
	{
        if (bigendian != is_bigendian())
        {
             i = swap_uint32(i);
        }
        return sizeof(uint64_t) == buffer.Write(&i, sizeof(uint32_t));
	}

	bool BufferHelper::WriteFixInt32(Buffer& buffer, int32_t i, bool bigendian)
	{
        if (bigendian != is_bigendian())
        {
             i = swap_int32(i);
        }
        return sizeof(uint64_t) == buffer.Write(&i, sizeof(int32_t));
	}

	bool BufferHelper::WriteFixFloat(Buffer& buffer, float i, bool bigendian)
	{
		union
		{
				float d;
				int32_t ul;
		} u;
		u.d = i;
		return WriteFixInt32(buffer, u.ul, bigendian);
	}

	bool BufferHelper::WriteFixDouble(Buffer& buffer, double i, bool bigendian)
	{

		union
		{
				double d;
				int64_t ul;
		} u;
		u.d = i;
		return WriteFixInt64(buffer, u.ul, bigendian);
	}

	bool BufferHelper::WriteFixUInt8(Buffer& buffer, uint8_t i)
	{
		return buffer.Write(&i, sizeof(uint8_t)) == sizeof(uint8_t);
	}

	bool BufferHelper::WriteFixInt8(Buffer& buffer, int8_t i)
	{
		return buffer.Write(&i, sizeof(int8_t)) == sizeof(int8_t);
	}

	bool BufferHelper::WriteFixUInt16(Buffer& buffer, uint16_t i,
			bool bigendian)
	{
        if (bigendian != is_bigendian())
        {
             i = swap_uint16(i);
        }
        return sizeof(uint64_t) == buffer.Write(&i, sizeof(uint16_t));
	}

	bool BufferHelper::WriteFixInt16(Buffer& buffer, int16_t i, bool bigendian)
	{
        if (bigendian != is_bigendian())
        {
             i = swap_int16(i);
        }
        return sizeof(uint64_t) == buffer.Write(&i, sizeof(int16_t));
	}

	bool BufferHelper::WriteFixString(Buffer& buffer, const string& str,
			bool bigendian)
	{
		return WriteFixUInt32(buffer, str.size(), bigendian)
				&& str.size() == (size_t) buffer.Write(str.c_str(), str.size());
	}

	bool BufferHelper::WriteFixString(Buffer& buffer, const char* str,
			bool bigendian)
	{
		if (NULL == str)
		{
			return WriteFixUInt32(buffer, 0, bigendian);
		} else
		{
			size_t len = strlen(str);
			return WriteFixUInt32(buffer, len, bigendian)
					&& len == (size_t) buffer.Write(str, len);
		}
	}

	bool BufferHelper::WriteVarUInt64(Buffer& buffer, uint64_t i)
	{
		uint8_t bytes[kMaxVarintBytes];
		uint32_t size = 0;
		while (i > 0x7F)
		{
			bytes[size++] = (static_cast<uint8_t>(i) & 0x7F) | 0x80;
			i >>= 7;
		}
		bytes[size++] = static_cast<uint8_t>(i) & 0x7F;
		return (int) size == buffer.Write(bytes, size);
	}

	bool BufferHelper::WriteVarInt64(Buffer& buffer, int64_t i)
	{
		return WriteVarUInt64(buffer, ZigZagEncode64(i));
	}

	bool BufferHelper::WriteVarUInt32(Buffer& buffer, uint32_t i)
	{
		uint8_t bytes[kMaxVarint32Bytes];
		uint32_t size = 0;
		while (i > 0x7F)
		{
			bytes[size++] = (static_cast<uint8_t>(i) & 0x7F) | 0x80;
			i >>= 7;
		}
		bytes[size++] = static_cast<uint8_t>(i) & 0x7F;
		return (int) size == buffer.Write(bytes, size);
	}

	bool BufferHelper::WriteVarInt32(Buffer& buffer, int32_t i)
	{
		return WriteVarUInt32(buffer, ZigZagEncode32(i));
	}

	bool BufferHelper::WriteVarUInt16(Buffer& buffer, uint16_t i)
	{
		uint32_t value = i;
		return WriteVarUInt32(buffer, value);
	}

	bool BufferHelper::WriteVarInt16(Buffer& buffer, int16_t i)
	{
		return WriteVarInt32(buffer, ZigZagEncode32(i));
	}

	bool BufferHelper::WriteVarString(Buffer& buffer, const string& str)
	{
		WriteVarUInt32(buffer, str.size());
		return (int) (str.size()) == buffer.Write(str.c_str(), str.size());
	}

	bool BufferHelper::WriteVarString(Buffer& buffer, const char* str)
	{
		if (NULL == str)
		{
			return WriteVarUInt32(buffer, 0);
		} else
		{
			size_t len = strlen(str);
			return WriteVarUInt32(buffer, len)
					&& len == (size_t) buffer.Write(str, len);
		}
	}

	bool BufferHelper::WriteVarDouble(Buffer& buffer, double d)
	{
		union
		{
				double d;
				uint64_t ull;
		} u;
		u.d = d;

		return WriteVarUInt64(buffer, u.ull);
	}

	bool BufferHelper::WriteVarSlice(Buffer& buffer, const Slice& data)
	{
		size_t len = data.size();
		return WriteVarUInt32(buffer, len)
				&& len == (size_t) buffer.Write(data.data(), len);
	}

}

