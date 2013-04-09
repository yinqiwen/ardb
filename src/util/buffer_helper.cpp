/*
 * BufferHelper.cpp
 *
 *  Created on: 2011-2-12
 *      Author: qiyingwang
 */
#include <arpa/inet.h>
#include "buffer_helper.hpp"
//#include "net/network_helper.hpp"

namespace ardb
{
	static const int kMaxVarintBytes = 10;
	static const int kMaxVarint32Bytes = 5;

	static uint64_t htonll(uint64_t v)
	{
		int num = 42;
		//big or little
		if (*(char *) &num == 42)
		{
			uint64_t temp = htonl(v & 0xFFFFFFFF);
			temp <<= 32;
			return temp | htonl(v >> 32);
		} else
		{
			return v;
		}

	}

	static uint64_t ntohll(uint64_t v)
	{
		return htonll(v);
	}

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
			bool fromNetwork)
	{
		uint64_t u;
		if (buffer.Read(&u, sizeof(uint64_t)) != sizeof(uint64_t))
		{
			return false;
		}
		if (!fromNetwork)
		{
			i = u;
		} else
		{
			i = ntohll(u);
		}
		return true;
	}

	bool BufferHelper::ReadFixInt64(Buffer& buffer, int64_t& i,
			bool fromNetwork)
	{
		uint64_t u;
		if (!ReadFixUInt64(buffer, u, fromNetwork))
		{
			return false;
		}
		i = u;
		return true;
	}

	bool BufferHelper::ReadFixUInt32(Buffer& buffer, uint32_t& i,
			bool fromNetwork)
	{
		uint32_t u;
		if (buffer.Read(&u, sizeof(uint32_t)) != sizeof(uint32_t))
		{
			return false;
		}
		if (!fromNetwork)
		{
			i = u;
		} else
		{
			i = ntohl(u);
		}
		return true;
	}

	bool BufferHelper::ReadFixInt32(Buffer& buffer, int32_t& i,
			bool fromNetwork)
	{
		uint32_t u;
		if (!ReadFixUInt32(buffer, u, fromNetwork))
		{
			return false;
		}
		i = u;
		return true;
	}

	bool BufferHelper::ReadFixUInt16(Buffer& buffer, uint16_t& i,
			bool fromNetwork)
	{
		uint16_t u;
		if (buffer.Read(&u, sizeof(uint16_t)) != sizeof(uint16_t))
		{
			return false;
		}
		if (!fromNetwork)
		{
			i = u;
		} else
		{
			i = ntohs(u);
		}
		return true;
	}

	bool BufferHelper::ReadFixInt16(Buffer& buffer, int16_t& i,
			bool fromNetwork)
	{
		uint16_t u;
		if (!ReadFixUInt16(buffer, u, fromNetwork))
		{
			return false;
		}
		i = u;
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
			bool fromNetwork)
	{
		uint32_t len;
		if (!ReadFixUInt32(buffer, len, fromNetwork))
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
			bool fromNetwork)
	{
		uint32_t len;
		if (!ReadFixUInt32(buffer, len, fromNetwork))
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
			bool toNetwork)
	{
		if (toNetwork)
		{
			i = htonll(i);
		}
		return sizeof(uint64_t) == buffer.Write(&i, sizeof(uint64_t));
	}

	bool BufferHelper::WriteFixInt64(Buffer& buffer, int64_t i, bool toNetwork)
	{
		uint64_t u = i;
		return WriteFixUInt64(buffer, u, toNetwork);
	}

	bool BufferHelper::WriteFixUInt32(Buffer& buffer, uint32_t i,
			bool toNetwork)
	{
		if (toNetwork)
		{
			i = htonl(i);
		}
		return sizeof(uint32_t) == buffer.Write(&i, sizeof(uint32_t));
	}

	bool BufferHelper::WriteFixInt32(Buffer& buffer, int32_t i, bool toNetwork)
	{
		uint32_t u = i;
		return WriteFixUInt32(buffer, u, toNetwork);
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
			bool toNetwork)
	{
		if (toNetwork)
		{
			i = htons(i);
		}
		return sizeof(uint16_t) == buffer.Write(&i, sizeof(uint16_t));
	}

	bool BufferHelper::WriteFixInt16(Buffer& buffer, int16_t i, bool toNetwork)
	{
		uint16_t u = i;
		return WriteFixUInt16(buffer, u, toNetwork);
	}

	bool BufferHelper::WriteFixString(Buffer& buffer, const string& str,
			bool toNetwork)
	{
		return WriteFixUInt32(buffer, str.size(), toNetwork)
				&& str.size() == (size_t) buffer.Write(str.c_str(), str.size());
	}

	bool BufferHelper::WriteFixString(Buffer& buffer, const char* str,
			bool toNetwork)
	{
		if (NULL == str)
		{
			return WriteFixUInt32(buffer, 0, toNetwork);
		} else
		{
			size_t len = strlen(str);
			return WriteFixUInt32(buffer, len, toNetwork)
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

