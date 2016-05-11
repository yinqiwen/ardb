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

#ifndef NOVA_BufferHELPER_HPP_
#define NOVA_BufferHELPER_HPP_
#include "buffer.hpp"
#include "types.hpp"
#include <string>
using std::string;
#define MAX_BYTEARRAY_BUFFER_SIZE 4096
namespace ardb
{
	class BufferHelper
	{
		public:
			static bool ReadFixUInt64(Buffer& buffer, uint64_t& i,
					bool bigendian = true);
			static bool ReadFixInt64(Buffer& buffer, int64_t& i,
					bool bigendian = true);
			static bool ReadFixUInt32(Buffer& buffer, uint32_t& i,
					bool bigendian = true);
			static bool ReadFixInt32(Buffer& buffer, int32_t& i,
					bool bigendian = true);
			static bool ReadFixUInt16(Buffer& buffer, uint16_t& i,
					bool bigendian = true);
			static bool ReadFixInt16(Buffer& buffer, int16_t& i,
					bool bigendian = true);
			static bool ReadFixUInt8(Buffer& buffer, uint8_t& i);
			static bool ReadFixInt8(Buffer& buffer, int8_t& i);
			static bool ReadFixString(Buffer& buffer, string& str,
					bool bigendian = true);
			static bool ReadFixString(Buffer& buffer, char*& str,
					bool bigendian = true);
			static bool ReadFixFloat(Buffer& buffer, float& i,
					bool bigendian = true);
			static bool ReadFixDouble(Buffer& buffer, double& i,
					bool bigendian = true);

			static bool ReadVarDouble(Buffer& buffer, double& i);
			static bool ReadVarUInt64(Buffer& buffer, uint64_t& i);
			static bool ReadVarInt64(Buffer& buffer, int64_t& i);
			static bool ReadVarUInt32(Buffer& buffer, uint32_t& i);
			static bool ReadVarUInt32IfEqual(Buffer& buffer, uint32_t v);

			static bool ReadVarInt32(Buffer& buffer, int32_t& i);
			static bool ReadVarUInt16(Buffer& buffer, uint16_t& i);
			static bool ReadVarInt16(Buffer& buffer, int16_t& i);
			static bool ReadVarString(Buffer& buffer, string& str);
			static bool ReadVarString(Buffer& buffer, char*& str);
			static bool ReadVarSlice(Buffer& buffer, Slice& str);
			static bool ReadBool(Buffer& buffer, bool& value);

			static bool WriteBool(Buffer& buffer, bool value);

			static bool WriteFixUInt64(Buffer& buffer, uint64_t i,
					bool bigendian = true);
			static bool WriteFixInt64(Buffer& buffer, int64_t i,
					bool bigendian = true);
			static bool WriteFixUInt32(Buffer& buffer, uint32_t i,
					bool bigendian = true);
			static bool WriteFixInt32(Buffer& buffer, int32_t i,
					bool bigendian = true);
			static bool WriteFixUInt16(Buffer& buffer, uint16_t i,
					bool bigendian = true);
			static bool WriteFixFloat(Buffer& buffer, float i, bool bigendian =
					true);
			static bool WriteFixDouble(Buffer& buffer, double i,
					bool bigendian = true);

			static bool WriteFixInt16(Buffer& buffer, int16_t i,
					bool bigendian = true);
			static bool WriteFixUInt8(Buffer& buffer, uint8_t i);
			static bool WriteFixInt8(Buffer& buffer, int8_t i);

			static bool WriteFixString(Buffer& buffer, const string& str,
					bool bigendian = true);
			static bool WriteFixString(Buffer& buffer, const char* str,
					bool bigendian = true);

			static bool WriteVarUInt64(Buffer& buffer, uint64_t i);
			static bool WriteVarInt64(Buffer& buffer, int64_t i);
			static bool WriteVarUInt32(Buffer& buffer, uint32_t i);
			static bool WriteVarUInt16(Buffer& buffer, uint16_t i);
			static bool WriteVarInt16(Buffer& buffer, int16_t i);
			static bool WriteVarInt32(Buffer& buffer, int32_t i);
			static bool WriteVarDouble(Buffer& buffer, double d);
			static bool WriteVarString(Buffer& buffer, const string& str);
			static bool WriteVarString(Buffer& buffer, const char* str);
			static bool WriteVarSlice(Buffer& buffer, const Slice& data);
	};
}

#endif /* BufferHELPER_HPP_ */
