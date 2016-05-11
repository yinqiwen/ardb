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

#ifndef MATH_HELPER_HPP_
#define MATH_HELPER_HPP_
#include "common.hpp"
#include <stdint.h>

namespace ardb
{
	uint32 upper_power_of_two(uint32 t);
	int32 random_int32();
	int32 random_between_int32(int32 min, int32 max);
	//uint32 digits10(uint64 v);
	uint32 digits10(int64 v);

	//! Byte swap unsigned short
	inline uint16_t swap_uint16( uint16_t val )
	{
	    return (val << 8) | (val >> 8 );
	}

	//! Byte swap short
	inline int16_t swap_int16( int16_t val )
	{
	    return (val << 8) | ((val >> 8) & 0xFF);
	}

	//! Byte swap unsigned int
	inline uint32_t swap_uint32( uint32_t val)
	{
	    val = ((val << 8) & 0xFF00FF00 ) | ((val >> 8) & 0xFF00FF );
	    return (val << 16) | (val >> 16);
	}

	//! Byte swap int
	inline int32_t swap_int32( int32_t val )
	{
	    val = ((val << 8) & 0xFF00FF00) | ((val >> 8) & 0xFF00FF );
	    return (val << 16) | ((val >> 16) & 0xFFFF);
	}

	inline int64_t swap_int64( int64_t val )
	{
	    val = ((val << 8) & 0xFF00FF00FF00FF00ULL ) | ((val >> 8) & 0x00FF00FF00FF00FFULL );
	    val = ((val << 16) & 0xFFFF0000FFFF0000ULL ) | ((val >> 16) & 0x0000FFFF0000FFFFULL );
	    return (val << 32) | ((val >> 32) & 0xFFFFFFFFULL);
	}

	inline uint64_t swap_uint64( uint64_t val)
	{
	    val = ((val << 8) & 0xFF00FF00FF00FF00ULL ) | ((val >> 8) & 0x00FF00FF00FF00FFULL );
	    val = ((val << 16) & 0xFFFF0000FFFF0000ULL ) | ((val >> 16) & 0x0000FFFF0000FFFFULL );
	    return (val << 32) | (val >> 32);
	}

}
#endif /* MATH_HELPER_HPP_ */
