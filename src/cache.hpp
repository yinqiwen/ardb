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
 *
 * cache.hpp
 *
 *  Created on: 2013-6-29
 *      Author: yinqiwen@gmail.com
 */

#ifndef CACHE_HPP_
#define CACHE_HPP_

#include <string>
#include "ardb_data.hpp"

namespace ardb
{
	struct CacheKey
	{
			std::string key;
			KeyType key_type;
			CacheKey(KeyType type = KV, const std::string& k = "") :
					key(k), key_type(type)
			{
			}
			inline bool operator<(const CacheKey& other) const
			{
				if (key_type < other.key_type)
				{
					return true;
				}
				if (key_type > other.key_type)
				{
					return false;
				}
				if (key.size() < other.key.size())
				{
					return true;
				}
				if (key.size() > other.key.size())
				{
					return false;
				}
				return key < other.key;
			}
	};

	class AdaptiveReplacementCache
	{

	};

	class LIRSCache{

	};


	class CacheManager
	{
		public:
			CacheManager(uint32 lock_num)
			{

			}
	};
}

#endif /* CACHE_HPP_ */
