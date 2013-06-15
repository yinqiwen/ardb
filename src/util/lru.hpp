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

#ifndef LRU_HPP_
#define LRU_HPP_
#include "common.hpp"
#include <list>
#include <utility>
#include <map>
namespace ardb
{
	template<typename K, typename V>
	class LRUCache
	{
		private:
			typedef K key_type;
			typedef V value_type;
			typedef std::pair<K, V> CacheEntry;
			typedef std::list<CacheEntry> CacheList;
			typedef std::map<K, typename CacheList::iterator> CacheEntryMap;
			CacheList m_cache_list;
			CacheEntryMap m_entry_map;
			uint32 m_max_size;
			bool GetLRUElement(value_type& value, bool remove)
			{
				if (m_cache_list.empty())
				{
					return false;
				}
				CacheEntry& entry = m_cache_list.back();
				value = entry.second;
				if (remove)
				{
					m_entry_map.erase(entry.first);
					m_cache_list.pop_back();
				}
				return true;
			}
		public:
			LRUCache(uint32 max_size = 1024) :
					m_max_size(max_size)
			{
			}
			void SetMaxCacheSize(uint32 size)
			{
				m_max_size = size;
			}
			void Clear()
			{
				m_cache_list.clear();
				m_entry_map.clear();
			}
			bool PopFront()
			{
				value_type value;
				return GetLRUElement(value, true);
			}
			bool PeekFront(value_type& value)
			{
				return GetLRUElement(value, false);
			}
			bool Get(const key_type& key, value_type& value)
			{
				typename CacheEntryMap::iterator found = m_entry_map.find(key);
				if (found != m_entry_map.end())
				{
					typename CacheList::iterator list_it = found->second;
					value = list_it->second;
					m_cache_list.erase(list_it);
					m_cache_list.push_front(std::make_pair(key, value));
					m_entry_map[key] = m_cache_list.begin();
					return true;
				}
				return false;
			}
			bool Insert(const key_type& key, const value_type& value,
					value_type& erased)
			{
				typename CacheEntryMap::iterator found = m_entry_map.find(key);
				bool erased_old = false;
				if (found != m_entry_map.end())
				{
					erased_old = true;
					erased = found->second->second;
					m_cache_list.erase(found->second);
				}
				m_cache_list.push_front(std::make_pair(key, value));
				m_entry_map[key] = m_cache_list.begin();
				if (m_entry_map.size() > m_max_size)
				{
					erased = m_cache_list.back().second;
					m_entry_map.erase(m_cache_list.back().first);
					m_cache_list.pop_back();
					erased_old = true;
				}
				return erased_old;
			}
	};
}

#endif /* LRU_HPP_ */
