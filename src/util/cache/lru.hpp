/*
 * lru.hpp
 *
 *  Created on: 2013?7?4?
 *      Author: wqy
 */

#ifndef LRU_HPP_
#define LRU_HPP_

#include "cache.hpp"
#include <list>
#include <map>

namespace ardb
{
	template<typename K, typename V>
	class LRUCache: public Cache<K, V>
	{
		private:

			typedef std::list<Entry> CacheList;
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
				CacheEntry & entry = m_cache_list.back();
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
			value_type* Get(const key_type& key)
			{
				typename CacheEntryMap::iterator found = m_entry_map.find(key);
				if (found != m_entry_map.end())
				{
					typename CacheList::iterator list_it = found->second;
					value_type & value = list_it->second;
					m_cache_list.erase(list_it);
					m_cache_list.push_front(std::make_pair(key, value));
					m_entry_map[key] = m_cache_list.begin();
					return &value;
				}
				return NULL;
			}
			bool Insert(const key_type& key, const value_type& value,
					value_type* erase_value)
			{
				typename CacheEntryMap::iterator found = m_entry_map.find(key);
				bool erased_old = false;
				if (found != m_entry_map.end())
				{
					erased_old = true;
					if (NULL != erase_value)
					{
						(*erase_value) = (*found->second);
					}
					m_cache_list.erase(found->second);
				}
				m_cache_list.push_front(std::make_pair(key, value));
				m_entry_map[key] = m_cache_list.begin();
				if (m_entry_map.size() > m_max_size)
				{
					if (NULL != erase_value)
					{
						(*erase_value) = m_cache_list.back();
					}
					m_entry_map.erase(m_cache_list.back().first);
					m_cache_list.pop_back();
					erased_old = true;
				}
				return erased_old;
			}
	};
}

#endif /* LRU_HPP_ */
