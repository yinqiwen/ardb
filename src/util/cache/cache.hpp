/*
 * cache.hpp
 *
 *  Created on: 2013?7?4?
 *      Author: wqy
 */

#ifndef CACHE_HPP_
#define CACHE_HPP_

#include "common.hpp"
#include <list>
#include <utility>
#include <map>
#include "util/thread/thread_mutex_lock.hpp"
namespace ardb
{
	template<typename K, typename V>
	class Cache
	{
		protected:
			typedef K key_type;
			typedef V value_type;
		public:
			typedef std::pair<K, V> Entry;
			virtual value_type* Get(const key_type& key) = 0;
			virtual bool Insert(const key_type& key,
					const value_type& value, Entry* erase_entry) = 0;
			virtual ~Cache()
			{
			}
	};

}
#endif /* CACHE_HPP_ */
