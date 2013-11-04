/*
 * arc.cpp
 *
 *  Created on: 2013?7?4?
 *      Author: wqy
 */
#include "cache.hpp"
#include <list>
#include <map>

namespace ardb
{
	template<typename K, typename V>
    class AdaptiveReplacementCache:public Cache<K, V>
	{
		private:
			value_type* Get(const key_type& key)
			{

			}
			value_type* Insert(const key_type& key,
								const value_type& value)
			{

			}
		public:
			~AdaptiveReplacementCache()
			{

			}
	};
}
