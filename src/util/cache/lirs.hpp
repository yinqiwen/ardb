/*
 * lirs.cpp
 *
 *  Created on: 2013?7?4?
 *      Author: wqy
 */
#include "common.hpp"
#include <list>
#include <utility>
#include <map>

#define LIRS_HOT_RATE  0.99

namespace ardb
{
    enum LIRSStatus
    {
        HOT,    // resident LIRS, in stack, never in queue
        COLD,   // resident HIRS, always in queue, sometimes in stack
        NONRES  // non-resident HIRS, may be in stack, never in queue
    };

    template<typename K, typename V>
    class LIRSEntry
    {
        private:
            typedef K key_type;
            typedef V value_type;
            LIRSStatus m_status;
            key_type m_key;
            value_type m_value;

            // LIRS stack S
            LIRSEntry<K, V>* m_previousInStack;
            LIRSEntry<K, V>* nextInStack;

            // LIRS queue Q
            LIRSEntry<K, V>* m_previousInQueue;
            LIRSEntry<K, V>* m_nextInQueue;
        public:
    };

    template<typename K, typename V>
    class LIRSCache
    {
        private:
            typedef K key_type;
            typedef V value_type;
        public:
            value_type* Get(const key_type& key)
            {

            }
    };

}

