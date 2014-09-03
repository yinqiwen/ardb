/*
 * mmap.hpp
 *
 *      Author: wqy
 */

#ifndef MMAP_HPP_
#define MMAP_HPP_
#include "common.hpp"

namespace ardb
{
    class MMapBuf
    {
        public:
            char* m_buf;
            uint64 m_size;
        public:
            MMapBuf() :
                    m_buf(0), m_size(0)
            {
            }
            int Init(const std::string& path, uint64 size, int advice_flag);
            ~MMapBuf();
    };
}

#endif /* MMAP_HPP_ */
