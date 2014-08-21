/*
 * version_helper.hpp
 *
 *      Author: wqy
 */

#ifndef VERSION_HELPER_HPP_
#define VERSION_HELPER_HPP_

#include <string>
#include <sstream>
#include "common.hpp"

namespace ardb
{
    template<uint32 N>
    void parse_version(int result[N], const std::string& input)
    {
        std::istringstream parser(input);
        parser >> result[0];
        for (uint32 idx = 1; idx < N; idx++)
        {
            parser.get(); //Skip period
            parser >> result[idx];
        }
    }

    template<uint32 N>
    int compare_version(const std::string& a, const std::string& b)
    {
        int parsedA[N], parsedB[N];
        parse_version<N>(parsedA, a);
        parse_version<N>(parsedB, b);
        for (uint32 i = 0; i < N; i++)
        {
            if (parsedA[i] != parsedB[i])
            {
                return parsedA[i] - parsedB[i];
            }
        }
        return 0;
    }
}

#endif /* VERSION_HELPER_HPP_ */
