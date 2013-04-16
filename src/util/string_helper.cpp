/*
 * string_helper.cpp
 *
 *  Created on: 2011-3-14
 *      Author: qiyingwang
 */
#include "util/string_helper.hpp"
#include "util/math_helper.hpp"
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <stdexcept>
#include <stdlib.h>
#include <stdio.h>

namespace ardb
{

	std::string get_basename(const std::string& filename)
	{
#if defined(_WIN32)
		char dir_sep('\\');
#else
		char dir_sep('/');
#endif

		std::string::size_type pos = filename.rfind(dir_sep);
		if (pos != std::string::npos)
			return filename.substr(pos + 1);
		else
			return filename;
	}

	char* trim_str(char* s, const char* cset)
	{
		RETURN_NULL_IF_NULL(s);
		if (NULL == cset)
		{
			return s;
		}
		char *start, *end, *sp, *ep;
		size_t len;

		sp = start = s;
		ep = end = s + strlen(s) - 1;
		while (*sp && strchr(cset, *sp))
			sp++;
		while (ep > sp && strchr(cset, *ep))
			ep--;
		len = (sp > ep) ? 0 : ((ep - sp) + 1);
		if (start != sp)
			memmove(start, sp, len);
		start[len] = '\0';
		return start;
	}

	std::string trim_string(const std::string& str, const std::string& cset)
	{
		if (str.empty())
		{
			return str;
		}
		size_t start = 0;
		size_t str_len = str.size();
		size_t end = str_len - 1;
		while (start < str_len && cset.find(str.at(start)) != std::string::npos)
		{
			start++;
		}
		while (end > start && end < str_len
				&& cset.find(str.at(end)) != std::string::npos)
		{
			end--;
		}
		return str.substr(start, end - start + 1);
	}

	std::vector<char*> split_str(char* str, const char* sep)
	{
		std::vector<char*> ret;
		char* start = str;
		char* found = NULL;
		uint32 sep_len = strlen(sep);
		while (*start && (found = strstr(start, sep)) != NULL)
		{
			*found = 0;
			if (*start)
			{
				ret.push_back(start);
			}
			start = (found + sep_len);
		}
		if (*start)
		{
			ret.push_back(start);
		}
		return ret;
	}

	std::vector<std::string> split_string(const std::string& str,
			const std::string& sep)
	{
		std::vector<std::string> ret;
		size_t start = 0;
		size_t str_len = str.size();
		size_t found = std::string::npos;
		while (start < str_len
				&& (found = str.find(sep, start)) != std::string::npos)
		{
			if (found > start)
			{
				ret.push_back(str.substr(start, found - start));
			}
			start = (found + sep.size());
		}
		if (start < str_len)
		{
			ret.push_back(str.substr(start));
		}
		return ret;
	}

	void split_string(const std::string& strs, const std::string& sp,
			std::vector<std::string>& res)
	{
		std::string::size_type pos1, pos2;

		pos2 = strs.find(sp);
		pos1 = 0;

		while (std::string::npos != pos2)
		{
			res.push_back(strs.substr(pos1, pos2 - pos1));
			pos1 = pos2 + sp.length();
			pos2 = strs.find(sp, pos1);
		}

		res.push_back(strs.substr(pos1));
	}

	int string_replace(std::string& str, const std::string& pattern,
			const std::string& newpat)
	{
		int count = 0;
		const size_t nsize = newpat.size();
		const size_t psize = pattern.size();

		for (size_t pos = str.find(pattern, 0); pos != std::string::npos; pos =
				str.find(pattern, pos + nsize))
		{
			str.replace(pos, psize, newpat);
			count++;
		}

		return count;
	}

	char* str_tolower(char* str)
	{
		char* start = str;
		while (*start)
		{
			*start = tolower(*start);
			start++;
		}
		return str;
	}
	char* str_toupper(char* str)
	{
		char* start = str;
		while (*start)
		{
			*start = toupper(*start);
			start++;
		}
		return str;
	}

	void lower_string(std::string& str)
	{
		uint32 i = 0;
		std::string ret;
		while (i < str.size())
		{
			str[i] = tolower(str.at(i));
			i++;
		}
	}
	void upper_string(std::string& str)
	{
		uint32 i = 0;
		std::string ret;
		while (i < str.size())
		{
			str[i] = toupper(str.at(i));
			i++;
		}
	}

	std::string string_tolower(const std::string& str)
	{
		uint32 i = 0;
		std::string ret;
		while (i < str.size())
		{
			char ch = tolower(str.at(i));
			ret.push_back(ch);
			i++;
		}
		return ret;
	}
	std::string string_toupper(const std::string& str)
	{
		uint32 i = 0;
		std::string ret;
		while (i < str.size())
		{
			char ch = toupper(str.at(i));
			ret.push_back(ch);
			i++;
		}
		return ret;
	}

	bool str_toint64(const char* str, int64& value)
	{
		RETURN_FALSE_IF_NULL(str);
		char *endptr = NULL;
		long long int val = strtoll(str, &endptr, 10);
		if (NULL == endptr || 0 != *endptr)
		{
			return false;
		}
		value = val;
		return true;
	}

	bool str_touint64(const char* str, uint64& value)
	{
		int64 temp;
		if (!str_toint64(str, temp))
		{
			return false;
		}
		value = (uint64) temp;
		return true;
	}

	bool str_tofloat(const char* str, float& value)
	{
		RETURN_FALSE_IF_NULL(str);
		char *endptr = NULL;
		float val = strtof(str, &endptr);
		if (NULL == endptr || 0 != *endptr)
		{
			return false;
		}
		value = val;
		return true;
	}

	bool str_todouble(const char* str, double& value)
	{
		RETURN_FALSE_IF_NULL(str);
		char *endptr = NULL;
		double val = strtod(str, &endptr);
		if (NULL == endptr || 0 != *endptr)
		{
			return false;
		}
		value = val;
		return true;
	}

	bool has_prefix(const std::string& str, const std::string& prefix)
	{
		if (prefix.empty())
		{
			return true;
		}
		if (str.size() < prefix.size())
		{
			return false;
		}
		return str.find(prefix) == 0;
	}
	bool has_suffix(const std::string& str, const std::string& suffix)
	{
		if (suffix.empty())
		{
			return true;
		}
		if (str.size() < suffix.size())
		{
			return false;
		}
		return str.rfind(suffix) == str.size() - suffix.size();
	}

	static inline void strreverse(char* begin, char* end)
	{
		char aux;
		while (end > begin)
			aux = *end, *end-- = *begin, *begin++ = aux;
	}

	static void ensure_space(char*& str, int& slen, char*& wstr)
	{
		if (wstr - str == slen)
		{
			int pos = slen;
			slen *= 2;
			str = (char*)realloc(str, slen);
			wstr = str + pos;
		}
	}

	void fast_dtoa(double value, int prec, std::string& result)
	{
		static const double powers_of_10[] = { 1, 10, 100, 1000, 10000, 100000,
				1000000, 10000000, 100000000, 1000000000 };
		/* Hacky test for NaN
		 * under -fast-math this won't work, but then you also won't
		 * have correct nan values anyways.  The alternative is
		 * to link with libmath (bad) or hack IEEE double bits (bad)
		 */

		if (!(value == value))
		{
//			str[0] = 'n';
//			str[1] = 'a';
//			str[2] = 'n';
//			str[3] = '\0';
			result ="nan";
			return;
		}
		int slen = 256;
		char* str = (char*)malloc(slen);
		/* if input is larger than thres_max, revert to exponential */
		const double thres_max = (double) (0x7FFFFFFF);

		int count;
		double diff = 0.0;
		char* wstr = str;

		if (prec < 0)
		{
			prec = 0;
		} else if (prec > 9)
		{
			/* precision of >= 10 can lead to overflow errors */
			prec = 9;
		}

		/* we'll work in positive values and deal with the
		 negative sign issue later */
		int neg = 0;
		if (value < 0)
		{
			neg = 1;
			value = -value;
		}

		int whole = (int) value;
		double tmp = (value - whole) * powers_of_10[prec];
		uint32_t frac = (uint32_t) (tmp);
		diff = tmp - frac;

		if (diff > 0.5)
		{
			++frac;
			/* handle rollover, e.g.  case 0.99 with prec 1 is 1.0  */
			if (frac >= powers_of_10[prec])
			{
				frac = 0;
				++whole;
			}
		} else if (diff == 0.5 && ((frac == 0) || (frac & 1)))
		{
			/* if halfway, round up if odd, OR
			 if last digit is 0.  That last part is strange */
			++frac;
		}

		/* for very large numbers switch back to native sprintf for exponentials.
		 anyone want to write code to replace this? */
		/*
		 normal printf behavior is to print EVERY whole number digit
		 which can be 100s of characters overflowing your buffers == bad
		 */
		if (value > thres_max)
		{
			while(snprintf(str, slen, "%e", neg ? -value : value) < 0)
			{
				slen *= 2;
				str = (char*)realloc(str, slen);
			}
			result.assign(str, (wstr-str));
			free(str);
			return;
		}

		if (prec == 0)
		{
			diff = value - whole;
			if (diff > 0.5)
			{
				/* greater than 0.5, round up, e.g. 1.6 -> 2 */
				++whole;
			} else if (diff == 0.5 && (whole & 1))
			{
				/* exactly 0.5 and ODD, then round up */
				/* 1.5 -> 2, but 2.5 -> 2 */
				++whole;
			}

			//vvvvvvvvvvvvvvvvvvv  Diff from modp_dto2
		} else if (frac)
		{
			count = prec;
			// now do fractional part, as an unsigned number
			// we know it is not 0 but we can have leading zeros, these
			// should be removed
			while (!(frac % 10))
			{
				--count;
				frac /= 10;
			}
			//^^^^^^^^^^^^^^^^^^^  Diff from modp_dto2

			// now do fractional part, as an unsigned number
			do
			{
				--count;
				ensure_space(str, slen, wstr);
				*wstr++ = (char) (48 + (frac % 10));
			} while (frac /= 10);
			// add extra 0s
			while (count-- > 0){
				ensure_space(str, slen, wstr);
				*wstr++ = '0';
			}
			// add decimal
			ensure_space(str, slen, wstr);
			*wstr++ = '.';
		}

		// do whole part
		// Take care of sign
		// Conversion. Number is reversed.
		do
		{
			ensure_space(str, slen, wstr);
			*wstr++ = (char) (48 + (whole % 10));
		} while (whole /= 10);
		if (neg)
		{
			ensure_space(str, slen, wstr);
			*wstr++ = '-';
		}
		ensure_space(str, slen, wstr);
		*wstr = '\0';
		strreverse(str, wstr - 1);
		result.assign(str, (wstr-str));
		free(str);
		return;
	}

	int fast_itoa(char* dst, uint32 dstlen, uint64 value)
	{
		static const char digits[201] =
				"0001020304050607080910111213141516171819"
						"2021222324252627282930313233343536373839"
						"4041424344454647484950515253545556575859"
						"6061626364656667686970717273747576777879"
						"8081828384858687888990919293949596979899";
		uint32_t const length = digits10(value);
		if (length >= dstlen)
		{
			return -1;
		}
		uint32_t next = length - 1;
		while (value >= 100)
		{
			uint32 i = (value % 100) * 2;
			value /= 100;
			dst[next] = digits[i + 1];
			dst[next - 1] = digits[i];
			next -= 2;
		}
		// Handle last 1-2 digits
		if (value < 10)
		{
			dst[next] = '0' + uint32_t(value);
		} else
		{
			uint32 i = uint32(value) * 2;
			dst[next] = digits[i + 1];
			dst[next - 1] = digits[i];
		}
		return length;
	}
}
