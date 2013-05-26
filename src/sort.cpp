/*
 * sort.cpp
 *
 *  Created on: 2013-4-18
 *      Author: wqy
 */
#include "ardb.hpp"
#include <algorithm>
#include <vector>

namespace ardb
{
	struct SortValue
	{
			ValueObject* value;
			ValueObject cmp;
			double score;
			SortValue(ValueObject* v) :
					value(v), score(0)
			{
			}
			int Compare(const SortValue& other) const
			{
				return value->Compare(*other.value);
			}
	};
	template<typename T>
	static bool greater_value(const T& v1, const T& v2)
	{
		return v1.Compare(v2) > 0;
	}
	template<typename T>
	static bool less_value(const T& v1, const T& v2)
	{
		return v1.Compare(v2) < 0;
	}

	static int parse_sort_options(SortOptions& options, const StringArray& args)
	{
		for (uint32 i = 1; i < args.size(); i++)
		{
			if (!strcasecmp(args[i].c_str(), "asc"))
			{
				options.is_desc = false;
			}
			else if (!strcasecmp(args[i].c_str(), "desc"))
			{
				options.is_desc = true;
			}
			else if (!strcasecmp(args[i].c_str(), "alpha"))
			{
				options.with_alpha = true;
			}
			else if (!strcasecmp(args[i].c_str(), "limit")
			        && i < args.size() - 2)
			{
				options.with_limit = true;
				if (!string_toint32(args[i + 1], options.limit_offset)
				        || !string_toint32(args[i + 2], options.limit_count))
				{
					return -1;
				}
				i += 2;
			}
			else if (!strcasecmp(args[i].c_str(), "store")
			        && i < args.size() - 1)
			{
				options.store_dst = args[i + 1].c_str();
				i++;
			}
			else if (!strcasecmp(args[i].c_str(), "by") && i < args.size() - 1)
			{
				options.by = args[i + 1].c_str();
				if (!strcasecmp(options.by, "nosort"))
				{
					options.by = NULL;
					options.nosort = true;
				}
				i++;
			}
			else if (!strcasecmp(args[i].c_str(), "get") && i < args.size() - 1)
			{
				options.get_patterns.push_back(args[i + 1].c_str());
				i++;
			}
			else
			{
				return -1;
			}
		}
		return 0;
	}

	int Ardb::GetValueByPattern(const DBID& db, const Slice& pattern,
	        ValueObject& subst, ValueObject& value)
	{
		const char *p, *f;
		const char* spat;
		/* If the pattern is "#" return the substitution object itself in order
		 * to implement the "SORT ... GET #" feature. */
		spat = pattern.data();
		if (spat[0] == '#' && spat[1] == '\0')
		{
			value = subst;
			return 0;
		}

		/* If we can't find '*' in the pattern we return NULL as to GET a
		 * fixed key does not make sense. */
		p = strchr(spat, '*');
		if (!p)
		{
			return -1;
		}

		f = strstr(spat, "->");
		if (NULL != f && *(f + 2) != '\0')
		{
			f = NULL;
		}
		std::string vstr;
		subst.ToString(vstr);
		std::string keystr(pattern.data(), pattern.size());
		string_replace(keystr, "*", vstr);
		if (f == NULL)
		{
			KeyObject k(keystr, KV, db);
			return GetValue(k, &value);
		}
		else
		{
			size_t pos = keystr.find("->");
			std::string field = keystr.substr(pos + 2);
			keystr = keystr.substr(0, pos);
			HashKeyObject hk(keystr, field, db);
			return GetValue(hk, &value);
		}
	}

	int Ardb::Sort(const DBID& db, const Slice& key, const StringArray& args,
	        ValueArray& values)
	{
		SortOptions options;
		if (parse_sort_options(options, args) < 0)
		{
			return ERR_INVALID_ARGS;
		}
		int type = Type(db, key);
		ValueArray sortvals;
		switch (type)
		{
			case LIST_META:
			{
				LRange(db, key, 0, -1, sortvals);
				break;
			}
			case SET_ELEMENT:
			{
				SMembers(db, key, sortvals);
				break;
			}
			case ZSET_ELEMENT_SCORE:
			{
				QueryOptions tmp;
				ZRange(db, key, 0, -1, sortvals, tmp);
				break;
			}
			default:
			{
				return ERR_INVALID_TYPE;
			}
		}

		if (sortvals.empty())
		{
			return 0;
		}
		if (options.with_limit)
		{
			if (options.limit_offset < 0)
			{
				options.limit_offset = 0;
			}
			if (options.limit_offset > sortvals.size())
			{
				values.clear();
				return 0;
			}
			if (options.limit_count < 0)
			{
				options.limit_count = sortvals.size();
			}
		}

		std::vector<SortValue> sortvec;
		if (!options.nosort)
		{
			if (NULL != options.by)
			{
				sortvec.reserve(sortvals.size());
			}
			for (uint32 i = 0; i < sortvals.size(); i++)
			{
				if (NULL != options.by)
				{
					sortvec.push_back(SortValue(&sortvals[i]));
					if (GetValueByPattern(db, options.by, sortvals[i],
					        sortvec[i].cmp) < 0)
					{
						sortvec[i].cmp.Clear();
						continue;
					}
				}
				if (options.with_alpha)
				{
					if (NULL != options.by)
					{
						value_convert_to_raw(sortvec[i].cmp);
					}
					else
					{
						value_convert_to_raw(sortvals[i]);
					}
				}
				else
				{
					if (NULL != options.by)
					{
						value_convert_to_number(sortvec[i].cmp);
					}
					else
					{
						value_convert_to_number(sortvals[i]);
					}
				}
			}
			if (NULL != options.by)
			{
				if (!options.is_desc)
				{
					std::sort(sortvec.begin(), sortvec.end(),
					        less_value<SortValue>);
				}
				else
				{
					std::sort(sortvec.begin(), sortvec.end(),
					        greater_value<SortValue>);
				}

			}
			else
			{
				if (!options.is_desc)
				{
					std::sort(sortvals.begin(), sortvals.end(),
					        less_value<ValueObject>);
				}
				else
				{
					std::sort(sortvals.begin(), sortvals.end(),
					        greater_value<ValueObject>);
				}
			}
		}

		if (!options.with_limit)
		{
			options.limit_offset = 0;
			options.limit_count = sortvals.size();
		}

		uint32 count = 0;
		for (uint32 i = options.limit_offset;
		        i < sortvals.size() && count < options.limit_count;
		        i++, count++)
		{
			ValueObject* patternObj = NULL;
			if (NULL != options.by)
			{
				patternObj = sortvec[i].value;
			}
			else
			{
				patternObj = &(sortvals[i]);
			}
			if (options.get_patterns.empty())
			{
				values.push_back(*patternObj);
			}
			else
			{
				for (uint32 j = 0; j < options.get_patterns.size(); j++)
				{
					ValueObject vo;
					if (GetValueByPattern(db, options.get_patterns[j],
					        *patternObj, vo) < 0)
					{
						vo.Clear();
					}
					values.push_back(vo);
				}
			}
		}

		if (options.store_dst != NULL && !values.empty())
		{
			BatchWriteGuard guard(GetEngine());
			LClear(db, options.store_dst);
			ValueArray::iterator it = values.begin();
			uint64 score = 0;

			while (it != values.end())
			{
				if (it->type != EMPTY)
				{
					ListKeyObject lk(options.store_dst, score, db);
					SetValue(lk, *it);
					score++;
				}
				it++;
			}
			ListMetaValue meta;
			meta.min_score = 0;
			meta.max_score = (score - 1);
			meta.size = score;
			SetListMetaValue(db, options.store_dst, meta);
		}
		return 0;
	}
}

