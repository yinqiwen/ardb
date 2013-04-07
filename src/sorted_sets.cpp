/*
 * sorted_sets.cpp
 *
 *  Created on: 2013-4-2
 *      Author: wqy
 */

#include "rddb.hpp"
#include <float.h>
#include <tr1/unordered_set>
#include <tr1/unordered_map>

namespace rddb
{
	static int parse_score(const std::string& score_str, double& score,
			bool& contain)
	{
		contain = true;
		const char* str = score_str.c_str();
		if (score_str.at(0) == '(')
		{
			contain = false;
			str++;
		}
		if (strcasecmp(str, "-inf") == 0)
		{
			score = -DBL_MAX;
		} else if (strcasecmp(str, "+inf") == 0)
		{
			score = DBL_MAX;
		} else
		{
			if (!str_todouble(str, score))
			{
				return ERR_INVALID_STR;
			}
		}
		return 0;
	}

	static bool DecodeZSetMetaData(ValueObject& v, ZSetMetaValue& meta)
	{
		if (v.type != RAW)
		{
			return false;
		}
		return BufferHelper::ReadVarUInt32(*(v.v.raw), meta.size)
				&& BufferHelper::ReadVarDouble(*(v.v.raw), meta.min_score)
				&& BufferHelper::ReadVarDouble(*(v.v.raw), meta.max_score);
	}
	static void EncodeZSetMetaData(ValueObject& v, ZSetMetaValue& meta)
	{
		v.type = RAW;
		if (v.v.raw == NULL)
		{
			v.v.raw = new Buffer(16);
		}
		BufferHelper::WriteVarUInt32(*(v.v.raw), meta.size);
		BufferHelper::WriteVarDouble(*(v.v.raw), meta.min_score);
		BufferHelper::WriteVarDouble(*(v.v.raw), meta.max_score);
	}

	int RDDB::ZAdd(DBID db, const Slice& key, double score, const Slice& value)
	{
		KeyObject k(key, ZSET_META);
		ValueObject v;
		ZSetMetaValue meta;
		if (0 == GetValue(db, k, v))
		{
			if (!DecodeZSetMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
		}
		if (score > meta.max_score)
		{
			meta.max_score = score;
		}
		if (score < meta.min_score)
		{
			meta.min_score = score;
		}

		ZSetScoreKeyObject zk(key, value);
		ValueObject zv;
		if (0 != GetValue(db, zk, zv))
		{
			meta.size++;
			zv.type = DOUBLE;
			zv.v.double_v = score;
			BatchWriteGuard guard(GetDB(db));
			SetValue(db, zk, zv);
			ZSetKeyObject zsk(key, value, score);
			ValueObject zsv;
			zsv.type = EMPTY;
			SetValue(db, zsk, zsv);
			EncodeZSetMetaData(v, meta);
			return SetValue(db, k, v) == 0 ? 1 : -1;
		} else
		{
			if (zv.v.double_v != score)
			{
				BatchWriteGuard guard(GetDB(db));
				ZSetKeyObject zsk(key, value, zv.v.double_v);
				DelValue(db, zsk);
				zsk.score = score;
				ValueObject zsv;
				zsv.type = EMPTY;
				SetValue(db, zsk, zsv);
				zv.type = DOUBLE;
				zv.v.double_v = score;
				return SetValue(db, zk, zv);
			}
		}
		return 0;
	}

	void RDDB::SetZSetMetaValue(DBID db, const Slice& key, ZSetMetaValue& meta)
	{
		KeyObject k(key, ZSET_META);
		ValueObject v;
		EncodeZSetMetaData(v, meta);
		SetValue(db, k, v);
	}

	int RDDB::GetZSetMetaValue(DBID db, const Slice& key, ZSetMetaValue& meta)
	{
		KeyObject k(key, ZSET_META);
		ValueObject v;
		if (0 == GetValue(db, k, v))
		{
			if (!DecodeZSetMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
			return 0;
		}
		return ERR_NOT_EXIST;
	}

	int RDDB::ZCard(DBID db, const Slice& key)
	{
		ZSetMetaValue meta;
		if (0 == GetZSetMetaValue(db, key, meta))
		{
			return meta.size;
		}
		return ERR_NOT_EXIST;
	}

	int RDDB::ZScore(DBID db, const Slice& key, const Slice& value,
			double& score)
	{
		ZSetScoreKeyObject zk(key, value);
		ValueObject zv;
		if (0 != GetValue(db, zk, zv))
		{
			return ERR_NOT_EXIST;
		}
		score = zv.v.double_v;
		return 0;
	}

	int RDDB::ZIncrby(DBID db, const Slice& key, double increment,
			const Slice& value, double& score)
	{
		ZSetScoreKeyObject zk(key, value);
		ValueObject zv;
		if (0 == GetValue(db, zk, zv))
		{
			BatchWriteGuard guard(GetDB(db));
			SetValue(db, zk, zv);
			ZSetKeyObject zsk(key, value, zv.v.double_v);
			DelValue(db, zsk);
			zsk.score += increment;
			ValueObject zsv;
			zsv.type = EMPTY;
			SetValue(db, zsk, zsv);
			score = zsk.score;
			return 0;
		}
		return ERR_NOT_EXIST;
	}

	int RDDB::ZClear(DBID db, const Slice& key)
	{
		Slice empty;
		ZSetKeyObject sk(key, empty, -DBL_MAX);
		BatchWriteGuard guard(GetDB(db));
		struct ZClearWalk: public WalkHandler
		{
				RDDB* z_db;
				DBID z_dbid;
				int OnKeyValue(KeyObject* k, ValueObject* value)
				{
					ZSetKeyObject* sek = (ZSetKeyObject*) k;
					ZSetScoreKeyObject tmp(sek->key, sek->value);
					z_db->DelValue(z_dbid, *sek);
					z_db->DelValue(z_dbid, tmp);
					return 0;
				}
				ZClearWalk(RDDB* db, DBID id) :
						z_db(db), z_dbid(id)
				{
				}
		} walk(this, db);
		Walk(db, sk, false, &walk);
		KeyObject k(key, ZSET_META);
		DelValue(db, k);
		return 0;
	}

	int RDDB::ZRem(DBID db, const Slice& key, const Slice& value)
	{
		ZSetScoreKeyObject zk(key, value);
		ValueObject zv;
		if (0 == GetValue(db, zk, zv))
		{
			BatchWriteGuard guard(GetDB(db));
			DelValue(db, zk);
			ZSetKeyObject zsk(key, value, zv.v.double_v);
			DelValue(db, zsk);
			ZSetMetaValue meta;
			if (0 == GetZSetMetaValue(db, key, meta))
			{
				if (meta.size > 1)
				{
					meta.size--;
					SetZSetMetaValue(db, key, meta);
				}
			}
			return 0;
		}
		return ERR_NOT_EXIST;
	}

	int RDDB::ZCount(DBID db, const Slice& key, const std::string& min,
			const std::string& max)
	{
		bool containmin = true;
		bool containmax = true;
		double min_score, max_score;
		if (parse_score(min, min_score, containmin) < 0
				|| parse_score(max, max_score, containmax) < 0)
		{
			return ERR_INVALID_ARGS;
		}

		Slice empty;
		ZSetKeyObject start(key, empty, min_score);
		struct ZCountWalk: public WalkHandler
		{
				double z_min_score;
				bool z_containmin;
				double z_max_score;
				bool z_containmax;
				int count;
				int OnKeyValue(KeyObject* k, ValueObject* v)
				{
					ZSetKeyObject* zko = (ZSetKeyObject*) k;
					if (zko->score > z_min_score && zko->score < z_max_score)
					{
						count++;
					}
					if (z_containmin && zko->score == z_min_score)
					{
						count++;
					}
					if (z_containmax && zko->score == z_max_score)
					{
						count++;
					}
					return 0;
				}
		} walk;
		walk.z_min_score = min_score;
		walk.z_max_score = max_score;
		walk.z_containmin = containmin;
		walk.z_containmax = containmax;
		walk.count = 0;
		Walk(db, start, false, &walk);
		return walk.count;
	}

	int RDDB::ZRank(DBID db, const Slice& key, const Slice& member)
	{
		ZSetMetaValue meta;
		if (0 != GetZSetMetaValue(db, key, meta))
		{
			return ERR_NOT_EXIST;
		}
		Slice empty;
		ZSetKeyObject start(key, empty, meta.min_score);
		struct ZRankWalk: public WalkHandler
		{
				int rank;
				const Slice& z_member;
				int foundRank;
				int OnKeyValue(KeyObject* k, ValueObject* v)
				{
					if (rank < 0)
						rank = 0;
					ZSetKeyObject* zko = (ZSetKeyObject*) k;
					if (zko->value.compare(z_member) == 0)
					{
						foundRank = rank;
						return -1;
					}
					rank++;
					return 0;
				}
				ZRankWalk(const Slice& m) :
						rank(0), z_member(m), foundRank(-1)
				{
				}
		} walk(member);
		Walk(db, start, false, &walk);
		return walk.foundRank;
	}

	int RDDB::ZRevRank(DBID db, const Slice& key, const Slice& member)
	{
		ZSetMetaValue meta;
		if (0 != GetZSetMetaValue(db, key, meta))
		{
			return ERR_NOT_EXIST;
		}
		Slice empty;
		ZSetKeyObject start(key, empty, meta.max_score + 1);
		struct ZRevRankWalk: public WalkHandler
		{
				int rank;
				const Slice& z_member;
				int foundRank;
				int OnKeyValue(KeyObject* k, ValueObject* v)
				{
					if (rank < 0)
						rank = 0;
					ZSetKeyObject* zko = (ZSetKeyObject*) k;
					if (zko->value.compare(z_member) == 0)
					{
						foundRank = rank;
						return -1;
					}
					rank++;
					return 0;
				}
				ZRevRankWalk(const Slice& m) :
						rank(0), z_member(m), foundRank(-1)
				{
				}
		} walk(member);
		Walk(db, start, true, &walk);
		return walk.foundRank;
	}

	int RDDB::ZRemRangeByRank(DBID db, const Slice& key, int start, int stop)
	{
		ZSetMetaValue meta;
		if (0 != GetZSetMetaValue(db, key, meta))
		{
			return ERR_NOT_EXIST;
		}
		if (start < 0)
		{
			start = start + meta.size;
		}
		if (stop < 0)
		{
			stop = stop + meta.size;
		}
		if (start < 0 || stop < 0 || start >= meta.size)
		{
			return ERR_INVALID_ARGS;
		}
		Slice empty;
		ZSetKeyObject tmp(key, empty, meta.min_score);
		BatchWriteGuard guard(GetDB(db));
		struct ZRemRangeByRankWalk: public WalkHandler
		{
				int rank;
				RDDB* z_db;
				DBID z_dbid;
				int z_start;
				int z_stop;
				ZSetMetaValue& z_meta;
				int z_count;
				int OnKeyValue(KeyObject* k, ValueObject* v)
				{
					ZSetKeyObject* zsk = (ZSetKeyObject*) k;
					if (rank >= z_start && rank <= z_stop)
					{
						ZSetScoreKeyObject zk(zsk->key, zsk->value);
						z_db->DelValue(z_dbid, zk);
						z_db->DelValue(z_dbid, *zsk);
						z_meta.size--;
						z_count++;
					}
					rank++;
					if (rank == z_stop)
					{
						return -1;
					}
					return 0;
				}
				ZRemRangeByRankWalk(RDDB* db, DBID dbid, int start, int stop,
						ZSetMetaValue& meta) :
						rank(0), z_db(db), z_dbid(z_dbid), z_start(start), z_stop(
								stop), z_meta(meta), z_count(0)
				{
				}
		} walk(this, db, start, stop, meta);
		Walk(db, tmp, false, &walk);
		SetZSetMetaValue(db, key, meta);
		return walk.z_count;
	}

	int RDDB::ZRemRangeByScore(DBID db, const Slice& key,
			const std::string& min, const std::string& max)
	{
		ZSetMetaValue meta;
		if (0 != GetZSetMetaValue(db, key, meta))
		{
			return ERR_NOT_EXIST;
		}
		bool containmin = true;
		bool containmax = true;
		double min_score, max_score;
		if (parse_score(min, min_score, containmin) < 0
				|| parse_score(max, max_score, containmax) < 0)
		{
			return ERR_INVALID_ARGS;
		}
		Slice empty;
		ZSetKeyObject tmp(key, empty, min_score);
		BatchWriteGuard guard(GetDB(db));
		struct ZRemRangeByScoreWalk: public WalkHandler
		{
				RDDB* z_db;
				DBID z_dbid;
				double z_min_score;
				bool z_containmin;
				bool z_containmax;
				double z_max_score;
				ZSetMetaValue& z_meta;
				int z_count;
				int OnKeyValue(KeyObject* k, ValueObject* v)
				{
					ZSetKeyObject* zsk = (ZSetKeyObject*) k;
					bool need_delete = false;
					need_delete =
							z_containmin ?
									zsk->score >= z_min_score :
									zsk->score > z_min_score;
					if (need_delete)
					{
						need_delete =
								z_containmax ?
										zsk->score <= z_max_score :
										zsk->score < z_max_score;
					}
					if (need_delete)
					{
						ZSetScoreKeyObject zk(zsk->key, zsk->value);
						z_db->DelValue(z_dbid, zk);
						z_db->DelValue(z_dbid, *zsk);
						z_meta.size--;
						z_count++;
					}
					if (zsk->score == z_max_score)
					{
						return -1;
					}
					return 0;
				}
				ZRemRangeByScoreWalk(RDDB* db, DBID dbid, ZSetMetaValue& meta) :
						z_db(db), z_dbid(z_dbid), z_meta(meta), z_count(0)
				{
				}
		} walk(this, db, meta);
		walk.z_containmax = containmax;
		walk.z_containmin = containmin;
		walk.z_max_score = max_score;
		walk.z_min_score = min_score;
		Walk(db, tmp, false, &walk);
		SetZSetMetaValue(db, key, meta);
		return walk.z_count;
	}

	int RDDB::ZRange(DBID db, const Slice& key, int start, int stop,
			StringArray& values, RDDBQueryOptions& options)
	{
		ZSetMetaValue meta;
		if (0 != GetZSetMetaValue(db, key, meta))
		{
			return ERR_NOT_EXIST;
		}
		if (start < 0)
		{
			start = start + meta.size;
		}
		if (stop < 0)
		{
			stop = stop + meta.size;
		}
		if (start < 0 || stop < 0 || start >= meta.size)
		{
			return ERR_INVALID_ARGS;
		}
		Slice empty;
		ZSetKeyObject tmp(key, empty, meta.min_score);
		struct ZRangeWalk: public WalkHandler
		{
				int rank;
				int z_start;
				int z_stop;
				StringArray& z_values;
				RDDBQueryOptions& z_options;
				int z_count;
				int OnKeyValue(KeyObject* k, ValueObject* v)
				{
					ZSetKeyObject* zsk = (ZSetKeyObject*) k;
					if (rank >= z_start && rank <= z_stop)
					{
						z_values.push_back(
								std::string(zsk->value.data(),
										zsk->value.size()));
						if (z_options.withscores)
						{
							char tmp[128];
							snprintf(tmp, sizeof(tmp) - 1, "%f", zsk->score);
							z_values.push_back(tmp);
						}
						z_count++;
					}
					rank++;
					if (rank == z_stop)
					{
						return -1;
					}
					return 0;
				}
				ZRangeWalk(int start, int stop, StringArray& v,
						RDDBQueryOptions& options) :
						rank(0), z_start(start), z_stop(stop), z_values(v), z_options(
								options), z_count(0)
				{
				}
		} walk(start, stop, values, options);
		Walk(db, tmp, false, &walk);
		return walk.z_count;
	}

	int RDDB::ZRangeByScore(DBID db, const Slice& key, const std::string& min,
			const std::string& max, StringArray& values,
			RDDBQueryOptions& options)
	{
		ZSetMetaValue meta;
		if (0 != GetZSetMetaValue(db, key, meta))
		{
			return ERR_NOT_EXIST;
		}
		bool containmin = true;
		bool containmax = true;
		double min_score, max_score;
		if (parse_score(min, min_score, containmin) < 0
				|| parse_score(max, max_score, containmax) < 0)
		{
			return ERR_INVALID_ARGS;
		}
		Slice empty;
		ZSetKeyObject tmp(key, empty, min_score);
		struct ZRangeByScoreWalk: public WalkHandler
		{
				StringArray& z_values;
				RDDBQueryOptions& z_options;
				double z_min_score;
				bool z_containmin;
				bool z_containmax;
				double z_max_score;
				int z_count;
				int OnKeyValue(KeyObject* k, ValueObject* v)
				{
					ZSetKeyObject* zsk = (ZSetKeyObject*) k;
					bool inrange = false;
					inrange =
							z_containmin ?
									zsk->score >= z_min_score :
									zsk->score > z_min_score;
					if (inrange)
					{
						inrange =
								z_containmax ?
										zsk->score <= z_max_score :
										zsk->score < z_max_score;
					}
					if (inrange)
					{
						if (z_options.withlimit)
						{
							if (z_count >= z_options.limit_offset
									&& z_count
											<= (z_options.limit_count
													+ z_options.limit_offset))
							{
								inrange = true;
							} else
							{
								inrange = false;
							}
						}
						z_count++;
						if (inrange)
						{
							z_values.push_back(
									std::string(zsk->value.data(),
											zsk->value.size()));
							if (z_options.withscores)
							{
								char tmp[128];
								snprintf(tmp, sizeof(tmp) - 1, "%f",
										zsk->score);
								z_values.push_back(tmp);
							}
						}
					}
					if (zsk->score == z_max_score
							|| (z_options.withlimit
									&& z_count
											> (z_options.limit_count
													+ z_options.limit_offset)))
					{
						return -1;
					}
					return 0;
				}
				ZRangeByScoreWalk(StringArray& v, RDDBQueryOptions& options) :
						z_values(v), z_options(options), z_count(0)
				{
				}
		} walk(values, options);
		walk.z_containmax = containmax;
		walk.z_containmin = containmin;
		walk.z_max_score = max_score;
		walk.z_min_score = min_score;
		Walk(db, tmp, false, &walk);
		return walk.z_count;
	}

	int RDDB::ZRevRange(DBID db, const Slice& key, int start, int stop,
			StringArray& values, RDDBQueryOptions& options)
	{
		ZSetMetaValue meta;
		if (0 != GetZSetMetaValue(db, key, meta))
		{
			return ERR_NOT_EXIST;
		}
		if (start < 0)
		{
			start = start + meta.size;
		}
		if (stop < 0)
		{
			stop = stop + meta.size;
		}
		if (start < 0 || stop < 0 || start >= meta.size)
		{
			return ERR_INVALID_ARGS;
		}
		Slice empty;
		ZSetKeyObject tmp(key, empty, meta.max_score + 1);
		struct ZRevRangeWalk: public WalkHandler
		{
				int rank;
				int count;
				int z_start;
				int z_stop;
				StringArray& z_values;
				RDDBQueryOptions& z_options;
				ZRevRangeWalk(int start, int stop, StringArray& values,
						RDDBQueryOptions& options) :
						rank(0), count(0), z_start(start), z_stop(stop), z_values(
								values), z_options(options)
				{
				}
				int OnKeyValue(KeyObject* k, ValueObject* v)
				{
					ZSetKeyObject* zsk = (ZSetKeyObject*) k;
					if (rank >= z_start && rank <= z_stop)
					{
						z_values.push_back(
								std::string(zsk->value.data(),
										zsk->value.size()));
						if (z_options.withscores)
						{
							char tmp[128];
							snprintf(tmp, sizeof(tmp) - 1, "%f", zsk->score);
							z_values.push_back(tmp);
						}
						count++;
					}
					rank++;
					if (rank == z_stop)
					{
						return -1;
					}
					return 0;
				}
		} walk(start, stop, values, options);
		Walk(db, tmp, true, &walk);
		return walk.count;
	}

	int RDDB::ZRevRangeByScore(DBID db, const Slice& key,
			const std::string& min, const std::string& max, StringArray& values,
			RDDBQueryOptions& options)
	{
		ZSetMetaValue meta;
		if (0 != GetZSetMetaValue(db, key, meta))
		{
			return ERR_NOT_EXIST;
		}
		bool containmin = true;
		bool containmax = true;
		double min_score, max_score;
		if (parse_score(min, min_score, containmin) < 0
				|| parse_score(max, max_score, containmax) < 0)
		{
			return ERR_INVALID_ARGS;
		}
		Slice empty;
		ZSetKeyObject tmp(key, empty, max_score + 1);
		struct ZRangeByScoreWalk: public WalkHandler
		{
				StringArray& z_values;
				RDDBQueryOptions& z_options;
				double z_min_score;
				bool z_containmin;
				bool z_containmax;
				double z_max_score;
				int z_count;
				int OnKeyValue(KeyObject* k, ValueObject* v)
				{
					ZSetKeyObject* zsk = (ZSetKeyObject*) k;
					bool inrange = false;
					inrange =
							z_containmin ?
									zsk->score >= z_min_score :
									zsk->score > z_min_score;
					if (inrange)
					{
						inrange =
								z_containmax ?
										zsk->score <= z_max_score :
										zsk->score < z_max_score;
					}
					if (inrange)
					{
						if (z_options.withlimit)
						{
							if (z_count >= z_options.limit_offset
									&& z_count
											<= (z_options.limit_count
													+ z_options.limit_offset))
							{
								inrange = true;
							} else
							{
								inrange = false;
							}
						}
						z_count++;
						if (inrange)
						{
							z_values.push_back(
									std::string(zsk->value.data(),
											zsk->value.size()));
							if (z_options.withscores)
							{
								char tmp[128];
								snprintf(tmp, sizeof(tmp) - 1, "%f",
										zsk->score);
								z_values.push_back(tmp);
							}
						}
					}
					if (zsk->score == z_min_score
							|| (z_options.withlimit
									&& z_count
											> (z_options.limit_count
													+ z_options.limit_offset)))
					{
						return -1;
					}
					return 0;
				}
				ZRangeByScoreWalk(StringArray& v, RDDBQueryOptions& options) :
						z_values(v), z_options(options), z_count(0)
				{
				}
		} walk(values, options);
		walk.z_containmax = containmax;
		walk.z_containmin = containmin;
		walk.z_max_score = max_score;
		walk.z_min_score = min_score;
		Walk(db, tmp, true, &walk);
		return walk.z_count;
	}

	int RDDB::ZUnionStore(DBID db, const Slice& dst, SliceArray& keys,
			WeightArray& weights, AggregateType type)
	{
		while (weights.size() < keys.size())
		{
			weights.push_back(1);
		}
		typedef std::tr1::unordered_map<std::string, double> ValueScoreMap;
		ValueScoreMap vm;
		struct ZUnionWalk: public WalkHandler
		{
				uint32_t z_weight;
				ValueScoreMap& z_vm;
				AggregateType z_aggre_type;
				ZUnionWalk(uint32_t ws, ValueScoreMap& vm, AggregateType type) :
						z_weight(ws), z_vm(vm), z_aggre_type(type)
				{
				}
				int OnKeyValue(KeyObject* k, ValueObject* value)
				{
					ZSetKeyObject* zsk = (ZSetKeyObject*) k;
					std::string vstr(zsk->value.data(), zsk->value.size());
					double score = 0;
					switch (z_aggre_type)
					{
						case AGGREGATE_MIN:
						{
							score = z_weight * zsk->score;
							if (score < z_vm[vstr])
							{
								z_vm[vstr] = score;
							}
							break;
						}
						case AGGREGATE_MAX:
						{
							score = z_weight * zsk->score;
							if (score > z_vm[vstr])
							{
								z_vm[vstr] = score;
							}
							break;
						}
						case AGGREGATE_SUM:
						default:
						{
							score = z_weight * zsk->score + z_vm[vstr];
							z_vm[vstr] = score;
							break;
						}
					}
					return 0;
				}
		};
		SliceArray::iterator kit = keys.begin();
		uint32_t idx = 0;
		while (kit != keys.end())
		{
			ZSetMetaValue meta;
			if (0 == GetZSetMetaValue(db, *kit, meta))
			{
				Slice empty;
				ZSetKeyObject tmp(*kit, empty, meta.min_score);
				ZUnionWalk walk(weights[idx], vm, type);
				Walk(db, tmp, false, &walk);
			}
			idx++;
			kit++;
		}
		if (vm.size() > 0)
		{
			double min_score = 0, max_score = 0;
			BatchWriteGuard guard(GetDB(db));
			ZClear(db, dst);
			ValueScoreMap::iterator it = vm.begin();
			while (it != vm.end())
			{
				Slice v(it->first);
				ZSetKeyObject zsk(dst, v, it->second);
				ValueObject zsv;
				zsv.type = EMPTY;
				ZSetScoreKeyObject zk(dst, v);
				ValueObject zv;
				zv.type = DOUBLE;
				zv.v.double_v = it->second;
				if (zv.v.double_v < min_score)
				{
					min_score = zv.v.double_v;
				}
				if (zv.v.double_v > max_score)
				{
					max_score = zv.v.double_v;
				}
				SetValue(db, zsk, zsv);
				SetValue(db, zk, zv);
				it++;
			}
			ZSetMetaValue meta;
			meta.size = vm.size();
			meta.min_score = min_score;
			meta.max_score = max_score;
			SetZSetMetaValue(db, dst, meta);
		}
		return vm.size();
	}

	int RDDB::ZInterStore(DBID db, const Slice& dst, SliceArray& keys,
			WeightArray& weights, AggregateType type)
	{
		while (weights.size() < keys.size())
		{
			weights.push_back(1);
		}
		uint32_t min_size = 0;
		typedef std::vector<ZSetMetaValue> ZSetMetaValueArray;
		ZSetMetaValueArray metas;
		uint32_t min_idx = 0;
		uint32_t idx = 0;
		SliceArray::iterator kit = keys.begin();
		while (kit != keys.end())
		{
			ZSetMetaValue meta;
			if (0 == GetZSetMetaValue(db, *kit, meta))
			{
				if (min_size == 0 || min_size > meta.size)
				{
					min_size = meta.size;
					min_idx = idx;
				}
			}
			metas.push_back(meta);
			kit++;
			idx++;
		}

		typedef std::tr1::unordered_map<std::string, double> ValueScoreMap;
		struct ZInterWalk: public WalkHandler
		{
				uint32_t z_weight;
				ValueScoreMap& z_cmp;
				ValueScoreMap& z_result;

				AggregateType z_aggre_type;
				ZInterWalk(uint32_t weight, ValueScoreMap& cmp,
						ValueScoreMap& result, AggregateType type) :
						z_weight(weight), z_cmp(cmp), z_result(result), z_aggre_type(
								type)
				{
				}
				int OnKeyValue(KeyObject* k, ValueObject* value)
				{
					ZSetKeyObject* zsk = (ZSetKeyObject*) k;
					std::string vstr(zsk->value.data(), zsk->value.size());
					if ((&z_cmp != &z_result) && z_cmp.count(vstr) == 0)
					{
						return 0;
					}
					switch (z_aggre_type)
					{
						case AGGREGATE_MIN:
						{
							double score = z_weight * zsk->score;
							ValueScoreMap::iterator it = z_cmp.find(vstr);
							if (it == z_cmp.end() || score < it->second)
							{
								z_result[vstr] = score;
							}
							break;
						}
						case AGGREGATE_MAX:
						{
							double score = z_weight * zsk->score;
							ValueScoreMap::iterator it = z_cmp.find(vstr);
							if (it == z_cmp.end() || score > it->second)
							{
								z_result[vstr] = score;
							}
							break;
						}
						case AGGREGATE_SUM:
						default:
						{
							z_result[vstr] = z_weight * zsk->score
									+ z_cmp[vstr];
							break;
						}
					}
					return 0;
				}
		};
		ValueScoreMap cmp1, cmp2;
		Slice empty;
		ZSetKeyObject cmp_start(keys[min_idx], empty, metas[min_idx].min_score);
		ZInterWalk walk(weights[idx], cmp1, cmp1, type);
		Walk(db, cmp_start, false, &walk);
		ValueScoreMap* cmp = &cmp1;
		ValueScoreMap* result = &cmp2;
		for (uint32_t i = 0; i < keys.size(); i++)
		{
			if (i != min_idx)
			{
				Slice empty;
				ZSetKeyObject tmp(keys.at(i), empty, metas[i].min_score);
				ZInterWalk walk(weights[i], *cmp, *result, type);
				Walk(db, tmp, false, &walk);
				cmp->clear();
				ValueScoreMap* old = cmp;
				cmp = result;
				result = old;
			}
		}

		if (cmp->size() > 0)
		{
			double min_score = 0, max_score = 0;
			BatchWriteGuard guard(GetDB(db));
			ZClear(db, dst);
			ValueScoreMap::iterator it = cmp->begin();
			while (it != cmp->end())
			{
				Slice v(it->first);
				ZSetKeyObject zsk(dst, v, it->second);
				ValueObject zsv;
				zsv.type = EMPTY;
				ZSetScoreKeyObject zk(dst, v);
				ValueObject zv;
				zv.type = DOUBLE;
				zv.v.double_v = it->second;
				if (zv.v.double_v < min_score)
				{
					min_score = zv.v.double_v;
				}
				if (zv.v.double_v > max_score)
				{
					max_score = zv.v.double_v;
				}
				SetValue(db, zsk, zsv);
				SetValue(db, zk, zv);
				it++;
			}
			ZSetMetaValue meta;
			meta.size = cmp->size();
			meta.min_score = min_score;
			meta.max_score = max_score;
			SetZSetMetaValue(db, dst, meta);
		}
		return cmp->size();
	}
}

