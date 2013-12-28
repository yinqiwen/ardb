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

#include "ardb.hpp"

namespace ardb
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

	ZSetMetaValue* Ardb::GetZSetMeta(const DBID& db, const Slice& key, int& err,
	bool& created)
	{
		CommonMetaValue* meta = GetMeta(db, key, false);
		if (NULL != meta && meta->header.type != ZSET_META)
		{
			DELETE(meta);
			err = ERR_INVALID_TYPE;
			return NULL;
		}
		if (NULL == meta)
		{
			meta = new ZSetMetaValue;
			created = true;
			err = ERR_NOT_EXIST;
		} else
		{
			created = false;
		}
		return (ZSetMetaValue*)meta;
	}

	int Ardb::ZAddLimit(const DBID& db, const Slice& key, DoubleArray& scores,
	        const SliceArray& svs, int setlimit, ValueArray& pops)
	{
		int err = 0;
		bool createZset = false;
		ZSetMetaValue* meta = GetZSetMeta(db, key, err, createZset);
		if (NULL == meta)
		{
			DELETE(meta);
			return err;
		}
		if (setlimit <= 0)
		{
			setlimit = meta->size + scores.size();
		}
		for (uint32 i = 0; i < scores.size(); i++)
		{
			TryZAdd(db, key, *meta, scores[i], svs[i]);
		}
		SetMeta(db, key, *meta);
		if (meta->size > (uint32) setlimit)
		{
			ZPop(db, key, false, meta->size - setlimit, pops);
		}
		DELETE(meta);
		return 0;
	}

	/*
	 * returns -1:no change  0:no meta&zset size change
	 *          1:meta change & no zset size change
	 *          2:meta change & zset size change
	 */
	int Ardb::TryZAdd(const DBID& db, const Slice& key, ZSetMetaValue& meta,
	        double score, const Slice& value)
	{
		bool metachange = false;
		if (score > meta.max_score)
		{
			meta.max_score = score;
			metachange = true;
		}
		if (score < meta.min_score)
		{
			meta.min_score = score;
			metachange = true;
		}
		ZSetScoreKeyObject zk(key, value, db);
		CommonValueObject zv;
		if (0 != GetKeyValueObject(zk, zv))
		{
			meta.size++;
			zv.data.SetValue(score);
			SetKeyValueObject(zk, zv);
			ZSetKeyObject zsk(key, value, score, db);
			EmptyValueObject zsv;
			SetKeyValueObject(zsk, zsv);
			return 2;
		} else
		{
			if (zv.data.double_value != score)
			{
				ZSetKeyObject zsk(key, value, zv.data.double_value, db);
				DelValue(zsk);
				zsk.e.score = score;
				EmptyValueObject zsv;
				SetKeyValueObject(zsk, zsv);
				zv.data.SetValue(score);
				SetKeyValueObject(zk, zv);
				return metachange ? 1 : 0;
			}
		}
		return -1;
	}
	int Ardb::ZAdd(const DBID& db, const Slice& key, double score,
	        const Slice& value)
	{
		DoubleArray scores;
		SliceArray svs;
		scores.push_back(score);
		svs.push_back(value);
		return ZAdd(db, key, scores, svs);
	}
	int Ardb::ZAdd(const DBID& db, const Slice& key, DoubleArray& scores,
	        const SliceArray& svs)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		int err = 0;
		bool createZset = false;
		ZSetMetaValue* meta = GetZSetMeta(db, key, err, createZset);
		if (NULL == meta)
		{
			DELETE(meta);
			return err;
		}
		BatchWriteGuard guard(GetEngine());
		int count = 0;
		bool metachange = false;
		for (uint32 i = 0; i < scores.size(); i++)
		{
			int tryret = TryZAdd(db, key, *meta, scores[i], svs[i]);
			if (tryret == 2)
			{
				count++;
			}
			if (!metachange && tryret > 0)
			{
				metachange = true;
			}
		}
		if (metachange)
		{
			SetMeta(db, key, *meta);
		}
		DELETE(meta);
		return count;
	}

	int Ardb::ZCard(const DBID& db, const Slice& key)
	{
		int err = 0;
		bool createZset = false;
		ZSetMetaValue* meta = GetZSetMeta(db, key, err, createZset);
		if (NULL == meta || createZset)
		{
			DELETE(meta);
			return err;
		}
		int size = meta->size;
		DELETE(meta);
		return size;
	}

	int Ardb::ZScore(const DBID& db, const Slice& key, const Slice& value,
	        double& score)
	{
		ZSetScoreKeyObject zk(key, value, db);
		CommonValueObject zv;
		if (0 != GetKeyValueObject(zk, zv))
		{
			return ERR_NOT_EXIST;
		}
		score = zv.data.double_value;
		return 0;
	}

	int Ardb::ZIncrby(const DBID& db, const Slice& key, double increment,
	        const Slice& value, double& score)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		ZSetScoreKeyObject zk(key, value, db);
		CommonValueObject zv;
		if (0 == GetKeyValueObject(zk, zv))
		{
			BatchWriteGuard guard(GetEngine());
			ZSetKeyObject zsk(key, value, zv.data.double_value, db);
			DelValue(zsk);
			zsk.e.score += increment;
			zv.data.SetValue(zsk.e.score);
			EmptyValueObject zsv;
			SetKeyValueObject(zsk, zsv);
			score = zsk.e.score;
			SetKeyValueObject(zk, zv);
			return 0;
		}
		return ERR_NOT_EXIST;
	}

	int Ardb::ZPop(const DBID& db, const Slice& key, bool reverse, uint32 num,
	        ValueArray& pops)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		int err = 0;
		bool createZset = false;
		ZSetMetaValue* meta = GetZSetMeta(db, key, err, createZset);
		if (NULL == meta || createZset)
		{
			DELETE(meta);
			return err;
		}
		Slice empty;
		ZSetKeyObject sk(key, empty,
		        reverse ? (meta->max_score + 1) : meta->min_score, db);
		BatchWriteGuard guard(GetEngine());
		struct ZPopWalk: public WalkHandler
		{
				Ardb* z_db;
				uint32 count;
				ValueArray& vs;
				int OnKeyValue(KeyObject* k, ValueObject* value, uint32 cursor)
				{
					ZSetKeyObject* sek = (ZSetKeyObject*) k;
					ZSetScoreKeyObject tmp(sek->key, sek->e.value, sek->db);
					vs.push_back(sek->e.value);
					count--;
					z_db->DelValue(*sek);
					z_db->DelValue(tmp);
					if (count == 0)
					{
						return -1;
					}
					return 0;
				}
				ZPopWalk(Ardb* db, uint32 i, ValueArray& v) :
						z_db(db), count(i), vs(v)
				{
				}
		} walk(this, num, pops);
		Walk(sk, reverse, false, &walk);
		if (walk.count < num)
		{
			meta->size -= (num - walk.count);
			SetMeta(db, key, *meta);
		}
		DELETE(meta);
		return 0;
	}

	int Ardb::ZClear(const DBID& db, const Slice& key)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		int err = 0;
		bool createZset = false;
		ZSetMetaValue* meta = GetZSetMeta(db, key, err, createZset);
		if (NULL == meta || createZset)
		{
			DELETE(meta);
			return err;
		}
		Slice empty;
		ZSetKeyObject sk(key, empty, meta->min_score, db);
		BatchWriteGuard guard(GetEngine());
		struct ZClearWalk: public WalkHandler
		{
				Ardb* z_db;
				int OnKeyValue(KeyObject* k, ValueObject* value, uint32 cursor)
				{
					ZSetKeyObject* sek = (ZSetKeyObject*) k;
					ZSetScoreKeyObject tmp(sek->key, sek->e.value, sek->db);
					z_db->DelValue(*sek);
					z_db->DelValue(tmp);
					return 0;
				}
				ZClearWalk(Ardb* db) :
						z_db(db)
				{
				}
		} walk(this);
		Walk(sk, false,false, &walk);
		DelMeta(db, key, meta);
		DELETE(meta);
		return 0;
	}

	int Ardb::ZRem(const DBID& db, const Slice& key, const Slice& value)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		ZSetScoreKeyObject zk(key, value, db);
		CommonValueObject zv;
		if (0 == GetKeyValueObject(zk, zv))
		{
			BatchWriteGuard guard(GetEngine());
			DelValue(zk);
			ZSetKeyObject zsk(key, value, zv.data.double_value, db);
			DelValue(zsk);
			int err = 0;
			bool createZset = false;
			ZSetMetaValue* meta = GetZSetMeta(db, key, err, createZset);
			if (NULL == meta || createZset)
			{
				DELETE(meta);
				return err;
			}
			meta->size--;
			SetMeta(db, key, *meta);
			DELETE(meta);
			return 0;
		}
		return ERR_NOT_EXIST;
	}

	int Ardb::ZCount(const DBID& db, const Slice& key, const std::string& min,
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
		ZSetKeyObject start(key, empty, min_score, db);
		struct ZCountWalk: public WalkHandler
		{
				double z_min_score;bool z_containmin;
				double z_max_score;bool z_containmax;
				int count;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					ZSetKeyObject* zko = (ZSetKeyObject*) k;
					std::string str;
					if (zko->e.score > z_min_score && zko->e.score < z_max_score)
					{
						count++;
					}
					if (z_containmin && zko->e.score == z_min_score)
					{
						count++;
					}
					if (z_containmax && zko->e.score == z_max_score)
					{
						count++;
					}
					if (zko->e.score > z_max_score)
					{
						return -1;
					}
					return 0;
				}
		} walk;
		walk.z_min_score = min_score;
		walk.z_max_score = max_score;
		walk.z_containmin = containmin;
		walk.z_containmax = containmax;
		walk.count = 0;
		Walk(start, false, false, &walk);
		return walk.count;
	}

	int Ardb::ZRank(const DBID& db, const Slice& key, const Slice& member)
	{
		int err = 0;
		bool createZset = false;
		ZSetMetaValue* meta = GetZSetMeta(db, key, err, createZset);
		if (NULL == meta || createZset)
		{
			DELETE(meta);
			return err;
		}
		Slice empty;
		ZSetKeyObject start(key, empty, meta->min_score, db);
		struct ZRankWalk: public WalkHandler
		{
				int rank;
				ValueData z_member;
				int foundRank;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					ZSetKeyObject* zko = (ZSetKeyObject*) k;
					std::string str;
					if (zko->e.value.Compare(z_member) == 0)
					{
						foundRank = rank;
						return -1;
					}
					rank++;
					return 0;
				}
				ZRankWalk(const Slice& m) :
						rank(0), foundRank(ERR_NOT_EXIST)
				{
					z_member.SetValue(m, true);
				}
		} walk(member);
		Walk(start, false, false,&walk);
		DELETE(meta);
		return walk.foundRank;
	}

	int Ardb::ZRevRank(const DBID& db, const Slice& key, const Slice& member)
	{
		int err = 0;
		bool createZset = false;
		ZSetMetaValue* meta = GetZSetMeta(db, key, err, createZset);
		if (NULL == meta || createZset)
		{
			DELETE(meta);
			return err;
		}
		Slice empty;
		ZSetKeyObject start(key, empty, meta->max_score + 1, db);
		struct ZRevRankWalk: public WalkHandler
		{
				int rank;
				ValueData z_member;
				int foundRank;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					if (rank < 0)
						rank = 0;
					ZSetKeyObject* zko = (ZSetKeyObject*) k;
					if (zko->e.value.Compare(z_member) == 0)
					{
						foundRank = rank;
						return -1;
					}
					rank++;
					return 0;
				}
				ZRevRankWalk(const Slice& m) :
						rank(0), foundRank(-1)
				{
					z_member.SetValue(m, true);
				}
		} walk(member);
		Walk(start, true, false,&walk);
		DELETE(meta);
		return walk.foundRank;
	}

	int Ardb::ZRemRangeByRank(const DBID& db, const Slice& key, int start,
	        int stop)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		int err = 0;
		bool createZset = false;
		ZSetMetaValue* meta = GetZSetMeta(db, key, err, createZset);
		if (NULL == meta || createZset)
		{
			DELETE(meta);
			return err;
		}
		if (start < 0)
		{
			start = start + meta->size;
		}
		if (stop < 0)
		{
			stop = stop + meta->size;
		}
		if (start < 0 || stop < 0 || (uint32) start >= meta->size)
		{
			DELETE(meta);
			return ERR_INVALID_ARGS;
		}
		Slice empty;
		ZSetKeyObject tmp(key, empty, meta->min_score, db);
		BatchWriteGuard guard(GetEngine());
		struct ZRemRangeByRankWalk: public WalkHandler
		{
				int rank;
				Ardb* z_db;
				int z_start;
				int z_stop;
				ZSetMetaValue& z_meta;
				int z_count;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					ZSetKeyObject* zsk = (ZSetKeyObject*) k;
					if (rank >= z_start && rank <= z_stop)
					{
						ZSetScoreKeyObject zk(zsk->key, zsk->e.value, zsk->db);
						z_db->DelValue(zk);
						z_db->DelValue(*zsk);
						z_meta.size--;
						z_count++;
					}
					rank++;
					if (rank > z_stop)
					{
						return -1;
					}
					return 0;
				}
				ZRemRangeByRankWalk(Ardb* db, int start, int stop,
				        ZSetMetaValue& meta) :
						rank(0), z_db(db), z_start(start), z_stop(stop), z_meta(
						        meta), z_count(0)
				{
				}
		} walk(this, start, stop, *meta);
		Walk(tmp, false,false, &walk);
		SetMeta(db, key, *meta);
		DELETE(meta);
		return walk.z_count;
	}

	int Ardb::ZRemRangeByScore(const DBID& db, const Slice& key,
	        const std::string& min, const std::string& max)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		int err = 0;
		bool createZset = false;
		ZSetMetaValue* meta = GetZSetMeta(db, key, err, createZset);
		if (NULL == meta || createZset)
		{
			DELETE(meta);
			return err;
		}
		bool containmin = true;
		bool containmax = true;
		double min_score, max_score;
		if (parse_score(min, min_score, containmin) < 0
		        || parse_score(max, max_score, containmax) < 0)
		{
			DELETE(meta);
			return ERR_INVALID_ARGS;
		}
		Slice empty;
		ZSetKeyObject tmp(key, empty, min_score, db);
		BatchWriteGuard guard(GetEngine());
		struct ZRemRangeByScoreWalk: public WalkHandler
		{
				Ardb* z_db;
				double z_min_score;bool z_containmin;bool z_containmax;
				double z_max_score;
				ZSetMetaValue& z_meta;
				int z_count;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					ZSetKeyObject* zsk = (ZSetKeyObject*) k;
					bool need_delete = false;
					need_delete =
					        z_containmin ?
					                zsk->e.score >= z_min_score :
					                zsk->e.score > z_min_score;
					if (need_delete)
					{
						need_delete =
						        z_containmax ?
						                zsk->e.score <= z_max_score :
						                zsk->e.score < z_max_score;
					}
					if (need_delete)
					{
						ZSetScoreKeyObject zk(zsk->key, zsk->e.value, zsk->db);
						z_db->DelValue(zk);
						z_db->DelValue(*zsk);
						z_meta.size--;
						z_count++;
					}
					if (zsk->e.score == z_max_score)
					{
						return -1;
					}
					return 0;
				}
				ZRemRangeByScoreWalk(Ardb* db, ZSetMetaValue& meta) :
						z_db(db), z_meta(meta), z_count(0)
				{
				}
		} walk(this, *meta);
		walk.z_containmax = containmax;
		walk.z_containmin = containmin;
		walk.z_max_score = max_score;
		walk.z_min_score = min_score;
		Walk(tmp, false, false, &walk);
		SetMeta(db, key, *meta);
		DELETE(meta);
		return walk.z_count;
	}

	int Ardb::ZRange(const DBID& db, const Slice& key, int start, int stop,
	        ValueArray& values, QueryOptions& options)
	{
		int err = 0;
		bool createZset = false;
		ZSetMetaValue* meta = GetZSetMeta(db, key, err, createZset);
		if (NULL == meta || createZset)
		{
			DELETE(meta);
			return err;
		}
		if (start < 0)
		{
			start = start + meta->size;
		}
		if (stop < 0)
		{
			stop = stop + meta->size;
		}
		if (start < 0 || stop < 0 || (uint32) start >= meta->size)
		{
			DELETE(meta);
			return ERR_INVALID_ARGS;
		}
		Slice empty;
		ZSetKeyObject tmp(key, empty, meta->min_score, db);

		struct ZRangeWalk: public WalkHandler
		{
				int rank;
				int z_start;
				int z_stop;
				ValueArray& z_values;
				QueryOptions& z_options;
				int z_count;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					ZSetKeyObject* zsk = (ZSetKeyObject*) k;
					if (rank >= z_start && rank <= z_stop)
					{
						z_values.push_back(zsk->e.value);
						if (z_options.withscores)
						{
							ValueData scorev;
							scorev.SetValue(zsk->e.score);
							z_values.push_back(scorev);
						}
						z_count++;
					}
					rank++;
					if (rank > z_stop)
					{
						return -1;
					}
					return 0;
				}
				ZRangeWalk(int start, int stop, ValueArray& v,
				        QueryOptions& options) :
						rank(0), z_start(start), z_stop(stop), z_values(v), z_options(
						        options), z_count(0)
				{
				}
		} walk(start, stop, values, options);
		Walk(tmp, false, false, &walk);
		DELETE(meta);
		return walk.z_count;
	}

	int Ardb::ZRangeByScore(const DBID& db, const Slice& key,
	        const std::string& min, const std::string& max, ValueArray& values,
	        QueryOptions& options)
	{
		int err = 0;
		bool createZset = false;
		ZSetMetaValue* meta = GetZSetMeta(db, key, err, createZset);
		if (NULL == meta || createZset)
		{
			DELETE(meta);
			return err;
		}
		bool containmin = true;
		bool containmax = true;
		double min_score, max_score;
		if (parse_score(min, min_score, containmin) < 0
		        || parse_score(max, max_score, containmax) < 0)
		{
			DELETE(meta);
			return ERR_INVALID_ARGS;
		}
		Slice empty;
		ZSetKeyObject tmp(key, empty, min_score, db);
		struct ZRangeByScoreWalk: public WalkHandler
		{
				ValueArray& z_values;
				QueryOptions& z_options;
				double z_min_score;bool z_containmin;bool z_containmax;
				double z_max_score;
				int z_count;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					ZSetKeyObject* zsk = (ZSetKeyObject*) k;
					bool inrange = false;
					inrange =
					        z_containmin ?
					                zsk->e.score >= z_min_score :
					                zsk->e.score > z_min_score;
					if (inrange)
					{
						inrange =
						        z_containmax ?
						                zsk->e.score <= z_max_score :
						                zsk->e.score < z_max_score;
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
							z_values.push_back(zsk->e.value);
							if (z_options.withscores)
							{
								z_values.push_back(ValueData(zsk->e.score));
							}
						}
					}
					if (zsk->e.score == z_max_score
					        || (z_options.withlimit
					                && z_count
					                        > (z_options.limit_count
					                                + z_options.limit_offset)))
					{
						return -1;
					}
					return 0;
				}
				ZRangeByScoreWalk(ValueArray& v, QueryOptions& options) :
						z_values(v), z_options(options), z_count(0)
				{
				}
		} walk(values, options);
		walk.z_containmax = containmax;
		walk.z_containmin = containmin;
		walk.z_max_score = max_score;
		walk.z_min_score = min_score;
		Walk(tmp, false, false, &walk);
		DELETE(meta);
		return walk.z_count;
	}

	int Ardb::ZRevRange(const DBID& db, const Slice& key, int start, int stop,
	        ValueArray& values, QueryOptions& options)
	{
		int err = 0;
		bool createZset = false;
		ZSetMetaValue* meta = GetZSetMeta(db, key, err, createZset);
		if (NULL == meta || createZset)
		{
			DELETE(meta);
			return err;
		}
		if (start < 0)
		{
			start = start + meta->size;
		}
		if (stop < 0)
		{
			stop = stop + meta->size;
		}
		if (start < 0 || stop < 0 || (uint32) start >= meta->size)
		{
			DELETE(meta);
			return ERR_INVALID_ARGS;
		}
		Slice empty;
		ZSetKeyObject tmp(key, empty, meta->max_score, db);
		struct ZRevRangeWalk: public WalkHandler
		{
				int rank;
				int count;
				int z_start;
				int z_stop;
				ValueArray& z_values;
				QueryOptions& z_options;
				ZRevRangeWalk(int start, int stop, ValueArray& values,
				        QueryOptions& options) :
						rank(0), count(0), z_start(start), z_stop(stop), z_values(
						        values), z_options(options)
				{
				}
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					ZSetKeyObject* zsk = (ZSetKeyObject*) k;
					if (rank >= z_start && rank <= z_stop)
					{
						z_values.push_back(zsk->e.value);
						if (z_options.withscores)
						{
							z_values.push_back(ValueData(zsk->e.score));
						}
						count++;
					}
					rank++;
					if (rank > z_stop)
					{
						return -1;
					}
					return 0;
				}
		} walk(start, stop, values, options);
		Walk(tmp, true, false, &walk);
		DELETE(meta);
		return walk.count;
	}

	int Ardb::ZRevRangeByScore(const DBID& db, const Slice& key,
	        const std::string& max, const std::string& min, ValueArray& values,
	        QueryOptions& options)
	{
		int err = 0;
		bool createZset = false;
		ZSetMetaValue* meta = GetZSetMeta(db, key, err, createZset);
		if (NULL == meta || createZset)
		{
			DELETE(meta);
			return err;
		}
		bool containmin = true;
		bool containmax = true;
		double min_score, max_score;
		if (parse_score(min, min_score, containmin) < 0
		        || parse_score(max, max_score, containmax) < 0)
		{
			DELETE(meta);
			return ERR_INVALID_ARGS;
		}
		Slice empty;

		ZSetKeyObject tmp(key, empty, max_score, db);
		struct ZRangeByScoreWalk: public WalkHandler
		{
				ValueArray& z_values;
				QueryOptions& z_options;
				double z_min_score;bool z_containmin;bool z_containmax;
				double z_max_score;
				int z_count;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					ZSetKeyObject* zsk = (ZSetKeyObject*) k;
					bool inrange = false;
					inrange =
					        z_containmin ?
					                zsk->e.score >= z_min_score :
					                zsk->e.score > z_min_score;
					if (inrange)
					{
						inrange =
						        z_containmax ?
						                zsk->e.score <= z_max_score :
						                zsk->e.score < z_max_score;
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
							z_values.push_back(zsk->e.value);
							if (z_options.withscores)
							{
								z_values.push_back(ValueData(zsk->e.score));
							}
						}
					}
					if (zsk->e.score == z_min_score
					        || (z_options.withlimit
					                && z_count
					                        > (z_options.limit_count
					                                + z_options.limit_offset)))
					{
						return -1;
					}
					return 0;
				}
				ZRangeByScoreWalk(ValueArray& v, QueryOptions& options) :
						z_values(v), z_options(options), z_count(0)
				{
				}
		} walk(values, options);
		walk.z_containmax = containmax;
		walk.z_containmin = containmin;
		walk.z_max_score = max_score;
		walk.z_min_score = min_score;
		Walk(tmp, true,false, &walk);
		DELETE(meta);
		return walk.z_count;
	}

	int Ardb::ZUnionStore(const DBID& db, const Slice& dst, SliceArray& keys,
	        WeightArray& weights, AggregateType type)
	{
		while (weights.size() < keys.size())
		{
			weights.push_back(1);
		}

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
				int OnKeyValue(KeyObject* k, ValueObject* value, uint32 cursor)
				{
					ZSetKeyObject* zsk = (ZSetKeyObject*) k;
					double score = 0;
					switch (z_aggre_type)
					{
						case AGGREGATE_MIN:
						{
							score = z_weight * zsk->e.score;
							if (score < z_vm[zsk->e.value])
							{
								z_vm[zsk->e.value] = score;
							}
							break;
						}
						case AGGREGATE_MAX:
						{
							score = z_weight * (zsk->e.score);
							if (score > z_vm[zsk->e.value])
							{
								z_vm[zsk->e.value] = score;
							}
							break;
						}
						case AGGREGATE_SUM:
						default:
						{
							score = z_weight * (zsk->e.score) + z_vm[zsk->e.value];
							z_vm[zsk->e.value] = score;
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
			int err = 0;
			bool createZset = false;
			ZSetMetaValue* meta = GetZSetMeta(db, *kit, err, createZset);
			if (NULL != meta && !createZset)
			{
				Slice empty;
				ZSetKeyObject tmp(*kit, empty, meta->min_score, db);
				ZUnionWalk walk(weights[idx], vm, type);
				Walk(tmp, false,false, &walk);
			}
			DELETE(meta);
			idx++;
			kit++;
		}
		if (vm.size() > 0)
		{
			double min_score = 0, max_score = 0;
			BatchWriteGuard guard(GetEngine());
			ZClear(db, dst);
			KeyLockerGuard keyguard(m_key_locker, db, dst);
			ZSetMetaValue meta;
			meta.size = vm.size();
			while (!vm.empty())
			{
				ValueScoreMap::iterator it = vm.begin();
				ZSetKeyObject zsk(dst, it->first, it->second, db);
				ValueData zsv;
				ZSetScoreKeyObject zk(dst, it->first, db);
				ValueData zv(it->second);
				if (zv.double_value < min_score)
				{
					min_score = zv.double_value;
				}
				if (zv.double_value > max_score)
				{
					max_score = zv.double_value;
				}
				SetKeyValueObject(zsk, zsv);
				SetKeyValueObject(zk, zv);
				//reduce memory footprint for huge data set
				vm.erase(it);
			}
			meta.min_score = min_score;
			meta.max_score = max_score;
			SetMeta(db, dst, meta);
		}
		return vm.size();
	}

	int Ardb::ZInterStore(const DBID& db, const Slice& dst, SliceArray& keys,
	        WeightArray& weights, AggregateType type)
	{
		while (weights.size() < keys.size())
		{
			weights.push_back(1);
		}
		uint32_t min_size = 0;
		ZSetMetaValueArray metas;
		uint32_t min_idx = 0;
		uint32_t idx = 0;
		SliceArray::iterator kit = keys.begin();
		while (kit != keys.end())
		{
			int err = 0;
			bool createZset = false;
			ZSetMetaValue* meta = GetZSetMeta(db, *kit, err, createZset);
			if (NULL == meta || createZset)
			{
				if (createZset)
				{
					err = 0;
				}
				DELETE(meta);
				delete_pointer_container(metas);
				return err;
			}
			if (min_size == 0 || min_size > meta->size)
			{
				min_size = meta->size;
				min_idx = idx;
			}
			metas.push_back(meta);
			kit++;
			idx++;
		}

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
				int OnKeyValue(KeyObject* k, ValueObject* value, uint32 cursor)
				{
					ZSetKeyObject* zsk = (ZSetKeyObject*) k;
					if ((&z_cmp != &z_result) && z_cmp.count(zsk->e.value) == 0)
					{
						return 0;
					}
					switch (z_aggre_type)
					{
						case AGGREGATE_MIN:
						{
							double score = z_weight * zsk->e.score;
							ValueScoreMap::iterator it = z_cmp.find(zsk->e.value);
							if (it == z_cmp.end() || score < it->second)
							{
								z_result[zsk->e.value] = score;
							}
							break;
						}
						case AGGREGATE_MAX:
						{
							double score = z_weight * zsk->e.score;
							ValueScoreMap::iterator it = z_cmp.find(zsk->e.value);
							if (it == z_cmp.end() || score > it->second)
							{
								z_result[zsk->e.value] = score;
							}
							break;
						}
						case AGGREGATE_SUM:
						default:
						{
							if (z_cmp.count(zsk->e.value) == 0)
							{
								z_result[zsk->e.value] = z_weight * (zsk->e.score);
							} else
							{
								z_result[zsk->e.value] = z_weight * (zsk->e.score)
								        + z_cmp[zsk->e.value];
							}
							break;
						}
					}
					return 0;
				}
		};
		ValueScoreMap cmp1, cmp2;
		Slice empty;
		ZSetKeyObject cmp_start(keys[min_idx], empty, metas[min_idx]->min_score,
		        db);
		ZInterWalk walk(weights[min_idx], cmp1, cmp1, type);
		Walk(cmp_start, false, false, &walk);
		ValueScoreMap* cmp = &cmp1;
		ValueScoreMap* result = &cmp2;
		for (uint32_t i = 0; i < keys.size(); i++)
		{
			if (i != min_idx)
			{
				Slice empty;
				ZSetKeyObject tmp(keys.at(i), empty, metas[i]->min_score, db);
				ZInterWalk walk(weights[i], *cmp, *result, type);
				Walk(tmp, false, false, &walk);
				cmp->clear();
				ValueScoreMap* old = cmp;
				cmp = result;
				result = old;
			}
		}

		if (cmp->size() > 0)
		{
			double min_score = 0, max_score = 0;
			BatchWriteGuard guard(GetEngine());
			ZClear(db, dst);
			KeyLockerGuard keyguard(m_key_locker, db, dst);
			ZSetMetaValue meta;
			meta.size = cmp->size();
			while (!cmp->empty())
			{
				ValueScoreMap::iterator it = cmp->begin();
				ZSetKeyObject zsk(dst, it->first, it->second, db);
				ValueData zsv;
				ZSetScoreKeyObject zk(dst, it->first, db);
				ValueData zv(it->second);
				if (zv.double_value < min_score)
				{
					min_score = zv.double_value;
				}
				if (zv.double_value > max_score)
				{
					max_score = zv.double_value;
				}
				SetKeyValueObject(zsk, zsv);
				SetKeyValueObject(zk, zv);
				//reduce memory footprint for huge data set
				cmp->erase(it);
			}
			meta.min_score = min_score;
			meta.max_score = max_score;
			SetMeta(db, dst, meta);
		}
		delete_pointer_container(metas);
		return cmp->size();
	}
}

