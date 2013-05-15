/*
 * lists.cpp
 *
 *  Created on: 2013-4-1
 *      Author: wqy
 */
#include "ardb.hpp"

namespace ardb
{
	static bool DecodeListMetaData(ValueObject& v, ListMetaValue& meta)
	{
		if (v.type != RAW)
		{
			return false;
		}
		return BufferHelper::ReadVarUInt32(*(v.v.raw), meta.size)
				&& BufferHelper::ReadFixFloat(*(v.v.raw), meta.min_score)
				&& BufferHelper::ReadFixFloat(*(v.v.raw), meta.max_score);
	}
	static void EncodeListMetaData(ValueObject& v, ListMetaValue& meta)
	{
		v.type = RAW;
		if (v.v.raw == NULL)
		{
			v.v.raw = new Buffer(16);
		}
		BufferHelper::WriteVarUInt32(*(v.v.raw), meta.size);
		BufferHelper::WriteFixFloat(*(v.v.raw), meta.min_score);
		BufferHelper::WriteFixFloat(*(v.v.raw), meta.max_score);
	}

	int Ardb::GetListMetaValue(const DBID& db, const Slice& key,
			ListMetaValue& meta)
	{
		KeyObject k(key, LIST_META);
		ValueObject v;
		if (0 == GetValue(db, k, &v))
		{
			if (!DecodeListMetaData(v, meta))
			{
				return -1;
			}
			return 0;
		} else
		{
			return 0;
		}
	}
	void Ardb::SetListMetaValue(const DBID& db, const Slice& key,
			ListMetaValue& meta)
	{
		ValueObject v;
		EncodeListMetaData(v, meta);
		KeyObject k(key, LIST_META);
		SetValue(db, k, v);
	}

	int Ardb::ListPush(const DBID& db, const Slice& key, const Slice& value,
			bool athead, bool onlyexist, float withscore)
	{
		KeyObject k(key, LIST_META);
		ValueObject v;
		ListMetaValue meta;
		float score = withscore != FLT_MAX ? withscore : 0;
		if (0 == GetValue(db, k, &v))
		{
			if (!DecodeListMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
			meta.size++;
			if (withscore != FLT_MAX)
			{
				score = withscore;
				if (score < meta.min_score)
				{
					meta.min_score = score;
				} else if (score > meta.max_score)
				{
					meta.max_score = score;
				}
			} else
			{
				if (athead)
				{
					meta.min_score--;
					score = meta.min_score;
				} else
				{
					meta.max_score++;
					score = meta.max_score;
				}
			}
		} else
		{
			if (onlyexist)
			{
				return ERR_NOT_EXIST;
			}
			meta.size++;
			score = 0;
		}
		BatchWriteGuard guard(GetDB(db));
		ListKeyObject lk(key, score);
		ValueObject lv;
		smart_fill_value(value, lv);
		if (0 == SetValue(db, lk, lv))
		{
			EncodeListMetaData(v, meta);
			return SetValue(db, k, v) == 0 ? meta.size : -1;
		}
		return -1;
	}
	int Ardb::RPush(const DBID& db, const Slice& key, const Slice& value)
	{
		return ListPush(db, key, value, false, false);
	}

	int Ardb::RPushx(const DBID& db, const Slice& key, const Slice& value)
	{
		int len = ListPush(db, key, value, false, true);
		return len < 0 ? 0 : len;
	}

	int Ardb::LPush(const DBID& db, const Slice& key, const Slice& value)
	{
		return ListPush(db, key, value, true, false);
	}

	int Ardb::LPushx(const DBID& db, const Slice& key, const Slice& value)
	{
		int len = ListPush(db, key, value, true, true);
		return len < 0 ? 0 : len;
	}

	int Ardb::LInsert(const DBID& db, const Slice& key, const Slice& opstr,
			const Slice& pivot, const Slice& value)
	{
		bool before;
		if (!strncasecmp(opstr.data(), "before", opstr.size()))
			before = true;
		else if (!strncasecmp(opstr.data(), "after", opstr.size()))
			before = false;
		else
		{
			return ERR_INVALID_OPERATION;
		}
		ListKeyObject lk(key, -FLT_MAX);
		struct LInsertWalk: public WalkHandler
		{
				bool before_pivot;
				float pivot_score;
				float next_score;
				bool found;
				const Slice& cmp_value;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					ListKeyObject* sek = (ListKeyObject*) k;
					if (found && !before_pivot)
					{
						next_score = sek->score;
						return -1;
					}
					value_convert_to_raw(*v);
					Slice cmp(v->v.raw->GetRawReadBuffer(),
							v->v.raw->ReadableBytes());
					if (cmp.compare(cmp_value) == 0)
					{
						pivot_score = sek->score;
						found = true;
						if (before_pivot)
						{
							return -1;
						}
					} else
					{
						if (before_pivot)
						{
							next_score = sek->score;
						}
					}
					return 0;
				}
				LInsertWalk(bool flag, const Slice& value) :
						before_pivot(flag), pivot_score(-FLT_MAX), next_score(
								-FLT_MAX), found(false), cmp_value(value)
				{
				}
		} walk(before, pivot);
		Walk(db, lk, false, &walk);
		if (!walk.found)
		{
			return ERR_NOT_EXIST;
		}
		float score = 0;
		if (walk.next_score != -FLT_MAX)
		{
			score = (walk.next_score + walk.pivot_score) / 2;
		} else
		{
			if (before)
			{
				score = walk.pivot_score - 1;
			} else
			{
				score = walk.pivot_score + 1;
			}
		}
		return ListPush(db, key, value, true, false, score);
	}

	int Ardb::ListPop(const DBID& db, const Slice& key, bool athead,
			std::string& value)
	{
		KeyObject k(key, LIST_META);
		ValueObject v;
		ListMetaValue meta;
		float score;
		if (0 == GetValue(db, k, &v))
		{
			if (!DecodeListMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
			if (meta.size <= 0)
			{
				return ERR_NOT_EXIST;
			}
			meta.size--;
			if (athead)
			{
				score = meta.min_score;
				//meta.min_score = meta.min_score + 1;
			} else
			{
				score = meta.max_score;
				//meta.max_score = meta.max_score-1;
			}
			if (meta.size == 0)
			{
				meta.min_score = meta.max_score = 0;
			}
			ListKeyObject lk(key, score);
			BatchWriteGuard guard(GetDB(db));
			struct LPopWalk: public WalkHandler
			{
					Ardb* ldb;
					const DBID& dbid;
					std::string& pop_value;
					ListMetaValue& lmeta;
					bool reverse;
					int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
					{
						if (cursor == 0)
						{
							ListKeyObject* lk = (ListKeyObject*) k;
							v->ToString(pop_value);
							ldb->DelValue(dbid, *lk);
							return 0;
						} else
						{
							ListKeyObject* lk = (ListKeyObject*) k;
							if (reverse)
							{
								lmeta.max_score = lk->score;
							} else
							{
								lmeta.min_score = lk->score;
							}
							return -1;
						}
					}
					LPopWalk(Ardb* db, const DBID& id, std::string& v,
							ListMetaValue& meta, bool r) :
							ldb(db), dbid(id), pop_value(v), lmeta(meta), reverse(
									r)
					{
					}
			} walk(this, db, value, meta, !athead);
			Walk(db, lk, !athead, &walk);
			EncodeListMetaData(v, meta);
			return SetValue(db, k, v);
		} else
		{
			return ERR_NOT_EXIST;
		}
	}

	int Ardb::LPop(const DBID& db, const Slice& key, std::string& value)
	{
		return ListPop(db, key, true, value);
	}
	int Ardb::RPop(const DBID& db, const Slice& key, std::string& v)
	{
		return ListPop(db, key, false, v);
	}

	int Ardb::LIndex(const DBID& db, const Slice& key, int index,
			std::string& v)
	{
		ListKeyObject lk(key, -FLT_MAX);
		bool reverse = false;
		if (index < 0)
		{
			index = 0 - index;
			lk.score = FLT_MAX;
			reverse = true;
		}
		struct LIndexWalk: public WalkHandler
		{
				int cursor;
				int index;
				std::string& found_value;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					//ListKeyObject* lck = (ListKeyObject*)k;
					if (cursor == index)
					{
						v->ToString(found_value);
						return -1;
					}
					cursor++;
					return 0;
				}
				LIndexWalk(int i, std::string& v) :
						cursor(0), index(i), found_value(v)
				{
				}
		} walk(index, v);
		Walk(db, lk, reverse, &walk);
		if (walk.cursor == walk.index)
		{
			return 0;
		}
		return ERR_NOT_EXIST;
	}

	int Ardb::LRange(const DBID& db, const Slice& key, int start, int end,
			ValueArray& values)
	{
		ListMetaValue meta;
		GetListMetaValue(db, key, meta);
		int len = meta.size;
		if (len < 0)
		{
			return len;
		}
		if (start < 0)
		{
			start += len;
		}
		if (end < 0)
		{
			end += len;
		}
		if (start < 0)
		{
			start = 0;
		}
		if (start >= len)
		{
			return 0;
		}
		if (end < 0)
		{
			end = 0;
		}
		ListKeyObject lk(key, meta.min_score);
		struct LRangeWalk: public WalkHandler
		{
				uint32 l_start;
				uint32 l_stop;
				ValueArray& found_values;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					if (cursor >= l_start && cursor <= l_stop)
					{
						found_values.push_back(*v);
					}
					if (cursor >= l_stop)
					{
						return -1;
					}
					return 0;
				}
				LRangeWalk(int start, int stop, ValueArray& vs) :
						l_start(start), l_stop(stop), found_values(vs)
				{
				}
		} walk(start, end, values);
		Walk(db, lk, false, &walk);
		return 0;
	}

	int Ardb::LClear(const DBID& db, const Slice& key)
	{
		ListKeyObject lk(key, -FLT_MAX);
		struct LClearWalk: public WalkHandler
		{
				Ardb* z_db;
				DBID z_dbid;
				int x;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					ListKeyObject* sek = (ListKeyObject*) k;
					z_db->DelValue(z_dbid, *sek);
					x++;
					return 0;
				}
				LClearWalk(Ardb* db, const DBID& dbid) :
						z_db(db), z_dbid(dbid), x(0)
				{
				}
		} walk(this, db);
		BatchWriteGuard guard(GetDB(db));
		Walk(db, lk, false, &walk);
		KeyObject k(key, LIST_META);
		DelValue(db, k);
		return 0;
	}

	int Ardb::LRem(const DBID& db, const Slice& key, int count,
			const Slice& value)
	{
		ListMetaValue meta;
		GetListMetaValue(db, key, meta);
		if (0 == meta.size)
		{
			return 0;
		}
		ListKeyObject lk(key, meta.min_score);
		int total = count;
		bool fromhead = true;
		if (count < 0)
		{
			fromhead = false;
			total = 0 - count;
			lk.score = meta.max_score;
		}
		struct LRemWalk: public WalkHandler
		{
				Ardb* z_db;
				DBID z_dbid;
				const Slice& cmp_value;
				int remcount;
				int rem_total;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					ListKeyObject* sek = (ListKeyObject*) k;
					Slice cmp(v->v.raw->GetRawReadBuffer(),
							v->v.raw->ReadableBytes());
					if (cmp.compare(cmp_value) == 0)
					{
						z_db->DelValue(z_dbid, *sek);
						remcount++;
						if (rem_total == remcount)
						{
							return -1;
						}
					}
					return 0;
				}
				LRemWalk(Ardb* db, const DBID& dbid, const Slice& v, int total) :
						z_db(db), z_dbid(dbid), cmp_value(v), remcount(0), rem_total(
								total)
				{
				}
		} walk(this, db, value, total);
		BatchWriteGuard guard(GetDB(db));
		Walk(db, lk, !fromhead, &walk);
		meta.size -= walk.remcount;
		SetListMetaValue(db, key, meta);
		return walk.remcount;
	}

	int Ardb::LSet(const DBID& db, const Slice& key, int index,
			const Slice& value)
	{
		int len = LLen(db, key);
		if (len <= 0)
		{
			return ERR_NOT_EXIST;
		}
		if (index < 0)
		{
			index += len;
		}
		if (index >= len)
		{
			return ERR_NOT_EXIST;
		}
		ListKeyObject lk(key, -FLT_MAX);
		struct LSetWalk: public WalkHandler
		{
				Ardb* z_db;
				DBID z_dbid;
				const Slice& set_value;
				int cursor;
				int dst_idx;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					ListKeyObject* sek = (ListKeyObject*) k;
					if (cursor == dst_idx)
					{
						ValueObject v;
						smart_fill_value(set_value, v);
						z_db->SetValue(z_dbid, *sek, v);
						return -1;
					}
					cursor++;
					return 0;
				}
				LSetWalk(Ardb* db, const DBID& dbid, const Slice& v, int index) :
						z_db(db), z_dbid(dbid), set_value(v), cursor(0), dst_idx(
								index)
				{
				}
		} walk(this, db, value, index);
		Walk(db, lk, false, &walk);
		if (walk.cursor != index)
		{
			return ERR_NOT_EXIST;
		}
		return 0;
	}

	int Ardb::LTrim(const DBID& db, const Slice& key, int start, int stop)
	{
		ListMetaValue meta;
		GetListMetaValue(db, key, meta);
		int len = meta.size;
		if (len <= 0)
		{
			return 0;
		}
		if (start < 0)
		{
			start += len;
		}
		if (stop < 0)
		{
			stop += len;
		}
		if (start < 0)
		{
			start = 0;
		}
		if (stop < 0)
		{
			stop = 0;
		}
		if (start >= len)
		{
			return LClear(db, key);
		}
		ListKeyObject lk(key, meta.min_score);

		struct LTrimWalk: public WalkHandler
		{
				Ardb* ldb;
				const DBID& dbid;
				uint32 lstart, lstop;
				ListMetaValue& lmeta;
				LTrimWalk(Ardb* db, const DBID& id, int start, int stop,
						ListMetaValue& meta) :
						ldb(db), dbid(id), lstart(start), lstop(stop), lmeta(
								meta)
				{
				}
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					ListKeyObject* lek = (ListKeyObject*) k;
					if (cursor >= lstart && cursor <= lstop)
					{
						if (lek->score < lmeta.min_score)
						{
							lmeta.min_score = lek->score;
						}
						if (lek->score > lmeta.max_score)
						{
							lmeta.max_score = lek->score;
						}
						return 0;
					} else
					{
						lmeta.size--;
						ldb->DelValue(dbid, *lek);
					}
					return 0;
				}
		} walk(this, db, start, stop, meta);
		BatchWriteGuard guard(GetDB(db));
		Walk(db, lk, false, &walk);
		SetListMetaValue(db, key, meta);
		return 0;
	}

	int Ardb::LLen(const DBID& db, const Slice& key)
	{
		ListMetaValue meta;
		GetListMetaValue(db, key, meta);
		return meta.size;
	}

	int Ardb::RPopLPush(const DBID& db, const Slice& key1, const Slice& key2,
			std::string& v)
	{
		if (0 == RPop(db, key1, v))
		{
			Slice sv(v.c_str(), v.size());
			return RPush(db, key2, sv);
		}
		return ERR_NOT_EXIST;
	}
}

