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

	ListMetaValue* Ardb::GetListMeta(const DBID& db, const Slice& key, int& err,
	        bool& created)
	{
		CommonMetaValue* meta = GetMeta(db, key, false);
		if (NULL != meta && meta->header.type != LIST_META)
		{
			DELETE(meta);
			err = ERR_INVALID_TYPE;
			return NULL;
		}
		if (NULL == meta)
		{
			meta = new ListMetaValue;
			created = true;
		} else
		{
			created = false;
		}
		return (ListMetaValue*) meta;
	}

	int Ardb::ListPush(const DBID& db, const Slice& key, const Slice& value,
	        bool athead, bool onlyexist, float withscore)
	{
		int err = 0;
		bool createList = false;
		ListMetaValue* lmeta = GetListMeta(db, key, err, createList);
		if (NULL == lmeta)
		{
			return err;
		}
		KeyLockerGuard keyguard(m_key_locker, db, key);
		float score = withscore != FLT_MAX ? withscore : 0;
		if (!createList)
		{
			lmeta->size++;
			if (withscore != FLT_MAX)
			{
				score = withscore;
				if (score < lmeta->min_score)
				{
					lmeta->min_score = score;
				} else if (score > lmeta->max_score)
				{
					lmeta->max_score = score;
				}
			} else
			{
				if (athead)
				{
					lmeta->min_score--;
					score = lmeta->min_score;
				} else
				{
					lmeta->max_score++;
					score = lmeta->max_score;
				}
			}
		} else
		{
			if (onlyexist)
			{
				return ERR_NOT_EXIST;
			}
			lmeta->size++;
			score = 0;
		}
		BatchWriteGuard guard(GetEngine());
		ListKeyObject lk(key, score, db);
		CommonValueObject lv;
		lv.data.SetValue(value, true);
		if (0 == SetKeyValueObject(lk, lv))
		{
			int ret = SetMeta(db, key, *lmeta);
			if (ret == 0)
			{
				ret = lmeta->size;
			}
			DELETE(lmeta);
			return ret;
		}
		DELETE(lmeta);
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
		ListKeyObject lk(key, -FLT_MAX, db);
		struct LInsertWalk: public WalkHandler
		{
				bool before_pivot;
				float pivot_score;
				float next_score;
				bool found;
				ValueData cmp_value;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					CommonValueObject* cv = (CommonValueObject*) v;
					ListKeyObject* sek = (ListKeyObject*) k;
					if (found && !before_pivot)
					{
						next_score = sek->score;
						return -1;
					}
					if (cv->data.Compare(cmp_value) == 0)
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
						        -FLT_MAX), found(false)
				{
					cmp_value.SetValue(value, true);
				}
		} walk(before, pivot);
		Walk(lk, false, true, &walk);
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
		int err = 0;
		bool createList = false;
		ListMetaValue* meta = GetListMeta(db, key, err, createList);
		if (NULL == meta)
		{
			return err;
		}
		KeyLockerGuard keyguard(m_key_locker, db, key);
		float score;
		if (!createList)
		{
			if (meta->size <= 0)
			{
				DELETE(meta);
				return ERR_NOT_EXIST;
			}
			meta->size--;
			if (athead)
			{
				score = meta->min_score;
				//meta.min_score = meta.min_score + 1;
			} else
			{
				score = meta->max_score;
				//meta.max_score = meta.max_score-1;
			}
			if (meta->size == 0)
			{
				meta->min_score = meta->max_score = 0;
			}
			ListKeyObject lk(key, score, db);
			BatchWriteGuard guard(GetEngine());
			struct LPopWalk: public WalkHandler
			{
					Ardb* ldb;
					std::string& pop_value;
					ListMetaValue& lmeta;
					bool reverse;
					int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
					{
						CommonValueObject* cv = (CommonValueObject*) v;
						if (cursor == 0)
						{
							ListKeyObject* lk = (ListKeyObject*) k;
							cv->data.ToString(pop_value);
							ldb->DelValue(*lk);
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
					LPopWalk(Ardb* db, std::string& v, ListMetaValue& meta,
					        bool r) :
							ldb(db), pop_value(v), lmeta(meta), reverse(r)
					{
					}
			} walk(this, value, *meta, !athead);
			Walk(lk, !athead, true, &walk);
			int ret = SetMeta(db, key, *meta);
			DELETE(meta);
			return ret;
		} else
		{
			DELETE(meta);
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
		ListKeyObject lk(key, -FLT_MAX, db);
		bool reverse = false;
		if (index < 0)
		{
			index = 0 - index;
			lk.score = FLT_MAX;
			reverse = true;
		}
		struct LIndexWalk: public WalkHandler
		{
				uint32 lcursor;
				uint32 index;
				std::string& found_value;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					CommonValueObject* cv = (CommonValueObject*) v;
					if (cursor == index)
					{
						cv->data.ToString(found_value);
						return -1;
					}
					lcursor++;
					return 0;
				}
				LIndexWalk(uint32 i, std::string& v) :
						lcursor(0), index(i), found_value(v)
				{
				}
		} walk((uint32) index, v);
		Walk(lk, reverse, true, &walk);
		if (walk.lcursor == walk.index)
		{
			return 0;
		}
		return ERR_NOT_EXIST;
	}

	int Ardb::LRange(const DBID& db, const Slice& key, int start, int end,
	        ValueArray& values)
	{
		int err = 0;
		bool createList = false;
		ListMetaValue* meta = GetListMeta(db, key, err, createList);
		if (NULL == meta || createList)
		{
			DELETE(meta);
			return err;
		}
		int len = meta->size;
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
		ListKeyObject lk(key, meta->min_score, db);
		struct LRangeWalk: public WalkHandler
		{
				uint32 l_start;
				uint32 l_stop;
				ValueArray& found_values;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					CommonValueObject* cv = (CommonValueObject*) v;
					if (cursor >= l_start && cursor <= l_stop)
					{
						found_values.push_back(cv->data);
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
		Walk(lk, false, true, &walk);
		DELETE(meta);
		return 0;
	}

	int Ardb::LClear(const DBID& db, const Slice& key)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		int err = 0;
		bool createList = false;
		ListMetaValue* meta = GetListMeta(db, key, err, createList);
		if (NULL == meta || createList)
		{
			DELETE(meta);
			return err;
		}
		ListKeyObject lk(key, -FLT_MAX, db);
		struct LClearWalk: public WalkHandler
		{
				Ardb* z_db;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					ListKeyObject* sek = (ListKeyObject*) k;
					z_db->DelValue(*sek);
					return 0;
				}
				LClearWalk(Ardb* db) :
						z_db(db)
				{
				}
		} walk(this);
		BatchWriteGuard guard(GetEngine());
		Walk(lk, false, false, &walk);
		DelMeta(db, key, meta);
		DELETE(meta);
		return 0;
	}

	int Ardb::LRem(const DBID& db, const Slice& key, int count,
	        const Slice& value)
	{
		int err = 0;
		bool createList = false;
		ListMetaValue* meta = GetListMeta(db, key, err, createList);
		if (NULL == meta || createList || meta->size == 0)
		{
			DELETE(meta);
			return err;
		}
		KeyLockerGuard keyguard(m_key_locker, db, key);
		ListKeyObject lk(key, meta->min_score, db);
		int total = count;
		bool fromhead = true;
		if (count < 0)
		{
			fromhead = false;
			total = 0 - count;
			lk.score = meta->max_score;
		}
		struct LRemWalk: public WalkHandler
		{
				Ardb* z_db;
				ValueData cmp_value;
				int remcount;
				int rem_total;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					CommonValueObject* cv = (CommonValueObject*) v;
					ListKeyObject* sek = (ListKeyObject*) k;
					if (cv->data.Compare(cmp_value) == 0)
					{
						z_db->DelValue(*sek);
						remcount++;
						if (rem_total == remcount)
						{
							return -1;
						}
					}
					return 0;
				}
				LRemWalk(Ardb* db, const Slice& v, int total) :
						z_db(db), remcount(0), rem_total(total)
				{
					cmp_value.SetValue(v, true);
				}
		} walk(this, value, total);
		BatchWriteGuard guard(GetEngine());
		Walk(lk, !fromhead, true, &walk);
		meta->size -= walk.remcount;
		SetMeta(db, key, *meta);
		DELETE(meta);
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
		ListKeyObject lk(key, -FLT_MAX, db);
		struct LSetWalk: public WalkHandler
		{
				Ardb* z_db;
				const Slice& set_value;
				uint32 lcursor;
				uint32 dst_idx;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					ListKeyObject* sek = (ListKeyObject*) k;
					if (cursor == dst_idx)
					{
						CommonValueObject cv;
						cv.data.SetValue(set_value, true);
						z_db->SetKeyValueObject(*sek, cv);
						return -1;
					}
					lcursor++;
					return 0;
				}
				LSetWalk(Ardb* db, const Slice& v, uint32 index) :
						z_db(db), set_value(v), lcursor(0), dst_idx(index)
				{
				}
		} walk(this, value, (uint32) index);
		Walk(lk, false, false, &walk);
		if (walk.lcursor != walk.dst_idx)
		{
			return ERR_NOT_EXIST;
		}
		return 0;
	}

	int Ardb::LTrim(const DBID& db, const Slice& key, int start, int stop)
	{
		int err = 0;
		bool createList = false;
		ListMetaValue* meta = GetListMeta(db, key, err, createList);
		if (NULL == meta || createList || meta->size == 0)
		{
			DELETE(meta);
			return err;
		}
		KeyLockerGuard keyguard(m_key_locker, db, key);
		int len = meta->size;
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
		ListKeyObject lk(key, meta->min_score, db);
		struct LTrimWalk: public WalkHandler
		{
				Ardb* ldb;
				uint32 lstart, lstop;
				ListMetaValue& lmeta;
				LTrimWalk(Ardb* db, int start, int stop, ListMetaValue& meta) :
						ldb(db), lstart(start), lstop(stop), lmeta(meta)
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
						ldb->DelValue(*lek);
					}
					return 0;
				}
		} walk(this, start, stop, *meta);
		BatchWriteGuard guard(GetEngine());
		Walk(lk, false, false, &walk);
		SetMeta(db, key, *meta);
		DELETE(meta);
		return 0;
	}

	int Ardb::LLen(const DBID& db, const Slice& key)
	{
		CommonMetaValue* meta = GetMeta(db, key, false);
		if (NULL != meta && meta->header.type != LIST_META)
		{
			DELETE(meta);
			return ERR_INVALID_TYPE;
		}
		ListMetaValue* lmeta = (ListMetaValue*) meta;
		int size = lmeta->size;
		DELETE(meta);
		return size;
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

