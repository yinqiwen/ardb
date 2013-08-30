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
		KeyObject k(key, LIST_META, db);
		ValueObject v;
		if (0 == GetValue(k, &v))
		{
			if (!DecodeListMetaData(v, meta))
			{
				return -1;
			}
			return 0;
		}
		else
		{
			return 0;
		}
	}
	void Ardb::SetListMetaValue(const DBID& db, const Slice& key,
	        ListMetaValue& meta)
	{
		ValueObject v;
		EncodeListMetaData(v, meta);
		KeyObject k(key, LIST_META, db);
		SetValue(k, v);
	}

	int Ardb::ListPush(const DBID& db, const Slice& key, const Slice& value,
	        bool athead, bool onlyexist, float withscore)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		KeyObject k(key, LIST_META, db);
		ValueObject v;
		ListMetaValue meta;
		float score = withscore != FLT_MAX ? withscore : 0;
		if (0 == GetValue(k, &v))
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
				}
				else if (score > meta.max_score)
				{
					meta.max_score = score;
				}
			}
			else
			{
				if (athead)
				{
					meta.min_score--;
					score = meta.min_score;
				}
				else
				{
					meta.max_score++;
					score = meta.max_score;
				}
			}
		}
		else
		{
			if (onlyexist)
			{
				return ERR_NOT_EXIST;
			}
			meta.size++;
			score = 0;
		}
		BatchWriteGuard guard(GetEngine());
		ListKeyObject lk(key, score, db);
		ValueObject lv;
		smart_fill_value(value, lv);
		if (0 == SetValue(lk, lv))
		{
			EncodeListMetaData(v, meta);
			return SetValue(k, v) == 0 ? meta.size : -1;
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
		ListKeyObject lk(key, -FLT_MAX, db);
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
					}
					else
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
		Walk(lk, false, &walk);
		if (!walk.found)
		{
			return ERR_NOT_EXIST;
		}
		float score = 0;
		if (walk.next_score != -FLT_MAX)
		{
			score = (walk.next_score + walk.pivot_score) / 2;
		}
		else
		{
			if (before)
			{
				score = walk.pivot_score - 1;
			}
			else
			{
				score = walk.pivot_score + 1;
			}
		}
		return ListPush(db, key, value, true, false, score);
	}

	int Ardb::ListPop(const DBID& db, const Slice& key, bool athead,
	        std::string& value)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		KeyObject k(key, LIST_META, db);
		ValueObject v;
		ListMetaValue meta;
		float score;
		if (0 == GetValue(k, &v))
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
			}
			else
			{
				score = meta.max_score;
				//meta.max_score = meta.max_score-1;
			}
			if (meta.size == 0)
			{
				meta.min_score = meta.max_score = 0;
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
						if (cursor == 0)
						{
							ListKeyObject* lk = (ListKeyObject*) k;
							v->ToString(pop_value);
							ldb->DelValue(*lk);
							return 0;
						}
						else
						{
							ListKeyObject* lk = (ListKeyObject*) k;
							if (reverse)
							{
								lmeta.max_score = lk->score;
							}
							else
							{
								lmeta.min_score = lk->score;
							}
							return -1;
						}
					}
					LPopWalk(Ardb* db, std::string& v,
					        ListMetaValue& meta, bool r) :
							ldb(db), pop_value(v), lmeta(meta), reverse(
							        r)
					{
					}
			} walk(this,value, meta, !athead);
			Walk(lk, !athead, &walk);
			EncodeListMetaData(v, meta);
			return SetValue(k, v);
		}
		else
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
					if (cursor == index)
					{
						v->ToString(found_value);
						return -1;
					}
					lcursor++;
					return 0;
				}
				LIndexWalk(uint32 i, std::string& v) :
						lcursor(0), index(i), found_value(v)
				{
				}
		} walk((uint32)index, v);
		Walk(lk, reverse, &walk);
		if (walk.lcursor == walk.index)
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
		ListKeyObject lk(key, meta.min_score, db);
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
		Walk(lk, false, &walk);
		return 0;
	}

	int Ardb::LClear(const DBID& db, const Slice& key)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		ListKeyObject lk(key, -FLT_MAX, db);
		struct LClearWalk: public WalkHandler
		{
				Ardb* z_db;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					ListKeyObject* sek = (ListKeyObject*) k;
					z_db->DelValue(*sek);
					std::string str;
					v->ToString(str);
					return 0;
				}
				LClearWalk(Ardb* db) :
						z_db(db)
				{
				}
		} walk(this);
		BatchWriteGuard guard(GetEngine());
		Walk(lk, false, &walk);
		KeyObject k(key, LIST_META, db);
		DelValue(k);
		SetExpiration(db, key, 0);
		return 0;
	}

	int Ardb::LRem(const DBID& db, const Slice& key, int count,
	        const Slice& value)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		ListMetaValue meta;
		GetListMetaValue(db, key, meta);
		if (0 == meta.size)
		{
			return 0;
		}
		ListKeyObject lk(key, meta.min_score, db);
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
						z_db(db), cmp_value(v), remcount(0), rem_total(total)
				{
				}
		} walk(this, value, total);
		BatchWriteGuard guard(GetEngine());
		Walk(lk, !fromhead, &walk);
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
						ValueObject v;
						smart_fill_value(set_value, v);
						z_db->SetValue(*sek, v);
						return -1;
					}
					lcursor++;
					return 0;
				}
				LSetWalk(Ardb* db, const Slice& v, uint32 index) :
						z_db(db), set_value(v), lcursor(0), dst_idx(index)
				{
				}
		} walk(this, value, (uint32)index);
		Walk(lk, false, &walk);
		if (walk.lcursor != walk.dst_idx)
		{
			return ERR_NOT_EXIST;
		}
		return 0;
	}

	int Ardb::LTrim(const DBID& db, const Slice& key, int start, int stop)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
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
		ListKeyObject lk(key, meta.min_score, db);
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
					}
					else
					{
						lmeta.size--;
						ldb->DelValue(*lek);
					}
					return 0;
				}
		} walk(this, start, stop, meta);
		BatchWriteGuard guard(GetEngine());
		Walk(lk, false, &walk);
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

