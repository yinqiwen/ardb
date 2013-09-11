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
#include <algorithm>

#define MAX_SET_INTER_NUM 500000
#define MAX_SET_UNION_NUM 100000
#define MAX_SET_DIFF_NUM  500000
#define MAX_SET_OP_STORE_NUM 10000

namespace ardb
{
	static bool DecodeSetMetaData(ValueObject& v, SetMetaValue& meta)
	{
		if (v.type != RAW)
		{
			return false;
		}
		return BufferHelper::ReadVarUInt32(*(v.v.raw), meta.size) && decode_value(*(v.v.raw), meta.min) && decode_value(*(v.v.raw), meta.max);
	}
	static void EncodeSetMetaData(ValueObject& v, SetMetaValue& meta)
	{
		v.type = RAW;
		if (v.v.raw == NULL)
		{
			v.v.raw = new Buffer(16);
		}
		BufferHelper::WriteVarUInt32(*(v.v.raw), meta.size);
		encode_value(*(v.v.raw), meta.min);
		encode_value(*(v.v.raw), meta.max);
	}

	int Ardb::GetSetMetaValue(const DBID& db, const Slice& key, SetMetaValue& meta)
	{
		KeyObject k(key, SET_META, db);
		//SetKeyObject k(key, Slice());
		ValueObject v;
		if (0 == GetValue(k, &v))
		{
			if (!DecodeSetMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
			return 0;
		}
		return ERR_NOT_EXIST;
	}
	void Ardb::SetSetMetaValue(const DBID& db, const Slice& key, SetMetaValue& meta)
	{
		KeyObject k(key, SET_META, db);
		//SetKeyObject k(key, Slice());
		ValueObject v;
		EncodeSetMetaData(v, meta);
		SetValue(k, v);
	}

	int Ardb::SAdd(const DBID& db, const Slice& key, const SliceArray& values)
	{
		int count = 0;
		for (uint32 i = 0; i < values.size(); i++)
		{
			count += SAdd(db, key, values[i]);
		}
		return count;
	}

	int Ardb::SAdd(const DBID& db, const Slice& key, const Slice& value)
	{
		KeyLockerGuard guard(m_key_locker, db, key);
		KeyObject k(key, SET_META, db);
		//SetKeyObject k(key, Slice());
		SetMetaValue meta;
		GetSetMetaValue(db, key, meta);
		ValueObject v;
		smart_fill_value(value, v);
		SetKeyObject sk(key, v, db);
		ValueObject sv;
		bool set_changed = false;
		if (meta.max.type == EMPTY)
		{
			meta.min = v;
			meta.max = v;
			meta.size++;
			set_changed = true;
		} else
		{
			if (meta.min.Compare(v) == 0 || meta.max.Compare(v) == 0)
			{
				return 0;
			}
			if (meta.min.Compare(v) > 0)
			{
				meta.size++;
				meta.min = v;
				set_changed = true;
			} else if (meta.max.Compare(v) < 0)
			{
				meta.size++;
				meta.max = v;
				set_changed = true;
			} else
			{
				if (0 != GetValue(sk, NULL))
				{
					meta.size++;
					if (meta.min.Compare(v) > 0)
					{
						meta.min = v;
					}
					if (meta.max.Compare(v) < 0)
					{
						meta.max = v;
					}
					set_changed = true;
				}
			}
		}

		if (set_changed)
		{
			sv.type = EMPTY;
			BatchWriteGuard guard(GetEngine());
			SetValue(sk, sv);
			SetSetMetaValue(db, key, meta);
			return 1;
		}
		return 0;
	}

	int Ardb::SCard(const DBID& db, const Slice& key)
	{
		KeyObject k(key, SET_META, db);
		ValueObject v;
		SetMetaValue meta;
		if (0 == GetValue(k, &v))
		{
			if (!DecodeSetMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
			return meta.size;
		}
		return ERR_NOT_EXIST;
	}

	bool Ardb::SIsMember(const DBID& db, const Slice& key, const Slice& value)
	{
		SetKeyObject sk(key, value, db);
		if (0 != GetValue(sk, NULL))
		{
			return false;
		}
		return true;
	}

	int Ardb::SRem(const DBID& db, const Slice& key, const SliceArray& values)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		SetMetaValue meta;
		if (0 != GetSetMetaValue(db, key, meta))
		{
			return ERR_NOT_EXIST;
		}
		BatchWriteGuard guard(GetEngine());
		int count = 0;
		SliceArray::const_iterator it = values.begin();
		while (it != values.end())
		{
			SetKeyObject sk(key, *it, db);
			if (0 == GetValue(sk, NULL))
			{
				meta.size--;
				DelValue(sk);
				count++;
			}
			it++;
		}
		if (count > 0)
		{
			if (meta.size == 0)
			{
				KeyObject k(key, SET_META, db);
				DelValue(k);
			} else
			{
				SetSetMetaValue(db, key, meta);
			}
		}
		return count;
	}

	int Ardb::SRem(const DBID& db, const Slice& key, const Slice& value)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		SetMetaValue meta;
		if (0 != GetSetMetaValue(db, key, meta))
		{
			return ERR_NOT_EXIST;
		}
		SetKeyObject sk(key, value, db);
		if (0 == GetValue(sk, NULL))
		{
			meta.size--;
			BatchWriteGuard guard(GetEngine());
			DelValue(sk);
			if (meta.size == 0)
			{
				KeyObject k(key, SET_META, db);
				DelValue(k);
			} else
			{
				SetSetMetaValue(db, key, meta);
			}
			return 1;
		}
		return 0;
	}

	int Ardb::SRevRange(const DBID& db, const Slice& key, const Slice& value_end, int count, bool with_end, ValueArray& values)
	{
		if (count == 0)
		{
			return 0;
		}
		SetMetaValue meta;
		if (0 != GetSetMetaValue(db, key, meta))
		{
			return ERR_NOT_EXIST;
		}
		ValueObject firstObj;
		if (value_end.empty() || !strcasecmp(value_end.data(), "+inf"))
		{
			firstObj = meta.max;
			with_end = true;
		} else
		{
			smart_fill_value(value_end, firstObj);
		}
		SetKeyObject sk(key, firstObj, db);
		struct SGetWalk: public WalkHandler
		{
				ValueArray& z_values;
				ValueObject& first;
				bool with_first;
				int l;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					SetKeyObject* sek = (SetKeyObject*) k;
					if (0 == cursor)
					{
						if (!with_first)
						{
							if (first.Compare(sek->value) == 0)
							{
								return 0;
							}
						}
					}
					z_values.push_back(sek->value);
					if (l > 0 && z_values.size() >= (uint32) l)
					{
						return -1;
					}
					return 0;
				}
				SGetWalk(ValueArray& vs, int count, ValueObject& s, bool with) :
						z_values(vs), first(s), with_first(with), l(count)
				{
				}
		} walk(values, count, firstObj, with_end);
		Walk(sk, true, &walk);
		return 0;
	}

	int Ardb::SetRange(const DBID& db, const Slice& key, const ValueObject& value_begin, const ValueObject& value_end, int32 limit, bool with_begin, ValueArray& values)
	{
		if (limit == 0)
		{
			return 0;
		}
		SetKeyObject sk(key, value_begin, db);
		struct SGetWalk: public WalkHandler
		{
				ValueArray& z_values;
				const ValueObject& first;
				const ValueObject& end;
				bool with_first;
				int32 l;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					SetKeyObject* sek = (SetKeyObject*) k;
					if (0 == cursor)
					{
						if (!with_first)
						{
							if (first.Compare(sek->value) == 0)
							{
								return 0;
							}
						}
						std::string str;
						v->ToString(str);
					}
					if (end.Compare(sek->value) < 0)
					{
						return -1;
					}
					z_values.push_back(sek->value);
					if (l > 0 && z_values.size() >= (uint32) l)
					{
						return -1;
					}
					return 0;
				}
				SGetWalk(ValueArray& vs, int32 count, const ValueObject& s, const ValueObject& e, bool with) :
						z_values(vs), first(s), end(e), with_first(with), l(count)
				{
				}
		} walk(values, limit, value_begin, value_end, with_begin);
		Walk(sk, false, &walk);
		return 0;
	}

	int Ardb::SRange(const DBID& db, const Slice& key, const Slice& value_begin, int count, bool with_begin, ValueArray& values)
	{
		if (count == 0)
		{
			return 0;
		}
		SetMetaValue meta;
		if (0 != GetSetMetaValue(db, key, meta))
		{
			return ERR_NOT_EXIST;
		}
		ValueObject firstObj;
		if (value_begin.empty() || !strcasecmp(value_begin.data(), "-inf"))
		{
			firstObj = meta.min;
			with_begin = true;
		} else
		{
			smart_fill_value(value_begin, firstObj);
		}
		SetKeyObject sk(key, firstObj, db);
		struct SGetWalk: public WalkHandler
		{
				ValueArray& z_values;
				ValueObject& first;
				bool with_first;
				int l;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					SetKeyObject* sek = (SetKeyObject*) k;
					if (0 == cursor)
					{
						if (!with_first)
						{
							if (first.Compare(sek->value) == 0)
							{
								return 0;
							}
						}
						std::string str;
						v->ToString(str);
					}
					z_values.push_back(sek->value);
					if (l > 0 && z_values.size() >= (uint32) l)
					{
						return -1;
					}
					return 0;
				}
				SGetWalk(ValueArray& vs, int count, ValueObject& s, bool with) :
						z_values(vs), first(s), with_first(with), l(count)
				{
				}
		} walk(values, count, firstObj, with_begin);
		Walk(sk, false, &walk);
		return 0;
	}

	int Ardb::SMembers(const DBID& db, const Slice& key, ValueArray& values)
	{
		SetMetaValue meta;
		if (0 != GetSetMetaValue(db, key, meta))
		{
			return ERR_NOT_EXIST;
		}
		SetKeyObject sk(key, meta.min, db);
		struct SMembersWalk: public WalkHandler
		{
				ValueArray& z_values;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					SetKeyObject* sek = (SetKeyObject*) k;
					z_values.push_back(sek->value);
					return 0;
				}
				SMembersWalk(ValueArray& vs) :
						z_values(vs)
				{
				}
		} walk(values);
		Walk(sk, false, &walk);
		return 0;
	}

	int Ardb::SClear(const DBID& db, const Slice& key)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		Slice empty;
		SetKeyObject sk(key, empty, db);
		SetMetaValue meta;
		if (0 != GetSetMetaValue(db, key, meta))
		{
			return 0;
		}
		struct SClearWalk: public WalkHandler
		{
				Ardb* z_db;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					SetKeyObject* sek = (SetKeyObject*) k;
					z_db->DelValue(*sek);
					return 0;
				}
				SClearWalk(Ardb* db) :
						z_db(db)
				{
				}
		} walk(this);
		BatchWriteGuard guard(GetEngine());
		Walk(sk, false, &walk);
		KeyObject k(key, SET_META, db);
		DelValue(k);
		SetExpiration(db, key, 0, false);
		return 0;
	}

	int Ardb::SDiffCount(const DBID& db, SliceArray& keys, uint32& count)
	{
		struct CountCallback: public SetOperationCallback
		{
				uint32 count;
				void OnSubset(ValueArray& array)
				{
					//INFO_LOG("size = %u",array.size());
					count += array.size();
				}
		} callback;
		callback.count = 0;
		SDiff(db, keys, &callback, MAX_SET_DIFF_NUM);
		count = callback.count;
		return 0;
	}

	int Ardb::SDiff(const DBID& db, SliceArray& keys, ValueArray& values)
	{
		if (keys.size() < 2)
		{
			return ERR_INVALID_ARGS;
		}
		struct DiffCallback: public SetOperationCallback
		{
				ValueArray& vs;
				DiffCallback(ValueArray& v) :
						vs(v)
				{
				}
				void OnSubset(ValueArray& array)
				{
					vs.insert(vs.end(), array.begin(), array.end());
				}
		} callback(values);
		SDiff(db, keys, &callback, MAX_SET_DIFF_NUM);
		return 0;
	}

	int Ardb::SDiffStore(const DBID& db, const Slice& dst, SliceArray& keys)
	{
		if (keys.size() < 2)
		{
			return ERR_INVALID_ARGS;
		}
		struct DiffCallback: public SetOperationCallback
		{
				Ardb* db;
				const DBID& dbid;
				const Slice& key;
				SetMetaValue meta;
				DiffCallback(Ardb* a, const DBID& id, const Slice& k) :
						db(a), dbid(id), key(k)
				{
				}
				void OnSubset(ValueArray& array)
				{
					BatchWriteGuard guard(db->GetEngine());
					meta.size += array.size();
					meta.max = *(array.rbegin());
					if (meta.min.type == EMPTY)
					{
						meta.min = *(array.begin());
					}
					ValueArray::iterator it = array.begin();
					while (it != array.end())
					{
						SetKeyObject sk(key, *it, dbid);
						ValueObject empty;
						db->SetValue(sk, empty);
						it++;
					}
					db->SetSetMetaValue(dbid, key, meta);
				}
		} callback(this, db, dst);
		callback.db = this;
		SClear(db, dst);
		KeyLockerGuard keyguard(m_key_locker, db, dst);
		SDiff(db, keys, &callback, MAX_SET_OP_STORE_NUM);
		return callback.meta.size;
	}

	int Ardb::SInterCount(const DBID& db, SliceArray& keys, uint32& count)
	{
		struct CountCallback: public SetOperationCallback
		{
				uint32 count;
				void OnSubset(ValueArray& array)
				{
					//INFO_LOG("size = %u",array.size());
					count += array.size();
				}
		} callback;
		callback.count = 0;
		SInter(db, keys, &callback, MAX_SET_INTER_NUM);
		count = callback.count;
		return 0;
	}

	int Ardb::SDiff(const DBID& db, SliceArray& keys, SetOperationCallback* callback, uint32 max_subset_num)
	{
		if (keys.size() < 2)
		{
			return ERR_INVALID_ARGS;
		}
		SetMetaValueArray metas;
		SliceArray::iterator kit = keys.begin();
		while (kit != keys.end())
		{
			SetMetaValue meta;
			GetSetMetaValue(db, *kit, meta);
			metas.push_back(meta);
			kit++;
		}
		std::vector<ValueObject> fromObjs;
		for (uint32 i = 0; i < keys.size(); i++)
		{
			fromObjs.push_back(metas[i].min);
		}
		bool isfirst = true;
		while (true)
		{
			ValueArray base;
			ValueArray next;
			ValueArray* cmp = &base;
			ValueArray* result = &next;
			SetRange(db, keys[0], fromObjs[0], metas[0].max, max_subset_num, isfirst, *cmp);
			if (cmp->empty())
			{
				return 0;
			}
			fromObjs[0] = *(cmp->rbegin());
			for (uint32 i = 1; i < keys.size(); i++)
			{
				ValueArray tmp;
				SetRange(db, keys[i], fromObjs[i], fromObjs[0], -1, isfirst, tmp);
				if (!tmp.empty())
				{
					fromObjs[i] = *(tmp.rbegin());
				} else
				{
					fromObjs[i] = fromObjs[0];
				}
				result->clear();
				result->resize(cmp->size());
				ValueArray::iterator ret = std::set_difference(cmp->begin(), cmp->end(), tmp.begin(), tmp.end(), result->begin());
				if (ret == result->begin())
				{
					break;
				} else
				{
					result->resize(ret - result->begin());
					ValueArray* p = cmp;
					cmp = result;
					result = p;
				}
			}
			isfirst = false;
			callback->OnSubset(*cmp);
		}
		return 0;
	}

	int Ardb::SUnion(const DBID& db, SliceArray& keys, SetOperationCallback* callback, uint32 max_subset_num)
	{
		if (keys.size() < 2)
		{
			return ERR_INVALID_ARGS;
		}
		SetMetaValueArray metas;
		ValueObject min;
		ValueObject max;
		SliceArray::iterator kit = keys.begin();
		while (kit != keys.end())
		{
			SetMetaValue meta;
			GetSetMetaValue(db, *kit, meta);
			metas.push_back(meta);
			if (min.type == EMPTY || min.Compare(meta.min) < 0)
			{
				min = meta.min;
			}
			if (max.type == EMPTY || max.Compare(meta.max) > 0)
			{
				max = meta.max;
			}
			kit++;
		}
		std::vector<ValueObject> fromObjs;
		std::set<uint32> readySetKeyIdxs;
		for (uint32 i = 0; i < keys.size(); i++)
		{
			fromObjs.push_back(metas[i].min);
			readySetKeyIdxs.insert(i);
		}

		bool isfirst = true;
		while (!readySetKeyIdxs.empty())
		{
			ValueArray base;
			ValueArray next;
			ValueArray* cmp = &base;
			ValueArray* result = &next;
			uint32 firtidx = *(readySetKeyIdxs.begin());
			SetRange(db, keys[0], fromObjs[firtidx], metas[firtidx].max, max_subset_num, isfirst, *cmp);
			if (!cmp->empty())
			{
				fromObjs[firtidx] = *(cmp->rbegin());
			}else
			{
				fromObjs[firtidx] = metas[firtidx].max;
			}

			std::set<uint32>::iterator iit = readySetKeyIdxs.begin();
			iit++;
			std::set<uint32> finishedidxs;
			while (iit != readySetKeyIdxs.end())
			{
				uint32 idx = *iit;
				ValueArray tmp;
				SetRange(db, keys[idx], fromObjs[idx], fromObjs[firtidx], -1, isfirst, tmp);
				if (!tmp.empty())
				{
					fromObjs[idx] = *(tmp.rbegin());
				} else
				{
					fromObjs[idx] = fromObjs[firtidx];
				}
				result->clear();
				result->resize(cmp->size() + tmp.size());
				ValueArray::iterator ret = std::set_union(cmp->begin(), cmp->end(), tmp.begin(), tmp.end(), result->begin());
				if (ret != result->begin())
				{
					result->resize(ret - result->begin());
					ValueArray* p = cmp;
					cmp = result;
					result = p;
				}
				if(fromObjs[idx] >= metas[idx].max)
				{
					finishedidxs.insert(idx);
				}
				iit++;
			}

			isfirst = false;
			callback->OnSubset(*cmp);
			if (fromObjs[firtidx] >= metas[firtidx].max)
			{
				finishedidxs.insert(firtidx);
			}
			std::set<uint32>::iterator eit = finishedidxs.begin();
			while(eit != finishedidxs.end())
			{
				readySetKeyIdxs.erase(*eit);
				eit++;
			}
		}
		return 0;
	}

	int Ardb::SInter(const DBID& db, SliceArray& keys, SetOperationCallback* callback, uint32 max_subset_num)
	{
		if (keys.size() < 2)
		{
			return ERR_INVALID_ARGS;
		}
		int32 min_size = -1;
		SetMetaValueArray metas;
		uint32 min_idx = 0;
		uint32 idx = 0;
		ValueObject min;
		ValueObject max;
		SliceArray::iterator kit = keys.begin();
		while (kit != keys.end())
		{
			SetMetaValue meta;
			if (0 == GetSetMetaValue(db, *kit, meta))
			{
				if (min_size == -1 || min_size > (int32) meta.size)
				{
					min_size = meta.size;
					min_idx = idx;
				}
			}
			if (meta.size == 0)
			{
				return 0;
			}
			if (min.type == EMPTY || min.Compare(meta.min) < 0)
			{
				min = meta.min;
			}
			if (max.type == EMPTY || max.Compare(meta.max) > 0)
			{
				max = meta.max;
			}
			metas.push_back(meta);
			kit++;
			idx++;
		}
		std::vector<ValueObject> fromObjs;
		for (uint32 i = 0; i < keys.size(); i++)
		{
			fromObjs.push_back(min);
		}
		bool isfirst = true;
		while (true)
		{
			ValueArray base;
			ValueArray next;
			ValueArray* cmp = &base;
			ValueArray* result = &next;
			/*
			 * Use the smallest set as the base set
			 */
			SetRange(db, keys[min_idx], fromObjs[min_idx], max, max_subset_num, isfirst, *cmp);
			if (cmp->empty())
			{
				return 0;
			}
			fromObjs[min_idx] = *(cmp->rbegin());
			for (uint32 i = 0; i < keys.size(); i++)
			{
				if (i != min_idx)
				{
					result->clear();
					result->resize(cmp->size());
					ValueArray tmp;
					SetRange(db, keys[i], fromObjs[i], fromObjs[min_idx], -1, isfirst, tmp);
					if (tmp.empty())
					{
						return 0;
					}
					fromObjs[i] = *(tmp.rbegin());
					result->resize(cmp->size());
					ValueArray::iterator ret = std::set_intersection(cmp->begin(), cmp->end(), tmp.begin(), tmp.end(), result->begin());
					if (ret == result->begin())
					{
						break;
					} else
					{
						result->resize(ret - result->begin());
						ValueArray* p = cmp;
						cmp = result;
						result = p;
					}
				}
			}
			isfirst = false;
			callback->OnSubset(*cmp);
		}
		return 0;
	}

	int Ardb::SInter(const DBID& db, SliceArray& keys, ValueArray& values)
	{
		if (keys.size() < 2)
		{
			return ERR_INVALID_ARGS;
		}
		struct InterCallback: public SetOperationCallback
		{
				ValueArray& vs;
				InterCallback(ValueArray& v) :
						vs(v)
				{
				}
				void OnSubset(ValueArray& array)
				{
					std::string str;
					vs.insert(vs.end(), array.begin(), array.end());
				}
		} callback(values);
		SInter(db, keys, &callback, MAX_SET_INTER_NUM);
		return 0;
	}

	int Ardb::SInterStore(const DBID& db, const Slice& dst, SliceArray& keys)
	{
		if (keys.size() < 2)
		{
			return ERR_INVALID_ARGS;
		}
		struct InterCallback: public SetOperationCallback
		{
				Ardb* db;
				const DBID& dbid;
				const Slice& key;
				SetMetaValue meta;
				InterCallback(Ardb* a, const DBID& id, const Slice& k) :
						db(a), dbid(id), key(k)
				{
				}
				void OnSubset(ValueArray& array)
				{
					BatchWriteGuard guard(db->GetEngine());
					meta.size += array.size();
					meta.max = *(array.rbegin());
					if (meta.min.type == EMPTY)
					{
						meta.min = *(array.begin());
					}
					ValueArray::iterator it = array.begin();
					while (it != array.end())
					{
						SetKeyObject sk(key, *it, dbid);
						ValueObject empty;
						db->SetValue(sk, empty);
						it++;
					}
					db->SetSetMetaValue(dbid, key, meta);
				}
		} callback(this, db, dst);
		callback.db = this;
		SClear(db, dst);
		KeyLockerGuard keyguard(m_key_locker, db, dst);
		SInter(db, keys, &callback, MAX_SET_OP_STORE_NUM);
		return callback.meta.size;
	}

	int Ardb::SMove(const DBID& db, const Slice& src, const Slice& dst, const Slice& value)
	{
		SetKeyObject sk(src, value, db);
		ValueObject sv;
		if (0 != GetValue(sk, &sv))
		{
			return 0;
		}
		SRem(db, src, value);
		SAdd(db, dst, value);
		return 1;
	}

	int Ardb::SPop(const DBID& db, const Slice& key, std::string& value)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		SetMetaValue meta;
		if (0 != GetSetMetaValue(db, key, meta))
		{
			return ERR_NOT_EXIST;
		}
		Slice empty;
		SetKeyObject sk(key, meta.min, db);
		Iterator* iter = FindValue(sk);
		bool delvalue = false;
		BatchWriteGuard guard(GetEngine());
		while (iter != NULL && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey, &sk);
			if (NULL == kk)
			{
				break;
			}
			SetKeyObject* sek = (SetKeyObject*) kk;
			if (delvalue)
			{
				meta.min = sek->value;
				DELETE(kk);
				break;
			}
			sek->value.ToString(value);
			meta.size--;
			DelValue(*sek);
			DELETE(kk);
			delvalue = true;
		}
		if (meta.size == 0)
		{
			KeyObject k(key, SET_META, db);
			DelValue(k);
		} else
		{
			SetSetMetaValue(db, key, meta);
		}
		DELETE(iter);
		return 0;
	}

	int Ardb::SRandMember(const DBID& db, const Slice& key, ValueArray& values, int count)
	{
		Slice empty;
		SetKeyObject sk(key, empty, db);
		Iterator* iter = FindValue(sk, true);
		uint32 total = count;
		if (count < 0)
		{
			total = 0 - count;
		}
		uint32 cursor = 0;
		while (iter != NULL && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey, &sk);
			if (NULL == kk)
			{
				DELETE(kk);
				break;
			}
			SetKeyObject* sek = (SetKeyObject*) kk;
			values.push_back(sek->value);
			DELETE(kk);
			iter->Next();
			cursor++;
			if (cursor == total)
			{
				break;
			}
		}
		DELETE(iter);
		while (!values.empty() && count < 0 && values.size() < total)
		{
			values.push_back(values.front());
		}
		return 0;
	}

	int Ardb::SUnionCount(const DBID& db, SliceArray& keys, uint32& count)
	{
		struct CountCallback: public SetOperationCallback
		{
				uint32 count;
				void OnSubset(ValueArray& array)
				{
					//INFO_LOG("size = %u",array.size());
					count += array.size();
				}
		} callback;
		callback.count = 0;
		SUnion(db, keys, &callback, MAX_SET_UNION_NUM);
		count = callback.count;
		return 0;
	}

	int Ardb::SUnion(const DBID& db, SliceArray& keys, ValueArray& values)
	{
		if (keys.size() < 2)
		{
			return ERR_INVALID_ARGS;
		}
		struct UnionCallback: public SetOperationCallback
		{
				ValueArray& result;
				UnionCallback(ValueArray& r) :
						result(r)
				{
				}
				void OnSubset(ValueArray& array)
				{
					result.insert(result.end(), array.begin(), array.end());
				}
		} callback(values);
		SUnion(db, keys, &callback, MAX_SET_UNION_NUM);
		return 0;
	}

	int Ardb::SUnionStore(const DBID& db, const Slice& dst, SliceArray& keys)
	{
		if (keys.size() < 2)
		{
			return ERR_INVALID_ARGS;
		}
		struct UnionCallback: public SetOperationCallback
		{
				Ardb* db;
				const DBID& dbid;
				const Slice& key;
				SetMetaValue meta;
				UnionCallback(Ardb* a, const DBID& id, const Slice& k) :
						db(a), dbid(id), key(k)
				{
				}
				void OnSubset(ValueArray& array)
				{
					BatchWriteGuard guard(db->GetEngine());
					meta.size += array.size();
					meta.max = *(array.rbegin());
					if (meta.min.type == EMPTY)
					{
						meta.min = *(array.begin());
					}
					ValueArray::iterator it = array.begin();
					while (it != array.end())
					{
						SetKeyObject sk(key, *it, dbid);
						ValueObject empty;
						db->SetValue(sk, empty);
						it++;
					}
					db->SetSetMetaValue(dbid, key, meta);
				}
		} callback(this, db, dst);
		callback.db = this;
		SClear(db, dst);
		KeyLockerGuard keyguard(m_key_locker, db, dst);
		SUnion(db, keys, &callback, MAX_SET_OP_STORE_NUM);
		return callback.meta.size;
	}
}

