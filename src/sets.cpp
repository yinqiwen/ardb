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
#define MAX_SET_QUERY_NUM 1000000

namespace ardb
{

	SetMetaValue* Ardb::GetSetMeta(const DBID& db, const Slice& key, int& err,
	bool& create)
	{
		CommonMetaValue* meta = GetMeta(db, key, false);
		if (NULL != meta && meta->header.type != SET_META)
		{
			DELETE(meta);
			err = ERR_INVALID_TYPE;
			return NULL;
		}
		if (NULL == meta)
		{
			meta = new SetMetaValue;
			create = true;
		} else
		{
			create = false;
		}
		return (SetMetaValue*)meta;
	}
	void Ardb::FindSetMinMaxValue(const DBID& db, const Slice& key,
	        ValueData& min, ValueData& max)
	{
		min.Clear();
		max.Clear();
		SetKeyObject sk(key, Slice(), db);
		Iterator* iter = FindValue(sk, false);
		if (NULL != iter && iter->Valid())
		{
			KeyObject* kk = decode_key(iter->Key(), &sk);
			if (NULL != kk)
			{
				SetKeyObject* mink = (SetKeyObject*) kk;
				min = mink->value;
			}
			DELETE(kk);
		}
		DELETE(iter);

		std::string next;
		next_key(key, next);
		SetKeyObject sk1(next, Slice(), db);
		iter = FindValue(sk1, false);
		if (NULL != iter && iter->Valid())
		{
			iter->Prev();
		}
		if (NULL != iter && iter->Valid())
		{
			KeyObject* kk = decode_key(iter->Key(), &sk);
			if (NULL != kk)
			{
				SetKeyObject* mink = (SetKeyObject*) kk;
				max = mink->value;
			}
			DELETE(kk);
		}
		DELETE(iter);
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
		int err = 0;
		bool createSet = false;
		SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
		if (NULL == meta)
		{
			return err;
		}
		SetKeyObject sk(key, value, db);
		bool set_changed = createSet;
		if (meta->max.type == EMPTY_VALUE)
		{
			meta->min = sk.value;
			meta->max = sk.value;
			meta->size++;
			set_changed = true;
		} else
		{
			if (meta->min.Compare(sk.value) == 0
			        || meta->max.Compare(sk.value) == 0)
			{
				DELETE(meta);
				return 0;
			}
			if (meta->min.Compare(sk.value) > 0)
			{
				meta->size++;
				meta->min = sk.value;
				set_changed = true;
			} else if (meta->max.Compare(sk.value) < 0)
			{
				meta->size++;
				meta->max = sk.value;
				set_changed = true;
			} else
			{
				EmptyValueObject empty;
				if (0 != GetKeyValueObject(sk, empty))
				{
					meta->size++;
					if (meta->min.Compare(sk.value) > 0)
					{
						meta->min = sk.value;
					}
					if (meta->max.Compare(sk.value) < 0)
					{
						meta->max = sk.value;
					}
					set_changed = true;
				}
			}
		}
		if (set_changed)
		{
			BatchWriteGuard guard(GetEngine());
			EmptyValueObject empty;
			SetKeyValueObject(sk, empty);
			SetMeta(db, key, *meta);
			DELETE(meta);
			return 1;
		}
		return 0;
	}

	int Ardb::SCard(const DBID& db, const Slice& key)
	{
		int err = 0;
		bool createSet = false;
		SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
		if (NULL == meta)
		{
			return err;
		}
		int size = meta->size;
		DELETE(meta);
		return size;
	}

	bool Ardb::SIsMember(const DBID& db, const Slice& key, const Slice& value)
	{
		SetKeyObject sk(key, value, db);
		std::string empty;
		if (0 != GetRawValue(sk, empty))
		{
			return false;
		}
		return true;
	}

	int Ardb::SRem(const DBID& db, const Slice& key, const SliceArray& values)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		int err = 0;
		bool createSet = false;
		SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
		if (NULL == meta || createSet)
		{
			DELETE(meta);
			return err;
		}
		int count = 0;
		bool retrive_min_max = false;
		{
			BatchWriteGuard guard(GetEngine());
			SliceArray::const_iterator it = values.begin();
			while (it != values.end())
			{
				SetKeyObject sk(key, *it, db);
				std::string empty;
				if (0 == GetRawValue(sk, empty))
				{
					meta->size--;
					DelValue(sk);
					count++;
					if (sk.value == meta->max || sk.value == meta->min)
					{
						retrive_min_max = true;
					}
				}
				it++;
			}
			if (count > 0)
			{
				if (meta->size == 0)
				{
					KeyObject k(key, SET_META, db);
					DelValue(k);
				} else
				{
					if (!retrive_min_max)
					{
						SetMeta(db, key, *meta);
					}
				}
			}
		}
		if (retrive_min_max)
		{
			FindSetMinMaxValue(db, key, meta->min, meta->max);
			SetMeta(db, key, *meta);
		}
		return count;
	}

	int Ardb::SRem(const DBID& db, const Slice& key, const Slice& value)
	{
		SliceArray vs;
		vs.push_back(value);
		return SRem(db, key, vs);
	}

	int Ardb::SRevRange(const DBID& db, const Slice& key,
	        const Slice& value_end, int count, bool with_end,
	        ValueArray& values)
	{
		if (count == 0)
		{
			return 0;
		}
		int err = 0;
		bool createSet = false;
		SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
		if (NULL == meta || createSet)
		{
			DELETE(meta);
			return err;
		}
		ValueData firstObj;
		if (value_end.empty() || !strcasecmp(value_end.data(), "+inf"))
		{
			firstObj = meta->max;
			with_end = true;
		} else
		{
			firstObj.SetValue(value_end, true);
		}
		SetKeyObject sk(key, firstObj, db);
		struct SGetWalk: public WalkHandler
		{
				ValueArray& z_values;
				ValueData& first;bool with_first;
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
				SGetWalk(ValueArray& vs, int count, ValueData& s, bool with) :
						z_values(vs), first(s), with_first(with), l(count)
				{
				}
		} walk(values, count, firstObj, with_end);
		Walk(sk, true, false, &walk);
		return 0;
	}

	int Ardb::SetRange(const DBID& db, const Slice& key,
	        const ValueData& value_begin, const ValueData& value_end,
	        int32 limit, bool with_begin, ValueArray& values)
	{
		if (limit == 0)
		{
			return 0;
		}
		SetKeyObject sk(key, value_begin, db);
		struct SGetWalk: public WalkHandler
		{
				ValueArray& z_values;
				const ValueData& first;
				const ValueData& end;bool with_first;
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
				SGetWalk(ValueArray& vs, int32 count, const ValueData& s,
				        const ValueData& e, bool with) :
						z_values(vs), first(s), end(e), with_first(with), l(
						        count)
				{
				}
		} walk(values, limit, value_begin, value_end, with_begin);
		Walk(sk, false, false, &walk);
		return 0;
	}

	int Ardb::SRange(const DBID& db, const Slice& key, const Slice& value_begin,
	        int count, bool with_begin, ValueArray& values)
	{
		if (count == 0)
		{
			return 0;
		}
		int err = 0;
		bool createSet = false;
		SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
		if (NULL == meta || createSet)
		{
			DELETE(meta);
			return err;
		}
		SetKeyObject sk(key, value_begin, db);
		if (value_begin.empty() || !strcasecmp(value_begin.data(), "-inf"))
		{
			sk.value = meta->min;
			with_begin = true;
		}

		struct SGetWalk: public WalkHandler
		{
				ValueArray& z_values;
				ValueData& first;bool with_first;
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
				SGetWalk(ValueArray& vs, int count, ValueData& s, bool with) :
						z_values(vs), first(s), with_first(with), l(count)
				{
				}
		} walk(values, count, sk.value, with_begin);
		Walk(sk, false, false, &walk);
		DELETE(meta);
		return 0;
	}

	int Ardb::SMembers(const DBID& db, const Slice& key, ValueArray& values)
	{
		int err = 0;
		bool createSet = false;
		SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
		if (NULL == meta || createSet)
		{
			DELETE(meta);
			return err;
		}
		if (meta->size >= MAX_SET_QUERY_NUM)
		{
			return ERR_TOO_LARGE_RESPONSE;
		}
		SetKeyObject sk(key, meta->min, db);
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
		Walk(sk, false, false, &walk);
		DELETE(meta);
		return 0;
	}

	int Ardb::SClear(const DBID& db, const Slice& key)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		Slice empty;
		SetKeyObject sk(key, empty, db);
		int err = 0;
		bool createSet = false;
		SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
		if (NULL == meta || createSet)
		{
			DELETE(meta);
			return err;
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
		Walk(sk, false, false, &walk);
		DelMeta(db, key, meta);
		DELETE(meta);
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
					if (meta.min.type == EMPTY_VALUE)
					{
						meta.min = *(array.begin());
					}
					ValueArray::iterator it = array.begin();
					while (it != array.end())
					{
						SetKeyObject sk(key, *it, dbid);
						EmptyValueObject empty;
						db->SetKeyValueObject(sk, empty);
						it++;
					}
					db->SetMeta(dbid, key, meta);
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

	int Ardb::SDiff(const DBID& db, SliceArray& keys,
	        SetOperationCallback* callback, uint32 max_subset_num)
	{
		if (keys.size() < 2)
		{
			return ERR_INVALID_ARGS;
		}
		SetMetaValueArray metas;
		SliceArray::iterator kit = keys.begin();
		while (kit != keys.end())
		{
			int err = 0;
			bool createSet = false;
			SetMetaValue* meta = GetSetMeta(db, *kit, err, createSet);
			if (NULL == meta)
			{
				delete_pointer_container(metas);
				return err;
			}
			metas.push_back(meta);
			kit++;
		}
		std::vector<ValueData> fromObjs;
		for (uint32 i = 0; i < keys.size(); i++)
		{
			fromObjs.push_back(metas[i]->min);
		}
		bool isfirst = true;
		while (true)
		{
			ValueArray base;
			ValueArray next;
			ValueArray* cmp = &base;
			ValueArray* result = &next;
			SetRange(db, keys[0], fromObjs[0], metas[0]->max, max_subset_num,
			        isfirst, *cmp);
			if (cmp->empty())
			{
				return 0;
			}
			fromObjs[0] = *(cmp->rbegin());
			for (uint32 i = 1; i < keys.size(); i++)
			{
				ValueArray tmp;
				SetRange(db, keys[i], fromObjs[i], fromObjs[0], -1, isfirst,
				        tmp);
				if (!tmp.empty())
				{
					fromObjs[i] = *(tmp.rbegin());
				} else
				{
					fromObjs[i] = fromObjs[0];
				}
				result->clear();
				result->resize(cmp->size());
				ValueArray::iterator ret = std::set_difference(cmp->begin(),
				        cmp->end(), tmp.begin(), tmp.end(), result->begin());
				if (ret == result->begin())
				{
					cmp->clear();
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
			if (!cmp->empty())
			{
				callback->OnSubset(*cmp);
			}
		}
		delete_pointer_container(metas);
		return 0;
	}

	int Ardb::SUnion(const DBID& db, SliceArray& keys,
	        SetOperationCallback* callback, uint32 max_subset_num)
	{
		if (keys.size() < 2)
		{
			return ERR_INVALID_ARGS;
		}
		SetMetaValueArray metas;
		ValueData min;
		ValueData max;
		SliceArray::iterator kit = keys.begin();
		while (kit != keys.end())
		{
			int err = 0;
			bool createSet = false;
			SetMetaValue* meta = GetSetMeta(db, *kit, err, createSet);
			if (NULL == meta)
			{
				delete_pointer_container(metas);
				return err;
			}
			metas.push_back(meta);
			if (min.type == EMPTY_VALUE || min.Compare(meta->min) < 0)
			{
				min = meta->min;
			}
			if (max.type == EMPTY_VALUE || max.Compare(meta->max) > 0)
			{
				max = meta->max;
			}
			kit++;
		}
		std::vector<ValueData> fromObjs;
		std::set<uint32> readySetKeyIdxs;
		for (uint32 i = 0; i < keys.size(); i++)
		{
			fromObjs.push_back(metas[i]->min);
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
			std::string str1, str2;
			SetRange(db, keys[firtidx], fromObjs[firtidx], metas[firtidx]->max,
			        max_subset_num, isfirst, *cmp);
			if (!cmp->empty())
			{
				fromObjs[firtidx] = *(cmp->rbegin());
			} else
			{
				fromObjs[firtidx] = metas[firtidx]->max;
			}
			std::set<uint32>::iterator iit = readySetKeyIdxs.begin();
			iit++;
			std::set<uint32> finishedidxs;
			while (iit != readySetKeyIdxs.end())
			{
				uint32 idx = *iit;
				ValueArray tmp;
				SetRange(db, keys[idx], fromObjs[idx], fromObjs[firtidx], -1,
				        isfirst, tmp);
				if (!tmp.empty())
				{
					fromObjs[idx] = *(tmp.rbegin());
				} else
				{
					fromObjs[idx] = fromObjs[firtidx];
				}
				result->clear();
				result->resize(cmp->size() + tmp.size());
				ValueArray::iterator ret = std::set_union(cmp->begin(),
				        cmp->end(), tmp.begin(), tmp.end(), result->begin());
				if (ret != result->begin())
				{
					result->resize(ret - result->begin());
					ValueArray* p = cmp;
					cmp = result;
					result = p;
				} else
				{

				}
				if (fromObjs[idx] >= metas[idx]->max)
				{
					finishedidxs.insert(idx);
				}
				iit++;
			}

			isfirst = false;
			if (!cmp->empty())
			{
				callback->OnSubset(*cmp);
			}
			if (fromObjs[firtidx] >= metas[firtidx]->max)
			{
				finishedidxs.insert(firtidx);
			}
			std::set<uint32>::iterator eit = finishedidxs.begin();
			while (eit != finishedidxs.end())
			{
				readySetKeyIdxs.erase(*eit);
				eit++;
			}
		}
		delete_pointer_container(metas);
		return 0;
	}

	int Ardb::SInter(const DBID& db, SliceArray& keys,
	        SetOperationCallback* callback, uint32 max_subset_num)
	{
		if (keys.size() < 2)
		{
			return ERR_INVALID_ARGS;
		}
		int32 min_size = -1;
		SetMetaValueArray metas;
		uint32 min_idx = 0;
		uint32 idx = 0;
		ValueData min;
		ValueData max;
		SliceArray::iterator kit = keys.begin();
		while (kit != keys.end())
		{
			int err = 0;
			bool createSet = false;
			SetMetaValue* meta = GetSetMeta(db, *kit, err, createSet);
			if (NULL == meta)
			{
				delete_pointer_container(metas);
				return err;
			}
			if (!createSet)
			{
				if (min_size == -1 || min_size > (int32) meta->size)
				{
					min_size = meta->size;
					min_idx = idx;
				}
			}
			if (meta->size == 0)
			{
				return 0;
			}
			if (min.type == EMPTY_VALUE || min.Compare(meta->min) < 0)
			{
				min = meta->min;
			}
			if (max.type == EMPTY_VALUE || max.Compare(meta->max) > 0)
			{
				max = meta->max;
			}
			metas.push_back(meta);
			kit++;
			idx++;
		}
		std::vector<ValueData> fromObjs;
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
			SetRange(db, keys[min_idx], fromObjs[min_idx], max, max_subset_num,
			        isfirst, *cmp);
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
					SetRange(db, keys[i], fromObjs[i], fromObjs[min_idx], -1,
					        isfirst, tmp);
					if (tmp.empty())
					{
						return 0;
					}
					fromObjs[i] = *(tmp.rbegin());
					result->resize(cmp->size());
					ValueArray::iterator ret = std::set_intersection(
					        cmp->begin(), cmp->end(), tmp.begin(), tmp.end(),
					        result->begin());
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
			if (!cmp->empty())
			{
				callback->OnSubset(*cmp);
			}
		}
		delete_pointer_container(metas);
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
					if (meta.min.type == EMPTY_VALUE)
					{
						meta.min = *(array.begin());
					}
					ValueArray::iterator it = array.begin();
					while (it != array.end())
					{
						SetKeyObject sk(key, *it, dbid);
						EmptyValueObject empty;
						db->SetKeyValueObject(sk, empty);
						it++;
					}
					db->SetMeta(dbid, key, meta);
				}
		} callback(this, db, dst);
		callback.db = this;
		SClear(db, dst);
		KeyLockerGuard keyguard(m_key_locker, db, dst);
		SInter(db, keys, &callback, MAX_SET_OP_STORE_NUM);
		return callback.meta.size;
	}

	int Ardb::SMove(const DBID& db, const Slice& src, const Slice& dst,
	        const Slice& value)
	{
		SetKeyObject sk(src, value, db);
		std::string sv;
		if (0 != GetRawValue(sk, sv))
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
		int err = 0;
		bool createSet = false;
		SetMetaValue* meta = GetSetMeta(db, key, err, createSet);
		if (NULL == meta)
		{
			return err;
		}
		Slice empty;
		SetKeyObject sk(key, meta->min, db);
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
				meta->min = sek->value;
				DELETE(kk);
				break;
			}
			sek->value.ToString(value);
			meta->size--;
			DelValue(*sek);
			DELETE(kk);
			delvalue = true;
		}
		if (meta->size == 0)
		{
			KeyObject k(key, SET_META, db);
			DelValue(k);
		} else
		{
			SetMeta(db, key, *meta);
		}
		DELETE(iter);
		DELETE(meta);
		return 0;
	}

	int Ardb::SRandMember(const DBID& db, const Slice& key, ValueArray& values,
	        int count)
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
					if (array.size() > 0)
					{
						meta.max = *(array.rbegin());
						if (meta.min.type == EMPTY_VALUE)
						{
							meta.min = *(array.begin());
						}
						ValueArray::iterator it = array.begin();
						while (it != array.end())
						{
							SetKeyObject sk(key, *it, dbid);
							EmptyValueObject empty;
							db->SetKeyValueObject(sk, empty);
							it++;
						}
						db->SetMeta(dbid, key, meta);
					}

				}
		} callback(this, db, dst);
		callback.db = this;
		SClear(db, dst);
		KeyLockerGuard keyguard(m_key_locker, db, dst);
		SUnion(db, keys, &callback, MAX_SET_OP_STORE_NUM);
		return callback.meta.size;
	}
}

