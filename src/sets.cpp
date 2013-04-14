/*
 * sets.cpp
 *
 *  Created on: 2013-4-2
 *      Author: wqy
 */

#include "ardb.hpp"

namespace ardb
{
static bool DecodeSetMetaData(ValueObject& v, SetMetaValue& meta)
{
	if (v.type != RAW)
	{
		return false;
	}
	return BufferHelper::ReadVarUInt32(*(v.v.raw), meta.size)
			&& decode_value(*(v.v.raw), meta.min)
			&& decode_value(*(v.v.raw), meta.max);
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
	KeyObject k(key, SET_META);
	ValueObject v;
	if (0 == GetValue(db, k, &v))
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
	KeyObject k(key, SET_META);
	ValueObject v;
	EncodeSetMetaData(v, meta);
	SetValue(db, k, v);
}

int Ardb::SAdd(const DBID& db, const Slice& key, const SliceArray& values)
{
	BatchWriteGuard guard(GetDB(db));
	int count = 0;
	for (int i = 0; i < values.size(); i++)
	{
		count += SAdd(db, key, values[i]);
	}
	return count;
}

int Ardb::SAdd(const DBID& db, const Slice& key, const Slice& value)
{
	KeyObject k(key, SET_META);
	SetMetaValue meta;
	GetSetMetaValue(db, key, meta);
	ValueObject v;
	smart_fill_value(value, v);
	SetKeyObject sk(key, v);
	ValueObject sv;
	if (0 != GetValue(db, sk, &sv))
	{
		meta.size++;
		if (meta.min.type == EMPTY || meta.min.Compare(v) > 0)
		{
			meta.min = v;
		}
		if (meta.max.type == EMPTY || meta.max.Compare(v) < 0)
		{
			meta.max = v;
		}
		sv.type = EMPTY;
		BatchWriteGuard guard(GetDB(db));
		SetValue(db, sk, sv);
		SetSetMetaValue(db, key, meta);
		return 1;
	}
	return 0;
}

int Ardb::SCard(const DBID& db, const Slice& key)
{
	KeyObject k(key, SET_META);
	ValueObject v;
	SetMetaValue meta;
	if (0 == GetValue(db, k, &v))
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
	SetKeyObject sk(key, value);
	if (0 != GetValue(db, sk, NULL))
	{
		return false;
	}
	return true;
}

int Ardb::SRem(const DBID& db, const Slice& key, const SliceArray& values)
{
	SetMetaValue meta;
	if (0 != GetSetMetaValue(db, key, meta))
	{
		return ERR_NOT_EXIST;
	}
	BatchWriteGuard guard(GetDB(db));
	int count = 0;
	SliceArray::const_iterator it = values.begin();
	while (it != values.end())
	{
		SetKeyObject sk(key, *it);
		if (0 == GetValue(db, sk, NULL))
		{
			meta.size--;
			DelValue(db, sk);
			count++;
		}
		it++;
	}
	if(count > 0)
	{
		SetSetMetaValue(db, key, meta);
	}
	return count;
}

int Ardb::SRem(const DBID& db, const Slice& key, const Slice& value)
{
	SetMetaValue meta;
	if (0 != GetSetMetaValue(db, key, meta))
	{
		return ERR_NOT_EXIST;
	}
	SetKeyObject sk(key, value);
	if (0 == GetValue(db, sk, NULL))
	{
		meta.size--;
		BatchWriteGuard guard(GetDB(db));
		DelValue(db, sk);
		SetSetMetaValue(db, key, meta);
		return 1;
	}
	return 0;
}

int Ardb::SMembers(const DBID& db, const Slice& key, ValueArray& values)
{
	Slice empty;
	SetKeyObject sk(key, empty);

	struct SMembersWalk: public WalkHandler
	{
		ValueArray& z_values;
		int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
		{
			SetKeyObject* sek = (SetKeyObject*) k;
			z_values.push_back(sek->value);
			return 0;
		}
		SMembersWalk(StringArray& vs) :
				z_values(vs)
		{
		}
	} walk(values);
	Walk(db, sk, false, &walk);
	return 0;
}

int Ardb::SClear(const DBID& db, const Slice& key)
{
	Slice empty;
	SetKeyObject sk(key, empty);
	struct SClearWalk: public WalkHandler
	{
		Ardb* z_db;
		DBID z_dbid;
		int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
		{
			SetKeyObject* sek = (SetKeyObject*) k;
			z_db->DelValue(z_dbid, *sek);
			return 0;
		}
		SClearWalk(Ardb* db, const DBID& dbid) :
				z_db(db), z_dbid(dbid)
		{
		}
	} walk(this, db);
	BatchWriteGuard guard(GetDB(db));
	Walk(db, sk, false, &walk);
	KeyObject k(key, SET_META);
	DelValue(db, k);
	return 0;
}

int Ardb::SDiff(const DBID& db, SliceArray& keys, ValueSet& values)
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
	struct SDiffWalk: public WalkHandler
	{
		int idx;
		ValueSet& vset;
		ValueObject vlimit;
		int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
		{
			SetKeyObject* sek = (SetKeyObject*) k;
			if (idx == 0)
			{
				vset.insert(sek->value);
			}
			else
			{
				if (vset.count(sek->value) != 0)
				{
					vset.erase(sek->value);
				}
				if (vset.size() == 0)
				{
					return -1;
				}
				if (vlimit.type != EMPTY && sek->value.Compare(vlimit) > 0)
				{
					return -1;
				}
			}
			return 0;
		}
		SDiffWalk(ValueSet& vs, int i, const ValueObject& limit) :
				vset(vs), idx(i), vlimit(limit)
		{
		}
	};
	for (int i = 0; i < keys.size(); i++)
	{
		Slice k = keys[i];
		ValueObject search_value;
		ValueObject limit;
		if (i > 0)
		{
			if (values.size() == 0)
			{
				break;
			}
			int cmp_min = metas[i].min.Compare(*(values.rbegin()));
			if (metas[i].min.Compare(*(values.rbegin())) > 0)
			{
				continue;
			}
			else if (metas[i].max.Compare(*(values.begin())) < 0)
			{
				continue;
			}
			if (metas[i].min.Compare(*(values.begin())) < 0)
			{
				search_value = *(values.begin());
			}
			if (metas[i].max.Compare(*(values.rbegin())) > 0)
			{
				limit = *(values.rbegin());
			}
		}
		SetKeyObject sk(k, search_value);
		SDiffWalk walk(values, i, limit);
		Walk(db, sk, false, &walk);
	}
	return 0;
}

int Ardb::SDiffStore(const DBID& db, const Slice& dst, SliceArray& keys)
{
	if (keys.size() < 2)
	{
		return ERR_INVALID_ARGS;
	}
	ValueSet vs;
	if (0 == SDiff(db, keys, vs) && vs.size() > 0)
	{
		BatchWriteGuard guard(GetDB(db));
		SClear(db, dst);
		SetMetaValue meta;
		StringArray::iterator it = vs.begin();
		while (it != vs.end())
		{
			std::string& v = *it;
			Slice sv(v.c_str(), v.size());
			SetKeyObject sk(dst, sv);
			ValueObject empty;
			empty.type = EMPTY;
			SetValue(db, sk, empty);
			meta.size++;
			if (meta.min.type == EMPTY || sk.value.Compare(meta.min) < 0)
			{
				meta.min = sk.value;
			}
			if (meta.max.type == EMPTY || sk.value.Compare(meta.max) > 0)
			{
				meta.max = sk.value;
			}
			it++;
		}
		SetSetMetaValue(db, dst, meta);
		return meta.size;
	}
	return 0;

}

int Ardb::SInter(const DBID& db, SliceArray& keys, ValueSet& values)
{
	if (keys.size() < 2)
	{
		return ERR_INVALID_ARGS;
	}
	uint32_t min_size = -1;
	SetMetaValueArray metas;
	uint32_t min_idx = 0;
	uint32_t idx = 0;
	ValueObject min;
	ValueObject max;
	SliceArray::iterator kit = keys.begin();
	while (kit != keys.end())
	{
		SetMetaValue meta;
		if (0 == GetSetMetaValue(db, *kit, meta))
		{
			if (min_size == -1 || min_size > meta.size)
			{
				min_size = meta.size;
				min_idx = idx;
			}
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

	struct SInterWalk: public WalkHandler
	{
		ValueSet& z_cmp;
		ValueSet& z_result;
		const ValueObject& s_min;
		const ValueObject& s_max;
		const ValueObject* current_min;
		const ValueObject* current_max;
		SInterWalk(ValueSet& cmp, ValueSet& result, const ValueObject& min,
				const ValueObject& max) :
				z_cmp(cmp), z_result(result), s_min(min), s_max(max), current_min(
						NULL), current_max(NULL)
		{
		}
		int OnKeyValue(KeyObject* k, ValueObject* value, uint32 cursor)
		{
			SetKeyObject* sk = (SetKeyObject*) k;
			if (sk->value.Compare(s_min) < 0)
			{
				return 0;
			}
			if (sk->value.Compare(s_max) > 0)
			{
				return -1;
			}
			if ((&z_cmp != &z_result) && z_cmp.count(sk->value) == 0)
			{
				return 0;
			}
			std::pair<ValueSet::iterator, bool> iter = z_result.insert(
					sk->value);
			if (NULL == current_min)
			{
				current_min = &(*(iter.first));
			}
			current_max = &(*(iter.first));
			return 0;
		}
	};
	ValueSet cmp1, cmp2;
	SetKeyObject cmp_start(keys[min_idx], min);
	SInterWalk walk(cmp1, cmp1, min, max);
	Walk(db, cmp_start, false, &walk);
	ValueSet* cmp = &cmp1;
	ValueSet* result = &cmp2;
	for (uint32_t i = 0; i < keys.size(); i++)
	{
		if (i != min_idx)
		{
			SetKeyObject tmp(keys.at(i), min);
			SInterWalk walk(*cmp, *result, min, max);
			Walk(db, tmp, false, &walk);
			cmp->clear();
			ValueSet* old = cmp;
			cmp = result;
			result = old;
			if (NULL != walk.current_min && min.Compare(*walk.current_min) < 0)
			{
				min = *walk.current_min;
			}
			if (NULL != walk.current_max && max.Compare(*walk.current_max) > 0)
			{
				max = *walk.current_max;
			}
		}
	}
	values = *cmp;
	return 0;
}

int Ardb::SInterStore(const DBID& db, const Slice& dst, SliceArray& keys)
{
	if (keys.size() < 2)
	{
		return ERR_INVALID_ARGS;
	}
	ValueSet vs;
	if (0 == SInter(db, keys, vs) && vs.size() > 0)
	{
		BatchWriteGuard guard(GetDB(db));
		SClear(db, dst);
		SetMetaValue meta;
		StringArray::iterator it = vs.begin();
		while (it != vs.end())
		{
			std::string& v = *it;
			Slice sv(v.c_str(), v.size());
			SetKeyObject sk(dst, sv);
			ValueObject empty;
			empty.type = EMPTY;
			SetValue(db, sk, empty);
			meta.size++;
			if (meta.min.type == EMPTY || sk.value.Compare(meta.min) < 0)
			{
				meta.min = sk.value;
			}
			if (meta.max.type == EMPTY || sk.value.Compare(meta.max) > 0)
			{
				meta.max = sk.value;
			}
			it++;
		}
		SetSetMetaValue(db, dst, meta);
		return meta.size;
	}
	return 0;
}

int Ardb::SMove(const DBID& db, const Slice& src, const Slice& dst,
		const Slice& value)
{
	SetKeyObject sk(src, value);
	ValueObject sv;
	if (0 != GetValue(db, sk, &sv))
	{
		return 0;
	}
	SRem(db, src, value);
	SAdd(db, dst, value);
	return 1;
}

int Ardb::SPop(const DBID& db, const Slice& key, std::string& value)
{
	Slice empty;
	SetKeyObject sk(key, empty);
	Iterator* iter = FindValue(db, sk);
	while (iter != NULL && iter->Valid())
	{
		Slice tmpkey = iter->Key();
		KeyObject* kk = decode_key(tmpkey);
		if (NULL == kk || kk->type != SET_ELEMENT || kk->key.compare(key) != 0)
		{
			DELETE(kk);
			break;
		}
		SetKeyObject* sek = (SetKeyObject*) kk;
		value.assign(sek->value.ToString());
		DelValue(db, *sek);
		DELETE(kk);
		return 0;
	}
	return -1;
}

int Ardb::SRandMember(const DBID& db, const Slice& key, ValueArray& values,
		int count)
{
	Slice empty;
	SetKeyObject sk(key, empty);
	Iterator* iter = FindValue(db, sk);
	int total = count;
	if (count < 0)
	{
		total = 0 - count;
	}
	int cursor = 0;
	while (iter != NULL && iter->Valid())
	{
		Slice tmpkey = iter->Key();
		KeyObject* kk = decode_key(tmpkey);
		if (NULL == kk || kk->type != SET_ELEMENT || kk->key.compare(key) != 0)
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

int Ardb::SUnion(const DBID& db, SliceArray& keys, ValueSet& values)
{
	for (int i = 0; i < keys.size(); i++)
	{
		Slice k = keys.at(i);
		std::string tmp(k.data(), k.size());
		ValueArray vss;
		SMembers(db, k, vss);
		ValueArray::iterator vit = vss.begin();
		while (vit != vss.end())
		{
			values.insert(*vit);
			vit++;
		}
	}
	return 0;
}

int Ardb::SUnionStore(const DBID& db, const Slice& dst, SliceArray& keys)
{
	ValueSet ss;
	if (0 == SUnion(db, keys, ss) && ss.size() > 0)
	{
		BatchWriteGuard guard(GetDB(db));
		SClear(db, dst);
		SetMetaValue meta;
		StringArray::iterator it = ss.begin();
		while (it != ss.end())
		{
			std::string& v = *it;
			Slice sv(v.c_str(), v.size());
			SetKeyObject sk(dst, sv);
			ValueObject empty;
			empty.type = EMPTY;
			SetValue(db, sk, empty);
			meta.size++;
			if (meta.min.type == EMPTY || sk.value.Compare(meta.min) < 0)
			{
				meta.min = sk.value;
			}
			if (meta.max.type == EMPTY || sk.value.Compare(meta.max) > 0)
			{
				meta.max = sk.value;
			}
			it++;
		}
		SetSetMetaValue(db, dst, meta);
		return meta.size;
	}
	return 0;
}

}

