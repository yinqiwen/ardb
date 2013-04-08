/*
 * hash.cpp
 *
 *  Created on: 2013-3-31
 *      Author: wqy
 */
#include "rddb.hpp"

namespace rddb
{
	int RDDB::SetHashValue(DBID db, const Slice& key, const Slice& field,
			ValueObject& value)
	{
		HashKeyObject k(key, field);
		return SetValue(db, k, value);
	}
	int RDDB::HSet(DBID db, const Slice& key, const Slice& field,
			const Slice& value)
	{
		ValueObject valueobject;
		fill_value(value, valueobject);
		return SetHashValue(db, key, field, valueobject);
	}

	int RDDB::HSetNX(DBID db, const Slice& key, const Slice& field,
			const Slice& value)
	{
		if (HExists(db, key, field))
		{
			return 0;
		}
		return HSet(db, key, field, value) == 0 ? 1 : 0;
	}

	int RDDB::HDel(DBID db, const Slice& key, const Slice& field)
	{
		HashKeyObject k(key, field);
		return DelValue(db, k);
	}

	int RDDB::HGet(DBID db, const Slice& key, const Slice& field,
			ValueObject& value)
	{
		HashKeyObject k(key, field);
		if (0 == GetValue(db, k, value))
		{
			return 0;
		}
		return ERR_NOT_EXIST;
	}

	int RDDB::HMSet(DBID db, const Slice& key, const SliceArray& fields,
			const SliceArray& values)
	{
		if (fields.size() != values.size())
		{
			return ERR_INVALID_ARGS;
		}
		BatchWriteGuard guard(GetDB(db));
		SliceArray::const_iterator it = fields.begin();
		SliceArray::const_iterator sit = values.begin();
		while (it != fields.end())
		{
			HSet(db, key, *it, *sit);
			it++;
			sit++;
		}
		return 0;
	}

	int RDDB::HMGet(DBID db, const Slice& key, const SliceArray& fields,
			ValueArray& values)
	{
		SliceArray::const_iterator it = fields.begin();
		while (it != fields.end())
		{
			HashKeyObject k(key, *it);
			ValueObject* v = new ValueObject;
			if (0 != GetValue(db, k, *v))
			{
				DELETE(v);
			}
			values.push_back(v);
			it++;
		}
		return 0;
	}

	int RDDB::HClear(DBID db, const Slice& key)
	{
		Slice empty;
		HashKeyObject sk(key, empty);
		struct HClearWalk: public WalkHandler
		{
				RDDB* z_db;
				DBID z_dbid;
				int OnKeyValue(KeyObject* k, ValueObject* v)
				{
					HashKeyObject* sek = (HashKeyObject*) k;
					z_db->DelValue(z_dbid, *sek);
					return 0;
				}
				HClearWalk(RDDB* db, DBID dbid) :
						z_db(db), z_dbid(dbid)
				{
				}
		} walk(this, db);
		BatchWriteGuard guard(GetDB(db));
		Walk(db, sk, false, &walk);
		return 0;
	}

	int RDDB::HKeys(DBID db, const Slice& key, StringArray& fields)
	{
		Slice empty;
		HashKeyObject k(key, empty);
		Iterator* it = FindValue(db, k);
		while (NULL != it && it->Valid())
		{
			Slice tmpkey = it->Key();
			KeyObject* kk = decode_key(tmpkey);
			if (NULL == kk || kk->type != HASH_FIELD
					|| kk->key.compare(key) != 0)
			{
				DELETE(kk);
				break;
			}
			HashKeyObject* hk = (HashKeyObject*) kk;
			std::string filed(hk->field.data(), hk->field.size());
			fields.push_back(filed);
			it->Next();
			DELETE(kk);
		}
		DELETE(it);
		return fields.empty() ? ERR_NOT_EXIST : 0;
	}

	int RDDB::HLen(DBID db, const Slice& key)
	{
		Slice empty;
		HashKeyObject k(key, empty);
		Iterator* it = FindValue(db, k);
		int len = 0;
		while (NULL != it && it->Valid())
		{
			Slice tmpkey = it->Key();
			KeyObject* kk = decode_key(tmpkey);
			if (NULL == kk || kk->type != HASH_FIELD
					|| kk->key.compare(key) != 0)
			{
				break;
			}
			len++;
			it->Next();
			DELETE(kk);
		}
		DELETE(it);
		return len;
	}

	int RDDB::HVals(DBID db, const Slice& key, ValueArray& values)
	{
		Slice empty;
		HashKeyObject k(key, empty);
		Iterator* it = FindValue(db, k);
		while (NULL != it && it->Valid())
		{
			Slice tmpkey = it->Key();
			KeyObject* kk = decode_key(tmpkey);
			if (NULL == kk || kk->type != HASH_FIELD
					|| kk->key.compare(key) != 0)
			{
				DELETE(kk);
				break;
			}
			ValueObject* v = new ValueObject();
			Buffer readbuf(const_cast<char*>(it->Value().data()), 0,
					it->Value().size());
			decode_value(readbuf, *v);
			values.push_back(v);
			it->Next();
			DELETE(kk);
		}
		DELETE(it);
		return values.empty() ? ERR_NOT_EXIST : 0;
	}

	int RDDB::HGetAll(DBID db, const Slice& key, StringArray& fields,
			ValueArray& values)
	{
		Slice empty;
		HashKeyObject k(key, empty);
		Iterator* it = FindValue(db, k);
		while (NULL != it && it->Valid())
		{
			Slice tmpkey = it->Key();
			KeyObject* kk = decode_key(tmpkey);
			if (NULL == kk)
			{
				break;
			}
			if (kk->type != HASH_FIELD || kk->key.compare(key) != 0)
			{
				break;
			}
			HashKeyObject* hk = (HashKeyObject*) kk;
			std::string filed(hk->field.data(), hk->field.size());
			fields.push_back(filed);
			ValueObject* v = new ValueObject();
			Buffer readbuf(const_cast<char*>(it->Value().data()), 0,
					it->Value().size());
			decode_value(readbuf, *v);
			values.push_back(v);
			it->Next();
		}
		DELETE(it);
		return fields.empty() ? ERR_NOT_EXIST : 0;
	}

	bool RDDB::HExists(DBID db, const Slice& key, const Slice& field)
	{
		ValueObject v;
		return HGet(db, key, field, v) == 0;
	}

	int RDDB::HIncrby(DBID db, const Slice& key, const Slice& field,
			int64_t increment, int64_t& value)
	{
		ValueObject v;
		v.type = INTEGER;
		HGet(db, key, field, v);
		value_convert_to_number(v);
		if (v.type != INTEGER)
		{
			return ERR_INVALID_TYPE;
		}
		v.v.int_v += increment;
		value = v.v.int_v;
		return SetHashValue(db, key, field, v);
	}

	int RDDB::HIncrbyFloat(DBID db, const Slice& key, const Slice& field,
			double increment, double& value)
	{
		ValueObject v;
		v.type = DOUBLE;
		HGet(db, key, field, v);
		value_convert_to_number(v);
		if (v.type != DOUBLE && v.type != INTEGER)
		{
			return ERR_INVALID_TYPE;
		}
		if(v.type == INTEGER){
			int64_t tmp = v.v.int_v;
			v.v.double_v = tmp;
		}
		v.v.double_v += increment;
		value = v.v.double_v;
		return SetHashValue(db, key, field, v);
	}
}

