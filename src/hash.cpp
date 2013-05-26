/*
 * hash.cpp
 *
 *  Created on: 2013-3-31
 *      Author: wqy
 */
#include "ardb.hpp"

namespace ardb
{
	int Ardb::SetHashValue(const DBID& db, const Slice& key, const Slice& field,
			ValueObject& value)
	{
		HashKeyObject k(key, field, db);
		return SetValue(k, value);
	}
	int Ardb::HSet(const DBID& db, const Slice& key, const Slice& field,
			const Slice& value)
	{
		ValueObject valueobject;
		smart_fill_value(value, valueobject);
		return SetHashValue(db, key, field, valueobject) == 0 ? 1 : 0;
	}

	int Ardb::HSetNX(const DBID& db, const Slice& key, const Slice& field,
			const Slice& value)
	{
		if (HExists(db, key, field))
		{
			return 0;
		}
		return HSet(db, key, field, value) > 0 ? 1 : 0;
	}

	int Ardb::HDel(const DBID& db, const Slice& key, const Slice& field)
	{
		HashKeyObject k(key, field, db);
		return DelValue( k);
	}

	int Ardb::HDel(const DBID& db, const Slice& key, const SliceArray& fields)
	{
		BatchWriteGuard guard(GetEngine());
		SliceArray::const_iterator it = fields.begin();
		while (it != fields.end())
		{
			HDel(db, key, *it);
			it++;
		}
		return fields.size();
	}

	int Ardb::HGetValue(const DBID& db, const Slice& key, const Slice& field,
			ValueObject* value)
	{
		HashKeyObject k(key, field, db);
		if (0 == GetValue(k, value))
		{
			return 0;
		}
		return ERR_NOT_EXIST;
	}

	int Ardb::HGet(const DBID& db, const Slice& key, const Slice& field,
			std::string* value)
	{
		HashKeyObject k(key, field, db);
		ValueObject v;
		if (0 == GetValue(k, &v))
		{
			if (NULL != value)
			{
				v.ToString(*value);
			}
			return 0;
		}
		return ERR_NOT_EXIST;
	}

	int Ardb::HMSet(const DBID& db, const Slice& key, const SliceArray& fields,
			const SliceArray& values)
	{
		if (fields.size() != values.size())
		{
			return ERR_INVALID_ARGS;
		}
		BatchWriteGuard guard(GetEngine());
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

	int Ardb::HMGet(const DBID& db, const Slice& key, const SliceArray& fields,
			ValueArray& values)
	{
		SliceArray::const_iterator it = fields.begin();
		int i = 0;
		while (it != fields.end())
		{
			ValueObject v;
			values.push_back(v);
			HGetValue(db, key, *it, &values[i]);
			it++;
			i++;
		}
		return 0;
	}

	int Ardb::HClear(const DBID& db, const Slice& key)
	{
		Slice empty;
		HashKeyObject sk(key, empty, db);
		struct HClearWalk: public WalkHandler
		{
				Ardb* z_db;

				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					HashKeyObject* sek = (HashKeyObject*) k;
					z_db->DelValue(*sek);
					return 0;
				}
				HClearWalk(Ardb* db) :
						z_db(db)
				{
				}
		} walk(this);
		BatchWriteGuard guard(GetEngine());
		Walk( sk, false, &walk);
		return 0;
	}

	int Ardb::HKeys(const DBID& db, const Slice& key, StringArray& fields)
	{
		Slice empty;
		HashKeyObject k(key, empty, db);
		Iterator* it = FindValue(k);
		while (NULL != it && it->Valid())
		{
			Slice tmpkey = it->Key();
			KeyObject* kk = decode_key(tmpkey, &k);
			if (NULL == kk)
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

	int Ardb::HLen(const DBID& db, const Slice& key)
	{
		Slice empty;
		HashKeyObject k(key, empty, db);
		Iterator* it = FindValue(k);
		int len = 0;
		while (NULL != it && it->Valid())
		{
			Slice tmpkey = it->Key();
			KeyObject* kk = decode_key(tmpkey, &k);
			if (NULL == kk)
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

	int Ardb::HVals(const DBID& db, const Slice& key, StringArray& values)
	{
		Slice empty;
		HashKeyObject k(key, empty, db);
		Iterator* it = FindValue(k);
		while (NULL != it && it->Valid())
		{
			Slice tmpkey = it->Key();
			KeyObject* kk = decode_key(tmpkey, &k);
			if (NULL == kk)
			{
				DELETE(kk);
				break;
			}
			ValueObject v;
			Buffer readbuf(const_cast<char*>(it->Value().data()), 0,
					it->Value().size());
			decode_value(readbuf, v);
			std::string str;
			values.push_back(v.ToString(str));
			it->Next();
			DELETE(kk);
		}
		DELETE(it);
		return values.empty() ? ERR_NOT_EXIST : 0;
	}

	int Ardb::HGetAll(const DBID& db, const Slice& key, StringArray& fields,
			ValueArray& values)
	{
		Slice empty;
		HashKeyObject k(key, empty, db);
		Iterator* it = FindValue(k);
		int i = 0;
		while (NULL != it && it->Valid())
		{
			Slice tmpkey = it->Key();
			KeyObject* kk = decode_key(tmpkey);
			if (NULL == kk)
			{
				break;
			}
			HashKeyObject* hk = (HashKeyObject*) kk;
			std::string filed(hk->field.data(), hk->field.size());
			fields.push_back(filed);
			ValueObject v;
			values.push_back(v);
			Buffer readbuf(const_cast<char*>(it->Value().data()), 0,
					it->Value().size());
			decode_value(readbuf, values[i]);
			i++;
			it->Next();
		}
		DELETE(it);
		return fields.empty() ? ERR_NOT_EXIST : 0;
	}

	bool Ardb::HExists(const DBID& db, const Slice& key, const Slice& field)
	{
		return HGet(db, key, field, NULL) == 0;
	}

	int Ardb::HIncrby(const DBID& db, const Slice& key, const Slice& field,
			int64_t increment, int64_t& value)
	{
		ValueObject v;
		v.type = INTEGER;
		HGetValue(db, key, field, &v);
		value_convert_to_number(v);
		if (v.type != INTEGER)
		{
			return ERR_INVALID_TYPE;
		}
		v.v.int_v += increment;
		value = v.v.int_v;
		return SetHashValue(db, key, field, v);
	}

	int Ardb::HMIncrby(const DBID& db, const Slice& key, const SliceArray& fields,
			const Int64Array& increments, Int64Array& vs)
	{
		if (fields.size() != increments.size())
		{
			return ERR_INVALID_ARGS;
		}
		vs.clear();
		BatchWriteGuard guard(GetEngine());
		SliceArray::const_iterator it = fields.begin();
		Int64Array::const_iterator sit = increments.begin();
		while (it != fields.end())
		{
			int64 v = 0;
			HIncrby(db, key, *it, *sit, v);
			vs.push_back(v);
			it++;
			sit++;
		}
		return 0;
	}

	int Ardb::HIncrbyFloat(const DBID& db, const Slice& key, const Slice& field,
			double increment, double& value)
	{
		ValueObject v;
		v.type = DOUBLE;
		HGetValue(db, key, field, &v);
		value_convert_to_number(v);
		if (v.type != DOUBLE && v.type != INTEGER)
		{
			return ERR_INVALID_TYPE;
		}
		if (v.type == INTEGER)
		{
			int64_t tmp = v.v.int_v;
			v.v.double_v = tmp;
		}
		v.v.double_v += increment;
		value = v.v.double_v;
		return SetHashValue(db, key, field, v);
	}
}

