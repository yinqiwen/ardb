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
    HashMetaValue* Ardb::GetHashMeta(const DBID& db, const Slice& key, int& err,
                    bool& create)
    {
        CommonMetaValue* meta = GetMeta(db, key, false);
        if (NULL != meta && meta->header.type != HASH_META)
        {
            DELETE(meta);
            err = ERR_INVALID_TYPE;
            return NULL;
        }
        if (NULL == meta)
        {
            err = ERR_NOT_EXIST;
            meta = new HashMetaValue;
            create = true;
        }
        else
        {
            create = false;
        }
        return (HashMetaValue*) meta;
    }

    int Ardb::GetHashZipEntry(HashMetaValue* meta, const ValueData& field,
                    ValueData*& value)
    {
        HashFieldMap::iterator it = meta->values.find(field);
        if (it != meta->values.end())
        {
            value = &(it->second);
            return 0;
        }
        return ERR_NOT_EXIST;
    }

    int Ardb::HSetValue(HashKeyObject& key, HashMetaValue* meta,
                    CommonValueObject& value)
    {
        KeyLockerGuard lockGuard(m_key_locker, key.db, key.key);
        BatchWriteGuard guard(GetEngine());
        if (!meta->ziped)
        {
            if (!meta->dirty)
            {
                meta->dirty = true;
                SetMeta(key.db, key.key, *meta);
            }
            return SetKeyValueObject(key, value);
        }
        else
        {
            ValueData* value_entry = NULL;
            bool zipSave = true;
            if (0 == GetHashZipEntry(meta, key.field, value_entry))
            {
                *value_entry = value.data; //set new value
                if (value.data.type == BYTES_VALUE
                                && value.data.bytes_value.size()
                                                >= (uint32) m_config.hash_max_ziplist_value)
                {
                    zipSave = false;
                }
            }
            else
            {
                if (key.field.type == BYTES_VALUE
                                && value.data.bytes_value.size()
                                                >= (uint32) m_config.hash_max_ziplist_value)
                {
                    zipSave = false;
                }
                else if (meta->values.size()
                                >= (uint32) m_config.hash_max_ziplist_entries)
                {
                    zipSave = false;
                }
                else
                {
                    zipSave = true;
                }
                meta->values[key.field] = value.data;
            }
            if (!zipSave)
            {
                meta->ziped = false;
                meta->dirty = false;
                meta->size = meta->values.size();
                HashFieldMap::iterator it = meta->values.begin();
                while (it != meta->values.end())
                {
                    HashKeyObject k(key.key, it->first, key.db);
                    CommonValueObject v(it->second);
                    SetKeyValueObject(k, v);
                    it++;
                }
                meta->values.clear();
            }
            SetMeta(key.db, key.key, *meta);
        }
        return 0;
    }
    int Ardb::HSet(const DBID& db, const Slice& key, const Slice& field,
                    const Slice& value)
    {
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta)
        {
            DELETE(meta);
            return err;
        }
        CommonValueObject valueobject;
        valueobject.data.SetValue(value, true);
        HashKeyObject hk(key, field, db);
        int ret = HSetValue(hk, meta, valueobject) == 0 ? 1 : 0;
        DELETE(meta);
        return ret;
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

    int Ardb::HDel(const DBID& db, const Slice& key, const SliceArray& fields)
    {
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta || createHash)
        {
            DELETE(meta);
            return err;
        }
        BatchWriteGuard guard(GetEngine());
        SliceArray::const_iterator it = fields.begin();
        while (it != fields.end())
        {
            if (meta->ziped)
            {
                ValueData field_value;
                field_value.SetValue(*it, true);
                meta->values.erase(field_value);
            }
            else
            {
                HashKeyObject k(key, *it, db);
                DelValue(k);
            }
            it++;
        }
        if (meta->ziped)
        {
            SetMeta(db, key, *meta);
        }
        else
        {
            if (!meta->dirty)
            {
                meta->dirty = true;
                SetMeta(db, key, *meta);
            }
        }
        DELETE(meta);
        return fields.size();
    }

    int Ardb::HGetValue(HashKeyObject& key, HashMetaValue* meta,
                    CommonValueObject& value)
    {
        if (meta->ziped)
        {
            ValueData* v = NULL;
            if (0 == GetHashZipEntry(meta, key.field, v))
            {
                value.data = *v;
                return 0;
            }
            return ERR_NOT_EXIST;
        }
        else
        {
            return GetKeyValueObject(key, value);
        }
    }

    int Ardb::HGet(const DBID& db, const Slice& key, const Slice& field,
                    std::string* value)
    {
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta || createHash)
        {
            DELETE(meta);
            return err;
        }
        HashKeyObject hk(key, field, db);
        CommonValueObject valueObj;
        int ret = HGetValue(hk, meta, valueObj);
        DELETE(meta);
        if (ret == 0)
        {
            if (NULL != value)
            {
                valueObj.data.ToString(*value);
            }
            return 0;
        }
        return ret;
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
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta || createHash)
        {
            DELETE(meta);
            return err;
        }
        SliceArray::const_iterator it = fields.begin();
        while (it != fields.end())
        {
            CommonValueObject valueObj;
            HashKeyObject hk(key, *it, db);
            HGetValue(hk, meta, valueObj);
            values.push_back(valueObj.data);
            it++;
        }
        DELETE(meta);
        return 0;
    }

    int Ardb::HClear(const DBID& db, const Slice& key)
    {
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta || createHash)
        {
            DELETE(meta);
            return err;
        }
        if (!meta->ziped)
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
            Walk(sk, false, false, &walk);
        }
        DelMeta(db, key, meta);
        DELETE(meta);
        return 0;
    }

    int Ardb::HKeys(const DBID& db, const Slice& key, StringArray& fields)
    {
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta || createHash)
        {
            DELETE(meta);
            return err;
        }
        if (meta->ziped)
        {
            HashFieldMap::iterator it = meta->values.begin();
            while (it != meta->values.end())
            {
                std::string str;
                fields.push_back(it->first.ToString(str));
                it++;
            }
        }
        else
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
                std::string filed;
                hk->field.ToString(filed);
                fields.push_back(filed);
                it->Next();
                DELETE(kk);
            }
            DELETE(it);
            if (meta->dirty)
            {
                meta->dirty = false;
                meta->size = fields.size();
                SetMeta(db, key, *meta);
            }
        }
        DELETE(meta);
        return fields.empty() ? ERR_NOT_EXIST : 0;
    }

    int Ardb::HLen(const DBID& db, const Slice& key)
    {
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta || createHash)
        {
            DELETE(meta);
            return err;
        }
        int len = 0;
        if (meta->ziped)
        {
            len = meta->values.size();
        }
        else
        {
            if (!meta->dirty)
            {
                len = meta->size;
            }
            else
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
                        break;
                    }
                    len++;
                    it->Next();
                    DELETE(kk);
                }
                DELETE(it);
                meta->size = len;
                meta->dirty = false;
                SetMeta(db, key, *meta);
            }
        }
        DELETE(meta);
        return len;
    }

    int Ardb::HVals(const DBID& db, const Slice& key, StringArray& values)
    {
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta || createHash)
        {
            DELETE(meta);
            return err;
        }
        if (meta->ziped)
        {
            HashFieldMap::iterator it = meta->values.begin();
            while (it != meta->values.end())
            {
                std::string str;
                values.push_back(it->second.ToString(str));
                it++;
            }
        }
        else
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
                    break;
                }
                ValueData v;
                Buffer readbuf(const_cast<char*>(it->Value().data()), 0,
                                it->Value().size());
                decode_value(readbuf, v);
                std::string str;
                values.push_back(v.ToString(str));
                it->Next();
                DELETE(kk);
            }
            DELETE(it);
            if (meta->dirty)
            {
                meta->dirty = false;
                meta->size = values.size();
                SetMeta(db, key, *meta);
            }
        }
        DELETE(meta);
        return values.empty() ? ERR_NOT_EXIST : 0;
    }

    int Ardb::HGetAll(const DBID& db, const Slice& key, StringArray& fields,
                    ValueArray& values)
    {
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta || createHash)
        {
            DELETE(meta);
            return err;
        }
        if (meta->ziped)
        {
            HashFieldMap::iterator it = meta->values.begin();
            while (it != meta->values.end())
            {
                std::string field;
                it->first.ToString(field);
                fields.push_back(field);
                values.push_back(it->second);
                it++;
            }
        }
        else
        {
            Slice empty;
            HashKeyObject k(key, empty, db);
            struct HashWalk: public WalkHandler
            {
                    StringArray& fs;
                    ValueArray& dst;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        CommonValueObject* cv = (CommonValueObject*) v;
                        HashKeyObject* sek = (HashKeyObject*) k;
                        std::string field;
                        sek->field.ToString(field);
                        fs.push_back(field);
                        dst.push_back(cv->data);
                        return 0;
                    }
                    HashWalk(StringArray& ff, ValueArray& vs) :
                                    fs(ff), dst(vs)
                    {
                    }
            } walk(fields, values);
            Walk(k, false, true, &walk);
            if (meta->dirty)
            {
                meta->dirty = false;
                meta->size = values.size();
                SetMeta(db, key, *meta);
            }
        }
        DELETE(meta);
        return fields.empty() ? ERR_NOT_EXIST : 0;
    }

    bool Ardb::HExists(const DBID& db, const Slice& key, const Slice& field)
    {
        return HGet(db, key, field, NULL) == 0;
    }

    int Ardb::HIncrby(const DBID& db, const Slice& key, const Slice& field,
                    int64_t increment, int64_t& value)
    {
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta)
        {
            DELETE(meta);
            return err;
        }
        HashKeyObject hk(key, field, db);
        CommonValueObject valueObj;
        int ret = HGetValue(hk, meta, valueObj);
        if (0 == ret)
        {
            if (valueObj.data.type != INTEGER_VALUE)
            {
                DELETE(meta);
                return ERR_INVALID_TYPE;
            }
            valueObj.data.Incrby(increment);
            value = valueObj.data.integer_value;
        }
        else
        {
            valueObj.data.SetValue(increment);
            value = valueObj.data.integer_value;
        }
        ret = HSetValue(hk, meta, valueObj);
        DELETE(meta);
        return ret;
    }

    int Ardb::HMIncrby(const DBID& db, const Slice& key,
                    const SliceArray& fields, const Int64Array& increments,
                    Int64Array& vs)
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
        int err = 0;
        bool createHash = false;
        HashMetaValue* meta = GetHashMeta(db, key, err, createHash);
        if (NULL == meta)
        {
            DELETE(meta);
            return err;
        }
        HashKeyObject hk(key, field, db);
        CommonValueObject valueObj;
        int ret = HGetValue(hk, meta, valueObj);
        if (0 == ret)
        {
            if (valueObj.data.type != INTEGER_VALUE
                            && valueObj.data.type != DOUBLE_VALUE)
            {
                DELETE(meta);
                return ERR_INVALID_TYPE;
            }
            valueObj.data.IncrbyFloat(increment);
            value = valueObj.data.double_value;
        }
        else
        {
            valueObj.data.SetValue(increment);
            value = valueObj.data.double_value;
        }
        ret = HSetValue(hk, meta, valueObj);
        DELETE(meta);
        return ret;
    }

    int Ardb::HFirstField(const DBID& db, const Slice& key, std::string& field)
    {
        HashKeyObject hk(key, "", db);
        Iterator* iter = FindValue(hk, false);
        if (iter != NULL && iter->Valid())
        {
            KeyObject* kk = decode_key(iter->Key(), NULL);
            if (NULL != kk && kk->type == HASH_FIELD
                            && kk->key.compare(key) == 0)
            {
                HashKeyObject* tmp = (HashKeyObject*) kk;
                tmp->field.ToString(field);
                DELETE(kk);
                DELETE(iter);
                return 0;
            }
            DELETE(kk);
        }
        DELETE(iter);
        return -1;
    }

    int Ardb::HLastField(const DBID& db, const Slice& key, std::string& field)
    {
        std::string next;
        next_key(key, next);
        HashKeyObject hk(key, next, db);
        Iterator* iter = FindValue(hk, false);
        if (iter != NULL && iter->Valid())
        {
            iter->Prev();
            if (iter->Valid())
            {
                KeyObject* kk = decode_key(iter->Key(), NULL);
                if (NULL != kk && kk->type == HASH_FIELD
                                && kk->key.compare(key) == 0)
                {
                    HashKeyObject* tmp = (HashKeyObject*) kk;
                    tmp->field.ToString(field);
                    DELETE(kk);
                    DELETE(iter);
                    return 0;
                }
                DELETE(kk);
            }
        }
        DELETE(iter);
        return -1;
    }

    int Ardb::RenameHash(const DBID& db1, const Slice& key1, const DBID& db2,
                    const Slice& key2, HashMetaValue* meta)
    {
        BatchWriteGuard guard(GetEngine());
        if (meta->ziped)
        {
            DelMeta(db1, key1, meta);
            SetMeta(db2, key2, *meta);
        }
        else
        {
            Slice empty;
            HashKeyObject k(key1, empty, db1);
            HashMetaValue hmeta;
            hmeta.dirty = false;
            hmeta.ziped = false;
            struct HashWalk: public WalkHandler
            {
                    Ardb* z_db;
                    DBID dstdb;
                    const Slice& dst;
                    HashMetaValue& hm;
                    int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
                    {
                        CommonValueObject* cv = (CommonValueObject*) v;
                        HashKeyObject* sek = (HashKeyObject*) k;
                        sek->key = dst;
                        sek->db = dstdb;
                        z_db->SetKeyValueObject(*sek, *cv);
                        hm.size++;
                        return 0;
                    }
                    HashWalk(Ardb* db, const DBID& dbid, const Slice& dstkey,
                                    HashMetaValue& m) :
                                    z_db(db), dstdb(dbid), dst(dstkey), hm(m)
                    {
                    }
            } walk(this, db2, key2, hmeta);
            Walk(k, false, true, &walk);
            SetMeta(db2, key2, hmeta);
            HClear(db1, key1);
        }
        return 0;
    }

//	int Ardb::HRange(const DBID& db, const Slice& key, const Slice& from,
//	        int32 limit, StringArray& fields, ValueArray& values)
//	{
//		if (limit == 0)
//		{
//			return 0;
//		}
//
//		HashKeyObject hk(key, from, db);
//		struct HGetWalk: public WalkHandler
//		{
//				StringArray& h_fileds;
//				ValueArray& z_values;
//				const ValueObject& first;
//				int l;
//				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
//				{
//					HashKeyObject* sek = (HashKeyObject*) k;
//					if (0 == cursor)
//					{
//						if (first.Compare(sek->field) == 0)
//						{
//							return 0;
//						}
//					}
//					std::string fstr;
//					sek->field.ToString(fstr);
//					h_fileds.push_back(fstr);
//					z_values.push_back(*v);
//					if (l > 0 && z_values.size() >= (uint32) l)
//					{
//						return -1;
//					}
//					return 0;
//				}
//				HGetWalk(StringArray& fs, ValueArray& vs, int count,
//				        const ValueObject& s) :
//						h_fileds(fs), z_values(vs), first(s), l(count)
//				{
//				}
//		} walk(fields, values, limit, hk.field);
//		Walk(hk, false, &walk);
//		return 0;
//	}
//
//	int Ardb::HRevRange(const DBID& db, const Slice& key, const Slice& from,
//	        int32 limit, StringArray& fields, ValueArray& values)
//	{
//		if (limit == 0)
//		{
//			return 0;
//		}
//		HashKeyObject hk(key, from, db);
//		std::string last_field;
//		std::string first_field;
//		if (from.size() == 0)
//		{
//			if (0 != HLastField(db, key, last_field))
//			{
//				return 0;
//			}
//			hk.field = last_field;
//		}
//		if (0 != HFirstField(db, key, first_field))
//		{
//			return 0;
//		}
//		struct HGetWalk: public WalkHandler
//		{
//				StringArray& h_fileds;
//				ValueArray& z_values;
//				const Slice& first;
//				const std::string& first_field;
//				int l;
//				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
//				{
//					HashKeyObject* sek = (HashKeyObject*) k;
//					if (0 == cursor)
//					{
//						if (first.compare(sek->field) == 0)
//						{
//							return 0;
//						}
//					}
//					std::string filed(sek->field.data(), sek->field.size());
//					h_fileds.push_back(filed);
//					z_values.push_back(*v);
//					if (l > 0 && z_values.size() >= (uint32) l)
//					{
//						return -1;
//					}
//					if (filed.compare(first_field) == 0)
//					{
//						return -1;
//					}
//					return 0;
//				}
//				HGetWalk(StringArray& fs, ValueArray& vs, int count,
//				        const Slice& s, const std::string& ff) :
//						h_fileds(fs), z_values(vs), first(s), first_field(ff), l(
//						        count)
//				{
//				}
//		} walk(fields, values, limit, from, first_field);
//		Walk(hk, true, &walk);
//		return 0;
//	}
}

