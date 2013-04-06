/*
 * lists.cpp
 *
 *  Created on: 2013-4-1
 *      Author: wqy
 */
#include "rddb.hpp"

namespace rddb
{
	static bool DecodeListMetaData(ValueObject& v, ListMetaValue& meta)
	{
		if (v.type != RAW)
		{
			return false;
		}
		return BufferHelper::ReadVarUInt32(*(v.v.raw), meta.size)
		        && BufferHelper::ReadVarDouble(*(v.v.raw), meta.min_score)
		        && BufferHelper::ReadVarDouble(*(v.v.raw), meta.max_score);
	}
	static void EncodeListMetaData(ValueObject& v, ListMetaValue& meta)
	{
		v.type = RAW;
		if (v.v.raw == NULL)
		{
			v.v.raw = new Buffer(16);
		}
		BufferHelper::WriteVarUInt32(*(v.v.raw), meta.size);
		BufferHelper::WriteVarDouble(*(v.v.raw), meta.min_score);
		BufferHelper::WriteVarDouble(*(v.v.raw), meta.max_score);
	}

	int RDDB::ListPush(DBID db, const Slice& key, const Slice& value,
	        bool athead, bool onlyexist, double withscore)
	{
		KeyObject k(key, LIST_META);
		ValueObject v;
		ListMetaValue meta;
		double score = withscore != DBL_MAX ? withscore : 0;
		if (0 == GetValue(db, k, v))
		{
			if (!DecodeListMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
			meta.size++;
			if (withscore != DBL_MAX)
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
		}
		BatchWriteGuard guard(GetDB(db));
		ListKeyObject lk(key, score);
		ValueObject lv;
		fill_value(value, lv);
		if (0 == SetValue(db, lk, lv))
		{
			EncodeListMetaData(v, meta);
			return SetValue(db, k, v);
		}
		return -1;
	}
	int RDDB::RPush(DBID db, const Slice& key, const Slice& value)
	{
		return ListPush(db, key, value, false, false);
	}

	int RDDB::RPushx(DBID db, const Slice& key, const Slice& value)
	{
		return ListPush(db, key, value, false, true);
	}

	int RDDB::LPush(DBID db, const Slice& key, const Slice& value)
	{
		return ListPush(db, key, value, true, false);
	}

	int RDDB::LPushx(DBID db, const Slice& key, const Slice& value)
	{
		return ListPush(db, key, value, true, true);
	}

	int RDDB::LInsert(DBID db, const Slice& key, const Slice& opstr,
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
		ListKeyObject lk(key, -DBL_MAX);
		uint32_t cursor = 0;
		Iterator* iter = FindValue(db, lk);
		bool found = false;
		double pivot_score = 0;
		double score = 0;
		while (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey);
			if (NULL == kk || kk->type != LIST_ELEMENT
			        || kk->key.compare(key) != 0)
			{
				break;
			}
			ValueObject v;
			Slice tmpvalue = iter->Value();
			Buffer readbuf(const_cast<char*>(tmpvalue.data()), 0,
			        tmpvalue.size());
			decode_value(readbuf, v);
			Slice cmp(v.v.raw->GetRawReadBuffer(), v.v.raw->ReadableBytes());
			if (cmp.compare(value) == 0)
			{
				ListKeyObject* lek = (ListKeyObject*) kk;
				pivot_score = lek->score;
				found = true;
				break;
			}
			cursor++;
			iter->Next();
		}
		if (found)
		{
			if (before)
			{
				iter->Prev();
			}
			else
			{
				iter->Next();
			}
			if (iter->Valid())
			{
				Slice tmpkey = iter->Key();
				KeyObject* kk = decode_key(tmpkey);
				if (NULL == kk || kk->type != LIST_ELEMENT
				        || kk->key.compare(key) != 0)
				{
					if (before)
					{
						score = pivot_score - 1;
					}
					else
					{
						score = pivot_score + 1;
					}
				}
				else
				{
					ListKeyObject* lek = (ListKeyObject*) kk;
					score = (lek->score + pivot_score) / 2;
				}
			}
			else
			{
				if (before)
				{
					score = pivot_score - 1;
				}
				else
				{
					score = pivot_score + 1;
				}
			}
			ListPush(db, key, value, true, false, score);
		}
		DELETE(iter);
		if (!found)
		{
			return ERR_NOT_EXIST;
		}
		return 0;
	}

	int RDDB::ListPop(DBID db, const Slice& key, bool athead,
	        ValueObject& value)
	{
		KeyObject k(key, LIST_META);
		ValueObject v;
		ListMetaValue meta;
		double score;
		if (0 == GetValue(db, k, v))
		{
			if (!DecodeListMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
			meta.size--;
			if (athead)
			{
				score = meta.min_score;
			}
			else
			{
				score = meta.max_score;
			}
			if (meta.size == 0)
			{
				meta.min_score = meta.max_score = 0;
			}
			else
			{
				ListKeyObject lk(key, score);
				Iterator* iter = FindValue(db, lk);
				if (NULL != iter && iter->Valid())
				{
					if (athead)
					{
						iter->Next();
					}
					else
					{
						iter->Prev();
					}
					Slice tmpkey = iter->Key();
					KeyObject* kk = decode_key(tmpkey);
					if (NULL == kk || kk->type != LIST_ELEMENT
					        || kk->key.compare(key) != 0)
					{
						//do nothing
					}
					else
					{
						ListKeyObject* lek = (ListKeyObject*) kk;
						if (athead)
						{
							meta.min_score = lek->score;
						}
						else
						{
							meta.max_score = lek->score;
						}
					}
					DELETE(kk);
				}
			}
			ListKeyObject lk(key, score);
			BatchWriteGuard guard(GetDB(db));
			DelValue(db, lk);
			EncodeListMetaData(v, meta);
			return SetValue(db, k, v);
		}
		else
		{
			return ERR_NOT_EXIST;
		}
	}

	int RDDB::LPop(DBID db, const Slice& key, ValueObject& value)
	{
		return ListPop(db, key, true, value);
	}
	int RDDB::RPop(DBID db, const Slice& key, ValueObject& v)
	{
		return ListPop(db, key, false, v);
	}

	int RDDB::LIndex(DBID db, const Slice& key, uint32_t index, ValueObject& v)
	{
		ListKeyObject lk(key, -DBL_MAX);
		uint32_t cursor = 0;
		Iterator* iter = FindValue(db, lk);
		bool found = false;
		while (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey);
			if (NULL == kk || kk->type != LIST_ELEMENT
			        || kk->key.compare(key) != 0)
			{
				DELETE(kk);
				break;
			}
			if (cursor == index)
			{
				found = true;
				Slice tmpvalue = iter->Value();
				Buffer readbuf(const_cast<char*>(tmpvalue.data()), 0,
				        tmpvalue.size());
				decode_value(readbuf, v);
				DELETE(kk);
				break;
			}
			cursor++;
			DELETE(kk);
			iter->Next();
		}
		DELETE(iter);
		if (!found)
		{
			return ERR_NOT_EXIST;
		}
		return 0;
	}

	int RDDB::LRange(DBID db, const Slice& key, int start, int end,
	        ValueArray& values)
	{
		int len = LLen(db, key);
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
		ListKeyObject lk(key, -DBL_MAX);
		uint32_t cursor = 0;
		Iterator* iter = FindValue(db, lk);
		bool insert = false;
		while (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey);
			if (NULL == kk || kk->type != LIST_ELEMENT
			        || kk->key.compare(key) != 0)
			{
				DELETE(kk);
				break;
			}
			if (cursor == start)
			{
				insert = true;
			}
			if (cursor > end)
			{
				DELETE(kk);
				break;
			}
			if (insert)
			{
				Slice tmpvalue = iter->Value();
				ValueObject* v = new ValueObject;
				Buffer readbuf(const_cast<char*>(tmpvalue.data()), 0,
				        tmpvalue.size());
				decode_value(readbuf, *v);
				values.push_back(v);
			}
			DELETE(kk);
			cursor++;
			iter->Next();
		}
		DELETE(iter);
		return 0;
	}

	int RDDB::LClear(DBID db, const Slice& key)
	{
		ListKeyObject lk(key, -DBL_MAX);
		uint32_t cursor = 0;
		Iterator* iter = FindValue(db, lk);
		BatchWriteGuard guard(GetDB(db));
		while (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey);
			if (NULL == kk || kk->type != LIST_ELEMENT
			        || kk->key.compare(key) != 0)
			{
				DELETE(kk);
				break;
			}
			ListKeyObject* lek = (ListKeyObject*) kk;
			DelValue(db, *lek);
			DELETE(kk);
			cursor++;
			iter->Next();
		}
		DELETE(iter);
		KeyObject k(key, LIST_META);
		DelValue(db, k);
		return 0;
	}

	int RDDB::LRem(DBID db, const Slice& key, int count, const Slice& value)
	{
		KeyObject k(key, LIST_META);
		ValueObject v;
		ListMetaValue meta;
		if (0 == GetValue(db, k, v))
		{
			if (!DecodeListMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
		}
		else
		{
			return ERR_NOT_EXIST;
		}
		Iterator* iter = NULL;
		int total = count;
		bool fromhead = true;
		if (count >= 0)
		{
			ListKeyObject lk(key, -DBL_MAX);
			iter = FindValue(db, lk);
			iter->Next();
		}
		else
		{
			fromhead = false;
			total = 0 - count;
			ListKeyObject lk(key, DBL_MAX);
			iter = FindValue(db, lk);
			iter->Prev();
		}
		int remcount = 0;
		BatchWriteGuard guard(GetDB(db));
		bool replace_min_score = false;
		bool replace_max_score = false;
		double last_score = 0;
		while (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey);
			if (NULL == kk || kk->type != LIST_ELEMENT
			        || kk->key.compare(key) != 0)
			{
				DELETE(kk);
				break;
			}
			Slice tmpvalue = iter->Value();
			ValueObject v;
			Buffer readbuf(const_cast<char*>(tmpvalue.data()), 0,
			        tmpvalue.size());
			decode_value(readbuf, v);
			Slice cmp(v.v.raw->GetRawReadBuffer(), v.v.raw->ReadableBytes());
			ListKeyObject* lek = (ListKeyObject*) kk;
			if (cmp.compare(value) == 0)
			{
				DelValue(db, *lek);
				if (lek->score == meta.min_score)
				{
					if (fromhead)
					{
						replace_min_score = true;
					}
					else
					{
						meta.min_score = last_score;
					}
				}
				if (lek->score == meta.max_score)
				{
					if (fromhead)
					{
						meta.max_score = last_score;
					}
					else
					{
						replace_max_score = true;
					}
				}
				remcount++;
			}
			else
			{
				last_score = lek->score;
				if (replace_min_score)
				{
					meta.min_score = last_score;
					replace_min_score = false;
				}
				if (replace_max_score)
				{
					meta.max_score = last_score;
					replace_max_score = false;
				}
			}
			if (total > 0 && remcount == total)
			{
				break;
			}
			if (count >= 0)
			{
				iter->Next();
			}
			else
			{
				iter->Prev();
			}
			DELETE(kk);
		}
		DELETE(iter);
		meta.size -= remcount;
		EncodeListMetaData(v, meta);
		SetValue(db, k, v);
		return remcount;
	}

	int RDDB::LSet(DBID db, const Slice& key, int index, const Slice& value)
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
		ListKeyObject lk(key, -DBL_MAX);
		uint32_t cursor = 0;
		Iterator* iter = FindValue(db, lk);
		bool found = false;
		while (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey);
			if (NULL == kk || kk->type != LIST_ELEMENT
			        || kk->key.compare(key) != 0)
			{
				DELETE(kk);
				break;
			}
			if (cursor == index)
			{
				found = true;
				ValueObject v;
				fill_value(iter->Value(), v);
				ListKeyObject* lek = (ListKeyObject*) kk;
				SetValue(db, *lek, v);
				DELETE(kk);
				break;
			}
			cursor++;
			DELETE(kk);
			iter->Next();
		}
		DELETE(iter);
		if (!found)
		{
			return ERR_NOT_EXIST;
		}
		return 0;
	}

	int RDDB::LTrim(DBID db, const Slice& key, int start, int stop)
	{
		int len = LLen(db, key);
		if (len <= 0)
		{
			return len;
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
		ListKeyObject lk(key, -DBL_MAX);
		uint32_t cursor = 0;
		Iterator* iter = FindValue(db, lk);
		KeyObject k(key, LIST_META);
		ValueObject v;
		ListMetaValue meta;
		BatchWriteGuard guard(GetDB(db));
		while (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey);
			if (NULL == kk || kk->type != LIST_ELEMENT
			        || kk->key.compare(key) != 0)
			{
				DELETE(kk);
				break;
			}
			ListKeyObject* lek = (ListKeyObject*) kk;
			if (cursor >= start && cursor <= stop)
			{
				//keep
				meta.size++;
				if (lek->score < meta.min_score)
				{
					meta.min_score = lek->score;
				}
				if (lek->score > meta.max_score)
				{
					meta.max_score = lek->score;
				}
			}
			else
			{
				DelValue(db, *lek);
			}
			DELETE(kk);
			cursor++;
			iter->Next();
		}
		DELETE(iter);
		EncodeListMetaData(v, meta);
		return SetValue(db, k, v);;
	}

	int RDDB::LLen(DBID db, const Slice& key)
	{
		KeyObject k(key, LIST_META);
		ValueObject v;
		ListMetaValue meta;
		if (0 == GetValue(db, k, v))
		{
			if (!DecodeListMetaData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
			return meta.size;
		}
		else
		{
			return ERR_NOT_EXIST;
		}
	}

	int RDDB::RPopLPush(DBID db, const Slice& key1, const Slice& key2)
	{
		ValueObject v;
		if(0 == RPop(db, key1, v))
		{
			value_convert_to_raw(v);
			Slice sv(v.v.raw->GetRawReadBuffer(), v.v.raw->ReadableBytes());
			return RPush(db, key2, sv);
		}
		return ERR_NOT_EXIST;
	}
}

