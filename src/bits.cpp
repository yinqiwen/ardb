/*
 * bits.cpp
 *
 *  Created on: 2013-6-4
 *      Author: wqy
 */
#include "ardb.hpp"

namespace ardb
{
	static const long BITOP_AND = 0;
	static const long BITOP_OR = 1;
	static const long BITOP_XOR = 2;
	static const long BITOP_NOT = 3;

	static const uint32 BIT_SUBSET_SIZE = 4096;
	static const uint32 BIT_SUBSET_BYTES_SIZE = BIT_SUBSET_SIZE >> 3;
	//copy from redis
	static long popcount(const void *s, long count)
	{
		long bits = 0;
		unsigned char *p;
		const uint32_t* p4 = (const uint32_t*) s;
		static const unsigned char bitsinbyte[256] =
			{ 0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3,
					3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3,
					3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5,
					5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3,
					3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4,
					4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5,
					5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4,
					4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3,
					3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5,
					5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4,
					4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6,
					6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5,
					5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8 };

		/* Count bits 16 bytes at a time */
		while (count >= 16)
		{
			uint32_t aux1, aux2, aux3, aux4;

			aux1 = *p4++;
			aux2 = *p4++;
			aux3 = *p4++;
			aux4 = *p4++;
			count -= 16;

			aux1 = aux1 - ((aux1 >> 1) & 0x55555555);
			aux1 = (aux1 & 0x33333333) + ((aux1 >> 2) & 0x33333333);
			aux2 = aux2 - ((aux2 >> 1) & 0x55555555);
			aux2 = (aux2 & 0x33333333) + ((aux2 >> 2) & 0x33333333);
			aux3 = aux3 - ((aux3 >> 1) & 0x55555555);
			aux3 = (aux3 & 0x33333333) + ((aux3 >> 2) & 0x33333333);
			aux4 = aux4 - ((aux4 >> 1) & 0x55555555);
			aux4 = (aux4 & 0x33333333) + ((aux4 >> 2) & 0x33333333);
			bits +=
					((((aux1 + (aux1 >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24)
							+ ((((aux2 + (aux2 >> 4)) & 0x0F0F0F0F) * 0x01010101)
									>> 24)
							+ ((((aux3 + (aux3 >> 4)) & 0x0F0F0F0F) * 0x01010101)
									>> 24)
							+ ((((aux4 + (aux4 >> 4)) & 0x0F0F0F0F) * 0x01010101)
									>> 24);
		}
		/* Count the remaining bytes */
		p = (unsigned char*) p4;
		while (count--)
			bits += bitsinbyte[*p++];
		return bits;
	}

	static bool DecodeSetMetaData(ValueObject& v, BitSetMetaValue& meta)
	{
		if (v.type != RAW)
		{
			return false;
		}
		return BufferHelper::ReadVarUInt64(*(v.v.raw), meta.bitcount)
				&& BufferHelper::ReadVarUInt64(*(v.v.raw), meta.min)
				&& BufferHelper::ReadVarUInt64(*(v.v.raw), meta.max);
	}
	static void EncodeSetMetaData(ValueObject& v, BitSetMetaValue& meta)
	{
		v.type = RAW;
		if (v.v.raw == NULL)
		{
			v.v.raw = new Buffer(128);
		}
		BufferHelper::WriteVarUInt64(*(v.v.raw), meta.bitcount);
		BufferHelper::WriteVarUInt64(*(v.v.raw), meta.min);
		BufferHelper::WriteVarUInt64(*(v.v.raw), meta.max);
	}

	static bool DecodeSetElementData(ValueObject& v, BitSetElementValue& meta)
	{
		if (v.type != RAW)
		{
			return false;
		}
		return BufferHelper::ReadVarUInt32(*(v.v.raw), meta.bitcount)
				&& BufferHelper::ReadVarUInt32(*(v.v.raw), meta.start)
				&& BufferHelper::ReadVarUInt32(*(v.v.raw), meta.limit)
				&& BufferHelper::ReadVarString(*(v.v.raw), meta.vals);
	}
	static void EncodeSetElementData(ValueObject& v, BitSetElementValue& meta)
	{
		v.type = RAW;
		if (v.v.raw == NULL)
		{
			v.v.raw = new Buffer(BIT_SUBSET_SIZE + 8);
		}
		BufferHelper::WriteVarUInt32(*(v.v.raw), meta.bitcount);
		BufferHelper::WriteVarUInt32(*(v.v.raw), meta.start);
		BufferHelper::WriteVarUInt32(*(v.v.raw), meta.limit);
		BufferHelper::WriteVarString(*(v.v.raw), meta.vals);
	}

	int Ardb::GetBitSetMetaValue(const DBID& db, const Slice& key,
			BitSetMetaValue& meta)
	{
		KeyObject k(key, BITSET_META, db);
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
	void Ardb::SetBitSetMetaValue(const DBID& db, const Slice& key,
			BitSetMetaValue& meta)
	{
		KeyObject k(key, BITSET_META, db);
		ValueObject v;
		EncodeSetMetaData(v, meta);
		SetValue(k, v);
	}

	int Ardb::GetBitSetElementValue(BitSetKeyObject& key,
			BitSetElementValue& meta)
	{
		ValueObject v;
		if (0 == GetValue(key, &v))
		{
			if (!DecodeSetElementData(v, meta))
			{
				return ERR_INVALID_TYPE;
			}
			return 0;
		}
		return ERR_NOT_EXIST;
	}
	void Ardb::SetBitSetElementValue(BitSetKeyObject& key,
			BitSetElementValue& meta)
	{
		ValueObject v;
		EncodeSetElementData(v, meta);
		SetValue(key, v);
	}

	int Ardb::SetBit(const DBID& db, const Slice& key, uint64 bitoffset,
			uint8 value)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		int byte, bit;
		int byteval, bitval;
		long on = value;
		/* Bits can only be set or cleared... */
		if (on & ~1)
		{
			return -1;
		}
		bool set_changed = false;
		BitSetMetaValue meta;
		if (-1 == GetBitSetMetaValue(db, key, meta))
		{
			set_changed = true;
		}
		uint64 index = (bitoffset / BIT_SUBSET_SIZE) + 1;
		if (meta.min == 0 || meta.min > index)
		{
			meta.min = index;
			set_changed = true;
		}
		if (meta.max == 0 || meta.max < index)
		{
			meta.max = index;
			set_changed = true;
		}

		bitoffset = bitoffset % BIT_SUBSET_SIZE;
		byte = bitoffset >> 3;
		BitSetKeyObject k(key, index, db);
		BitSetElementValue bitvalue;
		bool element_changed = false;
		GetBitSetElementValue(k, bitvalue);
		if (bitvalue.vals.size() < BIT_SUBSET_BYTES_SIZE)
		{
			uint32 tmpsize = BIT_SUBSET_BYTES_SIZE - bitvalue.vals.size();
			char tmp[tmpsize];
			memset(tmp, 0, tmpsize);
			bitvalue.vals.append(tmp, tmpsize);
		}

		if (bitvalue.limit < bitoffset)
		{
			bitvalue.limit = bitoffset + 1;
			element_changed = true;
		}
		if (bitvalue.start > bitoffset || bitvalue.start == 0)
		{
			bitvalue.start = bitoffset + 1;
			element_changed = true;
		}
		/* Get current values */
		byteval = ((const uint8*) bitvalue.vals.data())[byte];
		bit = 7 - (bitoffset & 0x7);
		bitval = byteval & (1 << bit);

		int tmp = byteval;

		BatchWriteGuard guard(GetEngine());
		/* Update byte with new bit value and return original value */
		byteval &= ~(1 << bit);
		byteval |= ((on & 0x1) << bit);
		if (byteval != tmp)
		{
			((uint8_t*) bitvalue.vals.data())[byte] = byteval;
			if (on == 1)
			{
				bitvalue.bitcount++;
				meta.bitcount++;
			} else
			{
				bitvalue.bitcount--;
				meta.bitcount--;
			}
			set_changed = true;
			element_changed = true;
		}
		if (set_changed)
		{
			SetBitSetMetaValue(db, key, meta);
		}
		if (element_changed)
		{
			SetBitSetElementValue(k, bitvalue);
		}
		return bitval != 0 ? 1 : 0;
	}

	int Ardb::GetBit(const DBID& db, const Slice& key, uint64 bitoffset)
	{
		uint64 index = (bitoffset / BIT_SUBSET_SIZE) + 1;
		BitSetKeyObject k(key, index, db);
		BitSetElementValue bitvalue;
		if (-1 == GetBitSetElementValue(k, bitvalue))
		{
			return 0;
		}
		size_t byte, bit;
		size_t bitval = 0;

		bitoffset = bitoffset % BIT_SUBSET_SIZE;
		byte = bitoffset >> 3;
		if (byte >= bitvalue.vals.size() || (bitoffset + 1) < bitvalue.start
				|| (bitoffset + 1) > bitvalue.limit)
		{
			return 0;
		}
		int byteval = ((const uint8_t*) bitvalue.vals.c_str())[byte];
		bit = 7 - (bitoffset & 0x7);
		bitval = byteval & (1 << bit);
		return bitval != 0 ? 1 : 0;
	}

	static void BitSetElementOp(uint32 op, BitSetElementValue& v1,
			BitSetElementValue& v2, BitSetElementValue& res)
	{
		if (op != BITOP_NOT && (v1.vals.empty() || v2.vals.empty()))
		{
			switch (op)
			{
				case BITOP_AND:
				{
					return;
				}
				case BITOP_OR:
				case BITOP_XOR:
				{
					if (v1.vals.empty())
					{
						res = v2;
					}
					if (v2.vals.empty())
					{
						res = v1;
					}
					return;
				}
				default:
				{
					return;
				}
			}
		}

		char dst[BIT_SUBSET_BYTES_SIZE];
		memset(dst, 0, BIT_SUBSET_BYTES_SIZE);
		uint32 bytestart = 0;
		uint32 byteend = 0;
		uint32 st = 0, et = 0;
		switch (op)
		{
			case BITOP_AND:
			{
				uint32 maxstart = v1.start < v2.start ? v2.start : v1.start;
				uint32 minend = v1.limit < v2.limit ? v1.limit : v2.limit;
				if (minend < maxstart)
				{
					return;
				}
				st = maxstart;
				et = minend;
				bytestart = (maxstart - 1) >> 3;
				byteend = (minend - 1) >> 3;
				break;
			}
			case BITOP_OR:
			case BITOP_XOR:
			{
				uint32 minstart = v1.start < v2.start ? v1.start : v2.start;
				uint32 maxend = v1.limit < v2.limit ? v2.limit : v1.limit;
				bytestart = (minstart - 1) >> 3;
				byteend = (maxend - 1) >> 3;
				st = minstart;
				et = maxend;
				break;
			}
			case BITOP_NOT:
			{
				bytestart = ((v1.start - 1) >> 3);
				byteend = ((v1.limit - 1) >> 3);
				st = v1.start;
				et = v1.limit;
				break;
			}
			default:
			{
				return;
			}
		}

		memcpy(dst + bytestart, v1.vals.c_str() + bytestart, byteend - bytestart + 1);
		uint64* lres = (uint64*) dst;
		const uint64* lp = (const uint64*) (v2.vals.c_str());
		uint32 xst = bytestart >>3;
		uint32 xet = byteend >> 3;
		while(xst <= xet)
		{
			switch (op)
			{
				case BITOP_AND:
				{
					lres[xst] &= lp[xst];
					break;
				}
				case BITOP_OR:
				{
					lres[xst] |= lp[xst];
					break;
				}
				case BITOP_XOR:
				{
					lres[xst] ^= lp[xst];
					break;
				}
				case BITOP_NOT:
				{
					lres[xst] = ~lres[xst];
					break;
				}
				default:
				{
					return;
				}
			}
			xst++;
		}
		res.vals.assign(dst, 0, BIT_SUBSET_BYTES_SIZE);
		res.limit = et;
		res.start = st;
		res.bitcount = popcount(dst + bytestart, byteend - bytestart + 1);
	}

	int Ardb::BitsOr(const DBID& db, SliceArray& keys,
			BitSetElementValueMap*& result, bool isXor)
	{
		struct BitSetOrWalk: public WalkHandler
		{
				Ardb* z_db;
				bool isxor;
				BitSetElementValueMap& res;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					BitSetKeyObject* bk = (BitSetKeyObject*) k;
					BitSetElementValue element;
					z_db->GetBitSetElementValue(*bk, element);
					BitSetElementValueMap::iterator found = res.find(bk->index);
					BitSetElementValue result;
					if (found != res.end())
					{
						BitSetElementOp(isxor ? BITOP_XOR : BITOP_OR,
								found->second, element, result);
					} else
					{
						BitSetElementOp(isxor ? BITOP_XOR : BITOP_OR, result,
								element, result);
					}
					if (result.bitcount > 0)
					{
						res[bk->index] = result;
					} else
					{
						res.erase(bk->index);
					}
					return 0;
				}
				BitSetOrWalk(Ardb* db, bool flag, BitSetElementValueMap& r) :
						z_db(db), isxor(flag), res(r)
				{
				}
		};
		for (uint32 i = 0; i < keys.size(); i++)
		{
			BitSetKeyObject setk(keys[i], 1, db);
			BitSetOrWalk walk(this, isXor, *result);
			Walk(setk, false, &walk);
		}
		return 0;
	}

	int Ardb::BitsAnd(const DBID& db, SliceArray& keys,
			BitSetElementValueMap*& result, BitSetElementValueMap*& tmp)
	{
		/* Lookup keys, and store pointers to the string objects into an array. */
		uint64 start_index = 0, end_index = 0;
		for (uint32 j = 0; j < keys.size(); j++)
		{
			BitSetMetaValue meta;
			GetBitSetMetaValue(db, keys[j], meta);
			if (0 != GetBitSetMetaValue(db, keys[j], meta))
			{
				return 0;
			}
			if (start_index == 0 || start_index > meta.min)
			{
				start_index = meta.min;
			}
			if (end_index == 0 || end_index > meta.max)
			{
				end_index = meta.max;
			}
		}
		if (start_index > end_index)
		{
			return 0;
		}
		struct BitSetAndWalk: public WalkHandler
		{
				Ardb* z_db;
				uint64 limit_index;
				BitSetElementValueMap& cmp;
				BitSetElementValueMap& res;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					BitSetKeyObject* bk = (BitSetKeyObject*) k;
					BitSetElementValue element;
					z_db->GetBitSetElementValue(*bk, element);
					if (&res == &cmp)
					{
						cmp[bk->index] = element;
						return 0;
					}
					BitSetElementValueMap::iterator found = cmp.find(bk->index);
					if (found != cmp.end())
					{
						BitSetElementValue result;
						BitSetElementOp(BITOP_AND, found->second, element,
								result);
						res[bk->index] = result;
					}
					if (bk->index == limit_index)
					{
						return -1;
					}
					return 0;
				}
				BitSetAndWalk(Ardb* db, uint64 l, BitSetElementValueMap& c,
						BitSetElementValueMap& r) :
						z_db(db), limit_index(l), cmp(c), res(r)
				{
				}
		};

		BitSetElementValueMap* tmpres = result;
		BitSetElementValueMap* tmpcmp = tmp;
		BitSetAndWalk firstWalk(this, end_index, *tmpcmp, *tmpcmp);
		BitSetKeyObject sk(keys[0], start_index, db);
		Walk(sk, false, &firstWalk);
		for (uint32 i = 1; i < keys.size(); i++)
		{
			BitSetKeyObject setk(keys[i], start_index, db);
			BitSetAndWalk walk(this, end_index, *tmpcmp, *tmpres);
			Walk(setk, false, &walk);
			if (i != keys.size() - 1)
			{
				BitSetElementValueMap* ptmp = tmpres;
				tmpres = tmpcmp;
				tmpcmp = ptmp;
				tmpcmp->clear();
			}
		}
		result = tmpres;
		tmp->clear();
		return 0;
	}

	int Ardb::BitsNot(const DBID& db, const Slice& key,
			BitSetElementValueMap*& result)
	{
		struct BitSetNotWalk: public WalkHandler
		{
				Ardb* z_db;
				BitSetElementValueMap& res;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					BitSetKeyObject* bk = (BitSetKeyObject*) k;
					BitSetElementValue element;
					z_db->GetBitSetElementValue(*bk, element);
					BitSetElementOp(BITOP_NOT, element, element, element);
					if (element.bitcount > 0)
					{
						res[bk->index] = element;
					}
					return 0;
				}
				BitSetNotWalk(Ardb* db, BitSetElementValueMap& r) :
						z_db(db), res(r)
				{
				}
		};
		BitSetKeyObject sk(key, 1, db);
		BitSetNotWalk walk(this, *result);
		Walk(sk, false, &walk);
		return 0;
	}
	int Ardb::BitOP(const DBID& db, const Slice& opstr, SliceArray& keys,
			BitSetElementValueMap*& res, BitSetElementValueMap*& tmp)
	{
		long op;
		/* Parse the operation name. */
		if (!strncasecmp(opstr.data(), "and", opstr.size()))
			op = BITOP_AND;
		else if (!strncasecmp(opstr.data(), "or", opstr.size()))
			op = BITOP_OR;
		else if (!strncasecmp(opstr.data(), "xor", opstr.size()))
			op = BITOP_XOR;
		else if (!strncasecmp(opstr.data(), "not", opstr.size()))
			op = BITOP_NOT;
		else
		{
			return ERR_INVALID_OPERATION;
		}

		/* Sanity check: NOT accepts only a single key argument. */
		if (op == BITOP_NOT && keys.size() != 1)
		{
			SetErrorCause("BITOP NOT must be called with a single source key.");
			return ERR_INVALID_OPERATION;
		}
		switch (op)
		{
			case BITOP_AND:
			{
				BitsAnd(db, keys, res, tmp);
				break;
			}
			case BITOP_OR:
			{
				BitsOr(db, keys, res, false);
				break;
			}
			case BITOP_XOR:
			{
				BitsOr(db, keys, res, true);
				break;
			}
			case BITOP_NOT:
			{
				BitsNot(db, keys[0], res);
				break;
			}
			default:
			{
				return -1;
			}
		}
		return 0;
	}

	int Ardb::BitOP(const DBID& db, const Slice& opstr, const Slice& dstkey,
			SliceArray& keys)
	{
		BitSetElementValueMap map1, map2;
		BitSetElementValueMap* res = &map1;
		BitSetElementValueMap* tmp = &map2;
		if (0 != BitOP(db, opstr, keys, res, tmp))
		{
			return -1;
		}
		BitClear(db, dstkey);
		KeyLockerGuard keyguard(m_key_locker, db, dstkey);
		BitSetMetaValue meta;
		if (!res->empty())
		{
			BatchWriteGuard guard(GetEngine());

			BitSetElementValueMap::iterator it = res->begin();
			while (it != res->end())
			{
				BitSetKeyObject sk(dstkey, it->first, db);
				SetBitSetElementValue(sk, it->second);
				meta.bitcount += it->second.bitcount;
				if (it == res->begin())
				{
					meta.min = it->first;
				}
				meta.max = it->first;
				it++;
			}
			SetBitSetMetaValue(db, dstkey, meta);
		}
		return meta.bitcount;
	}

	int64 Ardb::BitOPCount(const DBID& db, const Slice& opstr, SliceArray& keys)
	{
		BitSetElementValueMap map1, map2;
		BitSetElementValueMap* res = &map1;
		BitSetElementValueMap* tmp = &map2;
		if (0 != BitOP(db, opstr, keys, res, tmp))
		{
			return 0;
		}
		int64 count = 0;
		BitSetElementValueMap::iterator it = res->begin();
		while (it != res->end())
		{
			count += it->second.bitcount;
			it++;
		}
		return count;
	}

	int Ardb::BitCount(const DBID& db, const Slice& key, int64 start, int64 end)
	{
		BitSetMetaValue meta;
		GetBitSetMetaValue(db, key, meta);
		if (start <= 0 && (end < 0 || end >= (int64)(meta.max * BIT_SUBSET_SIZE)))
		{
			return meta.bitcount;
		}
		if (start < 0)
		{
			start = 0;
		}
		if (end < 0)
		{
			end = meta.max * BIT_SUBSET_SIZE;
		}
		if (start > end)
		{
			return 0;
		}
		uint64 startIndex = (start / BIT_SUBSET_SIZE) + 1;
		uint64 endIndex = (end / BIT_SUBSET_SIZE) + 1;
		uint32 so = start % BIT_SUBSET_SIZE;
		uint32 eo = end % BIT_SUBSET_SIZE;
		BitSetKeyObject sk(key, startIndex, db);
		struct BitSetWalk: public WalkHandler
		{
				Ardb* bdb;
				uint32 count;
				uint64 si;
				uint64 ei;
				uint32 so;
				uint32 eo;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					BitSetKeyObject* bk = (BitSetKeyObject*) k;
					BitSetElementValue element;
					bdb->GetBitSetElementValue(*bk, element);
					if (bk->index > si && bk->index < ei)
					{
						count += element.bitcount;
					} else
					{
						uint32 st = 0, et = 0;
						if (si == ei)
						{
							st = so;
							et = eo;
						} else
						{
							if (bk->index == si)
							{
								st = so;
								et = element.limit - 1;
							} else
							{
								st = element.start - 1;
								et = eo;
							}
						}
						if (element.start >= (st + 1)
								&& element.limit <= (et + 1))
						{
							count += element.bitcount;
						} else if (element.limit < st + 1
								|| element.start > (et + 1))
						{
							//do nothing
						} else
						{
							while (st % 8 != 0 && st <= et)
							{
								count += bdb->GetBit(bk->db, bk->key, st);
								st++;
							}
							if (st < et)
							{
								while (et % 8 != 0 && et > st)
								{
									count += bdb->GetBit(bk->db, bk->key, et);
									et--;
								}
							}
							if (et > st)
							{
								uint32 s = st >> 3;
								uint32 e = et >> 3;
								count += popcount(element.vals.c_str() + s,
										e - s);
							}
						}
					}
					if (bk->index == ei)
					{
						return -1;
					}
					return 0;
				}
				BitSetWalk(Ardb* dbi, uint64 s, uint64 e, uint32 ss, uint32 ee) :
						bdb(dbi), count(0), si(s), ei(e), so(ss), eo(ee)
				{
				}
		} walk(this, startIndex, endIndex, so, eo);
		Walk(sk, false, &walk);
		return walk.count;
	}

	int Ardb::BitClear(const DBID& db, const Slice& key)
	{
		KeyLockerGuard keyguard(m_key_locker, db, key);
		BitSetMetaValue meta;
		if (0 != GetBitSetMetaValue(db, key, meta))
		{
			return 0;
		}
		struct BitClearWalk: public WalkHandler
		{
				Ardb* z_db;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					BitSetKeyObject* sek = (BitSetKeyObject*) k;
					z_db->DelValue(*sek);
					return 0;
				}
				BitClearWalk(Ardb* db) :
						z_db(db)
				{
				}
		} walk(this);
		BatchWriteGuard guard(GetEngine());
		BitSetKeyObject bk(key, 1, db);
		Walk(bk, false, &walk);
		KeyObject k(key, BITSET_META, db);
		DelValue(k);
		return 0;
	}
}

