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

	static const long BIT_SUBSET_SIZE = 4096;
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
		        && BufferHelper::ReadVarUInt64(*(v.v.raw), meta.max)
		        && BufferHelper::ReadVarUInt64(*(v.v.raw), meta.limit);
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
		BufferHelper::WriteVarUInt64(*(v.v.raw), meta.limit);
	}

	static bool DecodeSetElementData(ValueObject& v, BitSetElementValue& meta)
	{
		if (v.type != RAW)
		{
			return false;
		}
		return BufferHelper::ReadVarUInt32(*(v.v.raw), meta.bitcount)
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
		if (meta.min == 0)
		{
			meta.min = index;
			set_changed = true;
		}
		if (meta.max == 0)
		{
			meta.max = index;
			set_changed = true;
		}
		if (meta.limit < bitoffset)
		{
			meta.limit = bitoffset;
			set_changed = true;
		}

		byte = (bitoffset % BIT_SUBSET_SIZE) >> 3;
		BitSetKeyObject k(key, index, db);
		BitSetElementValue bitvalue;
		GetBitSetElementValue(k, bitvalue);
		if (bitvalue.vals.size() < BIT_SUBSET_SIZE)
		{
			uint32 tmpsize = BIT_SUBSET_SIZE - bitvalue.vals.size();
			char tmp[tmpsize];
			memset(tmp, 0, tmpsize);
			bitvalue.vals.append(tmp, tmpsize);
		}

		if (bitvalue.limit < byte)
		{
			bitvalue.limit = byte;
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
			if (byteval == 1)
			{
				bitvalue.bitcount++;
				meta.bitcount++;
			}
			else
			{
				bitvalue.bitcount--;
				meta.bitcount--;
			}
			set_changed = true;
			if (bitvalue.limit < byte)
			{
				bitvalue.limit = byte;
			}
			SetBitSetElementValue(k, bitvalue);
		}
		else
		{
			if (bitvalue.limit < byte)
			{
				bitvalue.limit = byte;
				SetBitSetElementValue(k, bitvalue);
			}
		}
		if (set_changed)
		{
			SetBitSetMetaValue(db, key, meta);
		}
		return bitval;
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

		byte = (bitoffset % BIT_SUBSET_SIZE) >> 3;
		if (byte >= bitvalue.vals.size())
		{
			return 0;
		}
		int byteval = ((const uint8_t*) bitvalue.vals.c_str())[byte];
		bit = 7 - (bitoffset & 0x7);
		bitval = byteval & (1 << bit);
		return bitval;
	}

	static void BitElementsOP(int op, BitSetElementValue& v1,
	        BitSetElementValue& v2, BitSetElementValue& res)
	{
		if (v1.vals.empty() || v2.vals.empty())
		{
			if (op == BITOP_AND)
			{
				return;
			}
			if (op == BITOP_OR)
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
		}
		uint32 minlen = v1.limit < v2.limit ? v1.limit : v2.limit;
		uint32 maxlen = v1.limit < v2.limit ? v2.limit : v1.limit;
		const uint64* lp = (const uint64*) (v2.vals.c_str());
		char dst[maxlen];
		uint64* lres = (uint64*) dst;
		memset(dst, 0, maxlen);
		memcpy(dst, v1.vals.c_str(), v1.limit);
		int j = 0;

		if (op == BITOP_AND)
		{
			while (minlen >= sizeof(uint64))
			{
				lres[0] &= lp[0];
				lp++;
				lres++;
				j += sizeof(uint64);
				minlen -= sizeof(uint64);
			}
		}
		else if (op == BITOP_OR)
		{
			while (minlen >= sizeof(uint64))
			{
				lres[0] |= lp[0];
				lp++;
				lres++;
				j += sizeof(uint64);
				minlen -= sizeof(uint64);
			}
		}
		else if (op == BITOP_XOR)
		{
			while (minlen >= sizeof(uint64))
			{
				lres[0] ^= lp[0];
				lp++;
				lres++;
				j += sizeof(uint64);
				minlen -= sizeof(uint64);
			}

		}
		else if (op == BITOP_NOT)
		{
			while (minlen >= sizeof(uint64))
			{
				lres[0] = ~lres[0];
				lres++;
				j += sizeof(uint64);
				minlen -= sizeof(uint64);
			}
		}

		uint8 output;
		/* j is set to the next byte to process by the previous loop. */
		for (; j < maxlen; j++)
		{
			output = (v2.limit <= j) ? 0 : dst[j];
			if (op == BITOP_NOT)
				output = ~output;
			byte = (v2.limit <= j) ? 0 : v2.vals[j];
			switch (op)
			{
				case BITOP_AND:
					output &= byte;
					break;
				case BITOP_OR:
					output |= byte;
					break;
				case BITOP_XOR:
					output ^= byte;
					break;
			}
			dst[j] = output;
		}
		res.vals.assign(dst, 0, maxlen);
		res.limit = maxlen;
		res.bitcount = popcount(dst, maxlen);
	}

	int Ardb::Ardb::BitOPVals(const DBID& db, const Slice& opstr,
	        SliceArray& keys, BitSetElementValueMap& bitvals)
	{
		long op, j, numkeys;
		uint64 maxlen = 0;
		uint64 minlen = 0;
		uint32 minsizekey_index = 0;
		unsigned char *res = NULL;

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
		/* Lookup keys, and store pointers to the string objects into an array. */
		numkeys = keys.size();
		std::vector<uint32> lens;
		lens.reserve(numkeys);
		for (j = 0; j < numkeys; j++)
		{
			lens.push_back(0);
			BitSetMetaValue meta;
			GetBitSetMetaValue(db, keys[j], meta);
			if (0 != GetBitSetMetaValue(db, keys[j], meta))
			{
				continue;
			}
		}

		struct BitSetWalk: public WalkHandler
		{
				Ardb* z_db;
				int bop;
				BitSetElementValueMap& cmp;
				BitSetElementValueMap& res;
				int OnKeyValue(KeyObject* k, ValueObject* v, uint32 cursor)
				{
					BitSetKeyObject* bk = (BitSetKeyObject*) k;
					BitSetElementValue element;
					z_db->GetBitSetElementValue(*bk, element);
					if (&res == &cmp)
					{
						if (bop == BITOP_NOT)
						{
							BitElementsOP(BITOP_NOT, element, element, element);
						}
						cmp[bk->index] = element;

						return 0;
					}

					BitSetElementValue empty;
					BitSetElementValueMap::iterator found = cmp.find(bk->index);
					BitSetElementValue result;
					if (found != cmp.end())
					{
						BitElementsOP(bop, found->second, element, result);
					}
					else
					{
						BitElementsOP(bop, found->second, empty, result);
					}
					res[bk->index] = result;
					return 0;
				}
				BitSetWalk(Ardb* db) :
						z_db(db)
				{
				}
		} walk(this);

		return 0;

	}

	int Ardb::BitOP(const DBID& db, const Slice& opstr, const Slice& dstkey,
	        SliceArray& keys)
	{
		BitSetElementValueMap map;
		BitOPVals(db, opstr, keys, map);

	}

	int64 Ardb::BitOPCount(const DBID& db, const Slice& op, SliceArray& keys)
	{
		BitSetElementValueMap map;
		BitOPVals(db, op, keys, map);
		return map.size();
	}

	int Ardb::BitCount(const DBID& db, const Slice& key, int64 start, int64 end)
	{
		BitSetMetaValue meta;
		GetBitSetMetaValue(db, key, meta);
		if (start == 0 && (end < 0 || end >= (meta.max * BIT_SUBSET_SIZE)))
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
		uint32 eo = endIndex % BIT_SUBSET_SIZE;
		int count = 0;
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
					}
					else
					{
						if (si == ei)
						{
							count += popcount(element.vals.c_str() + so,
							        (eo - so + 1));
						}
						else
						{
							if (bk->index == si)
							{
								count += popcount(element.vals.c_str() + so,
								        (element.vals.size() - so + 1));
							}
							else
							{
								count += popcount(element.vals.c_str(), eo + 1);
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
		} walk(this, startIndex, endIndex);
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
					BitSetKeyObject* sek = (SetKeyObject*) k;
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

