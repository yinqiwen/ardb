/*
 * strings.cpp
 *
 *  Created on: 2013-3-29
 *      Author: wqy
 */
#include "ardb.hpp"

namespace ardb
{
	static const long BITOP_AND = 0;
	static const long BITOP_OR = 1;
	static const long BITOP_XOR = 2;
	static const long BITOP_NOT = 3;
	//copy from redis
	static long popcount(const void *s, long count)
	{
		long bits = 0;
		unsigned char *p;
		const uint32_t* p4 = (const uint32_t*) s;
		static const unsigned char bitsinbyte[256] = { 0, 1, 1, 2, 1, 2, 2, 3,
				1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3,
				4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3,
				3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3,
				4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5,
				4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3,
				4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3,
				3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4,
				5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
				3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3,
				4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5,
				5, 6, 5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6,
				7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8 };

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

	int Ardb::Append(const DBID& db, const Slice& key, const Slice& value)
	{
		KeyObject k(key);
		ValueObject v;
		if (GetValue(db, k, &v) < 0)
		{
			v.type = RAW;
			v.v.raw = new Buffer(const_cast<char*>(value.data()), 0,
					value.size());
		} else
		{
			value_convert_to_raw(v);
			v.v.raw->Write(value.data(), value.size());
		}

		uint32_t size = v.v.raw->ReadableBytes();
		int ret = SetValue(db, k, v);
		if (0 == ret)
		{
			return size;
		}
		return ret;
	}

	int Ardb::XIncrby(const DBID& db, const Slice& key, int64_t increment)
	{
		KeyObject k(key);
		return 0;
	}

	int Ardb::Incr(const DBID& db, const Slice& key, int64_t& value)
	{
		return Incrby(db, key, 1, value);
	}
	int Ardb::Incrby(const DBID& db, const Slice& key, int64_t increment,
			int64_t& value)
	{
		KeyObject k(key);
		ValueObject v;
		if (GetValue(db, k, &v) < 0)
		{
			return -1;
		}
		value_convert_to_number(v);
		if (v.type == INTEGER)
		{
			v.v.int_v += increment;
			SetValue(db, k, v);
			value = v.v.int_v;
			return 0;
		} else
		{
			return -1;
		}
	}

	int Ardb::Decr(const DBID& db, const Slice& key, int64_t& value)
	{
		return Decrby(db, key, 1, value);
	}

	int Ardb::Decrby(const DBID& db, const Slice& key, int64_t decrement,
			int64_t& value)
	{
		return Incrby(db, key, 0 - decrement, value);
	}

	int Ardb::IncrbyFloat(const DBID& db, const Slice& key, double increment,
			double& value)
	{
		KeyObject k(key);
		ValueObject v;
		if (GetValue(db, k, &v) < 0)
		{
			return -1;
		}
		value_convert_to_number(v);
		if (v.type == INTEGER)
		{
			v.type = DOUBLE;
			double dv = v.v.int_v;
			v.v.double_v = dv;
		}
		if (v.type == DOUBLE)
		{
			v.v.double_v += increment;
			SetValue(db, k, v);
			value = v.v.double_v;
			return 0;
		} else
		{
			return -1;
		}
	}

	int Ardb::GetRange(const DBID& db, const Slice& key, int start, int end,
			std::string& v)
	{
		KeyObject k(key);
		ValueObject vo;
		if (GetValue(db, k, &vo) < 0)
		{
			return ERR_NOT_EXIST;
		}
		if (vo.type != RAW)
		{
			value_convert_to_raw(vo);
		}
		start = RealPosition(vo.v.raw, start);
		end = RealPosition(vo.v.raw, end);
		if (start > end)
		{
			return ERR_OUTOFRANGE;
		}
		vo.v.raw->SetReadIndex(start);
		vo.v.raw->SetWriteIndex(end + 1);
		v = vo.ToString();
		return ARDB_OK;
	}

	int Ardb::SetRange(const DBID& db, const Slice& key, int start, const Slice& value)
	{
		KeyObject k(key);
		ValueObject v;
		if (GetValue(db, k, &v) < 0)
		{
			return ERR_NOT_EXIST;
		}
		if (v.type != RAW)
		{
			value_convert_to_raw(v);
		}
		start = RealPosition(v.v.raw, start);
		v.v.raw->SetWriteIndex(start);
		v.v.raw->Write(value.data(), value.size());
		return SetValue(db, k, v);
	}

	int Ardb::GetSet(const DBID& db, const Slice& key, const Slice& value,
			std::string& v)
	{
		if (Get(db, key, &v) < 0)
		{
			return ERR_NOT_EXIST;
		}
		return Set(db, key, value);
	}

	int Ardb::SetBit(const DBID& db, const Slice& key, uint32_t bitoffset, uint8_t value)
	{
		int byte, bit;
		int byteval, bitval;
		long on = value;
		/* Bits can only be set or cleared... */
		if (on & ~1)
		{
			return -1;
		}
		byte = bitoffset >> 3;
		KeyObject k(key);
		ValueObject v;
		if (GetValue(db, k, &v) < 0)
		{
			v.type = RAW;
			v.v.raw = new Buffer();

		}
		value_convert_to_raw(v);
		if (v.v.raw->Capacity() < byte + 1)
		{
			int tmp = byte + 1 - v.v.raw->Capacity();
			v.v.raw->EnsureWritableBytes(tmp, true);
		}

		/* Get current values */
		byteval = ((const uint8_t*) v.v.raw->GetRawReadBuffer())[byte];
		bit = 7 - (bitoffset & 0x7);
		bitval = byteval & (1 << bit);

		int tmp = byteval;

		/* Update byte with new bit value and return original value */
		byteval &= ~(1 << bit);
		byteval |= ((on & 0x1) << bit);
		if (byteval != tmp)
		{
			((uint8_t*) v.v.raw->GetRawReadBuffer())[byte] = byteval;
			if (byte >= v.v.raw->GetWriteIndex())
			{
				v.v.raw->SetWriteIndex(byte + 1);
			}
			SetValue(db, k, v);
		}
		return bitval;
	}

	int Ardb::GetBit(const DBID& db, const Slice& key, int bitoffset)
	{
		KeyObject k(key);
		ValueObject v;
		if (GetValue(db, k, &v) < 0)
		{
			return ERR_NOT_EXIST;
		}
		value_convert_to_raw(v);
		size_t byte, bit;
		size_t bitval = 0;

		byte = bitoffset >> 3;
		if (byte >= v.v.raw->ReadableBytes())
		{
			printf("####%d %d\n", byte, v.v.raw->ReadableBytes());
			return 0;
		}
		int byteval = ((const uint8_t*) v.v.raw->GetRawReadBuffer())[byte];
		bit = 7 - (bitoffset & 0x7);
		bitval = byteval & (1 << bit);
		return bitval;
	}

	int Ardb::BitOP(const DBID& db, const Slice& opstr, const Slice& dstkey,
			SliceArray& keys)
	{
		long op, j, numkeys;
		unsigned char **src;
		long maxlen = 0;
		long minlen = 0;
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
		std::vector<int> lens;
		ValueArray vs;
		lens.reserve(numkeys);
		src = (unsigned char **) malloc(sizeof(unsigned char*) * numkeys);
		for (j = 0; j < numkeys; j++)
		{
			lens.push_back(0);
			KeyObject k(keys[j]);
			vs.push_back(ValueObject());
			if (0 != GetValue(db, k, &vs[j]))
			{
				src[j] = NULL;
				continue;
			}
			value_convert_to_raw(vs[j]);

			lens[j] = vs[j].v.raw->ReadableBytes();
			src[j] = (unsigned char *) (vs[j].v.raw->GetRawReadBuffer());
			if (lens[j] > maxlen)
				maxlen = lens[j];
			if (j == 0 || lens[j] < minlen)
				minlen = lens[j];
		}

		/* Compute the bit operation, if at least one string is not empty. */
		if (maxlen)
		{
			res = (unsigned char*) malloc(maxlen);
			unsigned char output, byte;
			long i;

			/* Fast path: as far as we have data for all the input bitmaps we
			 * can take a fast path that performs much better than the
			 * vanilla algorithm. */
			j = 0;
			if (minlen && numkeys <= 16)
			{
				unsigned long *lp[16];
				unsigned long *lres = (unsigned long*) res;

				/* Note: sds pointer is always aligned to 8 byte boundary. */
				memcpy(lp, src, sizeof(unsigned long*) * numkeys);
				memcpy(res, src[0], minlen);

				/* Different branches per different operations for speed (sorry). */
				if (op == BITOP_AND)
				{
					while (minlen >= sizeof(unsigned long) * 4)
					{
						for (i = 1; i < numkeys; i++)
						{
							lres[0] &= lp[i][0];
							lres[1] &= lp[i][1];
							lres[2] &= lp[i][2];
							lres[3] &= lp[i][3];
							lp[i] += 4;
						}
						lres += 4;
						j += sizeof(unsigned long) * 4;
						minlen -= sizeof(unsigned long) * 4;
					}
				} else if (op == BITOP_OR)
				{
					while (minlen >= sizeof(unsigned long) * 4)
					{
						for (i = 1; i < numkeys; i++)
						{
							lres[0] |= lp[i][0];
							lres[1] |= lp[i][1];
							lres[2] |= lp[i][2];
							lres[3] |= lp[i][3];
							lp[i] += 4;
						}
						lres += 4;
						j += sizeof(unsigned long) * 4;
						minlen -= sizeof(unsigned long) * 4;
					}
				} else if (op == BITOP_XOR)
				{
					while (minlen >= sizeof(unsigned long) * 4)
					{
						for (i = 1; i < numkeys; i++)
						{
							lres[0] ^= lp[i][0];
							lres[1] ^= lp[i][1];
							lres[2] ^= lp[i][2];
							lres[3] ^= lp[i][3];
							lp[i] += 4;
						}
						lres += 4;
						j += sizeof(unsigned long) * 4;
						minlen -= sizeof(unsigned long) * 4;
					}
				} else if (op == BITOP_NOT)
				{
					while (minlen >= sizeof(unsigned long) * 4)
					{
						lres[0] = ~lres[0];
						lres[1] = ~lres[1];
						lres[2] = ~lres[2];
						lres[3] = ~lres[3];
						lres += 4;
						j += sizeof(unsigned long) * 4;
						minlen -= sizeof(unsigned long) * 4;
					}
				}
			}

			/* j is set to the next byte to process by the previous loop. */
			for (; j < maxlen; j++)
			{
				output = (lens[0] <= j) ? 0 : src[0][j];
				if (op == BITOP_NOT)
					output = ~output;
				for (i = 1; i < numkeys; i++)
				{
					byte = (lens[i] <= j) ? 0 : src[i][j];
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
				}
				res[j] = output;
			}
		}

		/* Store the computed value into the target key */
		if (maxlen)
		{
			ValueObject v;
			fill_raw_value(Slice((char*) res, maxlen), v);
			KeyObject k(dstkey);
			SetValue(db, k, v);
		}
		free(src);
		if (NULL != res)
		{
			free(res);
		}
		return maxlen;
	}

	int Ardb::BitCount(const DBID& db, const Slice& key, int start, int end)
	{
		KeyObject k(key);
		ValueObject v;
		if (GetValue(db, k, &v) < 0)
		{
			return ERR_NOT_EXIST;
		}
		value_convert_to_raw(v);
		start = RealPosition(v.v.raw, start);
		end = RealPosition(v.v.raw, end);
		if (start > end)
		{
			return 0;
		}
		long bytes = end - start + 1;
		return popcount(v.v.raw->GetRawReadBuffer() + start, bytes);
	}
}

