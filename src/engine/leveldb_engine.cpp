/*
 * leveldb_engine.cpp
 *
 *  Created on: 2013-3-31
 *      Author: wqy
 */
#include "leveldb_engine.hpp"
#include "ardb.hpp"
#include "ardb_data.hpp"
#include "comparator.hpp"
#include "util/helpers.hpp"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"
#include <string.h>

#define LEVELDB_SLICE(slice) leveldb::Slice(slice.data(), slice.size())
#define ARDB_SLICE(slice) Slice(slice.data(), slice.size())

namespace ardb
{
	int LevelDBComparator::Compare(const leveldb::Slice& a,
			const leveldb::Slice& b) const
	{
		return ardb_compare_keys(a.data(), a.size(), b.data(), b.size());
	}

	void LevelDBComparator::FindShortestSeparator(std::string* start,
			const leveldb::Slice& limit) const
	{
		Buffer ak_buf(const_cast<char*>(start->data()), 0, start->size());
		Buffer bk_buf(const_cast<char*>(limit.data()), 0, limit.size());
		if (!start->size() > 4 && limit.size() > 4)
		{
			uint32 aheader, bheader;
			BufferHelper::ReadFixUInt32(ak_buf, aheader);
			BufferHelper::ReadFixUInt32(bk_buf, bheader);
			uint8 type1 = aheader & 0xFF;
			uint8 type2 = bheader & 0xFF;
			uint32 adb = aheader >> 8;
			uint32 bdb = bheader >> 8;
			assert(adb <= bdb);
			if (adb < bdb || type1 < type2)
			{
				start->resize(4);
				start->assign(limit.data(), 0, 4);
			}
			else
			{
				uint32 akeysize = 0, bkeysize = 0;
				assert(BufferHelper::ReadVarUInt32(ak_buf, akeysize));
				assert(BufferHelper::ReadVarUInt32(bk_buf, bkeysize));
				assert(akeysize <= bkeysize);
				if (akeysize < bkeysize)
				{
					start->resize(ak_buf.GetReadIndex());
					start->assign(limit.data(), ak_buf.GetReadIndex());
				} else
				{
					size_t diff_index = ak_buf.GetReadIndex();
					while ((diff_index < start->size()
							&& diff_index < limit.size())
							&& ((*start)[diff_index] == limit[diff_index]))
					{
						diff_index++;
					}
					if (diff_index >= start->size())
					{
						// Do not shorten if one string is a prefix of the other
					} else
					{
						if (diff_index >= start->size() || limit.size())
						{
							return;
						}
						uint8_t diff_byte =
								static_cast<uint8_t>((*start)[diff_index]);
						if (diff_byte < static_cast<uint8_t>(0xff)
								&& diff_byte + 1
										< static_cast<uint8_t>(limit[diff_index]))
						{
							(*start)[diff_index]++;
							start->resize(diff_index + 1);
							assert(Compare(*start, limit) < 0);
						}
					}
				}
			}
		}
	}

	void LevelDBComparator::FindShortSuccessor(std::string* key) const
	{
		if (key->size() >= 4)
		{
			Buffer ak_buf(const_cast<char*>(key->data()), 0, key->size());
			uint32 aheader;
			BufferHelper::ReadFixUInt32(ak_buf, aheader);
			uint8 type = aheader & 0xFF;
			uint32 adb = aheader >> 8;
			aheader = adb << 8 + (type + 1);
			aheader = htonl(aheader);
			key->resize(4);
			key->assign((const char*) (&aheader), 4);
		}
	}

	LevelDBEngineFactory::LevelDBEngineFactory(const Properties& props)
	{
		ParseConfig(props, m_cfg);

	}

	void LevelDBEngineFactory::ParseConfig(const Properties& props,
			LevelDBConfig& cfg)
	{
		cfg.path = ".";
		conf_get_string(props, "dir", cfg.path);
		conf_get_int64(props, "leveldb.block_cache_size", cfg.block_cache_size);
		conf_get_int64(props, "leveldb.write_buffer_size",
				cfg.write_buffer_size);
		conf_get_int64(props, "leveldb.max_open_files", cfg.max_open_files);
		conf_get_int64(props, "leveldb.block_size", cfg.block_size);
		conf_get_int64(props, "leveldb.block_restart_interval",
				cfg.block_restart_interval);
		conf_get_int64(props, "leveldb.bloom_bits", cfg.bloom_bits);
		conf_get_int64(props, "leveldb.batch_commit_watermark",
				cfg.batch_commit_watermark);
	}

	KeyValueEngine* LevelDBEngineFactory::CreateDB(const std::string& name)
	{
		LevelDBEngine* engine = new LevelDBEngine();
		LevelDBConfig cfg = m_cfg;
		char tmp[cfg.path.size() + name.size() + 10];
		sprintf(tmp, "%s/%s", cfg.path.c_str(), name.c_str());
		cfg.path = tmp;
		if (engine->Init(cfg) != 0)
		{
			DELETE(engine);
			ERROR_LOG("Failed to create DB:%s", name.c_str());
			return NULL;
		}
		return engine;
	}
	void LevelDBEngineFactory::DestroyDB(KeyValueEngine* engine)
	{
		LevelDBEngine* leveldb = (LevelDBEngine*) engine;
		std::string path = leveldb->m_db_path;
		DELETE(engine);
		leveldb::Options options;
		leveldb::DestroyDB(path, options);
	}

	void LevelDBEngineFactory::CloseDB(KeyValueEngine* engine)
	{
		DELETE(engine);
	}
	void LevelDBIterator::SeekToFirst()
	{
		m_iter->SeekToFirst();
	}
	void LevelDBIterator::SeekToLast()
	{
		m_iter->SeekToLast();
	}
	void LevelDBIterator::Next()
	{
		m_iter->Next();
	}
	void LevelDBIterator::Prev()
	{
		m_iter->Prev();
	}
	Slice LevelDBIterator::Key() const
	{
		return ARDB_SLICE(m_iter->key());
	}
	Slice LevelDBIterator::Value() const
	{
		return ARDB_SLICE(m_iter->value());
	}
	bool LevelDBIterator::Valid()
	{
		return m_iter->Valid();
	}

	LevelDBEngine::LevelDBEngine() :
			m_db(NULL)
	{

	}
	LevelDBEngine::~LevelDBEngine()
	{
		DELETE(m_db);
		DELETE(m_options.block_cache);
		DELETE(m_options.filter_policy);
	}
	int LevelDBEngine::Init(const LevelDBConfig& cfg)
	{
		m_cfg = cfg;
		m_options.create_if_missing = true;
		m_options.comparator = &m_comparator;
		if (cfg.block_cache_size > 0)
		{
			leveldb::Cache* cache = leveldb::NewLRUCache(cfg.block_cache_size);
			m_options.block_cache = cache;
		}
		if (cfg.block_size > 0)
		{
			m_options.block_size = cfg.block_size;
		}
		if (cfg.block_restart_interval > 0)
		{
			m_options.block_restart_interval = cfg.block_restart_interval;
		}
		if (cfg.write_buffer_size > 0)
		{
			m_options.write_buffer_size = cfg.write_buffer_size;
		}
		m_options.max_open_files = cfg.max_open_files;
		if (cfg.bloom_bits > 0)
		{
			m_options.filter_policy = leveldb::NewBloomFilterPolicy(
					cfg.bloom_bits);
		}

		make_dir(cfg.path);
		m_db_path = cfg.path;
		leveldb::Status status = leveldb::DB::Open(m_options, cfg.path.c_str(),
				&m_db);
		do
		{
			if (!status.ok())
			{
				ERROR_LOG(
						"Failed to init engine:%s", status.ToString().c_str());
				if (status.IsCorruption())
				{
					status = leveldb::RepairDB(cfg.path.c_str(), m_options);
					if (!status.ok())
					{
						ERROR_LOG(
								"Failed to repair:%s for %s", cfg.path.c_str(), status.ToString().c_str());
						return -1;
					}
					status = leveldb::DB::Open(m_options, cfg.path.c_str(),
							&m_db);
				}
			} else
			{
				break;
			}
		} while (1);
		return status.ok() ? 0 : -1;
	}

	int LevelDBEngine::BeginBatchWrite()
	{
		m_batch_local.GetValue().AddRef();
		return 0;
	}
	int LevelDBEngine::CommitBatchWrite()
	{
		BatchHolder& holder = m_batch_local.GetValue();
		holder.ReleaseRef();
		if (holder.EmptyRef())
		{
			return FlushWriteBatch(holder);
		}
		return 0;
	}
	int LevelDBEngine::DiscardBatchWrite()
	{
		BatchHolder& holder = m_batch_local.GetValue();
		holder.ReleaseRef();
		holder.Clear();
		return 0;
	}

	int LevelDBEngine::FlushWriteBatch(BatchHolder& holder)
	{
		leveldb::Status s = m_db->Write(leveldb::WriteOptions(), &holder.batch);
		holder.Clear();
		return s.ok() ? 0 : -1;
	}

	void LevelDBEngine::CompactRange(const Slice& begin, const Slice& end)
	{
		leveldb::Slice s(begin.data(), begin.size());
		leveldb::Slice e(end.data(), end.size());
		leveldb::Slice* start = s.size() > 0 ? &s : NULL;
		leveldb::Slice* endpos = e.size() > 0 ? &e : NULL;
		m_db->CompactRange(start, endpos);
	}

	int LevelDBEngine::Put(const Slice& key, const Slice& value)
	{
		leveldb::Status s = leveldb::Status::OK();
		BatchHolder& holder = m_batch_local.GetValue();
		if (!holder.EmptyRef())
		{
			holder.batch.Put(LEVELDB_SLICE(key), LEVELDB_SLICE(value));
		}
		else
		{
			s = m_db->Put(leveldb::WriteOptions(), LEVELDB_SLICE(key),
					LEVELDB_SLICE(value));
		}
		return s.ok() ? 0 : -1;
	}
	int LevelDBEngine::Get(const Slice& key, std::string* value)
	{
		leveldb::Status s = m_db->Get(leveldb::ReadOptions(),
		LEVELDB_SLICE(key), value);
		return s.ok() ? 0 : -1;
	}
	int LevelDBEngine::Del(const Slice& key)
	{
		leveldb::Status s = leveldb::Status::OK();
		BatchHolder& holder = m_batch_local.GetValue();
		if (!holder.EmptyRef())
		{
			holder.batch.Delete(LEVELDB_SLICE(key));
		}
		else
		{
			s = m_db->Delete(leveldb::WriteOptions(), LEVELDB_SLICE(key));
		}
		return s.ok() ? 0 : -1;
	}

	Iterator* LevelDBEngine::Find(const Slice& findkey, bool cache)
	{
		leveldb::ReadOptions options;
		options.fill_cache = cache;
		leveldb::Iterator* iter = m_db->NewIterator(options);
		iter->Seek(LEVELDB_SLICE(findkey));
		return new LevelDBIterator(iter);
	}

	const std::string LevelDBEngine::Stats()
	{
		std::string str;
		m_db->GetProperty("leveldb.stats", &str);
		return str;
	}

}

