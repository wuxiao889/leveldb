// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  *table = nullptr;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }
  // 从文件结尾读取footer
  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  // 根据传入的 sstable size（Version::files_保存的 FileMetaData），首先读取文件末尾的footer。
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;
  Footer footer;
  // 解析 footer 数据(Footer::DecodeFrom() table/format.cc)，校验 magic,获得 index_block 和 metaindex_block 的 BlockHandle.
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;
  
  // Read the index block
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  // 根据 index_block 的 BlockHandle，读取 index_block(ReadBlock() table/format.cc)
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  // 成功读取了footer和index block,此时table 可以响应请求了
  // 构建table对象，读取metaindex 数据构建 filter policy
  // 如果option打开了cache，还要为table创建cache
  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0); // 分配 cacheID(ShardedLRUCache::NewId(), util/cache.cc)
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);    // 封装成 Table（调用者会将其加入 table cache， TableCache::NewIterator（））
    (*table)->ReadMeta(footer);
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  // 不需要metadata
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return; // 失败了也不报错，因为没有meta信息也没有关系
  }
  Block* meta = new Block(contents);

  // 根据读取的content构建block，找到指定的filter
  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  // 如果找到了就调用ReadFilter构建filter对象
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

// 根据指定的偏移和大小，读取filter
void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  // 从传入的filter_handle_value Decode出BlockHandle
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  // 根据解析出的位置读取filter内容。
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  // 如果block的heap_allocated为true, 表明需要自行释放内存，因此需要把指针
  // 保留在filter_data中。
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later
  }
  // 根据读取的data创建FilterBlockReader对象
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// 注册给 iterator 的回调
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  // 从参数中解析出 BlockHandle 对象 
  Table* table = reinterpret_cast<Table*>(arg); 
  Cache* block_cache = table->rep_->options.block_cache;  // 如果用户未指定自己的实现，使用内部的 ShardLRUCache。
  Block* block = nullptr; 
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value; // BlockHandle
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) {
      // 存在 block cache 根据传入的 BlockHandle，构造出 BlockCache 的 key 进行 lookup
      char cache_key_buffer[16];
      // 解码 cache key 
      // cache 中的 key 为 block 所在 sstable 加入 TableCache 时获得的 cacheID 加上 block 在 sstable
      // 中的 offset，value 为 未压缩的 block 数据。
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        // 存在，则读取 cache 中的 block 数据（Block）。
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            // 不存在，调用 ReadBlock()从磁盘上获得，同时插入 BlockCache。
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      // 如果不存在 block cache，直接调用 ReadBlock(),
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  // 根据得到的 Block，构造出 Block::Iter 返回。
  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    // 注册 Cleanup 回调
    if (cache_handle == nullptr) {
      // 如果没有LRU cache 直接删除 Block
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      // 如果有, 回调注册为RelaeaseBlock
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

// 对 sstable 进行 key 的查找遍历封装成 TwoLevelIterator(参见 Iterator)处理。
Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  // 非TwoLevelIterator，普通的IndexBlock Iterator
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  // 根据传入的key定位数据
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();  // 可能存在k 的dataBlock的 BlockHandle
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    // filter不为空，用 BloomFilter快速判断是否存在
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found 
    } else {
      // 读取block查找kv对， iiter->value = blockHandle
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      // 析构流程
      // 无Cache => delete Block
      //            heap_allocated => delete date_
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

// 并不是精确的定位，而是在Table中找到第一个 >= 指定key的 kv对
// 返回其 value 在sstable文件中的偏移
uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  // 使用 Block::iter seek函数定位
  index_iter->Seek(key);
  uint64_t result;
  // 如果index_iter是合法的值
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    // decode成功，返回offset
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
      // 其它情况，设置 result 为 metaindex_handle offset，
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
