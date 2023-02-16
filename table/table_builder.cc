// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;                // data block option
  Options index_block_options;    // index block option
  WritableFile* file;             // sstable file
  uint64_t offset;                // 要写入data block在sstable文件中的偏移
  Status status;
  BlockBuilder data_block;        // 当前操作的 data block
  BlockBuilder index_block;       // 当前操作的 index block，
                                  // 每次Flush填入偏移量大小，当pending_handle为true时插入索引索引
                                  // 最后finish时写入
  std::string last_key;           // 当前 data block 最后一个key
  int64_t num_entries;            // 当前 data block 个数
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block; // bloom filter ，快速定位key是否存在

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;     // 是否有index entry在等待写入 key , 等到下一个data_block的第一个key插入时才写入，目的是为了减少key空间
  BlockHandle pending_handle;  // Handle to add to index block，等待和 key 一起写入的 blockhandle
  std::string compressed_output; // 压缩后的data block，临时存储，写入后即被清空
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed); // 保证文件没有close, 即没有调用过 Finish / Abandon
  if (!ok()) return;  // status ok
  if (r->num_entries > 0) { // 如果有缓存的kv，保证新加入的key是最大的
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  // 直到遇到下一个data block 的第一对kv才添加 index block 
  // 目的是为了在index block中用更短的key
  if (r->pending_index_entry) {
    assert(r->data_block.empty());  
    // 找到最小sep字符,  r->last_key <= r->last_key < key
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    // offset size 在上一次 writeRawBlock 中设置
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    // index block 加入 last_key 到 data_block 的索引
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  // data block 过大，flush到文件中
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

// 1. 写入 data_block，设置 pending_hanlde 偏移，
// 2. 写入成功后，pending_index_entry 为true，以便根据下一个datablock 的 first key 调整 index entry key
void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    // 写入成功，flush文件
    r->pending_index_entry = true;  // 等待下一个data block的第一个key来决定 index entreis 索引
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    // 将data block在 sstable中的偏移加入到filter block中 创建一个新的filter
    r->filter_block->StartBlock(r->offset);
  }
}

// write block, set handle
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  // 完成block最后重启点的构建，返回一个序列化字符串的引用，
  // The returned slice will remain valid for the lifetime of this builder or until Reset() is called.
  Slice raw = block->Finish();

  // 根据配置参数决定是否压缩
  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        // 压缩率太低 < 12.5% , 不压缩
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  // 将data内容写入到文件，重置block为初始化状态，清空 compressed_output
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset(); // 写入后清空block，
}

// set BlockHandle offset and size, append block_contents and trailer to file
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  // pending handle index 设置datablock 的handle信息
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents); 
  if (r->status.ok()) {
    // 写入 1 btye type 和 4 byte crc32
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize)); // 写入 block trailer
    if (r->status.ok()) {
      // 更新 offset 为下一个 data block的写入偏移。
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();  // 写入最后一块data block
  assert(!r->closed);
  r->closed = true; // 设置关闭标志closed = true

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      // 添加filter.name 到filter data位置的映射
      // 当前leveldb 只有 filter meta block
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  if (ok()) {
    // 为最后一块data block设置index block，加入到index block 中
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
