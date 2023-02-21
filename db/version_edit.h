// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

struct FileMetaData {
  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) {}

  int refs;
  int allowed_seeks;      // Seeks allowed until compaction，如果某个文件被seek了太多次，说明需要压缩
  uint64_t number;        // 文件序列号
  uint64_t file_size;     // File size in bytes
  InternalKey smallest;   // Smallest internal key served by table
  InternalKey largest;    // Largest internal key served by table
};

// A Compaction encapsulates information about a compaction.
// compact过程中有一系列改变当前 Version 的操作
// (FileNumber增加，删除input sstable，增加输出的 sstable...)
// 为了缩小 Version 切换的时间点，将这些操作封装成 VersionEdit 
// compact 完成时，将 VersionEdit 中的操作一次应用到当前 Version
class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() = default;

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  void RemoveFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;

  std::string comparator_;    // 用 comparator 名字保证排序逻辑
  uint64_t log_number_;       // 日志文件序号，与 memtable 一一对应。当一个 memtable 生成为 sstable 后会将旧的日志文件删除
                              // 生成一个新的日志文件。日志文件形如 [\d]+.log
  uint64_t prev_log_number_;
  uint64_t next_file_number_;   // 下一个文件序号。leveldb中文件包括日志文件、sstable文件、manifest文件。
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  // 指示每个层级下一次进行 compaction 操作时需要从哪个键开始
  // 对于每个层级 L，会记录上次进行 compact 时的最大键
  // L层下一次进行 compact 选取文件时，选取所有 最小键大于记录的最大键，
  // 即每一次的 compact 都会在该层空间循环执行
  std::vector<std::pair<int, InternalKey>> compact_pointers_; 
  DeletedFileSet deleted_files_;    // 记录每个层级只从 compact 之后要删除的文件
  std::vector<std::pair<int, FileMetaData>> new_files_; // compact output
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
