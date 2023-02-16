// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

namespace log {
class Writer;
}

class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key);

// 每次 compact 后的最新数据状态定义为 Version，也就是当前 db 元信息以及
// 每个 level 上最新的 sstable 集合
// compact会在某个level上新加入或删除一些 sstable 
// 但可能这个时候，那些要删除的sstable正在被读，为了处理这种竞争情况
// 基于 sstable 只读的特性，每个 Version 加入引用计数，读以及解除读操作
// 会将引用计数相应加减一。 
// 这样，db中有多个 Version 同时存在（提供服务），它们通过链表连接起来。
// 当 Version 引用计数为 0 并且不是当前最新的 Version 时，它会从链表中移除。
// 此时该 Version 中的 sstable 就可以删除了，再下一次 compact 时清理掉
class Version {
 public:
  struct GetStats {
    FileMetaData* seek_file;
    int seek_file_level;
  };

  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held
  Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
             GetStats* stats);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  bool UpdateStats(const GetStats& stats);

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  bool RecordReadSample(Slice key);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref();
  void Unref();

  void GetOverlappingInputs(
      int level,
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      std::vector<FileMetaData*>* inputs);

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==nullptr represents a key smaller than all the DB's keys.
  // largest_user_key==nullptr represents a key largest than all the DB's keys.
  bool OverlapInLevel(int level, const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  int NumFiles(int level) const { return files_[level].size(); }

  // Return a human readable string that describes this version's contents.
  std::string DebugString() const;

 private:
  friend class Compaction;
  friend class VersionSet;

  class LevelFileNumIterator;

  explicit Version(VersionSet* vset)
      : vset_(vset),
        next_(this),
        prev_(this),
        refs_(0),
        file_to_compact_(nullptr),
        file_to_compact_level_(-1),
        compaction_score_(-1),
        compaction_level_(-1) {}

  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;

  ~Version();

  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

  // Call func(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                          bool (*func)(void*, int, FileMetaData*));

  VersionSet* vset_;  // VersionSet to which this Version belongs
  Version* next_;     // Next version in linked list
  Version* prev_;     // Previous version in linked list
  int refs_;          // Number of live refs to this version

  // List of files per level
  // 每个level 的所有 sstable 元信息
  // files_[i] 中的 FileMetaData 按照FileMetaData::smallest 排序
  // 这是在每次更新都保证的 （ VersionSet::Builder::Save() ）
  std::vector<FileMetaData*> files_[config::kNumLevels];

  // Next file to compact based on seek stats.
  FileMetaData* file_to_compact_;
  int file_to_compact_level_;

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  double compaction_score_;
  int compaction_level_;
};

/*

compaction_score_

leveldb 中分 level 管理 sstable，对于写，可以认为与 sstable 无关。

基于 get 的流程，各 level 中的 sstable 的 count，size 以及 range 分布，会直接影响读的效率。可以
预想的最佳情形可能是 level-0 中最多有一个 sstable，level-1 以及之上的各 level 中 key-
range 分布均匀，期望更多的查找可以遍历最少的 level 即可定位到。

将这种预想的最佳状态定义成: level 处于均衡的状态。当采用具体的参数量化，也就量化了各个
level 的不均衡比重，即 compact 权重： score。score 越大，表示该 level 越不均衡，需要更优
先进行 compact。

每个 level 的具体均衡参数及比重计算策略如下：

因为 level-0 的 sstable range 可能 overlap，所以如果 level-0 上有过多的 sstable，在做查
找时，会严重影响效率。同时，因为 level-0 中的 sstable 由 memtable 直接 dump 得到，并不受
kTargetFileSize（生成 sstable 的 size）的控制，所以 sstable 的 count 更有意义。
基于此，对于 level-0，均衡的状态需要满足：sstable 的 count < kL0_CompactionTrigger。
score = sstable 的 count/ kL0_CompactionTrigger。
为了控制这个数量， 另外还有 kL0_SlowdownWritesTrigger/kL0_StopWritesTrigger 两个阈值
来主动控制写的速率（参见 put 流程）。

对于 level-1 及以上的 level，sstable 均由 compact 过程产生，生成的 sstable 大小被
kTargetFileSize 控 制 ， 所 以 可 以 限 定 sstable 总 的 size 。 当 前 的 策 略 是 设 置 初 始 值
kBaseLevelSize，然后以 10 的指数级按 level 增长。每个 level 可以容纳的 quota_size =
kBaseLevelSize * 10^(level_number-1) 。
基于此，对于 level-1 及以上的 level
均衡的状态需要满足：sstable 的 size < quota_size。
score = sstable 的 size / quota_size。
每次 compact 完成，生效新的 Version 时（VersionSet::Finalize()），都会根据上述的策略，
计算出每个 level 的 score,取最大值作为当前 Version 的 compaction_score_,同时记录对应的
level(compaction_level_)。

file_to_compact_ ！！！！

leveldb 对单个 sstable 文件的 IO 也做了细化的优化，设计了一个巧妙的策略。
首先，一次查找如果对多于一个 sstable 进行了查找（对 sstable 进行了查找可以认为对其中的
datablock 进行了一次寻道 seek），说明处于低 level 上的 sstable 并没有提供高的 hit 比率，可
以认为它处在不最优的情况，而我们认为 compact 后会倾向于均衡的状态，所以在一个 sstable 的
seek 次数达到一定阈值后，主动对其进行 compact 是合理的。

这个具体 seek 次数阈值(allowed_seeks)的确定，依赖于 sas 盘的 IO 性能：
a. 一次磁盘寻道 seek 耗费 10ms。
b. 读或者写 1M 数据耗费 10ms （按 100M/s IO 吞吐能力）。
c. compact 1M 的数据需要 25M 的 IO：从 level-n 中读 1M 数据，从 level-n+1 中读 10～12M 数据，
写入 level-n+1 中 10～12M 数据。

所以，compact 1M 的数据的时间相当于做 25 次磁盘 seek，1 次 seek 相当于 compact 40k 数据。
那么，可以得到 seek 阈值 allowed_seeks=sstable_size / 40k。保守设置，
当前实际的 allowed_seeks = sstable_size / 16k。每次 compact 完成，构造新的 Version 时
（Builder::Apply()）,每个 sstable 的 allowed_seeks 会计算出来保存在 FileMetaData。
在每次 get 操作的时候，如果有超过一个 sstable 文件进行了查找，会将第一个进行查找的
sstable 的 allowed_seeks 减一，并检查其是否已经用光了 allowed_seeks,若是，则将该 sstable
记录成当前 Version 的 file_to_compact_,并记录其所在的 level(file_to_compact_level_)。
*/

/*
VersionSet 管理了 db 当前的状态。
包括了当前最新的 Versino 以及 其它正在服务的 Version 链表
全局的 SequnenceNumber, FileNumber; 当前的manifest_file_number；
封装 sstable 的 TableCache。每个 level 下一次 compact 要选取的 start_key
*/
class VersionSet {
 public:
  VersionSet(const std::string& dbname, const Options* options,
             TableCache* table_cache, const InternalKeyComparator*);
  VersionSet(const VersionSet&) = delete;
  VersionSet& operator=(const VersionSet&) = delete;

  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

  // Recover the last saved descriptor from persistent storage.
  Status Recover(bool* save_manifest);

  // Return the current version.
  Version* current() const { return current_; }

  // Return the current manifest file number
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  int NumLevelFiles(int level) const;

  // Return the combined file size of all files at the specified level.
  int64_t NumLevelBytes(int level) const;

  // Return the last sequence number.
  uint64_t LastSequence() const { return last_sequence_; }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Pick level and inputs for a new compaction.
  // Returns nullptr if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  Compaction* PickCompaction();

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns nullptr if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  Compaction* CompactRange(int level, const InternalKey* begin,
                           const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);

  // Returns true iff some level needs a compaction.
  bool NeedsCompaction() const {
    Version* v = current_;
    return (v->compaction_score_ >= 1) || (v->file_to_compact_ != nullptr);
  }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  void AddLiveFiles(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;

 private:
  class Builder;

  friend class Compaction;
  friend class Version;

  bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

  void Finalize(Version* v);

  void GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest,
                InternalKey* largest);

  void GetRange2(const std::vector<FileMetaData*>& inputs1,
                 const std::vector<FileMetaData*>& inputs2,
                 InternalKey* smallest, InternalKey* largest);

  void SetupOtherInputs(Compaction* c);

  // Save current contents to *log
  Status WriteSnapshot(log::Writer* log);

  void AppendVersion(Version* v);

  Env* const env_;
  const std::string dbname_;
  const Options* const options_;
  TableCache* const table_cache_;
  const InternalKeyComparator icmp_;
  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  uint64_t last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  // Opened lazily
  WritableFile* descriptor_file_;  // manifest 文件的封装
  log::Writer* descriptor_log_;     //  manifest 文件的 writer
  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  Version* current_;        // == dummy_versions_.prev_

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  // 为了尽量均匀 compact 每个 level，所以会将这一次 compact 的 end-key 作为
  // 下一次 compact 的 start_key。
  std::string compact_pointer_[config::kNumLevels];
};


class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  bool IsBaseLevelForKey(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();

 private:
  friend class Version;
  friend class VersionSet;

  Compaction(const Options* options, int level);

  int level_;
  uint64_t max_output_file_size_; // 生成sstable的最大size
  Version* input_version_;        // compact当时version
  VersionEdit edit_;              // compact过程中的操作

  // Each compaction reads inputs from "level_" and "level_+1"
  // inputs_[0]: 为 level-n sstable 文件信息
  // inputs_[1]: 为 level-n+1 sstable 文件信息
  std::vector<FileMetaData*> inputs_[2];  // The two sets of inputs

  // State used to check for number of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  // 位于 level-n+2，并且与 compact 的 key-range 有 overlap 的 sstable
  // 保存 grandparents_ 是因为 compact 最终会生成一系列 level-n+1 的sstable
  // 而如果生成的 sstable 与 level-n+2 中有过多 overlap 的话
  // 当 compact levle-n+1 时， 会产生过多 merge, 为了尽量避免这种情况，
  // compact 过程中需要检查与 level-n+2 中缠身 overlap 的 数量
  // 并与 kMaxGrandParentOverlapBytes 作比较，以便提前终止 compact
  std::vector<FileMetaData*> grandparents_;
  // compact 时 grandparents 中已经 overlap 的 index 
  size_t grandparent_index_;  // Index in grandparent_starts_
  bool seen_key_;             // Some output key has been seen
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).

  // compact 时， 当key的 ValueType 为 KTypeDeletion 时
  // 需要检查其在 level-n+1 以上是否存在 （ IsBaseLevelForKey() )
  // 来决定是否丢掉该key。 因为 compact 时， key 的遍历是顺序的
  // 所以每次从上一次检查结束的地方开始即可
  // 记录了上一次比较的 sstable 容器下标？？？ 
  size_t level_ptrs_[config::kNumLevels];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
