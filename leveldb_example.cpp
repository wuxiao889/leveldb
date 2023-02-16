#include <cassert>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/write_batch.h"

int main() {
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
  assert(status.ok());

  {
    std::string value;
    std::string key1 = "a";
    auto s = db->Put(leveldb::WriteOptions(), key1, "value1");
    if (s.ok()) s = db->Get(leveldb::ReadOptions(), key1, &value);
    if (s.ok()) s = db->Delete(leveldb::WriteOptions(), key1);
  }

  {
    // Note that if the process dies after the Put of key2 but before the delete
    // of key1, the same value may be left stored under multiple keys. Such
    // problems can be avoided by using the WriteBatch class to atomically apply
    // a set of updates:
    std::string value;
    std::string key1, key2;
    key1 = "b";
    key2 = "c";
    leveldb::Status s = db->Get(leveldb::ReadOptions(), key1, &value);
    if (s.ok()) {
      leveldb::WriteBatch batch;
      batch.Delete(key1);
      batch.Put(key2, value);
      s = db->Write(leveldb::WriteOptions(), &batch);
    }
    // The WriteBatch holds a sequence of edits to be made to the database, and
    // these edits within the batch are applied in order. Note that we called
    // Delete before Put so that if key1 is identical to key2, we do not end up
    // erroneously dropping the value entirely. Apart from its atomicity
    // benefits, WriteBatch may also be used to speed up bulk updates by placing
    // lots of individual mutations into the same batch.
  }

  {
    // By default, each write to leveldb is asynchronous: it returns after
    // pushing the write from the process into the operating system. The
    // transfer from operating system memory to the underlying persistent
    // storage happens asynchronously. The sync flag can be turned on for a
    // particular write to make the write operation not return until the data
    // being written has been pushed all the way to persistent storage. (On
    // Posix systems, this is implemented by calling either fsync(...) or
    // fdatasync(...) or msync(..., MS_SYNC) before the write operation
    // returns.)
    leveldb::WriteOptions w;
    w.sync = true;
    // Asynchronous writes are often more than a thousand times as fast as
    // synchronous writes. The downside of asynchronous writes is that a crash
    // of the machine may cause the last few updates to be lost. Note that a
    // crash of just the writing process (i.e., not a reboot) will not cause any
    // loss since even when sync is false, an update is pushed from the process
    // memory into the operating system before it is considered done.

    // Asynchronous writes can often be used safely. For example, when loading a
    // large amount of data into the database you can handle lost updates by
    // restarting the bulk load after a crash. A hybrid scheme is also possible
    // where every Nth write is synchronous, and in the event of a crash, the
    // bulk load is restarted just after the last synchronous write finished by
    // the previous run. (The synchronous write can update a marker that
    // describes where to restart on a crash.)

    // WriteBatch provides an alternative to asynchronous writes. Multiple
    // updates may be placed in the same WriteBatch and applied together using a
    // synchronous write (i.e., write_options.sync is set to true). The extra
    // cost of the synchronous write will be amortized across all of the writes
    // in the batch.
  }

  {
    // Snapshots provide consistent read-only views over the entire state of the
    // key-value store. ReadOptions::snapshot may be non-NULL to indicate that a
    // read should operate on a particular version of the DB state. If
    // ReadOptions::snapshot is NULL, the read will operate on an implicit
    // snapshot of the current state.
    leveldb::ReadOptions options;
    options.snapshot = db->GetSnapshot();
    // ... apply some updates to db... leveldb::Iterator* iter =
    db->NewIterator(options);
    // ... read using iter to view the state when the snapshot was created delete iter;
    db->ReleaseSnapshot(options.snapshot);
    // Note that when a snapshot is no longer needed, it should be released
    // using the DB::ReleaseSnapshot interface. This allows the implementation
    // to get rid of state that was being maintained just to support reading as
    // of that snapshot.
  }



  delete db;
}
