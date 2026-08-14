#include "server/raft/raft_group.h"

#include <sys/stat.h>

#include <filesystem>
#include <string>

#include "base/logging.h"
#include "server/raft/segment_log_storage.h"
#include "server/raft/snapshot_manager.h"

namespace dfly {

RaftGroup::RaftGroup(GroupId group_id)
    : group_id_(group_id),
      log_storage_(std::make_unique<CommandLog>()),
      state_machine_(nullptr) {
  node_.SetLogStorage(log_storage_.get());
}

RaftGroup::~RaftGroup() {
  Shutdown();
}

bool RaftGroup::InitStorage(const std::string& base_path, uint32_t fsync_interval_ms) {
  // Build paths: base_path/raft/group_N/wal/ and base_path/raft/group_N/snapshot/
  std::string group_dir = base_path;
  if (!group_dir.empty() && group_dir.back() != '/')
    group_dir += '/';
  group_dir += "raft/group_" + std::to_string(group_id_) + "/";

  wal_dir_ = group_dir + "wal/";
  snapshot_dir_ = group_dir + "snapshot/";

  // Create the full directory chain (base_path/raft/group_N/{wal,snapshot}).
  std::error_code ec;
  std::filesystem::create_directories(wal_dir_, ec);
  if (ec) {
    LOG(ERROR) << "RaftGroup " << group_id_ << ": failed to create WAL dir " << wal_dir_
               << ": " << ec.message();
    return false;
  }
  std::filesystem::create_directories(snapshot_dir_, ec);

  // Replace the in-memory log with a persistent segmented WAL.
  auto seg_storage = std::make_unique<SegmentLogStorage>(wal_dir_);
  if (!seg_storage->Open()) {
    LOG(ERROR) << "RaftGroup " << group_id_ << ": failed to open WAL in " << wal_dir_;
    return false;
  }
  log_storage_ = std::move(seg_storage);
  node_.SetLogStorage(log_storage_.get());

  // Batch-fsync policy: with fsync_interval_ms > 0, Appends write through to
  // the page cache (kill -9 safe) and a background fiber fsyncs periodically.
  // This is the WAL analogue of Redis AOF "everysec" — the durability window
  // on power failure is bounded by the interval, at a fraction of the fsync
  // cost per write.
  if (fsync_interval_ms > 0) {
    auto* seg = static_cast<SegmentLogStorage*>(log_storage_.get());
    seg->SetFsyncPerAppend(false);
    uint32_t interval_ms = fsync_interval_ms;
    if (!wal_flush_fiber_.IsJoinable()) {
      wal_flush_fiber_ = util::fb2::Fiber("raft_wal_flusher", [this, interval_ms] {
        while (!wal_flush_shutdown_.load(std::memory_order_acquire)) {
          // Sleep in small steps so shutdown stays responsive.
          uint32_t slept = 0;
          while (slept < interval_ms && !wal_flush_shutdown_.load(std::memory_order_acquire)) {
            util::ThisFiber::SleepFor(std::chrono::milliseconds(10));
            slept += 10;
          }
          if (wal_flush_shutdown_.load(std::memory_order_acquire))
            break;
          log_storage_->Flush();
        }
      });
      LOG(INFO) << "RaftGroup " << group_id_ << ": batch fsync every "
                << interval_ms << "ms";
    }
  }

  // Set up the RaftNode storage path (meta.json, apply.meta). This loads
  // any existing snapshot + hard state (term/voted_for/apply progress).
  // NOTE: state_machine_ must be set before this call (RaftEngine does it).
  node_.SetStoragePath(group_dir);

  node_.SetSnapshotDir(snapshot_dir_);

  // Create snapshot manager that owns the snapshot cycle.
  snapshot_manager_ = std::make_unique<RaftSnapshotManager>(
      snapshot_dir_, state_machine_.get(), log_storage_.get());
  node_.SetSnapshotManager(snapshot_manager_.get());

  // Start the snapshot driver: runs on the consensus thread (node lock),
  // so WAL compaction stays serialized with appends.
  if (!snapshot_fiber_.IsJoinable() && state_machine_) {
    snapshot_fiber_ = util::fb2::Fiber("raft_snapshot_driver", [this] {
      while (!snapshot_shutdown_.load(std::memory_order_acquire)) {
        node_.CreateSnapshotIfNeeded();
        for (int i = 0; i < 1000 && !snapshot_shutdown_.load(std::memory_order_acquire); i++) {
          util::ThisFiber::SleepFor(std::chrono::milliseconds(1));
        }
      }
    });
  }

  VLOG(1) << "RaftGroup " << group_id_ << " storage initialized: wal=" << wal_dir_
          << " snapshot=" << snapshot_dir_;
  return true;
}

void RaftGroup::Shutdown() {
  // Graceful stop order:
  //   1. Stop the snapshot driver and the WAL flusher (no more background
  //      fsyncs racing with shutdown).
  //   2. Stop ALL node background activity (heartbeat fiber + election timer).
  //   3. Final WAL flush so every acknowledged write is durable on disk.
  snapshot_shutdown_.store(true, std::memory_order_release);
  wal_flush_shutdown_.store(true, std::memory_order_release);
  if (snapshot_fiber_.IsJoinable())
    snapshot_fiber_.Join();
  if (wal_flush_fiber_.IsJoinable())
    wal_flush_fiber_.Join();
  if (snapshot_manager_) {
    snapshot_manager_->Stop();
  }
  node_.Shutdown();
  if (log_storage_) {
    log_storage_->Flush();
  }
}

}  // namespace dfly
