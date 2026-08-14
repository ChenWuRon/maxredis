// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/state_machine/kv_state_machine.h"

#include <absl/strings/numbers.h>
#include <absl/time/clock.h>

#include <atomic>
#include <cstdio>
#include <string>
#include <vector>

#include "server/raft/command_encoder.h"
#include "server/raft/raft_types.h"
#include "server/raft/snapshot_writer.h"
#include "server/service/command_registry.h"
#include "server/service/state_serializer.h"
#include "server/storage/common_types.h"
#include "server/storage/db_slice.h"
#include "server/storage/engine_shard_set.h"
#include "util/proactor_pool.h"

namespace dfly {

using namespace std;
using namespace util;

KvStateMachine::KvStateMachine(EngineShardSet* shard_set, ProactorPool* pp)
    : shard_set_(shard_set), pp_(pp) {
}

ShardId KvStateMachine::Shard(string_view key) const {
  return dfly::Shard(key, shard_set_->size());
}

ApplyResult KvStateMachine::Apply(const CommandId* cid, CmdArgList args) {
  string_view name = cid->name();
  if (name == "SET") {
    Set(0, ArgS(args, 1), ArgS(args, 2));
    return {ApplyOp::OK, 1};
  }
  if (name == "DEL") {
    bool deleted = Del(0, ArgS(args, 1));
    return {ApplyOp::OK, deleted ? 1u : 0u};
  }
  return {ApplyOp::ERROR, 0};
}

void KvStateMachine::Set(DbIndex db_ind, std::string_view key, std::string_view val) {
  if (barrier_)
    barrier_->BeginRead();
  ShardId sid = Shard(key);
  shard_set_->Await(sid, [db_ind, key, val] {
    EngineShard* es = EngineShard::tlocal();
    auto [it, res] = es->db_slice.AddOrFind(db_ind, key);
    it->second.value = val;
  });
  if (barrier_)
    barrier_->EndRead();
}

bool KvStateMachine::Del(DbIndex db_ind, std::string_view key) {
  if (barrier_)
    barrier_->BeginRead();
  ShardId sid = Shard(key);
  bool result = shard_set_->Await(sid, [db_ind, key] {
    EngineShard* es = EngineShard::tlocal();
    return es->db_slice.Del(db_ind, key);
  });
  if (barrier_)
    barrier_->EndRead();
  return result;
}

bool KvStateMachine::Expire(DbIndex db_ind, std::string_view key, uint64_t expire_at_ms) {
  if (barrier_)
    barrier_->BeginRead();
  ShardId sid = Shard(key);
  bool result = shard_set_->Await(sid, [db_ind, key, expire_at_ms] {
    EngineShard* es = EngineShard::tlocal();
    return es->db_slice.SetExpire(db_ind, key, expire_at_ms) == OpStatus::OK;
  });
  if (barrier_)
    barrier_->EndRead();
  return result;
}

bool KvStateMachine::ApplySet(DbIndex db_ind, std::string_view key, std::string_view val,
                              bool nx, bool xx, uint64_t expire_at_ms) {
  if (barrier_)
    barrier_->BeginRead();
  ShardId sid = Shard(key);
  bool applied = shard_set_->Await(sid, [db_ind, key, val, nx, xx, expire_at_ms] {
    EngineShard* es = EngineShard::tlocal();
    auto [it, inserted] = es->db_slice.AddOrFind(db_ind, key);
    if (nx && !inserted)
      return false;  // key exists → NX fails
    if (xx && inserted)
      return false;  // key missing → XX fails
    it->second.value = val;
    if (expire_at_ms > 0)
      es->db_slice.SetExpire(db_ind, key, expire_at_ms);
    return true;
  });
  if (barrier_)
    barrier_->EndRead();
  return applied;
}

OpResult<string> KvStateMachine::Get(DbIndex db_ind, std::string_view key,
                                    ReadConsistency consistency) {
  (void)consistency;
  ShardId sid = Shard(key);
  return shard_set_->Await(sid, [db_ind, key]() -> OpResult<string> {
    EngineShard* es = EngineShard::tlocal();
    OpResult<MainIterator> res = es->db_slice.Find(db_ind, key);
    if (res) {
      return res.value()->second.value;
    }
    return res.status();
  });
}

size_t KvStateMachine::DbSize(DbIndex db_ind) const {
  atomic_ulong total{0};
  shard_set_->RunBriefInParallel([db_ind, &total](EngineShard* es) {
    total += es->db_slice.DbSize(db_ind);
  });
  return total.load();
}

ApplyResult KvStateMachine::ApplyLogEntry(const LogEntry& entry) {
  // Parse the replicated command. Primary format is RESP (from
  // ReplicatedCommand::Serialize); the legacy space-joined format is still
  // accepted for backward-compatible tests.
  std::vector<std::string> args;
  if (ParseRespArray(entry.command, &args) && args.empty()) {
    return {ApplyOp::ERROR, 0};  // malformed RESP
  }
  if (args.empty()) {
    // Legacy fallback: space-split.
    std::string_view cmd = entry.command;
    auto split = [&](std::string_view s) {
      size_t pos = 0;
      while (pos < s.size()) {
        size_t end = s.find(' ', pos);
        if (end == std::string_view::npos) {
          args.emplace_back(s.substr(pos));
          break;
        }
        args.emplace_back(s.substr(pos, end - pos));
        pos = end + 1;
      }
    };
    split(cmd);
  }

  if (args.empty())
    return {ApplyOp::ERROR, 0};

  std::string_view name = args[0];
  if (name == "SET") {
    // SET key value [NX|XX] [EX seconds|PX milliseconds]
    if (args.size() < 3)
      return {ApplyOp::ERROR, 0};
    std::string_view key = args[1];
    std::string_view val = args[2];

    bool nx = false, xx = false;
    uint64_t expire_at_ms = 0;
    for (size_t i = 3; i < args.size(); i++) {
      std::string_view opt = args[i];
      if (opt == "NX") {
        nx = true;
      } else if (opt == "XX") {
        xx = true;
      } else if ((opt == "EX" || opt == "PX") && i + 1 < args.size()) {
        int64_t ttl;
        if (!absl::SimpleAtoi(args[i + 1], &ttl) || ttl < 0)
          return {ApplyOp::ERROR, 0};
        uint64_t ttl_ms = (opt == "EX") ? static_cast<uint64_t>(ttl) * 1000
                                        : static_cast<uint64_t>(ttl);
        expire_at_ms = NowMs() + ttl_ms;
        i++;
      }
    }
    bool applied = ApplySet(0, key, val, nx, xx, expire_at_ms);
    return {ApplyOp::OK, applied ? 1u : 0u};
  }
  if (name == "DEL") {
    // DEL key...
    if (args.size() < 2)
      return {ApplyOp::ERROR, 0};
    uint32_t deleted = 0;
    for (size_t i = 1; i < args.size(); i++) {
      if (Del(0, args[i]))
        deleted++;
    }
    return {ApplyOp::OK, deleted};
  }
  if (name == "EXPIRE") {
    // EXPIRE key seconds
    if (args.size() < 3)
      return {ApplyOp::ERROR, 0};
    std::string_view key = args[1];
    int64_t seconds = 0;
    if (!absl::SimpleAtoi(args[2], &seconds) || seconds < 0)
      return {ApplyOp::ERROR, 0};
    uint64_t expire_at_ms = NowMs() + static_cast<uint64_t>(seconds) * 1000;
    bool found = Expire(0, key, expire_at_ms);
    return {ApplyOp::OK, found ? 1u : 0u};
  }
  return {ApplyOp::ERROR, 0};
}

bool KvStateMachine::SaveSnapshot(const std::string& path) {
  // Collect entries from all shards in parallel.
  size_t shard_count = shard_set_->size();
  std::vector<SnapshotData> shard_data(shard_count);

  shard_set_->RunBriefInParallel([&](EngineShard* es) {
    ShardId sid = es->shard_id();
    shard_data[sid] = StateSerializer::Export(es->db_slice);
  });

  // Merge all shard snapshots.
  size_t total = 0;
  for (const auto& sd : shard_data)
    total += sd.entries.size();

  SnapshotWriter writer(path);
  if (!writer.Open())
    return false;

  for (auto& sd : shard_data) {
    for (auto& e : sd.entries) {
      SnapshotRecord record;
      record.key = std::move(e.key);
      record.value = std::move(e.value);
      record.expire_at = e.expire_ms;
      if (!writer.Add(record))
        return false;
    }
  }

  return writer.Finalize(total);
}

bool KvStateMachine::LoadSnapshot(const std::string& path) {
  FILE* fp = fopen(path.c_str(), "rb");
  if (!fp)
    return false;

  uint32_t magic;
  if (fread(&magic, sizeof(magic), 1, fp) != 1 || magic != kSnapshotMagic) {
    fclose(fp);
    return false;
  }

  uint32_t num_records;
  if (fread(&num_records, sizeof(num_records), 1, fp) != 1) {
    fclose(fp);
    return false;
  }

  uint64_t now = NowMs();
  uint32_t loaded = 0;

  for (uint32_t i = 0; i < num_records; i++) {
    uint32_t key_len, value_len;
    uint64_t expire_at;

    if (fread(&key_len, sizeof(key_len), 1, fp) != 1) break;
    if (fread(&value_len, sizeof(value_len), 1, fp) != 1) break;
    if (fread(&expire_at, sizeof(expire_at), 1, fp) != 1) break;

    std::string key(key_len, '\0');
    if (key_len > 0 && fread(key.data(), 1, key_len, fp) != key_len) break;

    std::string value(value_len, '\0');
    if (value_len > 0 && fread(value.data(), 1, value_len, fp) != value_len) break;

    if (expire_at > 0 && expire_at < now)
      continue;

    ShardId sid = Shard(key);
    shard_set_->Await(sid, [db_ind = 0, key = std::move(key), value = std::move(value), expire_at] {
      EngineShard* es = EngineShard::tlocal();
      auto [it, inserted] = es->db_slice.AddOrFind(0, key);
      it->second.value = value;
      if (expire_at > 0)
        es->db_slice.SetExpire(0, key, expire_at);
    });

    loaded++;
  }

  fclose(fp);

  return loaded == num_records;
}

void KvStateMachine::Schedule(DbIndex db_ind, std::string_view key,
                               std::function<void(EngineShard*)> cb) {
  ShardId sid = Shard(key);
  shard_set_->Add(sid, [cb = std::move(cb)] {
    EngineShard* es = EngineShard::tlocal();
    cb(es);
  });
}

}  // namespace dfly
