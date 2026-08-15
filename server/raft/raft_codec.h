// Copyright 2026, The MaxRedis Authors.
// See LICENSE for licensing terms.
//
// Binary wire codec for Raft RPCs over TCP.
//
// Every message is wrapped in a frame:
//
//   +----------------+------+-------+----------------+---------+------------+
//   | magic  "RF01"  | type |  seq  | payload_len LE | payload | crc32c LE  |
//   |     (4 bytes)  | (1B) | (4B)  |    (4 bytes)   |         |  (4 bytes) |
//   +----------------+------+-------+----------------+---------+------------+
//
// seq is a per-connection request sequence number (client-assigned, server-
// echoed). It lets the client REUSE a pooled connection after an RPC timeout:
// a late response to the timed-out request carries a stale seq and is skipped
// until the frame matching the current request arrives. Without seq, a timed-
// out connection must be closed and recreated (the late response would be
// misread as the new response) — and socket churn races helio's epoll arm
// slots (a freed slot reused by a new socket can receive the old fd's event,
// dereferencing a stale PendingReq on a recycled fiber stack).
//
// CRC32C covers magic + type + seq + payload_len + payload. A frame whose
// CRC does not match is rejected (drop the connection) — same corruption
// guarantee the segmented WAL provides.
//
// All multi-byte integers are little-endian. Strings are length-prefixed
// (uint32 LE). Vectors are count-prefixed (uint32 LE).

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "server/raft/append_entries_rpc.h"
#include "server/raft/heartbeat_rpc.h"
#include "server/raft/install_snapshot_rpc.h"
#include "server/raft/read_index_rpc.h"
#include "server/raft/timeout_now_rpc.h"
#include "server/raft/vote_rpc.h"

namespace dfly {

// RPC message types carried in a frame.
enum class RpcType : uint8_t {
  kAppendEntriesReq = 1,
  kAppendEntriesRsp = 2,
  kVoteReq = 3,
  kVoteRsp = 4,
  kHeartbeatReq = 5,
  kHeartbeatRsp = 6,
  kInstallSnapshotReq = 7,
  kInstallSnapshotRsp = 8,
  kReadIndexReq = 9,
  kReadIndexRsp = 10,
  kTimeoutNowReq = 11,
  kTimeoutNowRsp = 12,
};

inline constexpr char kRpcFrameMagic[] = "RF01";
inline constexpr size_t kRpcFrameHeaderSize = 4 + 1 + 4 + 4;  // magic + type + seq + len
inline constexpr size_t kRpcFrameCrcSize = 4;

// --- primitive (de)serialization helpers --------------------------------

// Appends little-endian primitives / length-prefixed strings to |out|.
class RaftEncoder {
 public:
  void U8(uint8_t v);
  void U32(uint32_t v);
  void U64(uint64_t v);
  void Bool(bool v);
  void Str(std::string_view s);
  void Bytes(std::string_view s) {
    Str(s);
  }

  const std::string& data() const {
    return buf_;
  }

  std::string Take() {
    return std::move(buf_);
  }

 private:
  std::string buf_;
};

// Bounds-checked reader over a serialized payload. All Read* methods return
// false on truncated/malformed input, leaving *out untouched on failure.
class RaftDecoder {
 public:
  explicit RaftDecoder(std::string_view data) : data_(data) {
  }

  bool U8(uint8_t* out);
  bool U32(uint32_t* out);
  bool U64(uint64_t* out);
  bool Bool(bool* out);
  bool Str(std::string_view* out);
  bool Str(std::string* out);

  // True if the whole buffer was consumed.
  bool AtEnd() const {
    return pos_ == data_.size();
  }

 private:
  bool Take(size_t n, std::string_view* out);

  std::string_view data_;
  size_t pos_ = 0;
};

// --- frame (de)serialization --------------------------------------------

// Encodes |payload| into a complete CRC-protected frame for |type| with the
// given request sequence number |seq|.
std::string EncodeRpcFrame(RpcType type, uint32_t seq, const std::string& payload);

// Parses a frame from |frame|. Verifies magic + CRC; returns false on any
// corruption or truncation. On success, *type, *seq and *payload are set.
bool ParseRpcFrame(std::string_view frame, RpcType* type, uint32_t* seq,
                   std::string_view* payload);

// --- payload (de)serialization ------------------------------------------

std::string SerializeAppendEntriesRequest(const AppendEntriesRequest& req);
bool ParseAppendEntriesRequest(std::string_view payload, AppendEntriesRequest* req);
std::string SerializeAppendEntriesResponse(const AppendEntriesResponse& rsp);
bool ParseAppendEntriesResponse(std::string_view payload, AppendEntriesResponse* rsp);

std::string SerializeVoteRequest(const VoteRequest& req);
bool ParseVoteRequest(std::string_view payload, VoteRequest* req);
std::string SerializeVoteResponse(const VoteResponse& rsp);
bool ParseVoteResponse(std::string_view payload, VoteResponse* rsp);

std::string SerializeHeartbeatRequest(const HeartbeatRequest& req);
bool ParseHeartbeatRequest(std::string_view payload, HeartbeatRequest* req);
std::string SerializeHeartbeatResponse(const HeartbeatResponse& rsp);
bool ParseHeartbeatResponse(std::string_view payload, HeartbeatResponse* rsp);

std::string SerializeInstallSnapshotRequest(const InstallSnapshotRequest& req);
bool ParseInstallSnapshotRequest(std::string_view payload, InstallSnapshotRequest* req);
std::string SerializeInstallSnapshotResponse(const InstallSnapshotResponse& rsp);
bool ParseInstallSnapshotResponse(std::string_view payload, InstallSnapshotResponse* rsp);

std::string SerializeReadIndexRequest(const ReadIndexRequest& req);
bool ParseReadIndexRequest(std::string_view payload, ReadIndexRequest* req);
std::string SerializeReadIndexResponse(const ReadIndexResponse& rsp);
bool ParseReadIndexResponse(std::string_view payload, ReadIndexResponse* rsp);

std::string SerializeTimeoutNowRequest(const TimeoutNowRequest& req);
bool ParseTimeoutNowRequest(std::string_view payload, TimeoutNowRequest* req);
std::string SerializeTimeoutNowResponse(const TimeoutNowResponse& rsp);
bool ParseTimeoutNowResponse(std::string_view payload, TimeoutNowResponse* rsp);

// Shared LogEntry encoding (AppendEntriesRequest::entries).
void SerializeLogEntry(RaftEncoder* enc, const LogEntry& e);
bool ParseLogEntry(RaftDecoder* dec, LogEntry* e);

}  // namespace dfly
