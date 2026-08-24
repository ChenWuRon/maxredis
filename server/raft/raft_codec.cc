// Copyright 2026, The MaxRedis Authors.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_codec.h"

#include <cstring>

#include "raft_rpc.pb.h"
#include "server/raft/crc32.h"

namespace dfly {

namespace pb = dfly::raft;

namespace {

void AppendU32LE(std::string* s, uint32_t v) {
  s->push_back(static_cast<char>(v & 0xFF));
  s->push_back(static_cast<char>((v >> 8) & 0xFF));
  s->push_back(static_cast<char>((v >> 16) & 0xFF));
  s->push_back(static_cast<char>((v >> 24) & 0xFF));
}

// Rejects empty payloads explicitly: protobuf would happily parse them as
// all-default messages, but a real RPC always carries at least one field.
// Also guards against size_t -> int truncation in ParseFromArray (frames are
// bounded by the TCP layer, but LocalTransport callers may pass anything).
bool ParseMessage(std::string_view payload, google::protobuf::MessageLite* msg) {
  if (payload.empty() || payload.size() > static_cast<size_t>(INT32_MAX))
    return false;
  return msg->ParseFromArray(payload.data(), static_cast<int>(payload.size()));
}

}  // namespace

// --- frames -------------------------------------------------------------

std::string EncodeRpcFrame(RpcType type, uint32_t seq, const std::string& payload) {
  std::string frame;
  frame.reserve(kRpcFrameHeaderSize + payload.size() + kRpcFrameCrcSize);
  frame.append(kRpcFrameMagic, 4);
  frame.push_back(static_cast<char>(type));
  AppendU32LE(&frame, seq);
  AppendU32LE(&frame, static_cast<uint32_t>(payload.size()));
  frame.append(payload);

  uint32_t crc = ComputeCrc32(frame.data(), frame.size());
  AppendU32LE(&frame, crc);
  return frame;
}

bool ParseRpcFrame(std::string_view frame, RpcType* type, uint32_t* seq,
                   std::string_view* payload) {
  if (frame.size() < kRpcFrameHeaderSize + kRpcFrameCrcSize)
    return false;
  if (memcmp(frame.data(), kRpcFrameMagic, 4) != 0)
    return false;

  std::string_view seq_chunk = frame.substr(5, 4);
  uint32_t seq_val = static_cast<uint32_t>(static_cast<uint8_t>(seq_chunk[0])) |
                     (static_cast<uint32_t>(static_cast<uint8_t>(seq_chunk[1])) << 8) |
                     (static_cast<uint32_t>(static_cast<uint8_t>(seq_chunk[2])) << 16) |
                     (static_cast<uint32_t>(static_cast<uint8_t>(seq_chunk[3])) << 24);

  std::string_view len_chunk = frame.substr(9, 4);
  uint32_t len = static_cast<uint32_t>(static_cast<uint8_t>(len_chunk[0])) |
                 (static_cast<uint32_t>(static_cast<uint8_t>(len_chunk[1])) << 8) |
                 (static_cast<uint32_t>(static_cast<uint8_t>(len_chunk[2])) << 16) |
                 (static_cast<uint32_t>(static_cast<uint8_t>(len_chunk[3])) << 24);
  if (len > frame.size() - kRpcFrameHeaderSize - kRpcFrameCrcSize)
    return false;

  std::string_view body = frame.substr(0, kRpcFrameHeaderSize + len);
  std::string_view crc_chunk = frame.substr(kRpcFrameHeaderSize + len, 4);
  uint32_t expected = static_cast<uint32_t>(static_cast<uint8_t>(crc_chunk[0])) |
                      (static_cast<uint32_t>(static_cast<uint8_t>(crc_chunk[1])) << 8) |
                      (static_cast<uint32_t>(static_cast<uint8_t>(crc_chunk[2])) << 16) |
                      (static_cast<uint32_t>(static_cast<uint8_t>(crc_chunk[3])) << 24);

  if (ComputeCrc32(body.data(), body.size()) != expected)
    return false;

  *type = static_cast<RpcType>(static_cast<uint8_t>(frame[4]));
  *seq = seq_val;
  *payload = frame.substr(kRpcFrameHeaderSize, len);
  return true;
}

// --- AppendEntries ------------------------------------------------------

std::string SerializeAppendEntriesRequest(const AppendEntriesRequest& req) {
  pb::AppendEntriesRequest msg;
  msg.set_group_id(req.group_id);
  msg.set_term(req.term);
  msg.set_leader_id(req.leader_id);
  msg.set_prev_log_index(req.prev_log_index);
  msg.set_prev_log_term(req.prev_log_term);
  for (const LogEntry& e : req.entries) {
    pb::LogEntry* pb_entry = msg.add_entries();
    pb_entry->set_term(e.term);
    pb_entry->set_index(e.index);
    pb_entry->set_command(e.command);
  }
  msg.set_leader_commit(req.leader_commit);
  std::string out;
  msg.SerializeToString(&out);
  return out;
}

bool ParseAppendEntriesRequest(std::string_view payload, AppendEntriesRequest* req) {
  pb::AppendEntriesRequest msg;
  if (!ParseMessage(payload, &msg))
    return false;
  req->group_id = msg.group_id();
  req->term = msg.term();
  req->leader_id = msg.leader_id();
  req->prev_log_index = msg.prev_log_index();
  req->prev_log_term = msg.prev_log_term();
  req->entries.resize(msg.entries_size());
  for (int i = 0; i < msg.entries_size(); ++i) {
    const pb::LogEntry& e = msg.entries(i);
    req->entries[i] = LogEntry(e.term(), e.index(), e.command());
  }
  req->leader_commit = msg.leader_commit();
  return true;
}

std::string SerializeAppendEntriesResponse(const AppendEntriesResponse& rsp) {
  pb::AppendEntriesResponse msg;
  msg.set_group_id(rsp.group_id);
  msg.set_term(rsp.term);
  msg.set_success(rsp.success);
  msg.set_last_log_index(rsp.last_log_index);
  std::string out;
  msg.SerializeToString(&out);
  return out;
}

bool ParseAppendEntriesResponse(std::string_view payload, AppendEntriesResponse* rsp) {
  pb::AppendEntriesResponse msg;
  if (!ParseMessage(payload, &msg))
    return false;
  rsp->group_id = msg.group_id();
  rsp->term = msg.term();
  rsp->success = msg.success();
  rsp->last_log_index = msg.last_log_index();
  return true;
}

// --- Vote ---------------------------------------------------------------

std::string SerializeVoteRequest(const VoteRequest& req) {
  pb::VoteRequest msg;
  msg.set_group_id(req.group_id);
  msg.set_term(req.term);
  msg.set_candidate_id(req.candidate_id);
  msg.set_last_log_index(req.last_log_index);
  msg.set_last_log_term(req.last_log_term);
  std::string out;
  msg.SerializeToString(&out);
  return out;
}

bool ParseVoteRequest(std::string_view payload, VoteRequest* req) {
  pb::VoteRequest msg;
  if (!ParseMessage(payload, &msg))
    return false;
  req->group_id = msg.group_id();
  req->term = msg.term();
  req->candidate_id = msg.candidate_id();
  req->last_log_index = msg.last_log_index();
  req->last_log_term = msg.last_log_term();
  return true;
}

std::string SerializeVoteResponse(const VoteResponse& rsp) {
  pb::VoteResponse msg;
  msg.set_group_id(rsp.group_id);
  msg.set_term(rsp.term);
  msg.set_vote_granted(rsp.vote_granted);
  std::string out;
  msg.SerializeToString(&out);
  return out;
}

bool ParseVoteResponse(std::string_view payload, VoteResponse* rsp) {
  pb::VoteResponse msg;
  if (!ParseMessage(payload, &msg))
    return false;
  rsp->group_id = msg.group_id();
  rsp->term = msg.term();
  rsp->vote_granted = msg.vote_granted();
  return true;
}

// --- Heartbeat ----------------------------------------------------------

std::string SerializeHeartbeatRequest(const HeartbeatRequest& req) {
  pb::HeartbeatRequest msg;
  msg.set_group_id(req.group_id);
  msg.set_term(req.term);
  msg.set_leader_id(req.leader_id);
  msg.set_leader_commit(req.leader_commit);
  std::string out;
  msg.SerializeToString(&out);
  return out;
}

bool ParseHeartbeatRequest(std::string_view payload, HeartbeatRequest* req) {
  pb::HeartbeatRequest msg;
  if (!ParseMessage(payload, &msg))
    return false;
  req->group_id = msg.group_id();
  req->term = msg.term();
  req->leader_id = msg.leader_id();
  req->leader_commit = msg.leader_commit();
  return true;
}

std::string SerializeHeartbeatResponse(const HeartbeatResponse& rsp) {
  pb::HeartbeatResponse msg;
  msg.set_group_id(rsp.group_id);
  msg.set_term(rsp.term);
  msg.set_success(rsp.success);
  msg.set_last_log_index(rsp.last_log_index);
  std::string out;
  msg.SerializeToString(&out);
  return out;
}

bool ParseHeartbeatResponse(std::string_view payload, HeartbeatResponse* rsp) {
  pb::HeartbeatResponse msg;
  if (!ParseMessage(payload, &msg))
    return false;
  rsp->group_id = msg.group_id();
  rsp->term = msg.term();
  rsp->success = msg.success();
  rsp->last_log_index = msg.last_log_index();
  return true;
}

// --- InstallSnapshot ----------------------------------------------------

std::string SerializeInstallSnapshotRequest(const InstallSnapshotRequest& req) {
  pb::InstallSnapshotRequest msg;
  msg.set_group_id(req.group_id);
  msg.set_term(req.term);
  msg.set_leader_id(req.leader_id);
  msg.set_last_included_index(req.last_included_index);
  msg.set_last_included_term(req.last_included_term);
  msg.set_offset(req.offset);
  msg.set_done(req.done);
  msg.set_data(req.data);
  std::string out;
  msg.SerializeToString(&out);
  return out;
}

bool ParseInstallSnapshotRequest(std::string_view payload, InstallSnapshotRequest* req) {
  pb::InstallSnapshotRequest msg;
  if (!ParseMessage(payload, &msg))
    return false;
  req->group_id = msg.group_id();
  req->term = msg.term();
  req->leader_id = msg.leader_id();
  req->last_included_index = msg.last_included_index();
  req->last_included_term = msg.last_included_term();
  req->offset = msg.offset();
  req->done = msg.done();
  req->data = msg.data();
  return true;
}

std::string SerializeInstallSnapshotResponse(const InstallSnapshotResponse& rsp) {
  pb::InstallSnapshotResponse msg;
  msg.set_group_id(rsp.group_id);
  msg.set_term(rsp.term);
  msg.set_success(rsp.success);
  std::string out;
  msg.SerializeToString(&out);
  return out;
}

bool ParseInstallSnapshotResponse(std::string_view payload, InstallSnapshotResponse* rsp) {
  pb::InstallSnapshotResponse msg;
  if (!ParseMessage(payload, &msg))
    return false;
  rsp->group_id = msg.group_id();
  rsp->term = msg.term();
  rsp->success = msg.success();
  return true;
}

// --- ReadIndex ----------------------------------------------------------

std::string SerializeReadIndexRequest(const ReadIndexRequest& req) {
  pb::ReadIndexRequest msg;
  msg.set_group_id(req.group_id);
  msg.set_term(req.term);
  msg.set_leader_id(req.leader_id);
  msg.set_request_id(req.request_id);
  std::string out;
  msg.SerializeToString(&out);
  return out;
}

bool ParseReadIndexRequest(std::string_view payload, ReadIndexRequest* req) {
  pb::ReadIndexRequest msg;
  if (!ParseMessage(payload, &msg))
    return false;
  req->group_id = msg.group_id();
  req->term = msg.term();
  req->leader_id = msg.leader_id();
  req->request_id = msg.request_id();
  return true;
}

std::string SerializeReadIndexResponse(const ReadIndexResponse& rsp) {
  pb::ReadIndexResponse msg;
  msg.set_group_id(rsp.group_id);
  msg.set_term(rsp.term);
  msg.set_success(rsp.success);
  msg.set_commit_index(rsp.commit_index);
  std::string out;
  msg.SerializeToString(&out);
  return out;
}

bool ParseReadIndexResponse(std::string_view payload, ReadIndexResponse* rsp) {
  pb::ReadIndexResponse msg;
  if (!ParseMessage(payload, &msg))
    return false;
  rsp->group_id = msg.group_id();
  rsp->term = msg.term();
  rsp->success = msg.success();
  rsp->commit_index = msg.commit_index();
  return true;
}

// --- TimeoutNow ---------------------------------------------------------

std::string SerializeTimeoutNowRequest(const TimeoutNowRequest& req) {
  pb::TimeoutNowRequest msg;
  msg.set_group_id(req.group_id);
  msg.set_term(req.term);
  msg.set_leader_id(req.leader_id);
  std::string out;
  msg.SerializeToString(&out);
  return out;
}

bool ParseTimeoutNowRequest(std::string_view payload, TimeoutNowRequest* req) {
  pb::TimeoutNowRequest msg;
  if (!ParseMessage(payload, &msg))
    return false;
  req->group_id = msg.group_id();
  req->term = msg.term();
  req->leader_id = msg.leader_id();
  return true;
}

std::string SerializeTimeoutNowResponse(const TimeoutNowResponse& rsp) {
  pb::TimeoutNowResponse msg;
  msg.set_group_id(rsp.group_id);
  msg.set_term(rsp.term);
  msg.set_accepted(rsp.accepted);
  std::string out;
  msg.SerializeToString(&out);
  return out;
}

bool ParseTimeoutNowResponse(std::string_view payload, TimeoutNowResponse* rsp) {
  pb::TimeoutNowResponse msg;
  if (!ParseMessage(payload, &msg))
    return false;
  rsp->group_id = msg.group_id();
  rsp->term = msg.term();
  rsp->accepted = msg.accepted();
  return true;
}

}  // namespace dfly
