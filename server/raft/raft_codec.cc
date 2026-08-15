// Copyright 2026, The MaxRedis Authors.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_codec.h"

#include <cstring>

#include "server/raft/crc32.h"

namespace dfly {

namespace {

void AppendU32LE(std::string* s, uint32_t v) {
  s->push_back(static_cast<char>(v & 0xFF));
  s->push_back(static_cast<char>((v >> 8) & 0xFF));
  s->push_back(static_cast<char>((v >> 16) & 0xFF));
  s->push_back(static_cast<char>((v >> 24) & 0xFF));
}

void AppendU64LE(std::string* s, uint64_t v) {
  AppendU32LE(s, static_cast<uint32_t>(v & 0xFFFFFFFF));
  AppendU32LE(s, static_cast<uint32_t>(v >> 32));
}

}  // namespace

// --- RaftEncoder --------------------------------------------------------

void RaftEncoder::U8(uint8_t v) {
  buf_.push_back(static_cast<char>(v));
}

void RaftEncoder::U32(uint32_t v) {
  AppendU32LE(&buf_, v);
}

void RaftEncoder::U64(uint64_t v) {
  AppendU64LE(&buf_, v);
}

void RaftEncoder::Bool(bool v) {
  buf_.push_back(v ? 1 : 0);
}

void RaftEncoder::Str(std::string_view s) {
  U32(static_cast<uint32_t>(s.size()));
  buf_.append(s.data(), s.size());
}

// --- RaftDecoder --------------------------------------------------------

bool RaftDecoder::Take(size_t n, std::string_view* out) {
  if (n > data_.size() - pos_)
    return false;
  *out = data_.substr(pos_, n);
  pos_ += n;
  return true;
}

bool RaftDecoder::U8(uint8_t* out) {
  std::string_view chunk;
  if (!Take(1, &chunk))
    return false;
  *out = static_cast<uint8_t>(chunk[0]);
  return true;
}

bool RaftDecoder::U32(uint32_t* out) {
  std::string_view chunk;
  if (!Take(4, &chunk))
    return false;
  *out = static_cast<uint32_t>(static_cast<uint8_t>(chunk[0])) |
         (static_cast<uint32_t>(static_cast<uint8_t>(chunk[1])) << 8) |
         (static_cast<uint32_t>(static_cast<uint8_t>(chunk[2])) << 16) |
         (static_cast<uint32_t>(static_cast<uint8_t>(chunk[3])) << 24);
  return true;
}

bool RaftDecoder::U64(uint64_t* out) {
  uint32_t lo = 0, hi = 0;
  if (!U32(&lo) || !U32(&hi))
    return false;
  *out = static_cast<uint64_t>(lo) | (static_cast<uint64_t>(hi) << 32);
  return true;
}

bool RaftDecoder::Bool(bool* out) {
  uint8_t v = 0;
  if (!U8(&v))
    return false;
  *out = v != 0;
  return true;
}

bool RaftDecoder::Str(std::string_view* out) {
  uint32_t len = 0;
  if (!U32(&len))
    return false;
  if (!Take(len, out))
    return false;
  return true;
}

bool RaftDecoder::Str(std::string* out) {
  std::string_view v;
  if (!Str(&v))
    return false;
  out->assign(v.data(), v.size());
  return true;
}

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

void SerializeLogEntry(RaftEncoder* enc, const LogEntry& e) {
  enc->U64(e.term);
  enc->U64(e.index);
  enc->Str(e.command);
}

bool ParseLogEntry(RaftDecoder* dec, LogEntry* e) {
  if (!dec->U64(&e->term) || !dec->U64(&e->index) || !dec->Str(&e->command))
    return false;
  return true;
}

std::string SerializeAppendEntriesRequest(const AppendEntriesRequest& req) {
  RaftEncoder enc;
  enc.U32(req.group_id);
  enc.U64(req.term);
  enc.Str(req.leader_id);
  enc.U64(req.prev_log_index);
  enc.U64(req.prev_log_term);
  enc.U32(static_cast<uint32_t>(req.entries.size()));
  for (const LogEntry& e : req.entries)
    SerializeLogEntry(&enc, e);
  enc.U64(req.leader_commit);
  return enc.Take();
}

bool ParseAppendEntriesRequest(std::string_view payload, AppendEntriesRequest* req) {
  RaftDecoder dec(payload);
  uint32_t count = 0;
  if (!dec.U32(&req->group_id) || !dec.U64(&req->term) ||
      !dec.Str(&req->leader_id) || !dec.U64(&req->prev_log_index) ||
      !dec.U64(&req->prev_log_term) || !dec.U32(&count))
    return false;
  req->entries.resize(count);
  for (uint32_t i = 0; i < count; ++i) {
    if (!ParseLogEntry(&dec, &req->entries[i]))
      return false;
  }
  if (!dec.U64(&req->leader_commit))
    return false;
  return dec.AtEnd();
}

std::string SerializeAppendEntriesResponse(const AppendEntriesResponse& rsp) {
  RaftEncoder enc;
  enc.U32(rsp.group_id);
  enc.U64(rsp.term);
  enc.Bool(rsp.success);
  enc.U64(rsp.last_log_index);
  return enc.Take();
}

bool ParseAppendEntriesResponse(std::string_view payload, AppendEntriesResponse* rsp) {
  RaftDecoder dec(payload);
  if (!dec.U32(&rsp->group_id) || !dec.U64(&rsp->term) || !dec.Bool(&rsp->success) ||
      !dec.U64(&rsp->last_log_index))
    return false;
  return dec.AtEnd();
}

// --- Vote ---------------------------------------------------------------

std::string SerializeVoteRequest(const VoteRequest& req) {
  RaftEncoder enc;
  enc.U32(req.group_id);
  enc.U64(req.term);
  enc.Str(req.candidate_id);
  enc.U64(req.last_log_index);
  enc.U64(req.last_log_term);
  return enc.Take();
}

bool ParseVoteRequest(std::string_view payload, VoteRequest* req) {
  RaftDecoder dec(payload);
  if (!dec.U32(&req->group_id) || !dec.U64(&req->term) ||
      !dec.Str(&req->candidate_id) || !dec.U64(&req->last_log_index) ||
      !dec.U64(&req->last_log_term))
    return false;
  return dec.AtEnd();
}

std::string SerializeVoteResponse(const VoteResponse& rsp) {
  RaftEncoder enc;
  enc.U32(rsp.group_id);
  enc.U64(rsp.term);
  enc.Bool(rsp.vote_granted);
  return enc.Take();
}

bool ParseVoteResponse(std::string_view payload, VoteResponse* rsp) {
  RaftDecoder dec(payload);
  if (!dec.U32(&rsp->group_id) || !dec.U64(&rsp->term) ||
      !dec.Bool(&rsp->vote_granted))
    return false;
  return dec.AtEnd();
}

// --- Heartbeat ----------------------------------------------------------

std::string SerializeHeartbeatRequest(const HeartbeatRequest& req) {
  RaftEncoder enc;
  enc.U32(req.group_id);
  enc.U64(req.term);
  enc.Str(req.leader_id);
  enc.U64(req.leader_commit);
  return enc.Take();
}

bool ParseHeartbeatRequest(std::string_view payload, HeartbeatRequest* req) {
  RaftDecoder dec(payload);
  if (!dec.U32(&req->group_id) || !dec.U64(&req->term) ||
      !dec.Str(&req->leader_id) || !dec.U64(&req->leader_commit))
    return false;
  return dec.AtEnd();
}

std::string SerializeHeartbeatResponse(const HeartbeatResponse& rsp) {
  RaftEncoder enc;
  enc.U32(rsp.group_id);
  enc.U64(rsp.term);
  enc.Bool(rsp.success);
  enc.U64(rsp.last_log_index);
  return enc.Take();
}

bool ParseHeartbeatResponse(std::string_view payload, HeartbeatResponse* rsp) {
  RaftDecoder dec(payload);
  if (!dec.U32(&rsp->group_id) || !dec.U64(&rsp->term) ||
      !dec.Bool(&rsp->success) || !dec.U64(&rsp->last_log_index))
    return false;
  return dec.AtEnd();
}

// --- InstallSnapshot ----------------------------------------------------

std::string SerializeInstallSnapshotRequest(const InstallSnapshotRequest& req) {
  RaftEncoder enc;
  enc.U32(req.group_id);
  enc.U64(req.term);
  enc.Str(req.leader_id);
  enc.U64(req.last_included_index);
  enc.U64(req.last_included_term);
  enc.U64(req.offset);
  enc.Bool(req.done);
  enc.Str(req.data);
  return enc.Take();
}

bool ParseInstallSnapshotRequest(std::string_view payload, InstallSnapshotRequest* req) {
  RaftDecoder dec(payload);
  if (!dec.U32(&req->group_id) || !dec.U64(&req->term) ||
      !dec.Str(&req->leader_id) || !dec.U64(&req->last_included_index) ||
      !dec.U64(&req->last_included_term) || !dec.U64(&req->offset) ||
      !dec.Bool(&req->done) || !dec.Str(&req->data))
    return false;
  return dec.AtEnd();
}

std::string SerializeInstallSnapshotResponse(const InstallSnapshotResponse& rsp) {
  RaftEncoder enc;
  enc.U32(rsp.group_id);
  enc.U64(rsp.term);
  enc.Bool(rsp.success);
  return enc.Take();
}

bool ParseInstallSnapshotResponse(std::string_view payload, InstallSnapshotResponse* rsp) {
  RaftDecoder dec(payload);
  if (!dec.U32(&rsp->group_id) || !dec.U64(&rsp->term) ||
      !dec.Bool(&rsp->success))
    return false;
  return dec.AtEnd();
}

// --- ReadIndex ----------------------------------------------------------

std::string SerializeReadIndexRequest(const ReadIndexRequest& req) {
  RaftEncoder enc;
  enc.U32(req.group_id);
  enc.U64(req.term);
  enc.Str(req.leader_id);
  enc.U64(req.request_id);
  return enc.Take();
}

bool ParseReadIndexRequest(std::string_view payload, ReadIndexRequest* req) {
  RaftDecoder dec(payload);
  if (!dec.U32(&req->group_id) || !dec.U64(&req->term) ||
      !dec.Str(&req->leader_id) || !dec.U64(&req->request_id))
    return false;
  return dec.AtEnd();
}

std::string SerializeReadIndexResponse(const ReadIndexResponse& rsp) {
  RaftEncoder enc;
  enc.U32(rsp.group_id);
  enc.U64(rsp.term);
  enc.Bool(rsp.success);
  enc.U64(rsp.commit_index);
  return enc.Take();
}

bool ParseReadIndexResponse(std::string_view payload, ReadIndexResponse* rsp) {
  RaftDecoder dec(payload);
  if (!dec.U32(&rsp->group_id) || !dec.U64(&rsp->term) ||
      !dec.Bool(&rsp->success) || !dec.U64(&rsp->commit_index))
    return false;
  return dec.AtEnd();
}

// --- TimeoutNow ---------------------------------------------------------

std::string SerializeTimeoutNowRequest(const TimeoutNowRequest& req) {
  RaftEncoder enc;
  enc.U32(req.group_id);
  enc.U64(req.term);
  enc.Str(req.leader_id);
  return enc.Take();
}

bool ParseTimeoutNowRequest(std::string_view payload, TimeoutNowRequest* req) {
  RaftDecoder dec(payload);
  if (!dec.U32(&req->group_id) || !dec.U64(&req->term) ||
      !dec.Str(&req->leader_id))
    return false;
  return dec.AtEnd();
}

std::string SerializeTimeoutNowResponse(const TimeoutNowResponse& rsp) {
  RaftEncoder enc;
  enc.U32(rsp.group_id);
  enc.U64(rsp.term);
  enc.Bool(rsp.accepted);
  return enc.Take();
}

bool ParseTimeoutNowResponse(std::string_view payload, TimeoutNowResponse* rsp) {
  RaftDecoder dec(payload);
  if (!dec.U32(&rsp->group_id) || !dec.U64(&rsp->term) ||
      !dec.Bool(&rsp->accepted))
    return false;
  return dec.AtEnd();
}

}  // namespace dfly
