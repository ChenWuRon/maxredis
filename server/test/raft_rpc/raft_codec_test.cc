// Copyright 2026, The MaxRedis Authors.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_codec.h"

#include <gmock/gmock.h>

#include "base/gtest.h"
#include "raft_rpc.pb.h"
#include "server/raft/crc32.h"

namespace dfly {

using namespace std;
using namespace testing;

TEST(RaftCodecTest, FrameRoundTrip) {
  string payload = "hello world \x00\x01 binary";
  string frame = EncodeRpcFrame(RpcType::kHeartbeatReq, 12345, payload);

  RpcType type;
  uint32_t seq = 0;
  string_view out_payload;
  ASSERT_TRUE(ParseRpcFrame(frame, &type, &seq, &out_payload));
  EXPECT_EQ(RpcType::kHeartbeatReq, type);
  EXPECT_EQ(12345u, seq);
  EXPECT_EQ(payload, out_payload);

  // A different seq produces a different frame (seq is in the header and
  // covered by the CRC — the transport relies on it to skip stale frames).
  string frame2 = EncodeRpcFrame(RpcType::kHeartbeatReq, 12346, payload);
  EXPECT_NE(frame, frame2);
}

TEST(RaftCodecTest, FrameCorruptionDetected) {
  string frame = EncodeRpcFrame(RpcType::kVoteReq, 7, "payload");

  // Flip one payload byte -> CRC mismatch. Payload starts after the 13-byte
  // header; corrupting any of the payload bytes must be detected.
  string corrupt = frame;
  corrupt[kRpcFrameHeaderSize] ^= 0xFF;
  RpcType type;
  uint32_t seq = 0;
  string_view payload;
  EXPECT_FALSE(ParseRpcFrame(corrupt, &type, &seq, &payload));

  // Flip the seq byte (inside the header, also CRC-covered).
  string corrupt_seq = frame;
  corrupt_seq[6] ^= 0xFF;
  EXPECT_FALSE(ParseRpcFrame(corrupt_seq, &type, &seq, &payload));

  // Wrong magic.
  string bad_magic = frame;
  bad_magic[0] = 'X';
  EXPECT_FALSE(ParseRpcFrame(bad_magic, &type, &seq, &payload));

  // Truncated frame.
  EXPECT_FALSE(ParseRpcFrame(frame.substr(0, frame.size() - 1), &type, &seq, &payload));
}

TEST(RaftCodecTest, AppendEntriesRoundTrip) {
  AppendEntriesRequest req;
  req.group_id = 7;
  req.term = 42;
  req.leader_id = "leader-1";
  req.prev_log_index = 100;
  req.prev_log_term = 41;
  req.entries = {
      {41, 101, "SET key1 value1"},
      {42, 102, string("binary\x00\x01", 8)},
  };
  req.leader_commit = 99;

  AppendEntriesRequest parsed;
  ASSERT_TRUE(ParseAppendEntriesRequest(SerializeAppendEntriesRequest(req), &parsed));
  EXPECT_EQ(req.group_id, parsed.group_id);
  EXPECT_EQ(req.term, parsed.term);
  EXPECT_EQ(req.leader_id, parsed.leader_id);
  EXPECT_EQ(req.prev_log_index, parsed.prev_log_index);
  EXPECT_EQ(req.prev_log_term, parsed.prev_log_term);
  ASSERT_EQ(req.entries.size(), parsed.entries.size());
  EXPECT_EQ(req.entries[0].term, parsed.entries[0].term);
  EXPECT_EQ(req.entries[0].index, parsed.entries[0].index);
  EXPECT_EQ(req.entries[0].command, parsed.entries[0].command);
  EXPECT_EQ(req.entries[1].term, parsed.entries[1].term);
  EXPECT_EQ(req.entries[1].command, parsed.entries[1].command);
  EXPECT_EQ(req.leader_commit, parsed.leader_commit);

  AppendEntriesResponse rsp{7, 42, true, 102};
  AppendEntriesResponse parsed_rsp;
  ASSERT_TRUE(ParseAppendEntriesResponse(SerializeAppendEntriesResponse(rsp), &parsed_rsp));
  EXPECT_EQ(rsp, parsed_rsp);
}

TEST(RaftCodecTest, VoteRoundTrip) {
  VoteRequest req{3, 9, "candidate", 88, 8};
  VoteRequest parsed;
  ASSERT_TRUE(ParseVoteRequest(SerializeVoteRequest(req), &parsed));
  EXPECT_EQ(req, parsed);

  VoteResponse rsp{3, 9, true};
  VoteResponse parsed_rsp;
  ASSERT_TRUE(ParseVoteResponse(SerializeVoteResponse(rsp), &parsed_rsp));
  EXPECT_EQ(rsp, parsed_rsp);
}

TEST(RaftCodecTest, HeartbeatRoundTrip) {
  // leader_commit must survive the wire: followers advance their commit
  // index from heartbeats (empty AppendEntries).
  HeartbeatRequest req{1, 5, "leader", 42};
  HeartbeatRequest parsed;
  ASSERT_TRUE(ParseHeartbeatRequest(SerializeHeartbeatRequest(req), &parsed));
  EXPECT_EQ(req, parsed);

  // Legacy 3-field aggregate still round-trips with leader_commit == 0.
  HeartbeatRequest no_commit{1, 5, "leader"};
  ASSERT_TRUE(ParseHeartbeatRequest(SerializeHeartbeatRequest(no_commit), &parsed));
  EXPECT_EQ(parsed.leader_commit, 0);

  HeartbeatResponse rsp{1, 5, true};
  HeartbeatResponse parsed_rsp;
  ASSERT_TRUE(ParseHeartbeatResponse(SerializeHeartbeatResponse(rsp), &parsed_rsp));
  EXPECT_EQ(rsp, parsed_rsp);
}

TEST(RaftCodecTest, InstallSnapshotRoundTrip) {
  InstallSnapshotRequest req;
  req.group_id = 2;
  req.term = 6;
  req.leader_id = "leader";
  req.last_included_index = 500;
  req.last_included_term = 5;
  req.offset = 65536;
  req.done = false;
  req.data = string(65536, 'x');

  InstallSnapshotRequest parsed;
  ASSERT_TRUE(
      ParseInstallSnapshotRequest(SerializeInstallSnapshotRequest(req), &parsed));
  EXPECT_EQ(req.last_included_index, parsed.last_included_index);
  EXPECT_EQ(req.last_included_term, parsed.last_included_term);
  EXPECT_EQ(req.offset, parsed.offset);
  EXPECT_EQ(req.done, parsed.done);
  EXPECT_EQ(req.data, parsed.data);

  InstallSnapshotResponse rsp{2, 6, true};
  InstallSnapshotResponse parsed_rsp;
  ASSERT_TRUE(ParseInstallSnapshotResponse(SerializeInstallSnapshotResponse(rsp), &parsed_rsp));
  EXPECT_EQ(rsp, parsed_rsp);
}

TEST(RaftCodecTest, ReadIndexRoundTrip) {
  ReadIndexRequest req{4, 11, "leader", 12345};
  ReadIndexRequest parsed;
  ASSERT_TRUE(ParseReadIndexRequest(SerializeReadIndexRequest(req), &parsed));
  EXPECT_EQ(req, parsed);

  ReadIndexResponse rsp{4, 11, true, 700};
  ReadIndexResponse parsed_rsp;
  ASSERT_TRUE(ParseReadIndexResponse(SerializeReadIndexResponse(rsp), &parsed_rsp));
  EXPECT_EQ(rsp, parsed_rsp);
}

TEST(RaftCodecTest, TimeoutNowRoundTrip) {
  TimeoutNowRequest req{5, 12, "leader"};
  TimeoutNowRequest parsed;
  ASSERT_TRUE(ParseTimeoutNowRequest(SerializeTimeoutNowRequest(req), &parsed));
  EXPECT_EQ(req, parsed);

  TimeoutNowResponse rsp{5, 12, true};
  TimeoutNowResponse parsed_rsp;
  ASSERT_TRUE(ParseTimeoutNowResponse(SerializeTimeoutNowResponse(rsp), &parsed_rsp));
  EXPECT_EQ(rsp, parsed_rsp);
}

TEST(RaftCodecTest, TruncatedPayloadRejected) {
  // Valid request payload cut in half must not parse.
  VoteRequest req{3, 9, "candidate", 88, 8};
  string payload = SerializeVoteRequest(req);
  VoteRequest parsed;
  EXPECT_FALSE(ParseVoteRequest(payload.substr(0, payload.size() / 2), &parsed));
  EXPECT_FALSE(ParseVoteRequest("", &parsed));
}

TEST(RaftCodecTest, DecoderBoundsChecks) {
  // A payload that declares a huge string length but has no data.
  string payload;
  payload.push_back(static_cast<char>(0xFF));
  payload.push_back(static_cast<char>(0xFF));
  payload.push_back(static_cast<char>(0xFF));
  payload.push_back(static_cast<char>(0xFF));
  VoteRequest parsed;
  EXPECT_FALSE(ParseVoteRequest(payload, &parsed));
}

TEST(RaftCodecTest, PayloadIsStandardProtobuf) {
  // The wire payload must be a plain protobuf message: any protobuf
  // implementation can decode it without knowing about our frame layer.
  VoteRequest req{3, 9, "candidate", 88, 8};
  string payload = SerializeVoteRequest(req);

  dfly::raft::VoteRequest pb;
  ASSERT_TRUE(pb.ParseFromString(payload));
  EXPECT_EQ(3u, pb.group_id());
  EXPECT_EQ(9u, pb.term());
  EXPECT_EQ("candidate", pb.candidate_id());
  EXPECT_EQ(88u, pb.last_log_index());
  EXPECT_EQ(8u, pb.last_log_term());
}

TEST(RaftCodecTest, UnknownProtoFieldsAreIgnored) {
  // Forward compatibility: a peer running a NEWER binary may append fields
  // this build does not know. protobuf must skip them instead of failing.
  VoteRequest req{3, 9, "candidate", 88, 8};
  string payload = SerializeVoteRequest(req);

  // Append an unknown field: field 99, varint, value 1 (tag 0x98 0x06).
  payload.push_back(static_cast<char>(0x98));
  payload.push_back(static_cast<char>(0x06));
  payload.push_back(static_cast<char>(0x01));

  VoteRequest parsed;
  ASSERT_TRUE(ParseVoteRequest(payload, &parsed));
  EXPECT_EQ(req, parsed);
}

}  // namespace dfly
