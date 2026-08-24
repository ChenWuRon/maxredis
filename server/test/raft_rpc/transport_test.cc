// Copyright 2026, The MaxRedis Authors.
// See LICENSE for licensing terms.
//
// transport_test.cc: Google Test coverage for the Raft transport layer.
//
// WireTransport wraps the in-process LocalTransport and forces every RPC to
// traverse the REAL production wire format — protobuf payload (raft_rpc.proto)
// inside a CRC-protected frame (raft_codec.h) — in both directions. This is
// what proves the multi-node cluster actually runs on the serialized wire
// protocol, not on in-process struct passing: corruption injected into the
// frame is detected by the CRC, exactly like a flipped bit on a TCP stream.

#include <gmock/gmock.h>

#include <atomic>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/gtest.h"
#include "server/raft/command_log.h"
#include "server/raft/local_transport.h"
#include "server/raft/raft_codec.h"
#include "server/raft/raft_node.h"
#include "server/state_machine/state_machine.h"

namespace dfly {

using namespace std;
using namespace testing;

namespace {

// Minimal in-memory KV state machine: applies space-delimited "SET key val"
// log entries (binary-safe values).
class InMemoryKV : public IStateMachine {
 public:
  ApplyResult Apply(const CommandId*, CmdArgList) override {
    return {ApplyOp::OK, 0};
  }

  ApplyResult ApplyLogEntry(const LogEntry& entry) override {
    vector<string> args;
    string_view cmd = entry.command;
    size_t pos = 0;
    while (pos < cmd.size()) {
      size_t end = cmd.find(' ', pos);
      if (end == string_view::npos) {
        args.emplace_back(cmd.substr(pos));
        break;
      }
      args.emplace_back(cmd.substr(pos, end - pos));
      pos = end + 1;
    }
    if (args.size() < 3 || args[0] != "SET")
      return {ApplyOp::ERROR, 0};
    store_[args[1]] = args[2];
    return {ApplyOp::OK, 1};
  }

  void Set(DbIndex, string_view key, string_view val) override {
    store_[string(key)] = string(val);
  }

  bool Del(DbIndex, string_view key) override {
    return store_.erase(string(key)) > 0;
  }

  bool Expire(DbIndex, string_view, uint64_t) override {
    return false;
  }

  OpResult<string> Get(DbIndex, string_view key, ReadConsistency) override {
    auto it = store_.find(string(key));
    if (it != store_.end())
      return it->second;
    return OpStatus::KEY_NOTFOUND;
  }

  size_t DbSize(DbIndex) const override {
    return store_.size();
  }

  void Schedule(DbIndex, string_view, function<void(EngineShard*)>) override {
  }

 private:
  absl::flat_hash_map<string, string> store_;
};

}  // namespace

// A Transport that serializes every RPC into the production wire format
// (protobuf payload + CRC frame), immediately "receives" it on the peer side
// (frame parse — the point where corruption is detected), decodes the
// protobuf payload and dispatches to the inner transport. Responses take the
// same path back. With corrupt_frames_ set, one payload byte is flipped
// before the receive-side parse, and the CRC check must reject the frame.
class WireTransport : public Transport {
 public:
  explicit WireTransport(Transport* inner) : inner_(inner) {
  }

  void set_corrupt_frames(bool corrupt) {
    corrupt_frames_ = corrupt;
  }

  void UnregisterNode(GroupId group_id, const NodeId& node_id) override {
    inner_->UnregisterNode(group_id, node_id);
  }

  VoteResponse SendVoteRequest(const NodeId& peer_id,
                               const VoteRequest& request) override {
    std::string body;
    if (!WireHop(RpcType::kVoteReq, SerializeVoteRequest(request), &body))
      return {request.group_id, 0, false};
    VoteRequest decoded;
    if (!ParseVoteRequest(body, &decoded))
      return {request.group_id, 0, false};
    VoteResponse rsp = inner_->SendVoteRequest(peer_id, decoded);
    if (!WireHop(RpcType::kVoteRsp, SerializeVoteResponse(rsp), &body))
      return {request.group_id, 0, false};
    VoteResponse parsed;
    if (!ParseVoteResponse(body, &parsed))
      return {request.group_id, 0, false};
    return parsed;
  }

  HeartbeatResponse SendHeartbeat(const NodeId& peer_id,
                                  const HeartbeatRequest& request) override {
    std::string body;
    if (!WireHop(RpcType::kHeartbeatReq, SerializeHeartbeatRequest(request), &body))
      return {request.group_id, 0, false, 0};
    HeartbeatRequest decoded;
    if (!ParseHeartbeatRequest(body, &decoded))
      return {request.group_id, 0, false, 0};
    HeartbeatResponse rsp = inner_->SendHeartbeat(peer_id, decoded);
    if (!WireHop(RpcType::kHeartbeatRsp, SerializeHeartbeatResponse(rsp), &body))
      return {request.group_id, 0, false, 0};
    HeartbeatResponse parsed;
    if (!ParseHeartbeatResponse(body, &parsed))
      return {request.group_id, 0, false, 0};
    return parsed;
  }

  AppendEntriesResponse SendAppendEntries(const NodeId& peer_id,
                                           const AppendEntriesRequest& request) override {
    std::string body;
    if (!WireHop(RpcType::kAppendEntriesReq, SerializeAppendEntriesRequest(request), &body))
      return {request.group_id, 0, false, 0};
    AppendEntriesRequest decoded;
    if (!ParseAppendEntriesRequest(body, &decoded))
      return {request.group_id, 0, false, 0};
    AppendEntriesResponse rsp = inner_->SendAppendEntries(peer_id, decoded);
    if (!WireHop(RpcType::kAppendEntriesRsp, SerializeAppendEntriesResponse(rsp), &body))
      return {request.group_id, 0, false, 0};
    AppendEntriesResponse parsed;
    if (!ParseAppendEntriesResponse(body, &parsed))
      return {request.group_id, 0, false, 0};
    return parsed;
  }

  InstallSnapshotResponse SendInstallSnapshot(const NodeId& peer_id,
                                               const InstallSnapshotRequest& request) override {
    std::string body;
    if (!WireHop(RpcType::kInstallSnapshotReq, SerializeInstallSnapshotRequest(request), &body))
      return {request.group_id, 0, false};
    InstallSnapshotRequest decoded;
    if (!ParseInstallSnapshotRequest(body, &decoded))
      return {request.group_id, 0, false};
    InstallSnapshotResponse rsp = inner_->SendInstallSnapshot(peer_id, decoded);
    if (!WireHop(RpcType::kInstallSnapshotRsp, SerializeInstallSnapshotResponse(rsp), &body))
      return {request.group_id, 0, false};
    InstallSnapshotResponse parsed;
    if (!ParseInstallSnapshotResponse(body, &parsed))
      return {request.group_id, 0, false};
    return parsed;
  }

  ReadIndexResponse SendReadIndex(const NodeId& peer_id,
                                   const ReadIndexRequest& request) override {
    std::string body;
    if (!WireHop(RpcType::kReadIndexReq, SerializeReadIndexRequest(request), &body))
      return {request.group_id, 0, false, 0};
    ReadIndexRequest decoded;
    if (!ParseReadIndexRequest(body, &decoded))
      return {request.group_id, 0, false, 0};
    ReadIndexResponse rsp = inner_->SendReadIndex(peer_id, decoded);
    if (!WireHop(RpcType::kReadIndexRsp, SerializeReadIndexResponse(rsp), &body))
      return {request.group_id, 0, false, 0};
    ReadIndexResponse parsed;
    if (!ParseReadIndexResponse(body, &parsed))
      return {request.group_id, 0, false, 0};
    return parsed;
  }

  TimeoutNowResponse SendTimeoutNow(const NodeId& peer_id,
                                     const TimeoutNowRequest& request) override {
    std::string body;
    if (!WireHop(RpcType::kTimeoutNowReq, SerializeTimeoutNowRequest(request), &body))
      return {request.group_id, 0, false};
    TimeoutNowRequest decoded;
    if (!ParseTimeoutNowRequest(body, &decoded))
      return {request.group_id, 0, false};
    TimeoutNowResponse rsp = inner_->SendTimeoutNow(peer_id, decoded);
    if (!WireHop(RpcType::kTimeoutNowRsp, SerializeTimeoutNowResponse(rsp), &body))
      return {request.group_id, 0, false};
    TimeoutNowResponse parsed;
    if (!ParseTimeoutNowResponse(body, &parsed))
      return {request.group_id, 0, false};
    return parsed;
  }

 private:
  // Encodes |payload| into a CRC frame, performs the wire hop (receive-side
  // frame parse — magic + CRC verification, optionally after corrupting one
  // payload byte) and returns the verified payload. False = frame rejected.
  bool WireHop(RpcType type, const std::string& payload, std::string* out) {
    uint32_t seq = seq_.fetch_add(1, std::memory_order_relaxed) + 1;
    std::string frame = EncodeRpcFrame(type, seq, payload);
    if (corrupt_frames_)
      frame[kRpcFrameHeaderSize] ^= 0xFF;  // flip one payload byte
    RpcType parsed_type;
    uint32_t parsed_seq = 0;
    std::string_view body;
    if (!ParseRpcFrame(frame, &parsed_type, &parsed_seq, &body))
      return false;
    if (parsed_type != type || parsed_seq != seq)
      return false;
    out->assign(body.data(), body.size());
    return true;
  }

  Transport* inner_;
  std::atomic<uint32_t> seq_{0};
  bool corrupt_frames_ = false;
};

class TransportTest : public Test {
 protected:
  struct Node {
    NodeId id;
    CommandLog log;
    InMemoryKV kv;
    unique_ptr<RaftNode> node;

    explicit Node(NodeId i) : id(std::move(i)), node(make_unique<RaftNode>(id)) {
    }
  };

  void SetUp() override {
    n1_ = make_unique<Node>("N1");
    n2_ = make_unique<Node>("N2");
    n3_ = make_unique<Node>("N3");
    for (Node* n : {n1_.get(), n2_.get(), n3_.get()}) {
      n->node->SetLogStorage(&n->log);
      n->node->SetStateMachine(&n->kv);
      inner_.RegisterNode(0, n->id, n->node.get());
    }
    n1_->node->SetTransport(&wire_);
    n1_->node->AddPeer("N2");
    n1_->node->AddPeer("N3");
  }

  void TearDown() override {
    for (Node* n : {n1_.get(), n2_.get(), n3_.get()}) {
      n->node->Shutdown();
      inner_.UnregisterNode(0, n->id);
    }
  }

  // Elects N1 and lets one heartbeat round establish authority + healthy
  // peer tracking (peer_hb_ok_) before replication is exercised.
  void ElectLeader() {
    ElectionResult result = n1_->node->StartElection();
    ASSERT_EQ(RaftRole::Leader, n1_->node->role());
    ASSERT_EQ(3u, result.votes_received);
    n1_->node->SendHeartbeatToPeers();
  }

  LocalTransport inner_;
  WireTransport wire_{&inner_};
  unique_ptr<Node> n1_, n2_, n3_;
};

// 3-node election where every VoteRequest/Response traverses the protobuf
// + CRC-frame wire codec in both directions.
TEST_F(TransportTest, ThreeNodeElectionOverWireCodec) {
  ElectionResult result = n1_->node->StartElection();

  EXPECT_EQ(3u, result.votes_received);
  EXPECT_EQ(0u, result.votes_rejected);
  EXPECT_EQ(RaftRole::Leader, n1_->node->role());
  EXPECT_EQ(1u, n1_->node->term());
  EXPECT_EQ("N1", n2_->node->voted_for());
  EXPECT_EQ("N1", n3_->node->voted_for());
}

// Client write path: SubmitEntry replicates through AppendEntries frames,
// heartbeats carry leader_commit so every node's state machine converges.
// The value is binary-safe and must survive the protobuf bytes field.
TEST_F(TransportTest, ThreeNodeReplicationOverWireCodec) {
  ElectLeader();

  const string bin_value("v\x00\xFFx", 4);
  string cmd = "SET bin ";
  cmd += bin_value;

  ApplyResult res = n1_->node->SubmitEntry(LogEntry{0, 0, cmd});
  EXPECT_EQ(ApplyOp::OK, res.op);
  EXPECT_EQ(1u, n1_->node->commit_index());

  // Heartbeat carries leader_commit: followers apply committed entries and
  // lagging followers are pushed the missing log tail (catch-up path).
  n1_->node->SendHeartbeatToPeers();
  n1_->node->SendHeartbeatToPeers();

  for (Node* n : {n1_.get(), n2_.get(), n3_.get()}) {
    SCOPED_TRACE(n->id);
    EXPECT_EQ(1u, n->node->commit_index());
    EXPECT_EQ(1u, n->node->last_applied());
    EXPECT_EQ(1u, n->log.LastIndex());
    ASSERT_NE(nullptr, n->log.Get(1));
    EXPECT_EQ(cmd, n->log.Get(1)->command);
    auto val = n->kv.Get(0, "bin", ReadConsistency::kLocal);
    ASSERT_TRUE(val);
    EXPECT_EQ(bin_value, val.value());
  }
}

// A corrupted frame must be rejected by the CRC check on the receiving hop:
// with corruption injected, no vote can be delivered, the round is lost and
// N1 steps back to Follower (§5.2); with a healthy wire the same node wins
// in a fresh term.
TEST_F(TransportTest, CorruptedWireFramesPreventElection) {
  wire_.set_corrupt_frames(true);
  ElectionResult result = n1_->node->StartElection();

  EXPECT_EQ(1u, result.votes_received);  // self vote only
  EXPECT_EQ(2u, result.votes_rejected);
  EXPECT_EQ(RaftRole::Follower, n1_->node->role());  // lost round → step back

  wire_.set_corrupt_frames(false);
  ElectionResult retry = n1_->node->StartElection();  // fresh term

  EXPECT_EQ(3u, retry.votes_received);
  EXPECT_EQ(0u, retry.votes_rejected);
  EXPECT_EQ(RaftRole::Leader, n1_->node->role());
}

// Routing to an unregistered node must fail closed for every RPC family.
TEST_F(TransportTest, RpcToUnknownPeerIsRejected) {
  VoteResponse vote = inner_.SendVoteRequest("ghost", VoteRequest{0, 1, "N1", 0, 0});
  EXPECT_FALSE(vote.vote_granted);

  AppendEntriesResponse ae = inner_.SendAppendEntries("ghost", AppendEntriesRequest{});
  EXPECT_FALSE(ae.success);

  HeartbeatResponse hb = inner_.SendHeartbeat("ghost", HeartbeatRequest{});
  EXPECT_FALSE(hb.success);

  EXPECT_FALSE(inner_.HasNode(0, "ghost"));
  EXPECT_TRUE(inner_.HasNode(0, "N1"));
}

}  // namespace dfly
