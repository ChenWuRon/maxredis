// Copyright 2026, The MaxRedis Authors.
// See LICENSE for licensing terms.
//
// TcpTransport: the production wire transport for Raft RPCs.
//
// Client side — implements Transport by serializing each RPC into a
// CRC-protected frame (see raft_codec.h) and exchanging it over a pooled,
// per-proactor TCP connection to the peer's Raft RPC port. Sending is a
// fiber-blocking synchronous call (matches the Transport interface): the
// caller fiber (heartbeat loop / election / replicate) parks until the
// response frame arrives or the RPC timeout elapses.
//
// Server side — RaftRpcServer owns a ListenerInterface that accepts
// connections; each connection fiber reads frames in a loop, resolves the
// target RaftNode by group_id and invokes the corresponding handler
// (OnRequestVote / OnAppendEntries / ...). Handlers run under the node's
// RPC lifetime guard (TryAcquireRpcRef) so a node torn down concurrently
// can never be entered.

#pragma once

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "server/raft/raft_codec.h"
#include "server/raft/transport.h"
#include "util/fibers/synchronization.h"

namespace util {
class FiberSocketBase;
class ListenerInterface;
namespace fb2 {
class ProactorBase;
}  // namespace fb2
}  // namespace util

namespace dfly {

class RaftNode;

// Peer registry: node_id -> (host, port). Shared by the client side of the
// transport; the RPC server does not need it (requests arrive addressed).
struct TcpPeer {
  std::string host;
  uint16_t port = 0;
};

class TcpTransport : public Transport {
 public:
  TcpTransport();
  ~TcpTransport() override;

  TcpTransport(const TcpTransport&) = delete;
  TcpTransport& operator=(const TcpTransport&) = delete;

  // Sets/updates the endpoint of a peer. Thread-safe.
  void SetPeerEndpoint(const NodeId& peer_id, TcpPeer peer);

  // Removes a peer's endpoint (no more client RPCs will be routed to it).
  void RemovePeerEndpoint(const NodeId& peer_id);

  // RPC timeout in milliseconds (connect + round-trip). Default 1000ms.
  void set_rpc_timeout_ms(uint32_t ms) {
    rpc_timeout_ms_ = ms;
  }

  // Heartbeat RPC timeout. Must be well below the follower election timeout
  // (300-600ms): a heartbeat to a partitioned peer parks only its own fiber
  // (heartbeats fan out concurrently), and the leader's round duration is
  // bounded by this value so healthy followers still get heartbeats in time.
  void set_heartbeat_timeout_ms(uint32_t ms) {
    heartbeat_timeout_ms_ = ms;
  }

  // Vote RPC timeout. An election round completes only after ALL votes are
  // collected; a partitioned peer therefore delays the round by the vote
  // timeout. Keep it short (healthy peers answer in ~1ms) so election rounds
  // converge quickly even with a dead member.
  void set_vote_timeout_ms(uint32_t ms) {
    vote_timeout_ms_ = ms;
  }

  // AppendEntries RPC timeout. Kept well below the follower election timeout
  // so replication rounds against a partitioned peer can never starve the
  // heartbeat loop (which shares the transport) into spurious elections.
  void set_append_timeout_ms(uint32_t ms) {
    append_timeout_ms_ = ms;
  }

  VoteResponse SendVoteRequest(const NodeId& peer_id,
                               const VoteRequest& request) override;

  HeartbeatResponse SendHeartbeat(const NodeId& peer_id,
                                  const HeartbeatRequest& request) override;

  AppendEntriesResponse SendAppendEntries(const NodeId& peer_id,
                                           const AppendEntriesRequest& request) override;

  InstallSnapshotResponse SendInstallSnapshot(const NodeId& peer_id,
                                               const InstallSnapshotRequest& request) override;

  ReadIndexResponse SendReadIndex(const NodeId& peer_id,
                                   const ReadIndexRequest& request) override;

  TimeoutNowResponse SendTimeoutNow(const NodeId& peer_id,
                                     const TimeoutNowRequest& request) override;

 private:
  // Executes one request/response round-trip:
  //   lookup endpoint -> get/create pooled socket -> connect if needed ->
  //   write frame -> read frame(s) -> parse + verify CRC + seq.
  // Late responses to previously timed-out requests carry a stale seq and
  // are skipped, so a timed-out connection is REUSED (no close/recreate
  // churn). Returns false when the RPC could not be completed (unknown
  // peer, no proactor, connect/IO failure, timeout, corrupt response).
  bool DoRpc(const NodeId& peer_id, RpcType req_type, const std::string& req_payload,
             RpcType expected_rsp_type, std::string* rsp_payload, uint32_t timeout_ms);

  // Per-proactor pooled connections (one socket per peer per thread). Each
  // entry carries a fiber mutex that guards the FULL request/response
  // round-trip: multiple fibers on the same proactor (heartbeat loop,
  // election, replicate, ReadIndex) may target the same peer concurrently,
  // and a FiberSocketBase cannot interleave two reads/writes. The mutex
  // serializes them — without it, responses get swapped between fibers and
  // helio aborts on double-Read/Write (read_req_ CHECK failure).
  struct ThreadConns {
    struct PeerConn {
      util::FiberSocketBase* sock = nullptr;
      // unique_ptr keeps PeerConn movable (flat_hash_map rehash); the mutex
      // identity is stable across moves.
      std::unique_ptr<util::fb2::Mutex> mu = std::make_unique<util::fb2::Mutex>();
    };
    absl::flat_hash_map<NodeId, PeerConn> conns;
    // Request sequence counter for the frame header (see raft_codec.h).
    uint32_t seq_counter = 0;
    // Sockets that were Close()d (connect/write failure, CRC corruption) but
    // deliberately NOT deleted: helio's epoll arm slots can still dispatch a
    // stale event for the closed fd after Disarm (known generation race in
    // epoll_proactor.cc). Deleting immediately lets a recycled fiber stack
    // receive a stale PendingReq activation. The retired sockets are deleted
    // only at thread exit, when no events can be pending.
    std::vector<util::FiberSocketBase*> retired;
    ~ThreadConns();
  };

  static ThreadConns& LocalConns();

  mutable util::fb2::Mutex mutex_;
  absl::flat_hash_map<NodeId, TcpPeer> peers_;
  uint32_t rpc_timeout_ms_ = 1000;
  uint32_t heartbeat_timeout_ms_ = 150;
  uint32_t vote_timeout_ms_ = 250;
  uint32_t append_timeout_ms_ = 200;
};

// Accepts Raft RPC connections and dispatches them to local RaftNodes.
// Creates a ListenerInterface whose ownership transfers to the caller (an
// AcceptServer deletes it). The resolver is stored by value inside the
// listener; it must stay callable until the accept server has stopped.
class RaftRpcServer {
 public:
  using NodeResolver = std::function<RaftNode*(GroupId)>;

  // Creates a listener wired to |resolver|. Ownership passes to the caller.
  static util::ListenerInterface* CreateListener(NodeResolver resolver);
};

}  // namespace dfly
