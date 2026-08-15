// Copyright 2026, The MaxRedis Authors.
// See LICENSE for licensing terms.
//

#include "server/raft/tcp_transport.h"

#include <boost/asio/ip/tcp.hpp>

#include "base/logging.h"
#include "io/io.h"
#include "server/raft/raft_node.h"
#include "util/connection.h"
#include "util/fiber_socket_base.h"
#include "util/fibers/proactor_base.h"
#include "util/listener_interface.h"

namespace dfly {

using namespace std;

namespace {

// Reads exactly |n| bytes from |sock| into |out|. Returns the number of
// bytes consumed (n on success). On failure (EOF/error/timeout) the return
// value is < n; the caller can tell a "clean" failure at a frame boundary
// (0 consumed) apart from a partial read that desynchronized the stream.
size_t ReadN(util::FiberSocketBase* sock, size_t n, std::string* out) {
  out->resize(n);
  size_t done = 0;
  while (done < n) {
    io::MutableBytes mb{reinterpret_cast<uint8_t*>(out->data() + done), n - done};
    io::Result<size_t> res = sock->Recv(mb);
    if (!res)
      break;
    if (*res == 0)
      break;
    done += *res;
  }
  out->resize(done);
  return done;
}

constexpr uint32_t kMaxFramePayload = 256u * 1024 * 1024;  // snapshot chunk bound
// InstallSnapshot may stream many 64KB chunks; give the round-trip room to
// breathe on slow links.
constexpr uint32_t kSnapshotRpcTimeoutMs = 30000;

}  // namespace

// ============================ TcpTransport ==============================

TcpTransport::TcpTransport() = default;

TcpTransport::~TcpTransport() = default;

TcpTransport::ThreadConns::~ThreadConns() {
  for (auto& [peer, pc] : conns) {
    if (pc.sock)
      pc.sock->Close();
    delete pc.sock;
  }
  for (auto* sock : retired)
    delete sock;
}

TcpTransport::ThreadConns& TcpTransport::LocalConns() {
  thread_local ThreadConns conns;
  return conns;
}

void TcpTransport::SetPeerEndpoint(const NodeId& peer_id, TcpPeer peer) {
  std::lock_guard<util::fb2::Mutex> guard(mutex_);
  peers_[peer_id] = std::move(peer);
}

void TcpTransport::RemovePeerEndpoint(const NodeId& peer_id) {
  std::lock_guard<util::fb2::Mutex> guard(mutex_);
  peers_.erase(peer_id);
}

bool TcpTransport::DoRpc(const NodeId& peer_id, RpcType req_type,
                         const std::string& req_payload, RpcType expected_rsp_type,
                         std::string* rsp_payload, uint32_t timeout_ms) {
  // Resolve the peer endpoint outside the hot path.
  TcpPeer peer;
  {
    std::lock_guard<util::fb2::Mutex> guard(mutex_);
    auto it = peers_.find(peer_id);
    if (it == peers_.end()) {
      VLOG(2) << "TcpTransport: unknown peer " << peer_id;
      return false;
    }
    peer = it->second;
  }

  util::fb2::ProactorBase* pb = util::fb2::ProactorBase::me();
  if (!pb) {
    LOG(WARNING) << "TcpTransport: RPC to " << peer_id
                 << " issued outside a proactor thread";
    return false;
  }

  ThreadConns& conns = LocalConns();
  ThreadConns::PeerConn& pc = conns.conns[peer_id];

  // Serialize the entire round-trip per (thread, peer). The fiber mutex is
  // held across blocking socket ops — safe: the blocked fiber parks and the
  // other fibers on this proactor run. RPCs to DIFFERENT peers still
  // proceed concurrently.
  std::lock_guard<util::fb2::Mutex> guard(*pc.mu);
  util::FiberSocketBase* sock = pc.sock;

  // Sockets are only ever RETIRED (Close + park, deleted at thread exit) —
  // never deleted inline. helio's epoll arm slots can dispatch a stale event
  // for a closed fd after Disarm; deleting immediately lets the slot (and the
  // fiber stack) be recycled under a pending event, corrupting the next
  // socket/fiber. See the generation-race TODO in epoll_proactor.cc.
  auto retire = [&](util::FiberSocketBase* s) {
    s->Close();
    conns.retired.push_back(s);
    pc.sock = nullptr;
  };

  if (!sock || !sock->IsOpen()) {
    if (sock)
      retire(sock);
    sock = pb->CreateSocket();
    pc.sock = sock;

    boost::asio::ip::tcp::endpoint ep(
        boost::asio::ip::make_address(peer.host), peer.port);
    sock->set_timeout(timeout_ms);
    std::error_code ec = sock->Connect(ep);
    if (ec) {
      VLOG(1) << "TcpTransport: connect to " << peer_id << " (" << peer.host
              << ":" << peer.port << ") failed: " << ec.message();
      retire(sock);
      return false;
    }
  }
  sock->set_timeout(timeout_ms);

  // One frame out, N frames in. Late responses to previously timed-out
  // requests carry stale seqs and are skipped until the frame matching OUR
  // seq arrives — so a timeout does NOT destroy the pooled connection
  // (closing it would desync against an in-flight late response and the
  // churn races helio's arm-slot generation counter).
  uint32_t seq = ++conns.seq_counter;
  std::string frame = EncodeRpcFrame(req_type, seq, req_payload);
  std::error_code wc = sock->Write(io::Bytes{reinterpret_cast<const uint8_t*>(frame.data()),
                                             frame.size()});
  if (wc) {
    VLOG(1) << "TcpTransport: write to " << peer_id << " failed: " << wc.message();
    retire(sock);
    return false;
  }

  // Response header: magic(4) + type(1) + seq(4) + len(4).
  while (true) {
    std::string header;
    size_t got = ReadN(sock, kRpcFrameHeaderSize, &header);
    if (got < kRpcFrameHeaderSize) {
      if (got == 0) {
        // Nothing consumed: the stream is still at a frame boundary. KEEP
        // the connection — a late response to this (or an earlier) request
        // will be skipped by its stale seq in the next DoRpc. This is what
        // makes heartbeats to a partitioned peer cheap: no churn.
        VLOG(2) << "TcpTransport: timeout waiting for response header from " << peer_id
                << " (connection kept, late frames are skipped by seq)";
        return false;
      }
      // Partial header: the stream is desynchronized — drop the connection.
      VLOG(1) << "TcpTransport: partial response header from " << peer_id
              << " (" << got << "/" << kRpcFrameHeaderSize << " bytes), reconnecting";
      retire(sock);
      return false;
    }

    std::string_view len_chunk{header.data() + 9, 4};
    uint32_t payload_len = static_cast<uint32_t>(static_cast<uint8_t>(len_chunk[0])) |
                           (static_cast<uint32_t>(static_cast<uint8_t>(len_chunk[1])) << 8) |
                           (static_cast<uint32_t>(static_cast<uint8_t>(len_chunk[2])) << 16) |
                           (static_cast<uint32_t>(static_cast<uint8_t>(len_chunk[3])) << 24);
    if (payload_len > kMaxFramePayload) {
      LOG(WARNING) << "TcpTransport: oversized response frame from " << peer_id
                   << ": " << payload_len << " bytes";
      retire(sock);
      return false;
    }

    std::string payload, crc_bytes;
    size_t pgot = ReadN(sock, payload_len, &payload);
    size_t cgot = ReadN(sock, kRpcFrameCrcSize, &crc_bytes);
    if (pgot != payload_len || cgot != kRpcFrameCrcSize) {
      VLOG(1) << "TcpTransport: truncated response frame from " << peer_id;
      retire(sock);
      return false;
    }

    std::string full_frame = std::move(header);
    full_frame.append(payload);
    full_frame.append(crc_bytes);

    RpcType rsp_type;
    uint32_t rsp_seq = 0;
    std::string_view payload_view;
    if (!ParseRpcFrame(full_frame, &rsp_type, &rsp_seq, &payload_view)) {
      LOG(WARNING) << "TcpTransport: corrupt (CRC/magic) response frame from " << peer_id;
      retire(sock);
      return false;
    }
    if (rsp_seq != seq) {
      VLOG(2) << "TcpTransport: skipping stale response frame (seq " << rsp_seq
              << " != " << seq << ") from " << peer_id;
      continue;  // late response to an earlier timed-out request
    }
    if (rsp_type != expected_rsp_type) {
      LOG(WARNING) << "TcpTransport: unexpected response type from " << peer_id;
      retire(sock);
      return false;
    }

    rsp_payload->assign(payload_view.data(), payload_view.size());
    return true;
  }
}

VoteResponse TcpTransport::SendVoteRequest(const NodeId& peer_id,
                                           const VoteRequest& request) {
  std::string rsp_payload;
  VoteResponse rsp{request.group_id, 0, false};
  if (!DoRpc(peer_id, RpcType::kVoteReq, SerializeVoteRequest(request),
             RpcType::kVoteRsp, &rsp_payload, vote_timeout_ms_))
    return rsp;
  if (!ParseVoteResponse(rsp_payload, &rsp))
    return VoteResponse{request.group_id, 0, false};
  return rsp;
}

HeartbeatResponse TcpTransport::SendHeartbeat(const NodeId& peer_id,
                                              const HeartbeatRequest& request) {
  std::string rsp_payload;
  HeartbeatResponse rsp{request.group_id, 0, false};
  if (!DoRpc(peer_id, RpcType::kHeartbeatReq, SerializeHeartbeatRequest(request),
             RpcType::kHeartbeatRsp, &rsp_payload, heartbeat_timeout_ms_))
    return rsp;
  if (!ParseHeartbeatResponse(rsp_payload, &rsp))
    return HeartbeatResponse{request.group_id, 0, false};
  return rsp;
}

AppendEntriesResponse TcpTransport::SendAppendEntries(const NodeId& peer_id,
                                                       const AppendEntriesRequest& request) {
  std::string rsp_payload;
  AppendEntriesResponse rsp{request.group_id, 0, false, 0};
  if (!DoRpc(peer_id, RpcType::kAppendEntriesReq, SerializeAppendEntriesRequest(request),
             RpcType::kAppendEntriesRsp, &rsp_payload, append_timeout_ms_))
    return rsp;
  if (!ParseAppendEntriesResponse(rsp_payload, &rsp))
    return AppendEntriesResponse{request.group_id, 0, false, 0};
  return rsp;
}

InstallSnapshotResponse TcpTransport::SendInstallSnapshot(const NodeId& peer_id,
                                                           const InstallSnapshotRequest& request) {
  std::string rsp_payload;
  InstallSnapshotResponse rsp{request.group_id, 0, false};
  if (!DoRpc(peer_id, RpcType::kInstallSnapshotReq, SerializeInstallSnapshotRequest(request),
             RpcType::kInstallSnapshotRsp, &rsp_payload, kSnapshotRpcTimeoutMs))
    return rsp;
  if (!ParseInstallSnapshotResponse(rsp_payload, &rsp))
    return InstallSnapshotResponse{request.group_id, 0, false};
  return rsp;
}

ReadIndexResponse TcpTransport::SendReadIndex(const NodeId& peer_id,
                                              const ReadIndexRequest& request) {
  std::string rsp_payload;
  ReadIndexResponse rsp{request.group_id, 0, false, 0};
  if (!DoRpc(peer_id, RpcType::kReadIndexReq, SerializeReadIndexRequest(request),
             RpcType::kReadIndexRsp, &rsp_payload, rpc_timeout_ms_))
    return rsp;
  if (!ParseReadIndexResponse(rsp_payload, &rsp))
    return ReadIndexResponse{request.group_id, 0, false, 0};
  return rsp;
}

TimeoutNowResponse TcpTransport::SendTimeoutNow(const NodeId& peer_id,
                                                const TimeoutNowRequest& request) {
  std::string rsp_payload;
  TimeoutNowResponse rsp{request.group_id, 0, false};
  if (!DoRpc(peer_id, RpcType::kTimeoutNowReq, SerializeTimeoutNowRequest(request),
             RpcType::kTimeoutNowRsp, &rsp_payload, rpc_timeout_ms_))
    return rsp;
  if (!ParseTimeoutNowResponse(rsp_payload, &rsp))
    return TimeoutNowResponse{request.group_id, 0, false};
  return rsp;
}

// ============================= RaftRpcServer ============================

namespace {

// One accepted Raft RPC connection: reads frames in a loop and dispatches
// them to the local node. All handlers are invoked under the node's RPC
// lifetime guard so a node that is being torn down concurrently can never
// be entered (TryAcquireRpcRef fails once RaftNode::Shutdown has flipped
// rpc_alive_).
class RaftRpcConnection : public util::Connection {
 public:
  explicit RaftRpcConnection(RaftRpcServer::NodeResolver resolver)
      : resolver_(std::move(resolver)) {
  }

 private:
  void HandleRequests() final;

  RaftRpcServer::NodeResolver resolver_;
};

class RaftRpcListener : public util::ListenerInterface {
 public:
  explicit RaftRpcListener(RaftRpcServer::NodeResolver resolver)
      : resolver_(std::move(resolver)) {
  }

  util::Connection* NewConnection(util::fb2::ProactorBase*) final {
    return new RaftRpcConnection(resolver_);
  }

 private:
  // Stored by value (copied per connection): the listener outlives the
  // Service in typical shutdown order (the AcceptServer deletes it after
  // RunEngine returns), but it never invokes the resolver after the
  // connections are closed, so the captured Service pointer is not used
  // beyond its lifetime.
  RaftRpcServer::NodeResolver resolver_;
};

}  // namespace

util::ListenerInterface* RaftRpcServer::CreateListener(NodeResolver resolver) {
  return new RaftRpcListener(std::move(resolver));
}

void RaftRpcConnection::HandleRequests() {
  util::FiberSocketBase* sock = socket();
  while (true) {
    // Header: magic(4) + type(1) + seq(4) + len(4).
    std::string header;
    size_t got = ReadN(sock, kRpcFrameHeaderSize, &header);
    if (got < kRpcFrameHeaderSize) {
      VLOG(2) << "RaftRpc: peer closed connection (EOF on header)";
      break;
    }

    std::string_view len_chunk{header.data() + 9, 4};
    uint32_t payload_len = static_cast<uint32_t>(static_cast<uint8_t>(len_chunk[0])) |
                           (static_cast<uint32_t>(static_cast<uint8_t>(len_chunk[1])) << 8) |
                           (static_cast<uint32_t>(static_cast<uint8_t>(len_chunk[2])) << 16) |
                           (static_cast<uint32_t>(static_cast<uint8_t>(len_chunk[3])) << 24);
    if (payload_len > kMaxFramePayload) {
      LOG(WARNING) << "RaftRpc: oversized request frame (" << payload_len
                   << " bytes), dropping connection";
      break;
    }

    std::string payload, crc_bytes;
    if (ReadN(sock, payload_len, &payload) != payload_len ||
        ReadN(sock, kRpcFrameCrcSize, &crc_bytes) != kRpcFrameCrcSize) {
      VLOG(2) << "RaftRpc: truncated request frame, dropping connection";
      break;
    }

    std::string full_frame = std::move(header);
    full_frame.append(payload);
    full_frame.append(crc_bytes);

    RpcType req_type;
    uint32_t req_seq = 0;
    std::string_view payload_view;
    if (!ParseRpcFrame(full_frame, &req_type, &req_seq, &payload_view)) {
      LOG(WARNING) << "RaftRpc: corrupt (CRC/magic) request frame, dropping connection";
      break;
    }

    // Parse the request, resolve the node by group id and dispatch. Every
    // handler returns its response struct; we encode it and write exactly
    // one response frame per request.
    std::string rsp_payload;
    RpcType rsp_type;
    bool dispatch_ok = false;

    switch (req_type) {
      case RpcType::kVoteReq: {
        VoteRequest req;
        if (!ParseVoteRequest(payload_view, &req))
          break;
        VoteResponse rsp{req.group_id, 0, false};
        RaftNode* node = resolver_(req.group_id);
        if (node && node->TryAcquireRpcRef()) {
          rsp = node->OnRequestVote(req);
          node->ReleaseRpcRef();
        }
        rsp_payload = SerializeVoteResponse(rsp);
        rsp_type = RpcType::kVoteRsp;
        dispatch_ok = true;
        break;
      }
      case RpcType::kHeartbeatReq: {
        HeartbeatRequest req;
        if (!ParseHeartbeatRequest(payload_view, &req))
          break;
        HeartbeatResponse rsp{req.group_id, 0, false};
        RaftNode* node = resolver_(req.group_id);
        if (node && node->TryAcquireRpcRef()) {
          rsp = node->OnHeartbeat(req);
          node->ReleaseRpcRef();
        }
        rsp_payload = SerializeHeartbeatResponse(rsp);
        rsp_type = RpcType::kHeartbeatRsp;
        dispatch_ok = true;
        break;
      }
      case RpcType::kAppendEntriesReq: {
        AppendEntriesRequest req;
        if (!ParseAppendEntriesRequest(payload_view, &req))
          break;
        AppendEntriesResponse rsp{req.group_id, 0, false, 0};
        RaftNode* node = resolver_(req.group_id);
        if (node && node->TryAcquireRpcRef()) {
          rsp = node->OnAppendEntries(req);
          node->ReleaseRpcRef();
        }
        rsp_payload = SerializeAppendEntriesResponse(rsp);
        rsp_type = RpcType::kAppendEntriesRsp;
        dispatch_ok = true;
        break;
      }
      case RpcType::kInstallSnapshotReq: {
        InstallSnapshotRequest req;
        if (!ParseInstallSnapshotRequest(payload_view, &req))
          break;
        InstallSnapshotResponse rsp{req.group_id, 0, false};
        RaftNode* node = resolver_(req.group_id);
        if (node && node->TryAcquireRpcRef()) {
          rsp = node->OnInstallSnapshot(req);
          node->ReleaseRpcRef();
        }
        rsp_payload = SerializeInstallSnapshotResponse(rsp);
        rsp_type = RpcType::kInstallSnapshotRsp;
        dispatch_ok = true;
        break;
      }
      case RpcType::kReadIndexReq: {
        ReadIndexRequest req;
        if (!ParseReadIndexRequest(payload_view, &req))
          break;
        ReadIndexResponse rsp{req.group_id, 0, false, 0};
        RaftNode* node = resolver_(req.group_id);
        if (node && node->TryAcquireRpcRef()) {
          rsp = node->OnReadIndex(req);
          node->ReleaseRpcRef();
        }
        rsp_payload = SerializeReadIndexResponse(rsp);
        rsp_type = RpcType::kReadIndexRsp;
        dispatch_ok = true;
        break;
      }
      case RpcType::kTimeoutNowReq: {
        TimeoutNowRequest req;
        if (!ParseTimeoutNowRequest(payload_view, &req))
          break;
        TimeoutNowResponse rsp{req.group_id, 0, false};
        RaftNode* node = resolver_(req.group_id);
        if (node && node->TryAcquireRpcRef()) {
          rsp = node->OnTimeoutNow(req);
          node->ReleaseRpcRef();
        }
        rsp_payload = SerializeTimeoutNowResponse(rsp);
        rsp_type = RpcType::kTimeoutNowRsp;
        dispatch_ok = true;
        break;
      }
      default:
        LOG(WARNING) << "RaftRpc: unknown request type "
                     << static_cast<int>(static_cast<uint8_t>(req_type));
        break;
    }

    if (!dispatch_ok) {
      LOG(WARNING) << "RaftRpc: undispatchable request (type "
                   << static_cast<int>(static_cast<uint8_t>(req_type))
                   << "), dropping connection";
      break;
    }

    // Echo the request's seq so the client can correlate the response and
    // skip late frames of previously timed-out requests.
    std::string rsp_frame = EncodeRpcFrame(rsp_type, req_seq, rsp_payload);
    std::error_code wc = sock->Write(io::Bytes{
        reinterpret_cast<const uint8_t*>(rsp_frame.data()), rsp_frame.size()});
    if (wc) {
      VLOG(2) << "RaftRpc: write failed (" << wc.message() << "), closing";
      break;
    }
  }
}

}  // namespace dfly
