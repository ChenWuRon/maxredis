// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/snapshot_sender.h"

#include <gmock/gmock.h>

#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include "base/gtest.h"
#include "server/raft/install_snapshot_rpc.h"
#include "server/raft/read_index_rpc.h"
#include "server/raft/timeout_now_rpc.h"
#include "server/raft/transport.h"

namespace dfly {

using namespace testing;

// A test transport that records all InstallSnapshot requests.
class TestSnapshotTransport : public Transport {
 public:
  // Return values for the next SendInstallSnapshot call.
  InstallSnapshotResponse next_response{0, 1, true};

  InstallSnapshotResponse SendInstallSnapshot(const NodeId& peer_id,
                                               const InstallSnapshotRequest& request) override {
    last_peer = peer_id;
    requests.push_back(request);
    return next_response;
  }

  VoteResponse SendVoteRequest(const NodeId&, const VoteRequest&) override {
    return {};
  }
  HeartbeatResponse SendHeartbeat(const NodeId&, const HeartbeatRequest&) override {
    return {};
  }
  AppendEntriesResponse SendAppendEntries(const NodeId&, const AppendEntriesRequest&) override {
    return {};
  }

  ReadIndexResponse SendReadIndex(const NodeId&, const ReadIndexRequest&) override {
    return {};
  }

  TimeoutNowResponse SendTimeoutNow(const NodeId&, const TimeoutNowRequest&) override {
    return {};
  }

  NodeId last_peer;
  std::vector<InstallSnapshotRequest> requests;
};

class SnapshotSenderTest : public Test {
 protected:
  void SetUp() override {
    path_ = "/tmp/snapshot_sender_test_" + std::to_string(getpid()) + ".bin";
  }

  void TearDown() override {
    unlink(path_.c_str());
  }

  // Creates a snapshot.bin with the given content.
  void CreateSnapshot(const std::string& content) {
    std::ofstream ofs(path_, std::ios::binary);
    ofs.write(content.data(), content.size());
  }

  std::string path_;
  TestSnapshotTransport transport_;
};

TEST_F(SnapshotSenderTest, SingleChunkSmallSnapshot) {
  CreateSnapshot("small_snapshot_data");

  SnapshotSender sender(path_, &transport_);
  bool ok = sender.SendSnapshot("F1", 0, 1, "L1", 100, 2);
  EXPECT_TRUE(ok);

  ASSERT_EQ(1u, transport_.requests.size());
  EXPECT_EQ(1u, transport_.requests[0].term);
  EXPECT_EQ("L1", transport_.requests[0].leader_id);
  EXPECT_EQ(100u, transport_.requests[0].last_included_index);
  EXPECT_EQ(2u, transport_.requests[0].last_included_term);
  EXPECT_EQ(0u, transport_.requests[0].offset);
  EXPECT_TRUE(transport_.requests[0].done);
  EXPECT_EQ("small_snapshot_data", transport_.requests[0].data);
  EXPECT_EQ("F1", transport_.last_peer);
}

TEST_F(SnapshotSenderTest, MultipleChunks) {
  // Create a snapshot larger than kChunkSize.
  std::string data(SnapshotSender::kChunkSize + 100, 'x');
  CreateSnapshot(data);

  SnapshotSender sender(path_, &transport_);
  bool ok = sender.SendSnapshot("F2", 0, 2, "L1", 200, 3);
  EXPECT_TRUE(ok);

  // Should send 2 chunks.
  ASSERT_EQ(2u, transport_.requests.size());

  // First chunk: full size, not done.
  EXPECT_EQ(0u, transport_.requests[0].offset);
  EXPECT_EQ(SnapshotSender::kChunkSize, transport_.requests[0].data.size());
  EXPECT_FALSE(transport_.requests[0].done);
  EXPECT_EQ(data.substr(0, SnapshotSender::kChunkSize), transport_.requests[0].data);

  // Second chunk: remaining 100 bytes, done.
  EXPECT_EQ(SnapshotSender::kChunkSize, transport_.requests[1].offset);
  EXPECT_EQ(100u, transport_.requests[1].data.size());
  EXPECT_TRUE(transport_.requests[1].done);
  EXPECT_EQ(data.substr(SnapshotSender::kChunkSize), transport_.requests[1].data);
}

TEST_F(SnapshotSenderTest, ExactlyOneChunk) {
  // Create a snapshot exactly equal to kChunkSize.
  std::string data(SnapshotSender::kChunkSize, 'y');
  CreateSnapshot(data);

  SnapshotSender sender(path_, &transport_);
  bool ok = sender.SendSnapshot("F3", 0, 3, "L1", 300, 4);
  EXPECT_TRUE(ok);

  ASSERT_EQ(1u, transport_.requests.size());
  EXPECT_EQ(0u, transport_.requests[0].offset);
  EXPECT_EQ(SnapshotSender::kChunkSize, transport_.requests[0].data.size());
  EXPECT_TRUE(transport_.requests[0].done);
}

TEST_F(SnapshotSenderTest, EmptySnapshot) {
  CreateSnapshot("");

  SnapshotSender sender(path_, &transport_);
  bool ok = sender.SendSnapshot("F4", 0, 4, "L1", 400, 5);
  EXPECT_TRUE(ok);

  // No chunks should be sent.
  EXPECT_EQ(0u, transport_.requests.size());
}

TEST_F(SnapshotSenderTest, StopsOnRejectedChunk) {
  std::string data(SnapshotSender::kChunkSize * 2 + 50, 'z');
  CreateSnapshot(data);

  transport_.next_response = {0, 1, false};  // reject first chunk

  SnapshotSender sender(path_, &transport_);
  bool ok = sender.SendSnapshot("F5", 0, 1, "L1", 500, 6);
  EXPECT_FALSE(ok);

  // Only first chunk should have been sent.
  ASSERT_EQ(1u, transport_.requests.size());
  EXPECT_EQ(0u, transport_.requests[0].offset);
}

TEST_F(SnapshotSenderTest, StopsOnHigherTerm) {
  std::string data(SnapshotSender::kChunkSize + 1, 'w');
  CreateSnapshot(data);

  transport_.next_response = {0, 5, true};  // follower has higher term 5 > term 1

  SnapshotSender sender(path_, &transport_);
  bool ok = sender.SendSnapshot("F6", 0, 1, "L1", 600, 7);
  EXPECT_FALSE(ok);

  // Only first chunk should have been sent.
  ASSERT_EQ(1u, transport_.requests.size());
}

TEST_F(SnapshotSenderTest, LargeSnapshot16Chunks) {
  // 1MB = 16 chunks of 64KB.
  constexpr size_t kNumChunks = 16;
  constexpr size_t kTotalSize = SnapshotSender::kChunkSize * kNumChunks;
  std::string data(kTotalSize, 'a');
  CreateSnapshot(data);

  SnapshotSender sender(path_, &transport_);
  bool ok = sender.SendSnapshot("F7", 0, 1, "L1", 700, 8);
  EXPECT_TRUE(ok);

  ASSERT_EQ(kNumChunks, transport_.requests.size());

  // All chunks except last should have done=false.
  for (size_t i = 0; i < kNumChunks; i++) {
    EXPECT_EQ(i * SnapshotSender::kChunkSize, transport_.requests[i].offset);
    EXPECT_EQ(SnapshotSender::kChunkSize, transport_.requests[i].data.size());
    EXPECT_EQ(i == kNumChunks - 1, transport_.requests[i].done);
    EXPECT_EQ(data.substr(i * SnapshotSender::kChunkSize, SnapshotSender::kChunkSize),
              transport_.requests[i].data);
  }
}

TEST_F(SnapshotSenderTest, FileNotFound) {
  SnapshotSender sender("/nonexistent/path.bin", &transport_);
  bool ok = sender.SendSnapshot("F8", 0, 1, "L1", 0, 0);
  EXPECT_FALSE(ok);
  EXPECT_EQ(0u, transport_.requests.size());
}

}  // namespace dfly
