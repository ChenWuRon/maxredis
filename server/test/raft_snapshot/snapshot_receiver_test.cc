// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/snapshot_receiver.h"

#include <gmock/gmock.h>

#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include "base/gtest.h"
#include "server/raft/install_snapshot_rpc.h"

namespace dfly {

using namespace testing;

class SnapshotReceiverTest : public Test {
 protected:
  void SetUp() override {
    dir_ = "/tmp/snapshot_recv_test_" + std::to_string(getpid()) + "/";
    mkdir(dir_.c_str(), 0755);
    receiver_ = std::make_unique<SnapshotReceiver>(dir_);
    receiver_->Init();
  }

  void TearDown() override {
    receiver_.reset();
    unlink((dir_ + "snapshot.recv.tmp").c_str());
    unlink((dir_ + "snapshot.bin").c_str());
    rmdir(dir_.c_str());
  }

  std::string ReadFile(const std::string& path) {
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs)
      return {};
    return std::string((std::istreambuf_iterator<char>(ifs)),
                        std::istreambuf_iterator<char>());
  }

  std::string dir_;
  std::unique_ptr<SnapshotReceiver> receiver_;
};

TEST_F(SnapshotReceiverTest, StaleTmpCleanedOnInit) {
  // Create a stale tmp file.
  {
    std::ofstream ofs(dir_ + "snapshot.recv.tmp", std::ios::binary);
    ofs << "stale_data";
  }
  EXPECT_TRUE(std::ifstream(dir_ + "snapshot.recv.tmp").good());

  // Re-init should clean it up.
  receiver_->Init();
  EXPECT_FALSE(std::ifstream(dir_ + "snapshot.recv.tmp").good());
}

TEST_F(SnapshotReceiverTest, SingleChunk) {
  InstallSnapshotRequest req;
  req.term = 1;
  req.leader_id = "L1";
  req.last_included_index = 100;
  req.last_included_term = 2;
  req.offset = 0;
  req.done = true;
  req.data = "snapshot_data";

  auto rsp = receiver_->HandleChunk(req);
  EXPECT_TRUE(rsp.success);
  EXPECT_EQ(1u, rsp.term);

  // Verify snapshot.bin was created.
  auto content = ReadFile(dir_ + "snapshot.bin");
  EXPECT_EQ("snapshot_data", content);

  // tmp should be gone.
  EXPECT_FALSE(std::ifstream(dir_ + "snapshot.recv.tmp").good());
}

TEST_F(SnapshotReceiverTest, MultipleChunks) {
  auto send_chunk = [&](uint64_t offset, bool done, const std::string& data) {
    InstallSnapshotRequest req;
    req.term = 1;
    req.leader_id = "L1";
    req.last_included_index = 200;
    req.last_included_term = 3;
    req.offset = offset;
    req.done = done;
    req.data = data;
    return receiver_->HandleChunk(req);
  };

  InstallSnapshotResponse rsp;

  rsp = send_chunk(0, false, "hello_");
  EXPECT_TRUE(rsp.success);

  rsp = send_chunk(6, false, "world_");
  EXPECT_TRUE(rsp.success);

  rsp = send_chunk(12, true, "final");
  EXPECT_TRUE(rsp.success);

  auto content = ReadFile(dir_ + "snapshot.bin");
  EXPECT_EQ("hello_world_final", content);
}

TEST_F(SnapshotReceiverTest, LargeSnapshot16Chunks) {
  // 1MB = 16 chunks of 64KB = 65536 bytes each.
  constexpr size_t kChunkSize = 65536;
  constexpr size_t kNumChunks = 16;
  constexpr size_t kTotalSize = kChunkSize * kNumChunks;

  InstallSnapshotResponse rsp;
  for (size_t i = 0; i < kNumChunks; i++) {
    InstallSnapshotRequest req;
    req.term = 1;
    req.leader_id = "L1";
    req.last_included_index = 500;
    req.last_included_term = 4;
    req.offset = i * kChunkSize;
    req.done = (i == kNumChunks - 1);
    req.data = std::string(kChunkSize, 'a' + (i % 26));
    rsp = receiver_->HandleChunk(req);
    EXPECT_TRUE(rsp.success) << "chunk " << i;
  }

  auto content = ReadFile(dir_ + "snapshot.bin");
  EXPECT_EQ(kTotalSize, content.size());

  // Verify each chunk's data.
  for (size_t i = 0; i < kNumChunks; i++) {
    std::string expected(kChunkSize, 'a' + (i % 26));
    EXPECT_EQ(expected, content.substr(i * kChunkSize, kChunkSize));
  }
}

TEST_F(SnapshotReceiverTest, RejectStaleTerm) {
  // First chunk with term 2.
  InstallSnapshotRequest req;
  req.term = 2;
  req.leader_id = "L1";
  req.last_included_index = 100;
  req.last_included_term = 1;
  req.offset = 0;
  req.done = true;
  req.data = "valid";

  auto rsp = receiver_->HandleChunk(req);
  EXPECT_TRUE(rsp.success);
  EXPECT_EQ(2u, rsp.term);
}

}  // namespace dfly
