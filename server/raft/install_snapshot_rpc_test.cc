// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/install_snapshot_rpc.h"

#include <gmock/gmock.h>

#include "base/gtest.h"

namespace dfly {

using namespace testing;

class InstallSnapshotRpcTest : public Test {
};

TEST_F(InstallSnapshotRpcTest, RequestDefault) {
  InstallSnapshotRequest req;
  EXPECT_EQ(0u, req.term);
  EXPECT_TRUE(req.leader_id.empty());
  EXPECT_EQ(0u, req.last_included_index);
  EXPECT_EQ(0u, req.last_included_term);
  EXPECT_EQ(0u, req.offset);
  EXPECT_FALSE(req.done);
  EXPECT_TRUE(req.data.empty());
}

TEST_F(InstallSnapshotRpcTest, RequestFields) {
  InstallSnapshotRequest req;
  req.term = 3;
  req.leader_id = "L1";
  req.last_included_index = 100;
  req.last_included_term = 2;
  req.offset = 4096;
  req.done = true;
  req.data = "chunk_data";

  EXPECT_EQ(3u, req.term);
  EXPECT_EQ("L1", req.leader_id);
  EXPECT_EQ(100u, req.last_included_index);
  EXPECT_EQ(2u, req.last_included_term);
  EXPECT_EQ(4096u, req.offset);
  EXPECT_TRUE(req.done);
  EXPECT_EQ("chunk_data", req.data);
}

TEST_F(InstallSnapshotRpcTest, RequestPartialChunk) {
  InstallSnapshotRequest req;
  req.term = 3;
  req.leader_id = "L1";
  req.last_included_index = 100;
  req.last_included_term = 2;
  req.offset = 0;
  req.done = false;
  req.data = "first_chunk";

  EXPECT_FALSE(req.done);
  EXPECT_EQ(0u, req.offset);
}

TEST_F(InstallSnapshotRpcTest, ResponseDefault) {
  InstallSnapshotResponse rsp;
  EXPECT_EQ(0u, rsp.term);
  EXPECT_FALSE(rsp.success);
}

TEST_F(InstallSnapshotRpcTest, ResponseFields) {
  InstallSnapshotResponse rsp{0, 5, true};
  EXPECT_EQ(5u, rsp.term);
  EXPECT_TRUE(rsp.success);
}

TEST_F(InstallSnapshotRpcTest, RequestEquality) {
  InstallSnapshotRequest a{0, 3, "L1", 100, 2, 0, false, "data"};
  InstallSnapshotRequest b{0, 3, "L1", 100, 2, 0, false, "data"};
  InstallSnapshotRequest c{0, 3, "L1", 100, 2, 0, false, "other"};
  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
}

TEST_F(InstallSnapshotRpcTest, ResponseEquality) {
  InstallSnapshotResponse a{0, 4, true}, b{0, 4, true}, c{0, 4, false};
  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
}

TEST_F(InstallSnapshotRpcTest, LargeDataField) {
  InstallSnapshotRequest req;
  req.term = 3;
  req.last_included_index = 500;
  req.last_included_term = 2;
  req.offset = 0;
  req.done = false;
  req.data = std::string(65536, 'x');  // 64KB chunk

  EXPECT_EQ(65536u, req.data.size());
  EXPECT_FALSE(req.done);
}

}  // namespace dfly
