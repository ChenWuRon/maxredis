// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/heartbeat_rpc.h"

#include <gmock/gmock.h>

#include "base/gtest.h"

namespace dfly {

using namespace testing;

class HeartbeatRpcTest : public Test {
};

TEST_F(HeartbeatRpcTest, HeartbeatRequestDefault) {
  HeartbeatRequest req;
  EXPECT_EQ(0u, req.term);
  EXPECT_TRUE(req.leader_id.empty());
}

TEST_F(HeartbeatRpcTest, HeartbeatRequestFields) {
  HeartbeatRequest req{0, 5, "node1"};
  EXPECT_EQ(5u, req.term);
  EXPECT_EQ("node1", req.leader_id);
}

TEST_F(HeartbeatRpcTest, HeartbeatResponseDefault) {
  HeartbeatResponse rsp;
  EXPECT_EQ(0u, rsp.term);
  EXPECT_FALSE(rsp.success);
}

TEST_F(HeartbeatRpcTest, HeartbeatResponseFields) {
  HeartbeatResponse rsp{0, 3, true};
  EXPECT_EQ(3u, rsp.term);
  EXPECT_TRUE(rsp.success);
}

TEST_F(HeartbeatRpcTest, HeartbeatRequestEquality) {
  HeartbeatRequest a{0, 2, "x"}, b{0, 2, "x"}, c{0, 3, "x"};
  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
}

TEST_F(HeartbeatRpcTest, HeartbeatResponseEquality) {
  HeartbeatResponse a{0, 4, true}, b{0, 4, true}, c{0, 4, false};
  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
}

}  // namespace dfly
