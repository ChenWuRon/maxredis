// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/vote_rpc.h"

#include <gmock/gmock.h>

#include "base/gtest.h"

namespace dfly {

using namespace testing;

class VoteRpcTest : public Test {
};

TEST_F(VoteRpcTest, VoteRequestDefault) {
  VoteRequest req;
  EXPECT_EQ(0u, req.term);
  EXPECT_TRUE(req.candidate_id.empty());
  EXPECT_EQ(0u, req.last_log_index);
  EXPECT_EQ(0u, req.last_log_term);
}

TEST_F(VoteRpcTest, VoteRequestFields) {
  VoteRequest req;
  req.term = 10;
  req.candidate_id = "node1";
  req.last_log_index = 100;
  req.last_log_term = 9;

  EXPECT_EQ(10u, req.term);
  EXPECT_EQ("node1", req.candidate_id);
  EXPECT_EQ(100u, req.last_log_index);
  EXPECT_EQ(9u, req.last_log_term);
}

TEST_F(VoteRpcTest, VoteRequestAggregateInit) {
  VoteRequest req{10, "node2", 200, 8};
  EXPECT_EQ(10u, req.term);
  EXPECT_EQ("node2", req.candidate_id);
  EXPECT_EQ(200u, req.last_log_index);
  EXPECT_EQ(8u, req.last_log_term);
}

TEST_F(VoteRpcTest, VoteResponseDefault) {
  VoteResponse rsp;
  EXPECT_EQ(0u, rsp.term);
  EXPECT_FALSE(rsp.vote_granted);
}

TEST_F(VoteRpcTest, VoteResponseFields) {
  VoteResponse rsp{5, true};
  EXPECT_EQ(5u, rsp.term);
  EXPECT_TRUE(rsp.vote_granted);
}

TEST_F(VoteRpcTest, VoteResponseAggregateInit) {
  VoteResponse rsp{7, false};
  EXPECT_EQ(7u, rsp.term);
  EXPECT_FALSE(rsp.vote_granted);
}

TEST_F(VoteRpcTest, VoteRequestCopy) {
  VoteRequest a{3, "node_a", 50, 2};
  VoteRequest b = a;
  EXPECT_EQ(a, b);
}

TEST_F(VoteRpcTest, VoteRequestMove) {
  VoteRequest a{4, "node_b", 60, 3};
  VoteRequest b = std::move(a);
  EXPECT_EQ(4u, b.term);
  EXPECT_EQ("node_b", b.candidate_id);
  EXPECT_EQ(60u, b.last_log_index);
  EXPECT_EQ(3u, b.last_log_term);
}

TEST_F(VoteRpcTest, VoteResponseCopy) {
  VoteResponse a{8, true};
  VoteResponse b = a;
  EXPECT_EQ(a, b);
}

TEST_F(VoteRpcTest, VoteResponseMove) {
  VoteResponse a{9, false};
  VoteResponse b = std::move(a);
  EXPECT_EQ(9u, b.term);
  EXPECT_FALSE(b.vote_granted);
}

TEST_F(VoteRpcTest, VoteRequestEquality) {
  VoteRequest a{2, "x", 10, 1};
  VoteRequest b{2, "x", 10, 1};
  VoteRequest c{3, "x", 10, 1};
  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
}

TEST_F(VoteRpcTest, VoteResponseEquality) {
  VoteResponse a{4, true};
  VoteResponse b{4, true};
  VoteResponse c{4, false};
  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
}

}  // namespace dfly
