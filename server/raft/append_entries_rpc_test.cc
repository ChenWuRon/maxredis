// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/append_entries_rpc.h"

#include <gmock/gmock.h>

#include "base/gtest.h"

namespace dfly {

using namespace testing;

class AppendEntriesRpcTest : public Test {
};

TEST_F(AppendEntriesRpcTest, RequestDefault) {
  AppendEntriesRequest req;
  EXPECT_EQ(0u, req.term);
  EXPECT_TRUE(req.leader_id.empty());
  EXPECT_EQ(0u, req.prev_log_index);
  EXPECT_EQ(0u, req.prev_log_term);
  EXPECT_TRUE(req.entries.empty());
  EXPECT_EQ(0u, req.leader_commit);
}

TEST_F(AppendEntriesRpcTest, RequestWithEntries) {
  AppendEntriesRequest req;
  req.term = 3;
  req.leader_id = "L1";
  req.prev_log_index = 5;
  req.prev_log_term = 2;
  req.entries = {LogEntry{3, 6, "SET a 1"}, LogEntry{3, 7, "SET b 2"}};
  req.leader_commit = 0;

  EXPECT_EQ(3u, req.term);
  EXPECT_EQ("L1", req.leader_id);
  EXPECT_EQ(2u, req.entries.size());
  EXPECT_EQ("SET a 1", req.entries[0].command);
}

TEST_F(AppendEntriesRpcTest, ResponseDefault) {
  AppendEntriesResponse rsp;
  EXPECT_EQ(0u, rsp.term);
  EXPECT_FALSE(rsp.success);
  EXPECT_EQ(0u, rsp.last_log_index);
}

TEST_F(AppendEntriesRpcTest, ResponseFields) {
  AppendEntriesResponse rsp{5, true, 10};
  EXPECT_EQ(5u, rsp.term);
  EXPECT_TRUE(rsp.success);
  EXPECT_EQ(10u, rsp.last_log_index);
}

TEST_F(AppendEntriesRpcTest, RequestEquality) {
  AppendEntriesRequest a{3, "L1", 5, 2, {LogEntry{3, 6, "x"}}, 0};
  AppendEntriesRequest b{3, "L1", 5, 2, {LogEntry{3, 6, "x"}}, 0};
  AppendEntriesRequest c{3, "L1", 5, 2, {LogEntry{3, 6, "y"}}, 0};
  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
}

TEST_F(AppendEntriesRpcTest, ResponseEquality) {
  AppendEntriesResponse a{4, true, 10}, b{4, true, 10}, c{4, false, 10};
  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
}

}  // namespace dfly
