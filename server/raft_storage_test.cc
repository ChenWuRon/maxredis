// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_storage.h"

#include <gmock/gmock.h>

#include "base/gtest.h"

namespace dfly {

using namespace testing;

// RaftStorage now stores only persistent metadata (term, voted_for).
// Log entries are managed via ILogStorage (see command_log_test.cc).

class RaftStorageTest : public Test {
};

TEST_F(RaftStorageTest, DefaultState) {
  RaftStorage s;
  EXPECT_EQ(0u, s.current_term());
  EXPECT_TRUE(s.voted_for().empty());
}

TEST_F(RaftStorageTest, CurrentTerm) {
  RaftStorage s;
  EXPECT_EQ(0u, s.current_term());
  s.set_current_term(3);
  EXPECT_EQ(3u, s.current_term());
}

TEST_F(RaftStorageTest, CurrentTermMonotonic) {
  RaftStorage s;
  s.set_current_term(5);
  s.set_current_term(10);
  EXPECT_EQ(10u, s.current_term());
}

TEST_F(RaftStorageTest, VotedFor) {
  RaftStorage s;
  EXPECT_TRUE(s.voted_for().empty());
  s.set_voted_for("node1");
  EXPECT_EQ("node1", s.voted_for());
}

TEST_F(RaftStorageTest, VotedForOverride) {
  RaftStorage s;
  s.set_voted_for("node1");
  s.set_voted_for("node2");
  EXPECT_EQ("node2", s.voted_for());
}

TEST_F(RaftStorageTest, Clear) {
  RaftStorage s;
  s.set_current_term(5);
  s.set_voted_for("node2");

  s.Clear();
  EXPECT_EQ(0u, s.current_term());
  EXPECT_TRUE(s.voted_for().empty());
}

}  // namespace dfly
