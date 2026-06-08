// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_storage.h"

#include <gmock/gmock.h>

#include "base/gtest.h"

namespace dfly {

using namespace testing;

class RaftStorageTest : public Test {
};

TEST_F(RaftStorageTest, Empty) {
  RaftStorage s;
  EXPECT_EQ(0u, s.current_term());
  EXPECT_TRUE(s.voted_for().empty());
  EXPECT_EQ(0u, s.LogSize());
  EXPECT_EQ(0u, s.LastLogIndex());
}

TEST_F(RaftStorageTest, AppendLog) {
  RaftStorage s;
  s.AppendLog(LogEntry(1, 0, "SET a 1"));
  EXPECT_EQ(1u, s.LogSize());
  EXPECT_EQ(1u, s.LastLogIndex());
  EXPECT_EQ(1u, s.LastLogTerm());
  EXPECT_EQ("SET a 1", s.EntryAt(1).command);
}

TEST_F(RaftStorageTest, AppendLogIndexAssignment) {
  RaftStorage s;
  s.AppendLog(LogEntry(1, 999, "x"));
  s.AppendLog(LogEntry(1, 888, "y"));
  s.AppendLog(LogEntry(2, 777, "z"));

  EXPECT_EQ(3u, s.LogSize());
  EXPECT_EQ(1u, s.EntryAt(1).index);
  EXPECT_EQ(2u, s.EntryAt(2).index);
  EXPECT_EQ(3u, s.EntryAt(3).index);
}

TEST_F(RaftStorageTest, ReadLogAll) {
  RaftStorage s;
  s.AppendLog(LogEntry(1, 0, "a"));
  s.AppendLog(LogEntry(1, 0, "b"));
  s.AppendLog(LogEntry(2, 0, "c"));

  auto entries = s.ReadLog(1);
  ASSERT_EQ(3u, entries.size());
  EXPECT_EQ("a", entries[0].command);
  EXPECT_EQ("b", entries[1].command);
  EXPECT_EQ("c", entries[2].command);
}

TEST_F(RaftStorageTest, ReadLogLimited) {
  RaftStorage s;
  s.AppendLog(LogEntry(1, 0, "a"));
  s.AppendLog(LogEntry(1, 0, "b"));
  s.AppendLog(LogEntry(2, 0, "c"));
  s.AppendLog(LogEntry(2, 0, "d"));

  auto entries = s.ReadLog(2, 2);
  ASSERT_EQ(2u, entries.size());
  EXPECT_EQ("b", entries[0].command);
  EXPECT_EQ("c", entries[1].command);
}

TEST_F(RaftStorageTest, CurrentTerm) {
  RaftStorage s;
  EXPECT_EQ(0u, s.current_term());
  s.set_current_term(3);
  EXPECT_EQ(3u, s.current_term());
}

TEST_F(RaftStorageTest, VotedFor) {
  RaftStorage s;
  EXPECT_TRUE(s.voted_for().empty());
  s.set_voted_for("node1");
  EXPECT_EQ("node1", s.voted_for());
}

TEST_F(RaftStorageTest, TruncateSuffix) {
  RaftStorage s;
  s.AppendLog(LogEntry(1, 0, "a"));
  s.AppendLog(LogEntry(1, 0, "b"));
  s.AppendLog(LogEntry(2, 0, "c"));
  EXPECT_EQ(3u, s.LogSize());

  s.TruncateSuffix(1);
  EXPECT_EQ(1u, s.LogSize());
  EXPECT_EQ("a", s.EntryAt(1).command);
}

TEST_F(RaftStorageTest, Clear) {
  RaftStorage s;
  s.set_current_term(5);
  s.set_voted_for("node2");
  s.AppendLog(LogEntry(1, 0, "x"));
  s.AppendLog(LogEntry(2, 0, "y"));

  s.Clear();
  EXPECT_EQ(0u, s.current_term());
  EXPECT_TRUE(s.voted_for().empty());
  EXPECT_EQ(0u, s.LogSize());
}

TEST_F(RaftStorageTest, AppendLogBatch) {
  RaftStorage s;
  std::vector<LogEntry> batch;
  batch.emplace_back(1, 0, "a");
  batch.emplace_back(1, 0, "b");
  batch.emplace_back(2, 0, "c");

  s.AppendLog(batch);
  EXPECT_EQ(3u, s.LogSize());
  EXPECT_EQ(1u, s.EntryAt(1).index);
  EXPECT_EQ(3u, s.EntryAt(3).index);
  EXPECT_EQ("c", s.EntryAt(3).command);
}

TEST_F(RaftStorageTest, LastLogTerm) {
  RaftStorage s;
  s.AppendLog(LogEntry(5, 0, "x"));
  EXPECT_EQ(5u, s.LastLogTerm());
  s.AppendLog(LogEntry(7, 0, "y"));
  EXPECT_EQ(7u, s.LastLogTerm());
}

}  // namespace dfly
