// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/command_log.h"

#include <gmock/gmock.h>

#include "base/gtest.h"

namespace dfly {

using namespace testing;

class CommandLogTest : public Test {
};

TEST_F(CommandLogTest, Empty) {
  CommandLog log;
  EXPECT_EQ(0u, log.Size());
  EXPECT_EQ(0u, log.LastIndex());
}

TEST_F(CommandLogTest, AppendSingle) {
  CommandLog log;
  log.Append(LogEntry(1, 0, "SET a 1"));
  EXPECT_EQ(1u, log.Size());
  EXPECT_EQ(1u, log.LastIndex());
}

TEST_F(CommandLogTest, GetEntry) {
  CommandLog log;
  log.Append(LogEntry(1, 0, "SET a 1"));
  const auto& entry = log.Get(1);
  EXPECT_EQ(1u, entry.term);
  EXPECT_EQ(1u, entry.index);
  EXPECT_EQ("SET a 1", entry.command);
}

TEST_F(CommandLogTest, IndexAutoAssignment) {
  CommandLog log;
  log.Append(LogEntry(1, 999, "a"));
  log.Append(LogEntry(1, 888, "b"));
  log.Append(LogEntry(2, 777, "c"));

  EXPECT_EQ(3u, log.Size());
  EXPECT_EQ(3u, log.LastIndex());
  EXPECT_EQ(1u, log.Get(1).index);
  EXPECT_EQ(2u, log.Get(2).index);
  EXPECT_EQ(3u, log.Get(3).index);
}

TEST_F(CommandLogTest, SequentialAppend) {
  CommandLog log;
  int n = 100;
  for (int i = 1; i <= n; i++) {
    log.Append(LogEntry(1, 0, "cmd" + std::to_string(i)));
  }

  EXPECT_EQ(100u, log.Size());
  EXPECT_EQ(100u, log.LastIndex());
  EXPECT_EQ(1u, log.Get(1).index);
  EXPECT_EQ(50u, log.Get(50).index);
  EXPECT_EQ(100u, log.Get(100).index);
  EXPECT_EQ("cmd50", log.Get(50).command);
}

TEST_F(CommandLogTest, GetPreservesTerms) {
  CommandLog log;
  log.Append(LogEntry(3, 0, "x"));
  log.Append(LogEntry(4, 0, "y"));

  EXPECT_EQ(3u, log.Get(1).term);
  EXPECT_EQ(4u, log.Get(2).term);
}

}  // namespace dfly
