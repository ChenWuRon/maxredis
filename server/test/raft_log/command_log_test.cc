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
  EXPECT_EQ(0u, log.LogSize());
  EXPECT_EQ(0u, log.LastIndex());
  EXPECT_EQ(0u, log.Size());
}

TEST_F(CommandLogTest, AppendSingle) {
  CommandLog log;
  log.Append(LogEntry(1, 0, "SET a 1"));
  EXPECT_EQ(1u, log.LogSize());
  EXPECT_EQ(1u, log.LastIndex());
}

TEST_F(CommandLogTest, GetEntry) {
  CommandLog log;
  log.Append(LogEntry(1, 0, "SET a 1"));
  const auto* entry = log.Get(1);
  ASSERT_NE(nullptr, entry);
  EXPECT_EQ(1u, entry->term);
  EXPECT_EQ(1u, entry->index);
  EXPECT_EQ("SET a 1", entry->command);
}

TEST_F(CommandLogTest, IndexAutoAssignment) {
  CommandLog log;
  log.Append(LogEntry(1, 999, "a"));
  log.Append(LogEntry(1, 888, "b"));
  log.Append(LogEntry(2, 777, "c"));

  EXPECT_EQ(3u, log.LogSize());
  EXPECT_EQ(3u, log.LastIndex());
  ASSERT_NE(nullptr, log.Get(1));
  EXPECT_EQ(1u, log.Get(1)->index);
  ASSERT_NE(nullptr, log.Get(2));
  EXPECT_EQ(2u, log.Get(2)->index);
  ASSERT_NE(nullptr, log.Get(3));
  EXPECT_EQ(3u, log.Get(3)->index);
}

TEST_F(CommandLogTest, SequentialAppend) {
  CommandLog log;
  int n = 100;
  for (int i = 1; i <= n; i++) {
    log.Append(LogEntry(1, 0, "cmd" + std::to_string(i)));
  }

  EXPECT_EQ(100u, log.LogSize());
  EXPECT_EQ(100u, log.LastIndex());
  ASSERT_NE(nullptr, log.Get(1));
  EXPECT_EQ(1u, log.Get(1)->index);
  ASSERT_NE(nullptr, log.Get(50));
  EXPECT_EQ(50u, log.Get(50)->index);
  ASSERT_NE(nullptr, log.Get(100));
  EXPECT_EQ(100u, log.Get(100)->index);
  EXPECT_EQ("cmd50", log.Get(50)->command);
}

TEST_F(CommandLogTest, GetPreservesTerms) {
  CommandLog log;
  log.Append(LogEntry(3, 0, "x"));
  log.Append(LogEntry(4, 0, "y"));

  ASSERT_NE(nullptr, log.Get(1));
  EXPECT_EQ(3u, log.Get(1)->term);
  ASSERT_NE(nullptr, log.Get(2));
  EXPECT_EQ(4u, log.Get(2)->term);
}

// --- ILogStorage interface tests ---

TEST_F(CommandLogTest, LastTerm) {
  CommandLog log;
  log.Append(LogEntry(5, 0, "x"));
  EXPECT_EQ(5u, log.LastTerm());
  log.Append(LogEntry(7, 0, "y"));
  EXPECT_EQ(7u, log.LastTerm());
}

TEST_F(CommandLogTest, GetRangeAll) {
  CommandLog log;
  log.Append(LogEntry(1, 0, "a"));
  log.Append(LogEntry(1, 0, "b"));
  log.Append(LogEntry(2, 0, "c"));

  auto entries = log.GetRange(1);
  ASSERT_EQ(3u, entries.size());
  EXPECT_EQ("a", entries[0].command);
  EXPECT_EQ("b", entries[1].command);
  EXPECT_EQ("c", entries[2].command);
}

TEST_F(CommandLogTest, GetRangeLimited) {
  CommandLog log;
  log.Append(LogEntry(1, 0, "a"));
  log.Append(LogEntry(1, 0, "b"));
  log.Append(LogEntry(2, 0, "c"));
  log.Append(LogEntry(2, 0, "d"));

  auto entries = log.GetRange(2, 2);
  ASSERT_EQ(2u, entries.size());
  EXPECT_EQ("b", entries[0].command);
  EXPECT_EQ("c", entries[1].command);
}

TEST_F(CommandLogTest, TruncateFrom) {
  CommandLog log;
  log.Append(LogEntry(1, 0, "a"));
  log.Append(LogEntry(1, 0, "b"));
  log.Append(LogEntry(2, 0, "c"));
  EXPECT_EQ(3u, log.LogSize());

  log.TruncateFrom(1);
  EXPECT_EQ(1u, log.LogSize());
  ASSERT_NE(nullptr, log.Get(1));
  EXPECT_EQ("a", log.Get(1)->command);
}

TEST_F(CommandLogTest, Clear) {
  CommandLog log;
  log.Append(LogEntry(1, 0, "x"));
  log.Append(LogEntry(2, 0, "y"));
  EXPECT_EQ(2u, log.LogSize());

  log.Clear();
  EXPECT_EQ(0u, log.LogSize());
  EXPECT_EQ(0u, log.LastIndex());
}

TEST_F(CommandLogTest, AppendLogBatch) {
  CommandLog log;
  std::vector<LogEntry> batch;
  batch.emplace_back(1, 0, "a");
  batch.emplace_back(1, 0, "b");
  batch.emplace_back(2, 0, "c");

  log.AppendLog(batch);
  EXPECT_EQ(3u, log.LogSize());
  ASSERT_NE(nullptr, log.Get(1));
  EXPECT_EQ(1u, log.Get(1)->index);
  ASSERT_NE(nullptr, log.Get(3));
  EXPECT_EQ(3u, log.Get(3)->index);
  EXPECT_EQ("c", log.Get(3)->command);
}

TEST_F(CommandLogTest, TruncateFromThenAppend) {
  CommandLog log;
  for (int i = 1; i <= 5; i++) {
    log.Append(LogEntry(1, 0, "cmd" + std::to_string(i)));
  }
  EXPECT_EQ(5u, log.LastIndex());

  log.TruncateFrom(2);
  EXPECT_EQ(2u, log.LastIndex());

  log.Append(LogEntry(2, 0, "new_cmd"));
  EXPECT_EQ(3u, log.LastIndex());
  ASSERT_NE(nullptr, log.Get(3));
  EXPECT_EQ("new_cmd", log.Get(3)->command);
  EXPECT_EQ(2u, log.Get(3)->term);
}

}  // namespace dfly
