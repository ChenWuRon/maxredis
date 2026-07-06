// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/log_storage.h"

#include <memory>

#include <gmock/gmock.h>

#include "base/gtest.h"
#include "server/raft/command_log.h"

namespace dfly {

using namespace testing;

class LogStorageTest : public Test {
};

template <typename T>
class TypedLogStorageTest : public Test {
};

// Test with CommandLog (in-memory ILogStorage implementation).
using StorageTypes = ::testing::Types<CommandLog>;

TYPED_TEST_SUITE(TypedLogStorageTest, StorageTypes);

TYPED_TEST(TypedLogStorageTest, Empty) {
  TypeParam log;
  EXPECT_EQ(0u, log.LogSize());
  EXPECT_EQ(0u, log.LastIndex());
  EXPECT_EQ(0u, log.LastTerm());
  // Index 0 is the sentinel entry, always accessible.
  ASSERT_NE(nullptr, log.Get(0));
  EXPECT_EQ(0u, log.Get(0)->index);
  // Indices >= 1 are out of range when empty.
  EXPECT_EQ(nullptr, log.Get(1));
  EXPECT_EQ(nullptr, log.Get(100));
}

TYPED_TEST(TypedLogStorageTest, AppendSingle) {
  TypeParam log;
  LogIndex idx = log.Append(LogEntry(1, 0, "cmd1"));
  EXPECT_EQ(1u, idx);
  EXPECT_EQ(1u, log.LogSize());
  EXPECT_EQ(1u, log.LastIndex());
  EXPECT_EQ(1u, log.LastTerm());
}

TYPED_TEST(TypedLogStorageTest, AutoAssignIndex) {
  TypeParam log;
  LogIndex i1 = log.Append(LogEntry(3, 999, "ignored_index"));
  EXPECT_EQ(1u, i1);
  LogIndex i2 = log.Append(LogEntry(3, 888, "also_ignored"));
  EXPECT_EQ(2u, i2);
  LogIndex i3 = log.Append(LogEntry(4, 777, "again"));
  EXPECT_EQ(3u, i3);

  EXPECT_EQ(3u, log.LogSize());
  EXPECT_EQ(3u, log.LastIndex());
  EXPECT_EQ(4u, log.LastTerm());
}

TYPED_TEST(TypedLogStorageTest, GetReturnsCorrectEntry) {
  TypeParam log;
  log.Append(LogEntry(1, 0, "a"));
  log.Append(LogEntry(2, 0, "b"));
  log.Append(LogEntry(3, 0, "c"));

  const LogEntry* e1 = log.Get(1);
  ASSERT_NE(nullptr, e1);
  EXPECT_EQ(1u, e1->term);
  EXPECT_EQ(1u, e1->index);
  EXPECT_EQ("a", e1->command);

  const LogEntry* e2 = log.Get(2);
  ASSERT_NE(nullptr, e2);
  EXPECT_EQ(2u, e2->term);
  EXPECT_EQ(2u, e2->index);
  EXPECT_EQ("b", e2->command);

  const LogEntry* e3 = log.Get(3);
  ASSERT_NE(nullptr, e3);
  EXPECT_EQ(3u, e3->term);
  EXPECT_EQ(3u, e3->index);
  EXPECT_EQ("c", e3->command);
}

TYPED_TEST(TypedLogStorageTest, GetReturnsNullForOutOfRange) {
  TypeParam log;
  // Index 0 is the sentinel entry, always accessible.
  ASSERT_NE(nullptr, log.Get(0));
  // Indices >= 1 are out of range when empty.
  EXPECT_EQ(nullptr, log.Get(1));
  EXPECT_EQ(nullptr, log.Get(100));

  log.Append(LogEntry(1, 0, "x"));
  EXPECT_NE(nullptr, log.Get(1));
  EXPECT_EQ(nullptr, log.Get(2));
  // Index 0 is still the sentinel.
  ASSERT_NE(nullptr, log.Get(0));
  EXPECT_EQ(0u, log.Get(0)->index);
}

TYPED_TEST(TypedLogStorageTest, SequentialAppend) {
  TypeParam log;
  int n = 100;
  for (int i = 1; i <= n; i++) {
    log.Append(LogEntry(1, 0, "cmd" + std::to_string(i)));
  }

  EXPECT_EQ(100u, log.LogSize());
  EXPECT_EQ(100u, log.LastIndex());

  const LogEntry* e50 = log.Get(50);
  ASSERT_NE(nullptr, e50);
  EXPECT_EQ(50u, e50->index);
  EXPECT_EQ("cmd50", e50->command);

  const LogEntry* e100 = log.Get(100);
  ASSERT_NE(nullptr, e100);
  EXPECT_EQ(100u, e100->index);
  EXPECT_EQ("cmd100", e100->command);
}

TYPED_TEST(TypedLogStorageTest, GetRangeAll) {
  TypeParam log;
  log.Append(LogEntry(1, 0, "a"));
  log.Append(LogEntry(1, 0, "b"));
  log.Append(LogEntry(2, 0, "c"));

  auto entries = log.GetRange(1);
  ASSERT_EQ(3u, entries.size());
  EXPECT_EQ("a", entries[0].command);
  EXPECT_EQ("b", entries[1].command);
  EXPECT_EQ("c", entries[2].command);
}

TYPED_TEST(TypedLogStorageTest, GetRangeLimited) {
  TypeParam log;
  log.Append(LogEntry(1, 0, "a"));
  log.Append(LogEntry(1, 0, "b"));
  log.Append(LogEntry(2, 0, "c"));
  log.Append(LogEntry(2, 0, "d"));

  auto entries = log.GetRange(2, 2);
  ASSERT_EQ(2u, entries.size());
  EXPECT_EQ("b", entries[0].command);
  EXPECT_EQ("c", entries[1].command);
}

TYPED_TEST(TypedLogStorageTest, TruncateFromPreservesEntry) {
  // TruncateFrom(N) keeps entries 1..N, removes N+1..end.
  TypeParam log;
  log.Append(LogEntry(1, 0, "a"));  // idx 1
  log.Append(LogEntry(1, 0, "b"));  // idx 2
  log.Append(LogEntry(2, 0, "c"));  // idx 3
  EXPECT_EQ(3u, log.LogSize());

  log.TruncateFrom(2);  // keep 1..2, remove 3

  EXPECT_EQ(2u, log.LogSize());
  EXPECT_EQ(2u, log.LastIndex());

  const LogEntry* e1 = log.Get(1);
  ASSERT_NE(nullptr, e1);
  EXPECT_EQ("a", e1->command);

  const LogEntry* e2 = log.Get(2);
  ASSERT_NE(nullptr, e2);
  EXPECT_EQ("b", e2->command);

  EXPECT_EQ(nullptr, log.Get(3));
}

TYPED_TEST(TypedLogStorageTest, TruncateFromToLast) {
  // TruncateFrom(LastIndex()) is a no-op.
  TypeParam log;
  log.Append(LogEntry(1, 0, "a"));
  log.Append(LogEntry(1, 0, "b"));
  EXPECT_EQ(2u, log.LogSize());

  log.TruncateFrom(2);
  EXPECT_EQ(2u, log.LogSize());
}

TYPED_TEST(TypedLogStorageTest, TruncateFromThenAppend) {
  // After truncate, new entries get fresh indices.
  TypeParam log;
  log.Append(LogEntry(1, 0, "a"));  // idx 1
  log.Append(LogEntry(1, 0, "b"));  // idx 2
  log.Append(LogEntry(1, 0, "c"));  // idx 3
  log.Append(LogEntry(1, 0, "d"));  // idx 4

  log.TruncateFrom(2);  // keep 1..2

  LogIndex idx = log.Append(LogEntry(3, 0, "e"));  // should be idx 3
  EXPECT_EQ(3u, idx);

  EXPECT_EQ(3u, log.LogSize());
  EXPECT_EQ(3u, log.LastIndex());

  const LogEntry* e3 = log.Get(3);
  ASSERT_NE(nullptr, e3);
  EXPECT_EQ(3u, e3->index);
  EXPECT_EQ(3u, e3->term);
  EXPECT_EQ("e", e3->command);
}

TYPED_TEST(TypedLogStorageTest, Clear) {
  TypeParam log;
  log.Append(LogEntry(1, 0, "x"));
  log.Append(LogEntry(2, 0, "y"));
  EXPECT_EQ(2u, log.LogSize());

  log.Clear();
  EXPECT_EQ(0u, log.LogSize());
  EXPECT_EQ(0u, log.LastIndex());
  EXPECT_EQ(0u, log.LastTerm());
  EXPECT_EQ(nullptr, log.Get(1));
}

TYPED_TEST(TypedLogStorageTest, ClearThenAppend) {
  TypeParam log;
  log.Append(LogEntry(1, 0, "x"));
  log.Clear();
  EXPECT_EQ(0u, log.LogSize());

  LogIndex idx = log.Append(LogEntry(5, 0, "y"));
  EXPECT_EQ(1u, idx);
  EXPECT_EQ(1u, log.LogSize());
  EXPECT_EQ(5u, log.LastTerm());
}

TYPED_TEST(TypedLogStorageTest, LastTermWithMultipleEntries) {
  TypeParam log;
  EXPECT_EQ(0u, log.LastTerm());

  log.Append(LogEntry(3, 0, "x"));
  EXPECT_EQ(3u, log.LastTerm());

  log.Append(LogEntry(5, 0, "y"));
  EXPECT_EQ(5u, log.LastTerm());

  log.Append(LogEntry(2, 0, "z"));
  EXPECT_EQ(2u, log.LastTerm());
}

TYPED_TEST(TypedLogStorageTest, GetRangeEmptyLog) {
  TypeParam log;

  auto entries = log.GetRange(1);
  EXPECT_TRUE(entries.empty());

  entries = log.GetRange(1, 10);
  EXPECT_TRUE(entries.empty());
}

TYPED_TEST(TypedLogStorageTest, GetRangeBeyondEnd) {
  TypeParam log;
  log.Append(LogEntry(1, 0, "a"));

  auto entries = log.GetRange(10);
  EXPECT_TRUE(entries.empty());

  entries = log.GetRange(1, 100);
  ASSERT_EQ(1u, entries.size());
  EXPECT_EQ("a", entries[0].command);
}

// Test that unique_ptr<ILogStorage> works (interface decoupling).
TEST_F(LogStorageTest, UniquePtrInterface) {
  auto log = std::make_unique<CommandLog>();
  ASSERT_NE(nullptr, log);

  LogIndex idx = log->Append(LogEntry(1, 0, "test"));
  EXPECT_EQ(1u, idx);
  EXPECT_EQ(1u, log->LogSize());

  const LogEntry* entry = log->Get(1);
  ASSERT_NE(nullptr, entry);
  EXPECT_EQ("test", entry->command);
}

}  // namespace dfly
