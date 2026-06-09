// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/segment_log_storage.h"

#include <memory>

#include <gmock/gmock.h>

#include "base/gtest.h"

namespace dfly {

using namespace testing;

class SegmentLogStorageTest : public Test {
};

TEST_F(SegmentLogStorageTest, Empty) {
  SegmentLogStorage seg;
  EXPECT_EQ(0u, seg.LogSize());
  EXPECT_EQ(0u, seg.LastIndex());
  EXPECT_EQ(0u, seg.LastTerm());
  EXPECT_EQ(nullptr, seg.Get(1));
  ASSERT_NE(nullptr, seg.Get(0));
}

TEST_F(SegmentLogStorageTest, AppendAndGet) {
  SegmentLogStorage seg;
  LogIndex idx = seg.Append(LogEntry{1, 0, "cmd1"});
  EXPECT_EQ(1u, idx);

  const LogEntry* e = seg.Get(1);
  ASSERT_NE(nullptr, e);
  EXPECT_EQ(1u, e->term);
  EXPECT_EQ(1u, e->index);
  EXPECT_EQ("cmd1", e->command);
}

TEST_F(SegmentLogStorageTest, Append100) {
  SegmentLogStorage seg;
  for (int i = 1; i <= 100; i++) {
    seg.Append(LogEntry{1, 0, "cmd" + std::to_string(i)});
  }

  EXPECT_EQ(100u, seg.LogSize());
  EXPECT_EQ(100u, seg.LastIndex());

  const LogEntry* e50 = seg.Get(50);
  ASSERT_NE(nullptr, e50);
  EXPECT_EQ(50u, e50->index);
  EXPECT_EQ("cmd50", e50->command);
}

TEST_F(SegmentLogStorageTest, GetReturnsNullForOutOfRange) {
  SegmentLogStorage seg;
  EXPECT_EQ(nullptr, seg.Get(1));
  seg.Append(LogEntry{1, 0, "a"});
  EXPECT_NE(nullptr, seg.Get(1));
  EXPECT_EQ(nullptr, seg.Get(2));
}

TEST_F(SegmentLogStorageTest, LastTerm) {
  SegmentLogStorage seg;
  EXPECT_EQ(0u, seg.LastTerm());
  seg.Append(LogEntry{5, 0, "x"});
  EXPECT_EQ(5u, seg.LastTerm());
  seg.Append(LogEntry{3, 0, "y"});
  EXPECT_EQ(3u, seg.LastTerm());
}

TEST_F(SegmentLogStorageTest, GetRange) {
  SegmentLogStorage seg;
  seg.Append(LogEntry{1, 0, "a"});
  seg.Append(LogEntry{1, 0, "b"});
  seg.Append(LogEntry{2, 0, "c"});

  auto entries = seg.GetRange(1);
  ASSERT_EQ(3u, entries.size());

  auto limited = seg.GetRange(2, 1);
  ASSERT_EQ(1u, limited.size());
  EXPECT_EQ("b", limited[0].command);
}

TEST_F(SegmentLogStorageTest, TruncateFrom) {
  SegmentLogStorage seg;
  seg.Append(LogEntry{1, 0, "a"});
  seg.Append(LogEntry{1, 0, "b"});
  seg.Append(LogEntry{2, 0, "c"});
  EXPECT_EQ(3u, seg.LogSize());

  seg.TruncateFrom(2);
  EXPECT_EQ(2u, seg.LogSize());
  EXPECT_NE(nullptr, seg.Get(2));
  EXPECT_EQ(nullptr, seg.Get(3));
}

TEST_F(SegmentLogStorageTest, Clear) {
  SegmentLogStorage seg;
  seg.Append(LogEntry{1, 0, "x"});
  seg.Clear();
  EXPECT_EQ(0u, seg.LogSize());
  EXPECT_EQ(nullptr, seg.Get(1));
}

TEST_F(SegmentLogStorageTest, WorksWithILogStoragePtr) {
  std::unique_ptr<ILogStorage> storage = std::make_unique<SegmentLogStorage>();
  LogIndex idx = storage->Append(LogEntry{1, 0, "via-interface"});
  EXPECT_EQ(1u, idx);
  const LogEntry* e = storage->Get(1);
  ASSERT_NE(nullptr, e);
  EXPECT_EQ("via-interface", e->command);
  EXPECT_EQ(1u, storage->LogSize());
}

}  // namespace dfly
