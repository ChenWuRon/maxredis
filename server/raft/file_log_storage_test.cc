// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/file_log_storage.h"

#include <gmock/gmock.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <random>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly {

using namespace testing;

class FileLogStorageTest : public Test {
 protected:
  void SetUp() override {
    path_ = "/tmp/file_log_storage_test_" + std::to_string(getpid()) + ".log";
  }

  void TearDown() override {
    unlink(path_.c_str());
  }

  std::string path_;
};

TEST_F(FileLogStorageTest, OpenClose) {
  FileLogStorage fs;
  EXPECT_TRUE(fs.Open(path_));
  EXPECT_EQ(0u, fs.LogSize());
  EXPECT_EQ(0u, fs.LastIndex());
}

TEST_F(FileLogStorageTest, AppendAndGet) {
  FileLogStorage fs;
  ASSERT_TRUE(fs.Open(path_));

  LogIndex idx = fs.Append(LogEntry{1, 0, "SET a 1"});
  EXPECT_EQ(1u, idx);
  EXPECT_EQ(1u, fs.LogSize());

  const LogEntry* e = fs.Get(1);
  ASSERT_NE(nullptr, e);
  EXPECT_EQ(1u, e->term);
  EXPECT_EQ(1u, e->index);
  EXPECT_EQ("SET a 1", e->command);
}

TEST_F(FileLogStorageTest, Append100) {
  FileLogStorage fs;
  ASSERT_TRUE(fs.Open(path_));

  for (int i = 1; i <= 100; i++) {
    fs.Append(LogEntry{static_cast<Term>(i % 3 + 1),
                       static_cast<LogIndex>(i),
                       "cmd" + std::to_string(i)});
  }

  EXPECT_EQ(100u, fs.LogSize());
  EXPECT_EQ(100u, fs.LastIndex());

  // Verify random access.
  const LogEntry* e50 = fs.Get(50);
  ASSERT_NE(nullptr, e50);
  EXPECT_EQ(50u, e50->index);
  EXPECT_EQ("cmd50", e50->command);

  const LogEntry* e1 = fs.Get(1);
  ASSERT_NE(nullptr, e1);
  EXPECT_EQ("cmd1", e1->command);

  const LogEntry* e100 = fs.Get(100);
  ASSERT_NE(nullptr, e100);
  EXPECT_EQ("cmd100", e100->command);
}

TEST_F(FileLogStorageTest, GetReturnsNullForOutOfRange) {
  FileLogStorage fs;
  ASSERT_TRUE(fs.Open(path_));
  EXPECT_EQ(nullptr, fs.Get(0));
  EXPECT_EQ(nullptr, fs.Get(1));

  fs.Append(LogEntry{1, 0, "a"});
  EXPECT_NE(nullptr, fs.Get(1));
  EXPECT_EQ(nullptr, fs.Get(2));
}

TEST_F(FileLogStorageTest, LastTerm) {
  FileLogStorage fs;
  ASSERT_TRUE(fs.Open(path_));
  EXPECT_EQ(0u, fs.LastTerm());

  fs.Append(LogEntry{5, 0, "x"});
  EXPECT_EQ(5u, fs.LastTerm());

  fs.Append(LogEntry{3, 0, "y"});
  EXPECT_EQ(3u, fs.LastTerm());
}

TEST_F(FileLogStorageTest, GetRange) {
  FileLogStorage fs;
  ASSERT_TRUE(fs.Open(path_));

  fs.Append(LogEntry{1, 0, "a"});
  fs.Append(LogEntry{1, 0, "b"});
  fs.Append(LogEntry{2, 0, "c"});

  auto entries = fs.GetRange(1);
  ASSERT_EQ(3u, entries.size());

  auto limited = fs.GetRange(2, 1);
  ASSERT_EQ(1u, limited.size());
  EXPECT_EQ("b", limited[0].command);
}

TEST_F(FileLogStorageTest, Clear) {
  FileLogStorage fs;
  ASSERT_TRUE(fs.Open(path_));

  fs.Append(LogEntry{1, 0, "a"});
  fs.Append(LogEntry{2, 0, "b"});
  EXPECT_EQ(2u, fs.LogSize());

  fs.Clear();
  EXPECT_EQ(0u, fs.LogSize());
  EXPECT_EQ(0u, fs.LastIndex());
  EXPECT_EQ(nullptr, fs.Get(1));
}

// --- Performance: 100k entries + random Get < 1ms ---

TEST_F(FileLogStorageTest, Append100kAndRandomGet) {
  FileLogStorage fs;
  ASSERT_TRUE(fs.Open(path_));

  const int kNumEntries = 100000;

  auto append_start = std::chrono::steady_clock::now();

  for (int i = 1; i <= kNumEntries; i++) {
    std::string cmd = "SET key" + std::to_string(i) + " value" + std::to_string(i);
    fs.Append(LogEntry{static_cast<Term>(i % 5 + 1),
                       static_cast<LogIndex>(i), cmd});
    // Flush every 1000 entries to balance batch efficiency with test time.
    if (i % 1000 == 0) {
      fs.Flush();
    }
  }
  fs.Flush();  // final flush

  auto append_end = std::chrono::steady_clock::now();
  auto append_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      append_end - append_start).count();

  EXPECT_EQ(static_cast<size_t>(kNumEntries), fs.LogSize());
  EXPECT_EQ(static_cast<LogIndex>(kNumEntries), fs.LastIndex());

  LOG(INFO) << "Append " << kNumEntries << " entries: " << append_ms << " ms";

  // Random Get performance test.
  std::mt19937 rng(42);
  std::uniform_int_distribution<LogIndex> dist(1, kNumEntries);

  const int kNumGets = 10000;
  int verified = 0;

  auto get_start = std::chrono::steady_clock::now();

  for (int i = 0; i < kNumGets; i++) {
    LogIndex idx = dist(rng);
    const LogEntry* e = fs.Get(idx);
    if (e != nullptr && e->index == idx) {
      verified++;
    }
  }

  auto get_end = std::chrono::steady_clock::now();
  auto get_us = std::chrono::duration_cast<std::chrono::microseconds>(
      get_end - get_start).count();

  LOG(INFO) << "Random Get " << kNumGets << " lookups: " << get_us << " us total, "
            << (get_us / kNumGets) << " us per lookup";

  EXPECT_EQ(kNumGets, verified);
  // Each random Get must be well under 1ms.
  EXPECT_LT(static_cast<double>(get_us) / kNumGets, 50.0)
      << "Average random Get time exceeds 50us";
}

}  // namespace dfly
