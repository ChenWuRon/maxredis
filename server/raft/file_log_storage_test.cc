// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/file_log_storage.h"
#include "server/raft/wal_writer.h"

#include <gmock/gmock.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <random>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly {

using namespace testing;

class FileLogStorageTest : public Test {
 protected:
  void SetUp() override {
    dir_ = "/tmp/file_log_storage_test_" + std::to_string(getpid());
    // Clean up any leftover from previous runs.
    CleanDir();
  }

  void TearDown() override {
    CleanDir();
  }

  // Recursively remove test directory.
  void CleanDir() {
    for (uint32_t i = 0; i < 100; i++) {
      char buf[64];
      snprintf(buf, sizeof(buf), "%s/segment_%08lu.log", dir_.c_str(),
               static_cast<unsigned long>(i));
      unlink(buf);
    }
    unlink((dir_ + "/manifest.json").c_str());
    unlink((dir_ + "/manifest.json.tmp").c_str());
    rmdir(dir_.c_str());
  }

  std::string dir_;
};

TEST_F(FileLogStorageTest, OpenClose) {
  FileLogStorage fs;
  EXPECT_TRUE(fs.Open(dir_));
  EXPECT_EQ(0u, fs.LogSize());
  EXPECT_EQ(0u, fs.LastIndex());
  EXPECT_EQ(0u, fs.LastTerm());
}

TEST_F(FileLogStorageTest, AppendAndGet) {
  FileLogStorage fs;
  ASSERT_TRUE(fs.Open(dir_));

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
  ASSERT_TRUE(fs.Open(dir_));

  for (int i = 1; i <= 100; i++) {
    fs.Append(LogEntry{static_cast<Term>(i % 3 + 1),
                       static_cast<LogIndex>(i),
                       "cmd" + std::to_string(i)});
  }

  EXPECT_EQ(100u, fs.LogSize());

  const LogEntry* e50 = fs.Get(50);
  ASSERT_NE(nullptr, e50);
  EXPECT_EQ(50u, e50->index);
  EXPECT_EQ("cmd50", e50->command);

  const LogEntry* e100 = fs.Get(100);
  ASSERT_NE(nullptr, e100);
  EXPECT_EQ("cmd100", e100->command);
}

TEST_F(FileLogStorageTest, CrossSegmentGet) {
  FileLogStorage fs;
  ASSERT_TRUE(fs.Open(dir_));

  // Small kMaxSegmentSize for testing: 1KB per segment.
  // Write enough entries to force rotation.
  const int kEntriesPerSegment = 50;
  std::string payload(40, 'x');  // ~60 bytes per entry (20 header + 40 data)

  for (int seg = 0; seg < 3; seg++) {
    for (int i = 0; i < kEntriesPerSegment; i++) {
      LogIndex idx = static_cast<LogIndex>(seg * kEntriesPerSegment + i + 1);
      fs.Append(LogEntry{1, idx, payload + std::to_string(idx)});
    }
    fs.RollSegment();  // force roll after each batch
  }

  EXPECT_EQ(3u * kEntriesPerSegment, static_cast<int>(fs.LogSize()));

  // Verify entries across all segments.
  for (int seg = 0; seg < 3; seg++) {
    for (int i = 0; i < kEntriesPerSegment; i++) {
      LogIndex idx = static_cast<LogIndex>(seg * kEntriesPerSegment + i + 1);
      const LogEntry* e = fs.Get(idx);
      ASSERT_NE(nullptr, e) << "Failed to get entry " << idx;
      EXPECT_EQ(idx, e->index) << "Wrong index at " << idx;
      EXPECT_EQ(payload + std::to_string(idx), e->command);
    }
  }

  // Verify manifest is persisted (segment ID after 3 rolls = 3).
  std::ifstream ifs(dir_ + "/manifest.json");
  ASSERT_TRUE(ifs.is_open());
  std::string content;
  std::getline(ifs, content);
  EXPECT_THAT(content, HasSubstr("\"current_segment\":3"));

  // Verify segment files exist for segments 0, 1, 2 (3 is current but empty).
  for (uint32_t seg = 0; seg <= 2; seg++) {
    char buf[64];
    snprintf(buf, sizeof(buf), "%s/segment_%08lu.log", dir_.c_str(),
             static_cast<unsigned long>(seg));
    std::ifstream seg_ifs(buf);
    EXPECT_TRUE(seg_ifs.is_open()) << "Missing segment file: " << buf;
  }
}

TEST_F(FileLogStorageTest, AutoRotate) {
  // Use a custom small max segment size: ~1KB per segment.
  // We can't easily change kMaxSegmentSize, so we'll call RollSegment manually
  // and verify rotation works with many segments.

  FileLogStorage fs;
  ASSERT_TRUE(fs.Open(dir_));

  // Write 10 segments worth of data via explicit rolls.
  const int kSegments = 10;
  const int kEntriesPerSegment = 20;

  for (int seg = 0; seg < kSegments; seg++) {
    for (int i = 0; i < kEntriesPerSegment; i++) {
      LogIndex idx = static_cast<LogIndex>(seg * kEntriesPerSegment + i + 1);
      fs.Append(LogEntry{1, idx, "entry" + std::to_string(idx)});
    }
    if (seg < kSegments - 1) {
      fs.RollSegment();
    }
  }

  EXPECT_EQ(static_cast<size_t>(kSegments * kEntriesPerSegment), fs.LogSize());
  EXPECT_EQ(static_cast<LogIndex>(kSegments * kEntriesPerSegment), fs.LastIndex());

  // Verify all entries are accessible.
  LogIndex total = kSegments * kEntriesPerSegment;
  for (LogIndex i = 1; i <= total; i++) {
    const LogEntry* e = fs.Get(i);
    ASSERT_NE(nullptr, e) << "Missing entry " << i;
    EXPECT_EQ(i, e->index);
  }
}

TEST_F(FileLogStorageTest, GetReturnsNullForOutOfRange) {
  FileLogStorage fs;
  ASSERT_TRUE(fs.Open(dir_));
  EXPECT_EQ(nullptr, fs.Get(0));
  EXPECT_EQ(nullptr, fs.Get(1));

  fs.Append(LogEntry{1, 0, "a"});
  EXPECT_NE(nullptr, fs.Get(1));
  EXPECT_EQ(nullptr, fs.Get(2));
}

TEST_F(FileLogStorageTest, LastTerm) {
  FileLogStorage fs;
  ASSERT_TRUE(fs.Open(dir_));
  EXPECT_EQ(0u, fs.LastTerm());

  fs.Append(LogEntry{5, 0, "x"});
  EXPECT_EQ(5u, fs.LastTerm());

  fs.Append(LogEntry{3, 0, "y"});
  EXPECT_EQ(3u, fs.LastTerm());
}

TEST_F(FileLogStorageTest, GetRangeAcrossSegments) {
  FileLogStorage fs;
  ASSERT_TRUE(fs.Open(dir_));

  for (int seg = 0; seg < 3; seg++) {
    for (int i = 0; i < 10; i++) {
      fs.Append(LogEntry{static_cast<Term>(seg + 1),
                         static_cast<LogIndex>(seg * 10 + i + 1),
                         "cmd" + std::to_string(seg * 10 + i + 1)});
    }
    fs.RollSegment();
  }

  auto entries = fs.GetRange(5, 15);
  ASSERT_EQ(15u, entries.size());
  EXPECT_EQ("cmd5", entries[0].command);
  EXPECT_EQ("cmd19", entries[14].command);
}

TEST_F(FileLogStorageTest, Clear) {
  FileLogStorage fs;
  ASSERT_TRUE(fs.Open(dir_));

  fs.Append(LogEntry{1, 0, "a"});
  fs.Append(LogEntry{2, 0, "b"});
  EXPECT_EQ(2u, fs.LogSize());

  fs.Clear();
  EXPECT_EQ(0u, fs.LogSize());
  EXPECT_EQ(0u, fs.LastIndex());
  EXPECT_EQ(nullptr, fs.Get(1));
}

// --- Performance: 200MB across multiple segments ---

TEST_F(FileLogStorageTest, TwoHundredMBRotate) {
  FileLogStorage fs;
  ASSERT_TRUE(fs.Open(dir_));

  const int kTargetMB = 200;
  const int kPayloadSize = 512;  // ~532 bytes per entry (20 header + 512 data)
  const int kEntriesPerFlush = 100;
  std::string payload(kPayloadSize, 'x');

  int entry_count = 0;
  size_t total_mb = 0;

  auto start = std::chrono::steady_clock::now();

  while (total_mb < kTargetMB) {
    entry_count++;
    fs.Append(LogEntry{1, static_cast<LogIndex>(entry_count),
                       payload + std::to_string(entry_count % 10)});
    if (entry_count % kEntriesPerFlush == 0) {
      fs.Flush();
    }
    // Approximate: each entry is ~532 bytes.
    total_mb = (entry_count * (sizeof(RecordHeader) + kPayloadSize)) / (1024 * 1024);
  }
  fs.Flush();

  auto end = std::chrono::steady_clock::now();
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      end - start).count();

  LOG(INFO) << "Wrote ~200MB in " << entry_count << " entries, "
            << elapsed_ms << " ms, "
            << (entry_count * 1000 / std::max(elapsed_ms, 1L)) << " entries/sec";
  (void)elapsed_ms;

  // Verify all entries are readable.
  int verified = 0;
  for (LogIndex i = 1; i <= static_cast<LogIndex>(entry_count); i++) {
    const LogEntry* e = fs.Get(i);
    if (e && e->index == i)
      verified++;
  }

  EXPECT_EQ(entry_count, verified);

  // Count segment files on disk.
  int segment_count = 0;
  for (uint32_t seg = 0; seg < 100; seg++) {
    char buf[64];
    snprintf(buf, sizeof(buf), "%s/segment_%08lu.log", dir_.c_str(),
             static_cast<unsigned long>(seg));
    std::ifstream ifs(buf);
    if (ifs.is_open())
      segment_count++;
  }

  LOG(INFO) << "Total segments: " << segment_count;
  EXPECT_GT(segment_count, 1);  // at least 2 segments at 200MB / 64MB
}

}  // namespace dfly
