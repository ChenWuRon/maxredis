// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/wal_writer.h"

#include <gmock/gmock.h>

#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>

#include "base/gtest.h"

namespace dfly {

using namespace testing;

class WalWriterTest : public Test {
 protected:
  void SetUp() override {
    path_ = "/tmp/wal_writer_test_" + std::to_string(getpid()) + ".log";
  }

  void TearDown() override {
    unlink(path_.c_str());
  }

  // Reads the raw file content.
  std::string ReadFile() {
    std::ifstream ifs(path_, std::ios::binary);
    if (!ifs.is_open())
      return "";
    std::ostringstream oss;
    oss << ifs.rdbuf();
    return oss.str();
  }

  // Reads the file and returns all parsed records.
  std::vector<LogEntry> ReadRecords() {
    std::string data = ReadFile();
    std::vector<LogEntry> entries;
    size_t offset = 0;
    while (offset + sizeof(RecordHeader) <= data.size()) {
      RecordHeader hdr;
      std::memcpy(&hdr, data.data() + offset, sizeof(RecordHeader));
      offset += sizeof(RecordHeader);

      if (offset + hdr.size > data.size())
        break;

      LogEntry entry;
      entry.index = hdr.index;
      entry.term = hdr.term;
      entry.command.assign(data.data() + offset, hdr.size);
      entries.push_back(std::move(entry));
      offset += hdr.size;
    }
    return entries;
  }

  std::string path_;
};

TEST_F(WalWriterTest, OpenClose) {
  WalWriter w;
  EXPECT_FALSE(w.IsOpen());
  EXPECT_TRUE(w.Open(path_));
  EXPECT_TRUE(w.IsOpen());
  EXPECT_EQ(0u, w.file_size());
  w.Close();
  EXPECT_FALSE(w.IsOpen());
}

TEST_F(WalWriterTest, AppendSingleEntry) {
  WalWriter w;
  ASSERT_TRUE(w.Open(path_));

  LogEntry entry{1, 42, "SET a 1"};
  w.Append(entry);
  EXPECT_TRUE(w.Flush());
  EXPECT_EQ(sizeof(RecordHeader) + entry.command.size(), w.file_size());

  w.Close();

  auto records = ReadRecords();
  ASSERT_EQ(1u, records.size());
  EXPECT_EQ(42u, records[0].index);
  EXPECT_EQ(1u, records[0].term);
  EXPECT_EQ("SET a 1", records[0].command);
}

TEST_F(WalWriterTest, AppendMultipleEntries) {
  WalWriter w;
  ASSERT_TRUE(w.Open(path_));

  int n = 10;
  for (int i = 1; i <= n; i++) {
    LogEntry entry{static_cast<Term>(i % 3 + 1),
                   static_cast<LogIndex>(i),
                   "cmd" + std::to_string(i)};
    w.Append(entry);
  }
  EXPECT_TRUE(w.Flush());
  w.Close();

  auto records = ReadRecords();
  ASSERT_EQ(10u, records.size());
  for (int i = 1; i <= n; i++) {
    EXPECT_EQ(static_cast<LogIndex>(i), records[i - 1].index);
    EXPECT_EQ("cmd" + std::to_string(i), records[i - 1].command);
  }
}

TEST_F(WalWriterTest, Append1000) {
  WalWriter w;
  ASSERT_TRUE(w.Open(path_));

  int n = 1000;
  size_t expected_data = 0;
  for (int i = 1; i <= n; i++) {
    std::string cmd = "SET key" + std::to_string(i) + " value" + std::to_string(i);
    LogEntry entry{1, static_cast<LogIndex>(i), cmd};
    w.Append(entry);
    expected_data += sizeof(RecordHeader) + cmd.size();
  }
  EXPECT_TRUE(w.Flush());
  w.Close();

  // Verify file size.
  std::ifstream ifs(path_, std::ios::binary | std::ios::ate);
  ASSERT_TRUE(ifs.is_open());
  size_t actual_size = ifs.tellg();
  EXPECT_EQ(expected_data, actual_size);

  // Verify all records.
  auto records = ReadRecords();
  ASSERT_EQ(1000u, records.size());
  EXPECT_EQ(1000u, records.back().index);
  EXPECT_EQ("SET key1000 value1000", records.back().command);
}

TEST_F(WalWriterTest, FlushWritesToDisk) {
  WalWriter w;
  ASSERT_TRUE(w.Open(path_));

  LogEntry entry{2, 5, "DEL x"};
  w.Append(entry);
  EXPECT_TRUE(w.Flush());

  // After flush, file should exist with content.
  std::string content = ReadFile();
  EXPECT_FALSE(content.empty());
  EXPECT_EQ(sizeof(RecordHeader) + entry.command.size(), content.size());

  w.Close();
}

TEST_F(WalWriterTest, DoubleOpenFails) {
  WalWriter w;
  ASSERT_TRUE(w.Open(path_));
  EXPECT_FALSE(w.Open(path_));
  w.Close();
}

TEST_F(WalWriterTest, CloseIsIdempotent) {
  WalWriter w;
  ASSERT_TRUE(w.Open(path_));
  w.Close();
  w.Close();  // second close should be a no-op
  EXPECT_FALSE(w.IsOpen());
}

TEST_F(WalWriterTest, FileSizeAccuracy) {
  WalWriter w;
  ASSERT_TRUE(w.Open(path_));

  std::string cmd = "EXPIRE x 100";
  size_t expected = sizeof(RecordHeader) + cmd.size();

  w.Append(LogEntry{1, 1, cmd});
  EXPECT_TRUE(w.Flush());
  EXPECT_EQ(expected, w.file_size());

  cmd = "GET x";
  expected += sizeof(RecordHeader) + cmd.size();
  w.Append(LogEntry{1, 2, cmd});
  EXPECT_TRUE(w.Flush());
  EXPECT_EQ(expected, w.file_size());

  w.Close();

  std::ifstream ifs(path_, std::ios::binary | std::ios::ate);
  ASSERT_TRUE(ifs.is_open());
  EXPECT_EQ(expected, static_cast<size_t>(ifs.tellg()));
}

TEST_F(WalWriterTest, DestructorFlushes) {
  {
    WalWriter w;
    ASSERT_TRUE(w.Open(path_));
    w.Append(LogEntry{1, 1, "PING"});
    // Destructor should flush and close.
  }

  auto records = ReadRecords();
  ASSERT_EQ(1u, records.size());
  EXPECT_EQ("PING", records[0].command);
}

}  // namespace dfly
