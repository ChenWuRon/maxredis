// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/segment_log_storage.h"

#include <memory>
#include <unistd.h>

#include <gmock/gmock.h>

#include "base/gtest.h"
#include "server/raft/wal_writer.h"
#include "server/storage/common_types.h"

namespace dfly {

using namespace testing;

class SegmentLogStorageTest : public Test {
};

class SegmentLogStorageRecoveryTest : public Test {
 protected:
  void SetUp() override {
    dir_ = "/tmp/segment_log_storage_recovery_test_" + std::to_string(getpid());
    CleanDir();
  }

  void TearDown() override {
    CleanDir();
  }

  void CleanDir() {
    for (uint32_t i = 0; i < 20; i++) {
      char buf[256];
      snprintf(buf, sizeof(buf), "%s/segment_%08lu.log", dir_.c_str(),
               static_cast<unsigned long>(i));
      unlink(buf);
    }
    unlink((dir_ + "/manifest.json").c_str());
    unlink((dir_ + "/manifest.json.tmp").c_str());
    rmdir(dir_.c_str());
  }

  // Writes 'count' entries to a segment file using WalWriter.
  void WriteSegment(uint32_t segment_id, uint32_t count, Term term = 1,
                    LogIndex start_index = 1) {
    char buf[256];
    snprintf(buf, sizeof(buf), "%s/segment_%08lu.log", dir_.c_str(),
             static_cast<unsigned long>(segment_id));

    WalWriter writer;
    ASSERT_TRUE(writer.Open(buf));
    for (uint32_t i = 0; i < count; i++) {
      LogEntry entry;
      entry.term = term;
      entry.index = start_index + i;
      entry.command = "cmd" + std::to_string(start_index + i);
      writer.Append(entry);
    }
    ASSERT_TRUE(writer.Flush());
    writer.Close();
  }

  // Creates a segment with 'count' valid entries followed by a partial write.
  // Writes a header with a too-large size so the payload read fails.
  void WriteSegmentWithCorruptedTail(uint32_t segment_id, uint32_t count) {
    char buf[256];
    snprintf(buf, sizeof(buf), "%s/segment_%08lu.log", dir_.c_str(),
             static_cast<unsigned long>(segment_id));

    // Write valid entries.
    WalWriter writer;
    ASSERT_TRUE(writer.Open(buf));
    for (uint32_t i = 0; i < count; i++) {
      LogEntry entry;
      entry.term = 1;
      entry.index = i + 1;
      entry.command = "good_" + std::to_string(i + 1);
      writer.Append(entry);
    }
    ASSERT_TRUE(writer.Flush());

    // Write a partial record: header with large size but no payload.
    RecordHeader hdr;
    hdr.index = count + 1;
    hdr.term = 1;
    hdr.size = 99999;  // larger than any real payload
    hdr.crc32 = 0;
    FILE* fp = fopen(buf, "ab");
    ASSERT_NE(nullptr, fp);
    fwrite(&hdr, sizeof(hdr), 1, fp);
    fclose(fp);
  }

  // Writes a segment with a specific set of indices (used to test validation).
  // Each entry uses the same term=1 and command="payload".
  void WriteSegmentWithIndices(uint32_t segment_id,
                               const std::vector<LogIndex>& indices) {
    char buf[256];
    snprintf(buf, sizeof(buf), "%s/segment_%08lu.log", dir_.c_str(),
             static_cast<unsigned long>(segment_id));
    WalWriter writer;
    ASSERT_TRUE(writer.Open(buf));
    for (LogIndex idx : indices) {
      LogEntry entry;
      entry.term = 1;
      entry.index = idx;
      entry.command = "x";
      writer.Append(entry);
    }
    ASSERT_TRUE(writer.Flush());
    writer.Close();
  }

  std::string dir_;
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

// --- Recovery scan tests ---

TEST_F(SegmentLogStorageRecoveryTest, OpenEmptyDir) {
  SegmentLogStorage seg(dir_);
  ASSERT_TRUE(seg.Open());
  EXPECT_EQ(0u, seg.LogSize());
  EXPECT_EQ(0u, seg.LastIndex());
  EXPECT_EQ(0u, seg.LastTerm());
}

TEST_F(SegmentLogStorageRecoveryTest, DiscoverSingleSegment) {
  WriteSegment(1, 100);

  SegmentLogStorage seg(dir_);
  ASSERT_TRUE(seg.Open());
  EXPECT_EQ(100u, seg.LogSize());
  EXPECT_EQ(100u, seg.LastIndex());

  const LogEntry* e = seg.Get(50);
  ASSERT_NE(nullptr, e);
  EXPECT_EQ(50u, e->index);
  EXPECT_EQ("cmd50", e->command);

  // Last entry has higher term.
  EXPECT_EQ("cmd100", seg.Get(100)->command);
}

TEST_F(SegmentLogStorageRecoveryTest, DiscoverMultipleSegments) {
  WriteSegment(1, 50, 1, 1);
  WriteSegment(2, 30, 2, 51);
  WriteSegment(3, 20, 3, 81);

  SegmentLogStorage seg(dir_);
  ASSERT_TRUE(seg.Open());
  EXPECT_EQ(100u, seg.LogSize());
  EXPECT_EQ(100u, seg.LastIndex());

  // First entry from segment 1.
  const LogEntry* e1 = seg.Get(1);
  ASSERT_NE(nullptr, e1);
  EXPECT_EQ(1u, e1->index);
  EXPECT_EQ(1u, e1->term);
  EXPECT_EQ("cmd1", e1->command);

  // Entry from segment 2.
  const LogEntry* e60 = seg.Get(60);
  ASSERT_NE(nullptr, e60);
  EXPECT_EQ(60u, e60->index);
  EXPECT_EQ(2u, e60->term);
  EXPECT_EQ("cmd60", e60->command);

  // Last entry from segment 3.
  const LogEntry* e100 = seg.Get(100);
  ASSERT_NE(nullptr, e100);
  EXPECT_EQ(100u, e100->index);
  EXPECT_EQ(3u, e100->term);
  EXPECT_EQ("cmd100", e100->command);
}

TEST_F(SegmentLogStorageRecoveryTest, ScanCorruptedTail) {
  WriteSegmentWithCorruptedTail(1, 50);

  SegmentLogStorage seg(dir_);
  ASSERT_TRUE(seg.Open());
  EXPECT_EQ(50u, seg.LogSize());
  EXPECT_EQ(50u, seg.LastIndex());

  // All good entries should be present.
  for (uint32_t i = 1; i <= 50; i++) {
    const LogEntry* e = seg.Get(i);
    ASSERT_NE(nullptr, e) << "missing entry " << i;
    EXPECT_EQ("good_" + std::to_string(i), e->command);
  }
}

TEST_F(SegmentLogStorageRecoveryTest, DiscoverSegmentsOutOfOrder) {
  // Write segments in non-sequential order to test sorting.
  WriteSegment(3, 20, 3, 81);
  WriteSegment(1, 50, 1, 1);
  WriteSegment(2, 30, 2, 51);

  SegmentLogStorage seg(dir_);
  ASSERT_TRUE(seg.Open());
  EXPECT_EQ(100u, seg.LogSize());

  // Verify entries are in order (segment 1, then 2, then 3).
  EXPECT_EQ("cmd1", seg.Get(1)->command);
  EXPECT_EQ("cmd51", seg.Get(51)->command);
  EXPECT_EQ("cmd81", seg.Get(81)->command);
  EXPECT_EQ(1u, seg.Get(1)->term);
  EXPECT_EQ(2u, seg.Get(51)->term);
  EXPECT_EQ(3u, seg.Get(81)->term);
}

// --- Index rebuild tests ---

TEST_F(SegmentLogStorageRecoveryTest, RebuildIndexAccess) {
  WriteSegment(1, 10000);

  SegmentLogStorage seg(dir_);
  ASSERT_TRUE(seg.Open());
  EXPECT_EQ(10000u, seg.LogSize());
  EXPECT_EQ(10000u, seg.LastIndex());

  // Random access via rebuilt index.
  const LogEntry* e = seg.Get(5000);
  ASSERT_NE(nullptr, e);
  EXPECT_EQ(5000u, e->index);
  EXPECT_EQ("cmd5000", e->command);
}

TEST_F(SegmentLogStorageRecoveryTest, RebuildIndexRandomAccess) {
  WriteSegment(1, 10000);

  SegmentLogStorage seg(dir_);
  ASSERT_TRUE(seg.Open());

  const LogEntry* e1 = seg.Get(1);
  ASSERT_NE(nullptr, e1);
  EXPECT_EQ("cmd1", e1->command);

  const LogEntry* e500 = seg.Get(500);
  ASSERT_NE(nullptr, e500);
  EXPECT_EQ("cmd500", e500->command);

  const LogEntry* e9999 = seg.Get(9999);
  ASSERT_NE(nullptr, e9999);
  EXPECT_EQ("cmd9999", e9999->command);

  EXPECT_EQ(10000u, seg.LastIndex());
}

TEST_F(SegmentLogStorageRecoveryTest, LastIndexFromRecoveredIndex) {
  WriteSegment(1, 500);

  SegmentLogStorage seg(dir_);
  ASSERT_TRUE(seg.Open());
  // LastIndex should come from rebuilt index, not WAL re-scan.
  EXPECT_EQ(500u, seg.LastIndex());
  EXPECT_EQ(500u, seg.LogSize());

  // Verify get on last entry works.
  ASSERT_NE(nullptr, seg.Get(500));
  EXPECT_EQ("cmd500", seg.Get(500)->command);
}

TEST_F(SegmentLogStorageRecoveryTest, RejectDuplicateIndex) {
  // Write valid entries then append one with duplicate index.
  WriteSegmentWithIndices(1, {1, 2, 3, 3, 4});

  SegmentLogStorage seg(dir_);
  ASSERT_TRUE(seg.Open());
  // Should stop at index 3 (1, 2, 3), reject the duplicate 3.
  EXPECT_EQ(3u, seg.LogSize());
  ASSERT_NE(nullptr, seg.Get(3));
  EXPECT_EQ(nullptr, seg.Get(4));
}

TEST_F(SegmentLogStorageRecoveryTest, RejectNonMonotonicIndex) {
  // Write entries with decreasing index.
  WriteSegmentWithIndices(1, {1, 2, 5, 3});

  SegmentLogStorage seg(dir_);
  ASSERT_TRUE(seg.Open());
  // Should accept 1, 2, 5 (monotonic), reject 3 (< 5).
  EXPECT_EQ(3u, seg.LogSize());
  EXPECT_EQ(5u, seg.LastIndex());
  ASSERT_NE(nullptr, seg.Get(5));
  EXPECT_EQ(nullptr, seg.Get(3));
}

TEST_F(SegmentLogStorageRecoveryTest, RebuildIndexCrossSegment) {
  // Entries spanning multiple segments.
  WriteSegment(1, 50, 1, 1);
  WriteSegment(2, 50, 2, 51);

  SegmentLogStorage seg(dir_);
  ASSERT_TRUE(seg.Open());
  EXPECT_EQ(100u, seg.LogSize());
  EXPECT_EQ(100u, seg.LastIndex());

  // Verify cross-segment reads.
  const LogEntry* e50 = seg.Get(50);
  ASSERT_NE(nullptr, e50);
  EXPECT_EQ(50u, e50->index);
  EXPECT_EQ(1u, e50->term);

  const LogEntry* e51 = seg.Get(51);
  ASSERT_NE(nullptr, e51);
  EXPECT_EQ(51u, e51->index);
  EXPECT_EQ(2u, e51->term);
}

// --- LastIndex / LastTerm recovery tests ---

TEST_F(SegmentLogStorageRecoveryTest, RecoverLastTermMultiTerm) {
  // Entry: index=1 term=1, 2 term=1, 3 term=2, 4 term=2
  WriteSegmentWithIndices(1, {1, 2});
  WriteSegment(2, 2, 2, 3);

  SegmentLogStorage seg(dir_);
  ASSERT_TRUE(seg.Open());
  EXPECT_EQ(4u, seg.LastIndex());
  EXPECT_EQ(2u, seg.LastTerm());
}

TEST_F(SegmentLogStorageRecoveryTest, RecoverLastTermCorruptedTail) {
  // Write 1000 entries with term=5, then corrupted tail.
  WriteSegmentWithCorruptedTail(1, 1000);

  SegmentLogStorage seg(dir_);
  ASSERT_TRUE(seg.Open());
  EXPECT_EQ(1000u, seg.LogSize());
  EXPECT_EQ(1000u, seg.LastIndex());
  EXPECT_EQ(1u, seg.LastTerm());  // WriteSegmentWithCorruptedTail uses term=1
}

TEST_F(SegmentLogStorageRecoveryTest, RecoverLastTermEmptyDir) {
  SegmentLogStorage seg(dir_);
  ASSERT_TRUE(seg.Open());
  EXPECT_EQ(0u, seg.LastIndex());
  EXPECT_EQ(0u, seg.LastTerm());
}

TEST_F(SegmentLogStorageRecoveryTest, IntegrationCrashRecovery) {
  // Simulate pre-crash state: write segments via WalWriter.
  WriteSegment(1, 10000, 3, 1);

  // Crash and restart.
  SegmentLogStorage seg(dir_);
  ASSERT_TRUE(seg.Open());
  EXPECT_EQ(10000u, seg.LastIndex());
  EXPECT_EQ(3u, seg.LastTerm());

  // Verify random read.
  const LogEntry* e = seg.Get(10000);
  ASSERT_NE(nullptr, e);
  EXPECT_EQ(10000u, e->index);
  EXPECT_EQ("cmd10000", e->command);

  // Continue appending after recovery.
  LogIndex idx = seg.Append(LogEntry{4, 0, "post-recovery"});
  EXPECT_EQ(10001u, idx);
  EXPECT_EQ(10001u, seg.LastIndex());
  EXPECT_EQ(4u, seg.LastTerm());

  const LogEntry* e2 = seg.Get(10001);
  ASSERT_NE(nullptr, e2);
  EXPECT_EQ("post-recovery", e2->command);
}

TEST_F(SegmentLogStorageRecoveryTest, IntegrationCrashRecoveryMultiSegment) {
  // Two segments with different terms.
  WriteSegment(1, 500, 1, 1);
  WriteSegment(2, 500, 5, 501);

  // Crash and restart.
  SegmentLogStorage seg(dir_);
  ASSERT_TRUE(seg.Open());
  EXPECT_EQ(1000u, seg.LastIndex());
  EXPECT_EQ(5u, seg.LastTerm());

  // Continue appending.
  seg.Append(LogEntry{6, 0, "after-recovery"});
  EXPECT_EQ(1001u, seg.LastIndex());
  EXPECT_EQ(6u, seg.LastTerm());
}

// --- Compaction recovery tests ---

class SegmentLogCompactionTest : public SegmentLogStorageRecoveryTest {
};

// Test 1, 2, 3: Write 1M logs across segments, set snapshot anchor,
// compact segments, then restart and verify data.
TEST_F(SegmentLogCompactionTest, Compact1MillionLogs) {
  // Write 1M entries across 3 segments:
  //   segment_0: 1..400000, segment_1: 400001..700000, segment_2: 700001..1000000
  uint64_t t0 = NowMs();
  WriteSegment(0, 400000, 1, 1);
  WriteSegment(1, 300000, 1, 400001);
  WriteSegment(2, 300000, 1, 700001);

  // Open and verify.
  {
    SegmentLogStorage seg(dir_);
    ASSERT_TRUE(seg.Open());
    EXPECT_EQ(1000000u, seg.LastIndex());
    EXPECT_EQ(1000000u, seg.LogSize());

    // Compact at snapshot index 500000.
    // segment_0 is fully covered → should be deleted.
    // segment_1 contains the snapshot index → kept.
    // segment_2 is after → kept.
    seg.CompactLogs(500000, 1);
    EXPECT_EQ(1000000u, seg.LastIndex());

    // After CompactUpTo(500000), FirstIndex should be 500001.
    EXPECT_EQ(500001u, seg.FirstIndex());
    EXPECT_EQ(500000u, seg.LogSize());

    // Snapshot anchor should preserve GetTerm(500000).
    EXPECT_EQ(1u, seg.GetTerm(500000));

    // Verify entries after snapshot are accessible.
    const LogEntry* e = seg.Get(700001);
    ASSERT_NE(nullptr, e);
    EXPECT_EQ(700001u, e->index);

    // segment_0 should have been deleted.
    char buf[256];
    snprintf(buf, sizeof(buf), "%s/segment_%08lu.log", dir_.c_str(),
             static_cast<unsigned long>(0));
    EXPECT_NE(0, access(buf, F_OK)) << "segment_0 should be deleted";
  }

  // Test 4, 5: "Crash and restart" — reopen and verify data.
  // Note: CompactUpTo only truncates the in-memory vector; on-disk segment
  // files (segment_1 and segment_2) still contain 600k entries total.
  // So after restart, FirstIndex is 400001 (first entry in segment_1),
  // and LogSize is 600000 (all remaining on-disk entries).
  {
    SegmentLogStorage seg(dir_);
    ASSERT_TRUE(seg.Open());
    EXPECT_EQ(1000000u, seg.LastIndex());
    EXPECT_EQ(400001u, seg.FirstIndex());
    EXPECT_EQ(600000u, seg.LogSize());

    // Anchor should still work (set by CompactLogs before crash).
    EXPECT_EQ(1u, seg.GetTerm(500000));

    // Entries after compaction should be readable.
    const LogEntry* e = seg.Get(800000);
    ASSERT_NE(nullptr, e);
    EXPECT_EQ(800000u, e->index);

    // Continue appending after recovery.
    LogIndex idx = seg.Append(LogEntry{2, 0, "after-crash"});
    EXPECT_EQ(1000001u, idx);
    EXPECT_EQ(1000001u, seg.LastIndex());
  }

  uint64_t t1 = NowMs();
  VLOG(1) << "CompactionTest: " << (t1 - t0) << "ms";
}

// Test that compaction doesn't delete the segment containing the snapshot index.
TEST_F(SegmentLogCompactionTest, KeepSnapshotSegment) {
  WriteSegment(0, 100000, 1, 1);
  WriteSegment(1, 100000, 2, 100001);
  WriteSegment(2, 100000, 3, 200001);

  SegmentLogStorage seg(dir_);
  ASSERT_TRUE(seg.Open());

  // Snapshot at index 150000 (inside segment_1).
  seg.CompactLogs(150000, 2);

  // segment_0 should be deleted.
  char buf[256];
  snprintf(buf, sizeof(buf), "%s/segment_%08lu.log", dir_.c_str(),
           static_cast<unsigned long>(0));
  EXPECT_NE(0, access(buf, F_OK)) << "segment_0 should be deleted";

  // segment_1 (contains snapshot index 150000) should exist.
  snprintf(buf, sizeof(buf), "%s/segment_%08lu.log", dir_.c_str(),
           static_cast<unsigned long>(1));
  EXPECT_EQ(0, access(buf, F_OK)) << "segment_1 should be kept";

  // segment_2 should exist.
  snprintf(buf, sizeof(buf), "%s/segment_%08lu.log", dir_.c_str(),
           static_cast<unsigned long>(2));
  EXPECT_EQ(0, access(buf, F_OK)) << "segment_2 should be kept";
}

// Test AppendEntries consistency check after compaction.
TEST_F(SegmentLogCompactionTest, AppendEntriesConsistencyAfterCompaction) {
  WriteSegment(0, 1000, 1, 1);

  SegmentLogStorage seg(dir_);
  ASSERT_TRUE(seg.Open());

  // Compact at snapshot index 800.
  seg.CompactLogs(800, 1);

  // prevLogIndex=800, prevLogTerm=1 — should match anchor.
  EXPECT_EQ(1u, seg.GetTerm(800));
  EXPECT_EQ(800u, seg.snapshot_anchor().index);
  EXPECT_EQ(1u, seg.snapshot_anchor().term);

  // index 801 should be the first stored entry.
  const LogEntry* e = seg.Get(801);
  ASSERT_NE(nullptr, e);
  EXPECT_EQ(1u, e->term);

  // index 799 was compacted — GetTerm should not find it.
  // The anchor only preserves the snapshot index (800), not 799.
  EXPECT_EQ(0u, seg.GetTerm(799));
}

}  // namespace dfly
