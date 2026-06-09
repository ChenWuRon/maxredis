// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/state_machine/kv_state_machine.h"

#include <gmock/gmock.h>

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>

#include "base/gtest.h"
#include "server/raft/snapshot_meta.h"
#include "server/raft/snapshot_writer.h"
#include "server/storage/engine_shard_set.h"
#include "util/fibers/pool.h"

namespace dfly {

using namespace testing;

// Reads records from a snapshot.bin file without EngineShardSet infrastructure.
static bool ReadSnapshotRecords(const std::string& path,
                                std::vector<SnapshotRecord>* records) {
  FILE* fp = fopen(path.c_str(), "rb");
  if (!fp)
    return false;

  uint32_t magic;
  if (fread(&magic, sizeof(magic), 1, fp) != 1 || magic != kSnapshotMagic) {
    fclose(fp);
    return false;
  }

  uint32_t num_records;
  if (fread(&num_records, sizeof(num_records), 1, fp) != 1) {
    fclose(fp);
    return false;
  }

  for (uint32_t i = 0; i < num_records; i++) {
    uint32_t key_len, value_len;
    uint64_t expire_at;

    if (fread(&key_len, sizeof(key_len), 1, fp) != 1) break;
    if (fread(&value_len, sizeof(value_len), 1, fp) != 1) break;
    if (fread(&expire_at, sizeof(expire_at), 1, fp) != 1) break;

    SnapshotRecord rec;
    rec.key.resize(key_len);
    if (key_len > 0 && fread(rec.key.data(), 1, key_len, fp) != key_len) break;

    rec.value.resize(value_len);
    if (value_len > 0 && fread(rec.value.data(), 1, value_len, fp) != value_len) break;

    rec.expire_at = expire_at;
    records->push_back(std::move(rec));
  }

  fclose(fp);
  return records->size() == num_records;
}

class SnapshotBinaryTest : public Test {
 protected:
  void SetUp() override {
    path_ = "/tmp/snapshot_bin_test_" + std::to_string(getpid()) + ".bin";
  }

  void TearDown() override {
    unlink(path_.c_str());
  }

  std::string path_;
};

TEST_F(SnapshotBinaryTest, RoundTripRecords) {
  std::vector<SnapshotRecord> expected = {
    {"key1", "value1", 0},
    {"key2", "value2", 1000},
    {"", "", 0},
    {"longkey", "longvalue", 999999},
  };

  {
    SnapshotWriter writer(path_);
    ASSERT_TRUE(writer.Open());
    ASSERT_TRUE(writer.AddBatch(expected.data(), expected.size()));
    ASSERT_TRUE(writer.Finalize(expected.size()));
  }

  std::vector<SnapshotRecord> actual;
  EXPECT_TRUE(ReadSnapshotRecords(path_, &actual));
  EXPECT_EQ(expected.size(), actual.size());

  for (size_t i = 0; i < expected.size(); i++) {
    EXPECT_EQ(expected[i].key, actual[i].key);
    EXPECT_EQ(expected[i].value, actual[i].value);
    EXPECT_EQ(expected[i].expire_at, actual[i].expire_at);
  }
}

TEST_F(SnapshotBinaryTest, EmptySnapshot) {
  {
    SnapshotWriter writer(path_);
    ASSERT_TRUE(writer.Open());
    ASSERT_TRUE(writer.Finalize(0));
  }

  std::vector<SnapshotRecord> actual;
  EXPECT_TRUE(ReadSnapshotRecords(path_, &actual));
  EXPECT_TRUE(actual.empty());
}

TEST_F(SnapshotBinaryTest, BadMagicReturnsFalse) {
  FILE* fp = fopen(path_.c_str(), "wb");
  ASSERT_TRUE(fp);
  uint32_t bad = 0xDEADBEEF;
  fwrite(&bad, sizeof(bad), 1, fp);
  fclose(fp);

  std::vector<SnapshotRecord> actual;
  EXPECT_FALSE(ReadSnapshotRecords(path_, &actual));
}

TEST_F(SnapshotBinaryTest, MissingFileReturnsFalse) {
  std::vector<SnapshotRecord> actual;
  EXPECT_FALSE(ReadSnapshotRecords("/tmp/nonexistent.bin", &actual));
}

TEST_F(SnapshotBinaryTest, LargeNumberOfRecords) {
  const int kNumKeys = 10000;
  std::vector<SnapshotRecord> expected;
  expected.reserve(kNumKeys);
  for (int i = 0; i < kNumKeys; i++) {
    expected.push_back({"k" + std::to_string(i), "v" + std::to_string(i), 0});
  }

  {
    SnapshotWriter writer(path_);
    ASSERT_TRUE(writer.Open());
    ASSERT_TRUE(writer.AddBatch(expected.data(), expected.size()));
    ASSERT_TRUE(writer.Finalize(expected.size()));
  }

  std::vector<SnapshotRecord> actual;
  EXPECT_TRUE(ReadSnapshotRecords(path_, &actual));
  EXPECT_EQ(static_cast<size_t>(kNumKeys), actual.size());
  EXPECT_EQ("v42", actual[42].value);
  EXPECT_EQ("v9999", actual[9999].value);
}

TEST_F(SnapshotBinaryTest, RecordsWithExpiration) {
  std::vector<SnapshotRecord> expected = {
    {"no_expiry", "val1", 0},
    {"future", "val2", 999999999999ULL},
  };

  {
    SnapshotWriter writer(path_);
    ASSERT_TRUE(writer.Open());
    ASSERT_TRUE(writer.AddBatch(expected.data(), expected.size()));
    ASSERT_TRUE(writer.Finalize(expected.size()));
  }

  std::vector<SnapshotRecord> actual;
  EXPECT_TRUE(ReadSnapshotRecords(path_, &actual));
  EXPECT_EQ(2u, actual.size());
  EXPECT_EQ(0u, actual[0].expire_at);
  EXPECT_EQ(999999999999ULL, actual[1].expire_at);
}

}  // namespace dfly
