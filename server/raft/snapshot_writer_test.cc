// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/snapshot_writer.h"

#include <gmock/gmock.h>

#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include "base/gtest.h"

namespace dfly {

using namespace testing;

// Reads binary snapshot file and returns records.
// Used to verify the written file is valid.
static std::vector<SnapshotRecord> ReadSnapshot(const std::string& path) {
  FILE* fp = fopen(path.c_str(), "rb");
  if (!fp)
    return {};

  uint32_t magic, num_records;
  if (fread(&magic, sizeof(magic), 1, fp) != 1) {
    fclose(fp);
    return {};
  }
  if (magic != kSnapshotMagic) {
    ADD_FAILURE() << "Bad magic: 0x" << std::hex << magic
                  << " expected: 0x" << kSnapshotMagic << std::dec;
    fclose(fp);
    return {};
  }
  if (fread(&num_records, sizeof(num_records), 1, fp) != 1) {
    fclose(fp);
    return {};
  }

  std::vector<SnapshotRecord> records;
  records.reserve(num_records);
  for (uint32_t i = 0; i < num_records; i++) {
    uint32_t key_len, value_len;
    uint64_t expire_at;
    if (fread(&key_len, sizeof(key_len), 1, fp) != 1)
      break;
    if (fread(&value_len, sizeof(value_len), 1, fp) != 1)
      break;
    if (fread(&expire_at, sizeof(expire_at), 1, fp) != 1)
      break;

    SnapshotRecord rec;
    rec.key.resize(key_len);
    rec.value.resize(value_len);
    rec.expire_at = expire_at;
    if (key_len > 0)
      fread(&rec.key[0], 1, key_len, fp);
    if (value_len > 0)
      fread(&rec.value[0], 1, value_len, fp);
    records.push_back(std::move(rec));
  }

  fclose(fp);
  return records;
}

class SnapshotWriterTest : public Test {
 protected:
  void SetUp() override {
    path_ = "/tmp/snapshot_writer_test_" + std::to_string(getpid()) + ".bin";
  }

  void TearDown() override {
    unlink(path_.c_str());
    unlink((path_ + ".tmp").c_str());
  }

  std::string path_;
};

TEST_F(SnapshotWriterTest, EmptySnapshot) {
  SnapshotWriter writer(path_);
  ASSERT_TRUE(writer.Open());
  ASSERT_TRUE(writer.Finalize(0));

  // Verify file exists and has correct header
  FILE* fp = fopen(path_.c_str(), "rb");
  ASSERT_NE(nullptr, fp) << "File not found: " << path_;
  uint32_t magic, num;
  ASSERT_EQ(1u, fread(&magic, sizeof(magic), 1, fp));
  ASSERT_EQ(1u, fread(&num, sizeof(num), 1, fp));
  ASSERT_EQ(kSnapshotMagic, magic);
  ASSERT_EQ(0u, num);
  fclose(fp);

  auto records = ReadSnapshot(path_);
  EXPECT_EQ(0u, records.size());
}

TEST_F(SnapshotWriterTest, SingleRecord) {
  {
    SnapshotWriter writer(path_);
    ASSERT_TRUE(writer.Open());
    ASSERT_TRUE(writer.Add({"k1", "v1", 0}));
    ASSERT_TRUE(writer.Finalize(1));
  }  // writer destroyed

  // Read header
  FILE* fp = fopen(path_.c_str(), "rb");
  ASSERT_NE(nullptr, fp);
  uint32_t magic, num;
  ASSERT_EQ(1u, fread(&magic, sizeof(magic), 1, fp));
  ASSERT_EQ(1u, fread(&num, sizeof(num), 1, fp));
  ASSERT_EQ(kSnapshotMagic, magic);
  ASSERT_EQ(1u, num);

  // Read record
  uint32_t key_len, value_len;
  uint64_t expire_at;
  ASSERT_EQ(1u, fread(&key_len, sizeof(key_len), 1, fp));
  ASSERT_EQ(1u, fread(&value_len, sizeof(value_len), 1, fp));
  ASSERT_EQ(1u, fread(&expire_at, sizeof(expire_at), 1, fp));
  EXPECT_EQ(2u, key_len);
  EXPECT_EQ(2u, value_len);
  EXPECT_EQ(0u, expire_at);

  char key[3] = {}, val[3] = {};
  ASSERT_EQ(2u, fread(key, 1, 2, fp));
  ASSERT_EQ(2u, fread(val, 1, 2, fp));
  EXPECT_STREQ("k1", key);
  EXPECT_STREQ("v1", val);
  fclose(fp);

  // Also verify via ReadSnapshot helper
  auto records = ReadSnapshot(path_);
  ASSERT_EQ(1u, records.size());
  EXPECT_EQ("k1", records[0].key);
  EXPECT_EQ("v1", records[0].value);
  EXPECT_EQ(0u, records[0].expire_at);
}

TEST_F(SnapshotWriterTest, MultipleRecords) {
  SnapshotWriter writer(path_);
  ASSERT_TRUE(writer.Open());

  SnapshotRecord records_in[] = {
    {"key1", "val1", 100},
    {"key2", "val2", 200},
    {"key3", "val3", 300},
  };

  ASSERT_TRUE(writer.AddBatch(records_in, 3));
  ASSERT_TRUE(writer.Finalize(3));

  auto records_out = ReadSnapshot(path_);
  ASSERT_EQ(3u, records_out.size());
  EXPECT_EQ("key1", records_out[0].key);
  EXPECT_EQ("val1", records_out[0].value);
  EXPECT_EQ(100u, records_out[0].expire_at);
  EXPECT_EQ("key2", records_out[1].key);
  EXPECT_EQ("val2", records_out[1].value);
  EXPECT_EQ("key3", records_out[2].key);
  EXPECT_EQ("val3", records_out[2].value);
}

TEST_F(SnapshotWriterTest, LargeRecord) {
  std::string big_key(1000, 'A');
  std::string big_val(10000, 'B');

  SnapshotWriter writer(path_);
  ASSERT_TRUE(writer.Open());
  ASSERT_TRUE(writer.Add({big_key, big_val, 999}));
  ASSERT_TRUE(writer.Finalize(1));

  auto records = ReadSnapshot(path_);
  ASSERT_EQ(1u, records.size());
  EXPECT_EQ(big_key, records[0].key);
  EXPECT_EQ(big_val, records[0].value);
  EXPECT_EQ(999u, records[0].expire_at);
}

TEST_F(SnapshotWriterTest, FinalizeRenamesFile) {
  SnapshotWriter writer(path_);
  ASSERT_TRUE(writer.Open());
  ASSERT_TRUE(writer.Add({"k", "v", 0}));
  ASSERT_TRUE(writer.Finalize(1));

  // .tmp should not exist after Finalize.
  EXPECT_NE(0u, access((path_ + ".tmp").c_str(), F_OK));
  // .bin should exist.
  EXPECT_EQ(0u, access(path_.c_str(), F_OK));

  auto records = ReadSnapshot(path_);
  ASSERT_EQ(1u, records.size());
}

}  // namespace dfly
