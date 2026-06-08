// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "persistence/snapshot_manager.h"

#include <gmock/gmock.h>

#include <absl/strings/str_cat.h>

#include <cstdio>

#include "base/gtest.h"

namespace dfly {

using namespace std;
using namespace testing;

class SnapshotManagerTest : public Test {
 protected:
  void SetUp() override {
    path_ = "/tmp/snapshot.bin";
    std::remove(path_.c_str());
  }

  void TearDown() override {
    std::remove(path_.c_str());
  }

  string path_;
};

static SnapshotData MakeTestData(int n) {
  SnapshotData data;
  data.entries.reserve(n);
  for (int i = 0; i < n; i++) {
    SnapshotEntry e;
    e.key = absl::StrCat("key", i);
    e.value = absl::StrCat("value", i);
    e.expire_ms = (i % 3 == 0) ? 1000 + i : 0;
    data.entries.push_back(std::move(e));
  }
  return data;
}

TEST_F(SnapshotManagerTest, EncodeDecodeRoundTrip) {
  auto original = MakeTestData(100);

  SnapshotEncoder encoder;
  string encoded = encoder.Encode(original);

  SnapshotDecoder decoder;
  SnapshotData decoded;
  ASSERT_TRUE(decoder.Decode(encoded, &decoded));

  EXPECT_EQ(original.entries.size(), decoded.entries.size());
  for (size_t i = 0; i < original.entries.size(); i++) {
    EXPECT_EQ(original.entries[i].key, decoded.entries[i].key);
    EXPECT_EQ(original.entries[i].value, decoded.entries[i].value);
    EXPECT_EQ(original.entries[i].expire_ms, decoded.entries[i].expire_ms);
  }
}

TEST_F(SnapshotManagerTest, SaveAndLoadFile) {
  auto original = MakeTestData(100);

  SnapshotManager mgr;
  ASSERT_TRUE(mgr.Save(path_, original));

  SnapshotData loaded;
  ASSERT_TRUE(mgr.Load(path_, &loaded));

  EXPECT_EQ(original.entries.size(), loaded.entries.size());
  for (size_t i = 0; i < original.entries.size(); i++) {
    EXPECT_EQ(original.entries[i].key, loaded.entries[i].key);
    EXPECT_EQ(original.entries[i].value, loaded.entries[i].value);
    EXPECT_EQ(original.entries[i].expire_ms, loaded.entries[i].expire_ms);
  }
}

TEST_F(SnapshotManagerTest, HandlesMissingFile) {
  SnapshotManager mgr;
  SnapshotData data;
  EXPECT_FALSE(mgr.Load("/tmp/nonexistent_snapshot.bin", &data));
}

TEST_F(SnapshotManagerTest, HandlesEmptyData) {
  SnapshotData original;
  SnapshotEncoder encoder;
  string encoded = encoder.Encode(original);

  SnapshotDecoder decoder;
  SnapshotData decoded;
  ASSERT_TRUE(decoder.Decode(encoded, &decoded));
  EXPECT_TRUE(decoded.entries.empty());
}

TEST_F(SnapshotManagerTest, HandlesCorruptData) {
  SnapshotDecoder decoder;
  SnapshotData data;
  EXPECT_FALSE(decoder.Decode("\x01\x00\x00\x00\xff", &data));
}

}  // namespace dfly
