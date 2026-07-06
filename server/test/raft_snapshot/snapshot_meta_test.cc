// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/snapshot_meta.h"

#include <gmock/gmock.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>

#include "base/gtest.h"

namespace dfly {

using namespace testing;

TEST(SnapshotMetaTest, DefaultState) {
  SnapshotMetaStorage sms;
  EXPECT_EQ(0u, sms.meta().index);
  EXPECT_EQ(0u, sms.meta().term);
  EXPECT_EQ(0u, sms.meta().timestamp_ms);
}

TEST(SnapshotMetaTest, SetAndQuery) {
  SnapshotMetaStorage sms;
  sms.SetMeta({1000, 8, 1730000000});
  EXPECT_EQ(1000u, sms.meta().index);
  EXPECT_EQ(8u, sms.meta().term);
  EXPECT_EQ(1730000000u, sms.meta().timestamp_ms);
}

TEST(SnapshotMetaTest, Override) {
  SnapshotMetaStorage sms;
  sms.SetMeta({500, 3, 100});
  sms.SetMeta({1000, 8, 200});
  EXPECT_EQ(1000u, sms.meta().index);
  EXPECT_EQ(8u, sms.meta().term);
}

class SnapshotMetaPersistenceTest : public Test {
 protected:
  void SetUp() override {
    path_ = "/tmp/snapshot_meta_test_" + std::to_string(getpid()) + ".json";
  }

  void TearDown() override {
    unlink(path_.c_str());
    unlink((path_ + ".tmp").c_str());
  }

  std::string ReadRawFile() const {
    std::ifstream ifs(path_);
    if (!ifs.is_open())
      return "";
    std::ostringstream oss;
    oss << ifs.rdbuf();
    return oss.str();
  }

  std::string path_;
};

TEST_F(SnapshotMetaPersistenceTest, PersistAndReload) {
  SnapshotMetaStorage sms(path_);
  ASSERT_TRUE(sms.Load());
  EXPECT_EQ(0u, sms.meta().index);
  EXPECT_EQ(0u, sms.meta().term);

  sms.SetMeta({1000, 8, 1730000000});
  EXPECT_EQ(1000u, sms.meta().index);
  EXPECT_EQ(8u, sms.meta().term);

  SnapshotMetaStorage sms2(path_);
  EXPECT_TRUE(sms2.Load());
  EXPECT_EQ(1000u, sms2.meta().index);
  EXPECT_EQ(8u, sms2.meta().term);
  EXPECT_EQ(1730000000u, sms2.meta().timestamp_ms);
}

TEST_F(SnapshotMetaPersistenceTest, LoadNonExistent) {
  unlink(path_.c_str());
  SnapshotMetaStorage sms(path_);
  EXPECT_TRUE(sms.Load());
  EXPECT_EQ(0u, sms.meta().index);
  EXPECT_EQ(0u, sms.meta().term);
}

TEST_F(SnapshotMetaPersistenceTest, JsonFormat) {
  SnapshotMetaStorage sms(path_);
  ASSERT_TRUE(sms.Load());
  sms.SetMeta({1000, 8, 1730000000});

  std::string content = ReadRawFile();
  EXPECT_THAT(content, HasSubstr("\"index\":1000"));
  EXPECT_THAT(content, HasSubstr("\"term\":8"));
  EXPECT_THAT(content, HasSubstr("\"timestamp_ms\":1730000000"));
}

TEST_F(SnapshotMetaPersistenceTest, LoadExistingFile) {
  {
    std::ofstream ofs(path_);
    ofs << "{\"index\":999,\"term\":7,\"timestamp_ms\":100}\n";
  }

  SnapshotMetaStorage sms(path_);
  EXPECT_TRUE(sms.Load());
  EXPECT_EQ(999u, sms.meta().index);
  EXPECT_EQ(7u, sms.meta().term);
  EXPECT_EQ(100u, sms.meta().timestamp_ms);
}

TEST_F(SnapshotMetaPersistenceTest, RestartRecovery) {
  {
    SnapshotMetaStorage sms(path_);
    ASSERT_TRUE(sms.Load());
    sms.SetMeta({1000, 8, 1730000000});
  }
  {
    SnapshotMetaStorage sms(path_);
    EXPECT_TRUE(sms.Load());
    EXPECT_EQ(1000u, sms.meta().index);
    EXPECT_EQ(8u, sms.meta().term);
  }
  {
    SnapshotMetaStorage sms(path_);
    EXPECT_TRUE(sms.Load());
    sms.SetMeta({2000, 12, 1730000001});
  }
  SnapshotMetaStorage sms(path_);
  EXPECT_TRUE(sms.Load());
  EXPECT_EQ(2000u, sms.meta().index);
  EXPECT_EQ(12u, sms.meta().term);
}

}  // namespace dfly
