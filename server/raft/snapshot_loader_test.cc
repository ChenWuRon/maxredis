// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/snapshot_loader.h"

#include <gmock/gmock.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>

#include "base/gtest.h"
#include "server/raft/snapshot_meta.h"
#include "server/raft/snapshot_writer.h"

namespace dfly {

using namespace testing;

class SnapshotLoaderTest : public Test {
 protected:
  void SetUp() override {
    dir_ = "/tmp/snapshot_loader_test_" + std::to_string(getpid()) + "/";
    mkdir(dir_.c_str(), 0755);
  }

  void TearDown() override {
    unlink((dir_ + "snapshot.meta").c_str());
    unlink((dir_ + "snapshot.meta.tmp").c_str());
    unlink((dir_ + "snapshot.bin").c_str());
    unlink((dir_ + "snapshot.bin.tmp").c_str());
    rmdir(dir_.c_str());
  }

  void WriteMeta(LogIndex index, Term term) {
    SnapshotMetaStorage sms(dir_ + "snapshot.meta");
    ASSERT_TRUE(sms.Load());
    sms.SetMeta({index, term, 100});
  }

  void WriteBin() {
    SnapshotWriter writer(dir_ + "snapshot.bin");
    ASSERT_TRUE(writer.Open());
    ASSERT_TRUE(writer.Add({"k1", "v1", 0}));
    ASSERT_TRUE(writer.Finalize(1));
  }

  void WriteEmptyFile(const std::string& name) {
    std::ofstream ofs(dir_ + name);
    ofs << "";
    ofs.close();
  }

  std::string dir_;
};

TEST_F(SnapshotLoaderTest, LoadValidSnapshot) {
  WriteMeta(1000, 8);
  WriteBin();

  SnapshotLoader loader(dir_);
  LoadedSnapshot loaded;
  EXPECT_EQ(SnapshotLoadStatus::OK, loader.Load(&loaded));
  EXPECT_EQ(1000u, loaded.meta.index);
  EXPECT_EQ(8u, loaded.meta.term);
  EXPECT_EQ(100u, loaded.meta.timestamp_ms);
  EXPECT_EQ(dir_ + "snapshot.bin", loaded.bin_path);
}

TEST_F(SnapshotLoaderTest, MissingMeta) {
  WriteBin();

  SnapshotLoader loader(dir_);
  LoadedSnapshot loaded;
  EXPECT_EQ(SnapshotLoadStatus::NoSnapshot, loader.Load(&loaded));
}

TEST_F(SnapshotLoaderTest, MissingBin) {
  WriteMeta(1000, 8);

  SnapshotLoader loader(dir_);
  LoadedSnapshot loaded;
  EXPECT_EQ(SnapshotLoadStatus::Corrupted, loader.Load(&loaded));
}

TEST_F(SnapshotLoaderTest, EmptyMeta) {
  WriteEmptyFile("snapshot.meta");

  SnapshotLoader loader(dir_);
  LoadedSnapshot loaded;
  // Empty meta file gets default (index=0, term=0) → NoSnapshot.
  EXPECT_EQ(SnapshotLoadStatus::NoSnapshot, loader.Load(&loaded));
}

TEST_F(SnapshotLoaderTest, ZeroIndex) {
  WriteMeta(0, 8);
  WriteBin();

  SnapshotLoader loader(dir_);
  LoadedSnapshot loaded;
  EXPECT_EQ(SnapshotLoadStatus::NoSnapshot, loader.Load(&loaded));
}

TEST_F(SnapshotLoaderTest, ZeroTerm) {
  WriteMeta(1000, 0);
  WriteBin();

  SnapshotLoader loader(dir_);
  LoadedSnapshot loaded;
  EXPECT_EQ(SnapshotLoadStatus::Corrupted, loader.Load(&loaded));
}

TEST_F(SnapshotLoaderTest, EmptyBin) {
  WriteMeta(1000, 8);
  WriteEmptyFile("snapshot.bin");

  SnapshotLoader loader(dir_);
  LoadedSnapshot loaded;
  EXPECT_EQ(SnapshotLoadStatus::Corrupted, loader.Load(&loaded));
}

TEST_F(SnapshotLoaderTest, CorruptedBinBadMagic) {
  WriteMeta(1000, 8);
  {
    std::ofstream ofs(dir_ + "snapshot.bin", std::ios::binary);
    uint32_t bad_magic = 0xDEADBEEF;
    ofs.write(reinterpret_cast<const char*>(&bad_magic), sizeof(bad_magic));
  }

  SnapshotLoader loader(dir_);
  LoadedSnapshot loaded;
  EXPECT_EQ(SnapshotLoadStatus::Corrupted, loader.Load(&loaded));
}

TEST_F(SnapshotLoaderTest, LoadWithNullOutput) {
  WriteMeta(1000, 8);
  WriteBin();

  SnapshotLoader loader(dir_);
  EXPECT_EQ(SnapshotLoadStatus::OK, loader.Load(nullptr));
}

}  // namespace dfly
