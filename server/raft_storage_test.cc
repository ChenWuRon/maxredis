// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/raft_storage.h"

#include <unistd.h>

#include <cstdio>
#include <fstream>
#include <sstream>

#include <gmock/gmock.h>

#include "base/gtest.h"

namespace dfly {

using namespace testing;

class RaftStorageTest : public Test {
};

// — In-memory tests (no path) —

TEST_F(RaftStorageTest, DefaultState) {
  RaftStorage s;
  EXPECT_EQ(0u, s.current_term());
  EXPECT_TRUE(s.voted_for().empty());
}

TEST_F(RaftStorageTest, CurrentTerm) {
  RaftStorage s;
  EXPECT_EQ(0u, s.current_term());
  s.set_current_term(3);
  EXPECT_EQ(3u, s.current_term());
}

TEST_F(RaftStorageTest, CurrentTermMonotonic) {
  RaftStorage s;
  s.set_current_term(5);
  s.set_current_term(10);
  EXPECT_EQ(10u, s.current_term());
}

TEST_F(RaftStorageTest, VotedFor) {
  RaftStorage s;
  EXPECT_TRUE(s.voted_for().empty());
  s.set_voted_for("node1");
  EXPECT_EQ("node1", s.voted_for());
}

TEST_F(RaftStorageTest, VotedForOverride) {
  RaftStorage s;
  s.set_voted_for("node1");
  s.set_voted_for("node2");
  EXPECT_EQ("node2", s.voted_for());
}

TEST_F(RaftStorageTest, Clear) {
  RaftStorage s;
  s.set_current_term(5);
  s.set_voted_for("node2");

  s.Clear();
  EXPECT_EQ(0u, s.current_term());
  EXPECT_TRUE(s.voted_for().empty());
}

// — Persistence tests —

class RaftStoragePersistenceTest : public Test {
 protected:
  void SetUp() override {
    // Use a unique path per test case.
    path_ = "/tmp/raft_storage_test_" + std::to_string(getpid()) + ".meta.json";
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

TEST_F(RaftStoragePersistenceTest, LoadNonExistentCreatesFile) {
  // File doesn't exist yet.
  EXPECT_FALSE(std::ifstream(path_).is_open());

  RaftStorage s(path_);
  EXPECT_TRUE(s.Load());
  // Load should have created the file.
  EXPECT_TRUE(std::ifstream(path_).is_open());
  EXPECT_EQ(0u, s.current_term());
  EXPECT_TRUE(s.voted_for().empty());
}

TEST_F(RaftStoragePersistenceTest, PersistTerm) {
  RaftStorage s(path_);
  ASSERT_TRUE(s.Load());
  EXPECT_EQ(0u, s.current_term());

  s.set_current_term(15);
  EXPECT_EQ(15u, s.current_term());

  // Reload from a new instance.
  RaftStorage s2(path_);
  EXPECT_TRUE(s2.Load());
  EXPECT_EQ(15u, s2.current_term());
  EXPECT_TRUE(s2.voted_for().empty());
}

TEST_F(RaftStoragePersistenceTest, PersistVotedFor) {
  RaftStorage s(path_);
  ASSERT_TRUE(s.Load());

  s.set_voted_for("node-2");
  EXPECT_EQ("node-2", s.voted_for());

  // Reload from a new instance.
  RaftStorage s2(path_);
  EXPECT_TRUE(s2.Load());
  EXPECT_EQ(0u, s2.current_term());
  EXPECT_EQ("node-2", s2.voted_for());
}

TEST_F(RaftStoragePersistenceTest, PersistBoth) {
  RaftStorage s(path_);
  ASSERT_TRUE(s.Load());

  s.set_current_term(42);
  s.set_voted_for("leader-1");

  RaftStorage s2(path_);
  EXPECT_TRUE(s2.Load());
  EXPECT_EQ(42u, s2.current_term());
  EXPECT_EQ("leader-1", s2.voted_for());
}

TEST_F(RaftStoragePersistenceTest, JsonFormat) {
  RaftStorage s(path_);
  ASSERT_TRUE(s.Load());

  s.set_current_term(7);
  s.set_voted_for("n1");

  std::string content = ReadRawFile();
  EXPECT_THAT(content, HasSubstr("\"current_term\":7"));
  EXPECT_THAT(content, HasSubstr("\"voted_for\":\"n1\""));
}

TEST_F(RaftStoragePersistenceTest, LoadExistingFile) {
  // Write a valid file manually.
  {
    std::ofstream ofs(path_);
    ofs << "{\"current_term\":99,\"voted_for\":\"restored-node\"}\n";
  }

  RaftStorage s(path_);
  EXPECT_TRUE(s.Load());
  EXPECT_EQ(99u, s.current_term());
  EXPECT_EQ("restored-node", s.voted_for());
}

TEST_F(RaftStoragePersistenceTest, LoadEmptyFile) {
  // Write an empty file.
  {
    std::ofstream ofs(path_);
  }

  RaftStorage s(path_);
  EXPECT_TRUE(s.Load());
  // Should reset to defaults.
  EXPECT_EQ(0u, s.current_term());
  EXPECT_TRUE(s.voted_for().empty());
}

TEST_F(RaftStoragePersistenceTest, JsonEscaping) {
  RaftStorage s(path_);
  ASSERT_TRUE(s.Load());

  s.set_voted_for("node\"quote");
  EXPECT_EQ("node\"quote", s.voted_for());

  RaftStorage s2(path_);
  EXPECT_TRUE(s2.Load());
  EXPECT_EQ("node\"quote", s2.voted_for());
}

TEST_F(RaftStoragePersistenceTest, JsonBackslashEscaping) {
  RaftStorage s(path_);
  ASSERT_TRUE(s.Load());

  s.set_voted_for("path\\with\\backslash");
  EXPECT_EQ("path\\with\\backslash", s.voted_for());

  RaftStorage s2(path_);
  EXPECT_TRUE(s2.Load());
  EXPECT_EQ("path\\with\\backslash", s2.voted_for());
}

TEST_F(RaftStoragePersistenceTest, CrashRecovery) {
  // Simulate a crash by writing truncated JSON.
  {
    std::ofstream ofs(path_);
    ofs << "{\"current_term\":50";  // truncated — no closing brace
  }

  RaftStorage s(path_);
  EXPECT_TRUE(s.Load());
  // The partial current_term was parsed before the truncation.
  EXPECT_EQ(50u, s.current_term());
  EXPECT_TRUE(s.voted_for().empty());
}

TEST_F(RaftStoragePersistenceTest, RestartRecovery) {
  // Full cycle: start, write, destroy, restart, verify.
  {
    RaftStorage s(path_);
    ASSERT_TRUE(s.Load());
    EXPECT_EQ(0u, s.current_term());

    s.set_current_term(5);
    s.set_voted_for("node-a");
  }

  // Simulate restart: destroy old object, create new one.
  {
    RaftStorage s(path_);
    EXPECT_TRUE(s.Load());
    EXPECT_EQ(5u, s.current_term());
    EXPECT_EQ("node-a", s.voted_for());
  }

  // Update after restart.
  {
    RaftStorage s(path_);
    EXPECT_TRUE(s.Load());
    EXPECT_EQ(5u, s.current_term());

    s.set_current_term(6);
    EXPECT_EQ("node-a", s.voted_for());
  }

  // Verify final state.
  RaftStorage s(path_);
  EXPECT_TRUE(s.Load());
  EXPECT_EQ(6u, s.current_term());
  EXPECT_EQ("node-a", s.voted_for());
}

}  // namespace dfly
