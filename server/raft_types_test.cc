// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft_types.h"

#include <gmock/gmock.h>

#include <cstring>
#include <sstream>

#include "base/gtest.h"

namespace dfly {

using namespace testing;

class RaftTypesTest : public Test {
};

TEST_F(RaftTypesTest, RaftRoleValues) {
  EXPECT_EQ(0, static_cast<int>(RaftRole::Follower));
  EXPECT_EQ(1, static_cast<int>(RaftRole::Candidate));
  EXPECT_EQ(2, static_cast<int>(RaftRole::Leader));
}

TEST_F(RaftTypesTest, TypeSizes) {
  EXPECT_EQ(1, sizeof(RaftRole));
  EXPECT_EQ(8, sizeof(Term));
  EXPECT_EQ(8, sizeof(LogIndex));
}

TEST_F(RaftTypesTest, LogEntryDefault) {
  LogEntry e;
  EXPECT_EQ(0u, e.term);
  EXPECT_EQ(0u, e.index);
  EXPECT_TRUE(e.command.empty());
}

TEST_F(RaftTypesTest, LogEntryCustom) {
  LogEntry e(3, 42, "SET a 1");
  EXPECT_EQ(3u, e.term);
  EXPECT_EQ(42u, e.index);
  EXPECT_EQ("SET a 1", e.command);
}

TEST_F(RaftTypesTest, LogEntryBinarySerialization) {
  LogEntry original(5, 100, "SET x y");
  std::string buf;
  buf.append(reinterpret_cast<const char*>(&original.term), sizeof(original.term));
  buf.append(reinterpret_cast<const char*>(&original.index), sizeof(original.index));
  uint32_t cmd_len = original.command.size();
  buf.append(reinterpret_cast<const char*>(&cmd_len), sizeof(cmd_len));
  buf.append(original.command);

  LogEntry restored;
  size_t pos = 0;
  std::memcpy(&restored.term, buf.data() + pos, sizeof(restored.term));
  pos += sizeof(restored.term);
  std::memcpy(&restored.index, buf.data() + pos, sizeof(restored.index));
  pos += sizeof(restored.index);
  uint32_t len;
  std::memcpy(&len, buf.data() + pos, sizeof(len));
  pos += sizeof(len);
  restored.command.assign(buf.data() + pos, len);

  EXPECT_EQ(original.term, restored.term);
  EXPECT_EQ(original.index, restored.index);
  EXPECT_EQ(original.command, restored.command);
}

TEST_F(RaftTypesTest, LogEntryStreamSerialization) {
  LogEntry original(7, 200, "DEL k1 k2");

  std::ostringstream os;
  os << original.term << '|' << original.index << '|' << original.command.size() << '|'
     << original.command;
  std::string serialized = os.str();

  std::istringstream is(serialized);
  LogEntry restored;
  std::string token;
  std::getline(is, token, '|');
  restored.term = std::stoull(token);
  std::getline(is, token, '|');
  restored.index = std::stoull(token);
  std::getline(is, token, '|');
  size_t cmd_len = std::stoul(token);
  restored.command.resize(cmd_len);
  is.read(restored.command.data(), cmd_len);

  EXPECT_EQ(original.term, restored.term);
  EXPECT_EQ(original.index, restored.index);
  EXPECT_EQ(original.command, restored.command);
}

TEST_F(RaftTypesTest, LogEntryVector) {
  std::vector<LogEntry> entries;
  entries.emplace_back(1, 10, "PING");
  entries.emplace_back(1, 11, "SET a 1");
  entries.emplace_back(2, 12, "SET b 2");

  EXPECT_EQ(3, entries.size());
  EXPECT_EQ(1u, entries[0].term);
  EXPECT_EQ(10u, entries[0].index);
  EXPECT_EQ("PING", entries[0].command);
  EXPECT_EQ(2u, entries[2].term);
  EXPECT_EQ("SET b 2", entries[2].command);
}

}  // namespace dfly
