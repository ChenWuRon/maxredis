// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/command_serializer.h"

#include <gmock/gmock.h>

#include "base/gtest.h"

namespace dfly {

using namespace testing;

class CommandSerializerTest : public Test {};

TEST_F(CommandSerializerTest, SET) {
  std::string result = CommandSerializer::Serialize({"SET", "a", "1"});
  EXPECT_EQ("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n", result);
}

TEST_F(CommandSerializerTest, GET) {
  std::string result = CommandSerializer::Serialize({"GET", "a"});
  EXPECT_EQ("*2\r\n$3\r\nGET\r\n$1\r\na\r\n", result);
}

TEST_F(CommandSerializerTest, DEL) {
  std::string result = CommandSerializer::Serialize({"DEL", "a", "b"});
  EXPECT_EQ("*3\r\n$3\r\nDEL\r\n$1\r\na\r\n$1\r\nb\r\n", result);
}

}  // namespace dfly
