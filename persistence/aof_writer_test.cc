// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "persistence/aof_writer.h"

#include <gmock/gmock.h>
#include <cstdio>

#include "base/gtest.h"
#include "server/command_serializer.h"

namespace dfly {

using namespace testing;

class AofWriterTest : public Test {
 protected:
  void SetUp() override {
    path_ = "/tmp/appendonly.aof";
    std::remove(path_.c_str());
  }

  void TearDown() override {
    std::remove(path_.c_str());
  }

  std::string path_;
};

TEST_F(AofWriterTest, WriteAndReadThreeRecords) {
  {
    AofWriter writer;
    ASSERT_TRUE(writer.Open(path_));

    writer.Append(CommandSerializer::Serialize({"SET", "key1", "value1"}));
    writer.Append(CommandSerializer::Serialize({"SET", "key2", "value2"}));
    writer.Append(CommandSerializer::Serialize({"SET", "key3", "value3"}));
    writer.Flush();
  }

  FILE* file = fopen(path_.c_str(), "r");
  ASSERT_TRUE(file);

  char buf[1024];
  size_t len = fread(buf, 1, sizeof(buf) - 1, file);
  buf[len] = '\0';
  fclose(file);

  std::string content(buf);

  EXPECT_THAT(content, HasSubstr("*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n"));
  EXPECT_THAT(content, HasSubstr("*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n"));
  EXPECT_THAT(content, HasSubstr("*3\r\n$3\r\nSET\r\n$4\r\nkey3\r\n$6\r\nvalue3\r\n"));
}

}  // namespace dfly
