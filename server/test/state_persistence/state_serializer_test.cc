// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/service/state_serializer.h"

#include <gmock/gmock.h>

#include <vector>

#include <absl/strings/str_cat.h>

#include "base/gtest.h"
#include "server/storage/db_slice.h"

namespace dfly {

using namespace std;
using namespace testing;

class StateSerializerTest : public Test {
};

TEST_F(StateSerializerTest, ExportImport100Keys) {
  DbSlice slice(0, nullptr);

  int num_keys = 100;
  // Capture the values written so verification never re-derives them from a
  // (possibly different) wall-clock reading — NowMs() advances between the
  // insert loop and the verify loop, making the re-derived expectation flaky
  // under load.
  std::vector<uint64_t> expected_expire(num_keys);
  for (int i = 0; i < num_keys; i++) {
    string key = absl::StrCat("key", i);
    string value = absl::StrCat("value", i);
    auto [it, _] = slice.AddOrFind(0, key);
    it->second.value = value;
    expected_expire[i] = (i % 2 == 0) ? NowMs() + 3600000 : 0;
    it->second.expire_ms = expected_expire[i];
  }

  EXPECT_EQ(slice.DbSize(0), num_keys);

  auto data = StateSerializer::Export(slice);
  EXPECT_EQ(data.entries.size(), num_keys);

  DbSlice slice2(0, nullptr);
  StateSerializer::Import(&slice2, data);

  EXPECT_EQ(slice2.DbSize(0), num_keys);

  for (int i = 0; i < num_keys; i++) {
    string key = absl::StrCat("key", i);
    string expected_value = absl::StrCat("value", i);

    auto res1 = slice.Find(0, key);
    ASSERT_TRUE(res1.ok()) << "key=" << key;
    EXPECT_EQ(res1.value()->second.value, expected_value);
    EXPECT_EQ(res1.value()->second.expire_ms, expected_expire[i]);

    auto res2 = slice2.Find(0, key);
    ASSERT_TRUE(res2.ok()) << "key=" << key;
    EXPECT_EQ(res2.value()->second.value, expected_value);
    EXPECT_EQ(res2.value()->second.expire_ms, expected_expire[i]);
  }
}

}  // namespace dfly
