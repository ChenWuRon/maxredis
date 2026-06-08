// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/command_log.h"

#include "base/logging.h"

namespace dfly {

CommandLog::CommandLog() {
  entries_.emplace_back(0, 0, "");  // sentinel at index 0
}

size_t CommandLog::Size() const {
  return entries_.size() - 1;
}

LogIndex CommandLog::LastIndex() const {
  return entries_.size() - 1;
}

void CommandLog::Append(LogEntry entry) {
  entry.index = entries_.size();
  entries_.push_back(std::move(entry));
}

const LogEntry& CommandLog::Get(LogIndex index) const {
  DCHECK_LT(index, entries_.size());
  return entries_[index];
}

}  // namespace dfly
