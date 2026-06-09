// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/command_log.h"

#include <algorithm>

#include "base/logging.h"

namespace dfly {

CommandLog::CommandLog() {
  entries_.emplace_back(0, 0, "");  // sentinel at index 0
}

size_t CommandLog::LogSize() const {
  return entries_.size() - 1;
}

LogIndex CommandLog::LastIndex() const {
  return entries_.size() - 1;
}

Term CommandLog::LastTerm() const {
  DCHECK_GT(entries_.size(), 1u);
  return entries_.back().term;
}

const LogEntry& CommandLog::Get(LogIndex index) const {
  DCHECK_LT(index, entries_.size());
  return entries_[index];
}

void CommandLog::Append(LogEntry entry) {
  entry.index = entries_.size();
  entries_.push_back(std::move(entry));
}

std::vector<LogEntry> CommandLog::GetRange(LogIndex start, size_t limit) const {
  DCHECK_GE(start, 1u);
  DCHECK_LE(start, entries_.size());

  size_t end = (limit == 0) ? entries_.size()
                            : std::min(entries_.size(), size_t(start + limit));
  std::vector<LogEntry> result;
  result.reserve(end - start);
  for (size_t i = start; i < end; i++) {
    result.push_back(entries_[i]);
  }
  return result;
}

void CommandLog::TruncateFrom(LogIndex new_last) {
  DCHECK_LT(new_last, entries_.size());
  entries_.resize(new_last + 1);  // +1 for sentinel
}

void CommandLog::Clear() {
  entries_.resize(1);  // keep sentinel
}

void CommandLog::AppendLog(const std::vector<LogEntry>& entries) {
  for (const auto& e : entries) {
    LogEntry copy = e;
    copy.index = entries_.size();
    entries_.push_back(std::move(copy));
  }
}

void CommandLog::AppendBatch(const std::vector<LogEntry>& entries) {
  AppendLog(entries);
}

}  // namespace dfly
