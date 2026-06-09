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
  if (entries_.size() <= 1)
    return 0;
  return LastIndex() - FirstIndex() + 1;
}

LogIndex CommandLog::FirstIndex() const {
  if (entries_.size() <= 1)
    return 0;
  LogIndex first = base_index_ + 1;
  return (first <= LastIndex()) ? first : 0;
}

LogIndex CommandLog::LastIndex() const {
  if (entries_.size() <= 1)
    return 0;
  return base_index_ + entries_.size() - 1;
}

Term CommandLog::LastTerm() const {
  if (entries_.size() <= 1)
    return 0;
  return entries_.back().term;
}

const LogEntry* CommandLog::Get(LogIndex index) const {
  if (index < base_index_ || index > LastIndex())
    return nullptr;
  return &entries_[index - base_index_];
}

LogIndex CommandLog::Append(LogEntry entry) {
  entry.index = LastIndex() + 1;
  entries_.push_back(std::move(entry));
  return entry.index;
}

std::vector<LogEntry> CommandLog::GetRange(LogIndex start, size_t limit) const {
  if (start < FirstIndex() || start > LastIndex())
    return {};

  size_t physical_start = start - base_index_;
  DCHECK_GE(physical_start, 1u);
  size_t end = (limit == 0) ? entries_.size()
                            : std::min(entries_.size(), physical_start + limit);
  std::vector<LogEntry> result;
  result.reserve(end - physical_start);
  for (size_t i = physical_start; i < end; i++) {
    result.push_back(entries_[i]);
  }
  return result;
}

void CommandLog::TruncateFrom(LogIndex new_last) {
  if (new_last <= base_index_) {
    entries_.resize(1);
    return;
  }
  size_t physical = new_last - base_index_;
  DCHECK_LT(physical, entries_.size());
  entries_.resize(physical + 1);  // +1 for sentinel
}

bool CommandLog::CompactUpTo(LogIndex index) {
  if (index <= base_index_)
    return true;

  if (index >= LastIndex()) {
    base_index_ = index;
    entries_.resize(1);
    return true;
  }

  // Remove entries [1 .. index - base_index_] and update base_index_.
  // Physical index of the first entry to keep:
  size_t keep_from = index - base_index_ + 1;
  if (keep_from < entries_.size()) {
    // Shift remaining entries to start after sentinel.
    std::move(entries_.begin() + keep_from, entries_.end(),
              entries_.begin() + 1);
    entries_.resize(entries_.size() - keep_from + 1);
  } else {
    entries_.resize(1);
  }
  base_index_ = index;
  return true;
}

void CommandLog::Clear() {
  base_index_ = 0;
  entries_.resize(1);  // keep sentinel
}

void CommandLog::AppendLog(const std::vector<LogEntry>& entries) {
  for (const auto& e : entries) {
    LogEntry copy = e;
    copy.index = LastIndex() + 1;
    entries_.push_back(std::move(copy));
  }
}

void CommandLog::AppendBatch(const std::vector<LogEntry>& entries) {
  AppendLog(entries);
}

}  // namespace dfly
