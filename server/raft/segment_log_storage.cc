// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/segment_log_storage.h"

#include <algorithm>

#include "base/logging.h"

namespace dfly {

SegmentLogStorage::SegmentLogStorage() {
  entries_.emplace_back(0, 0, "");
}

size_t SegmentLogStorage::LogSize() const {
  return entries_.size() - 1;
}

LogIndex SegmentLogStorage::LastIndex() const {
  return entries_.size() - 1;
}

Term SegmentLogStorage::LastTerm() const {
  if (entries_.size() <= 1)
    return 0;
  return entries_.back().term;
}

const LogEntry* SegmentLogStorage::Get(LogIndex index) const {
  if (index >= entries_.size())
    return nullptr;
  return &entries_[index];
}

LogIndex SegmentLogStorage::Append(LogEntry entry) {
  entry.index = entries_.size();
  entries_.push_back(std::move(entry));
  return entry.index;
}

std::vector<LogEntry> SegmentLogStorage::GetRange(LogIndex start, size_t limit) const {
  if (start > LastIndex())
    return {};

  DCHECK_GE(start, 1u);
  size_t end = (limit == 0) ? entries_.size()
                            : std::min(entries_.size(), size_t(start + limit));
  std::vector<LogEntry> result;
  result.reserve(end - start);
  for (size_t i = start; i < end; i++) {
    result.push_back(entries_[i]);
  }
  return result;
}

void SegmentLogStorage::TruncateFrom(LogIndex new_last) {
  DCHECK_LT(new_last, entries_.size());
  entries_.resize(new_last + 1);
}

void SegmentLogStorage::Clear() {
  entries_.resize(1);
}

}  // namespace dfly
