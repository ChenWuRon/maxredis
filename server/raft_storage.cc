// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft_storage.h"

#include <absl/strings/str_cat.h>

#include <stdexcept>

#include "base/logging.h"

namespace dfly {

RaftStorage::RaftStorage() {
  entries_.emplace_back(0, 0, "");  // sentinel at index 0
}

void RaftStorage::set_current_term(Term term) {
  DCHECK_GE(term, current_term_);
  current_term_ = term;
}

void RaftStorage::set_voted_for(NodeId node_id) {
  voted_for_ = std::move(node_id);
}

size_t RaftStorage::LogSize() const {
  return entries_.size() - 1;  // subtract sentinel
}

const LogEntry& RaftStorage::LastLogEntry() const {
  DCHECK_GT(entries_.size(), 1u);
  return entries_.back();
}

LogIndex RaftStorage::LastLogIndex() const {
  return entries_.size() - 1;
}

Term RaftStorage::LastLogTerm() const {
  DCHECK_GT(entries_.size(), 1u);
  return entries_.back().term;
}

const LogEntry& RaftStorage::EntryAt(LogIndex index) const {
  DCHECK_LT(index, entries_.size());
  return entries_[index];
}

void RaftStorage::AppendLog(LogEntry entry) {
  entry.index = entries_.size();
  entries_.push_back(std::move(entry));
}

void RaftStorage::AppendLog(const std::vector<LogEntry>& entries) {
  for (const auto& e : entries) {
    LogEntry copy = e;
    copy.index = entries_.size();
    entries_.push_back(std::move(copy));
  }
}

std::vector<LogEntry> RaftStorage::ReadLog(LogIndex start, size_t limit) const {
  DCHECK_GE(start, 1u);
  DCHECK_LE(start, entries_.size());

  size_t end = (limit == 0) ? entries_.size() : std::min(entries_.size(), size_t(start + limit));
  std::vector<LogEntry> result;
  result.reserve(end - start);
  for (size_t i = start; i < end; i++) {
    result.push_back(entries_[i]);
  }
  return result;
}

void RaftStorage::TruncateSuffix(LogIndex new_last) {
  DCHECK_LT(new_last, entries_.size());
  entries_.resize(new_last + 1);  // +1 for sentinel
}

void RaftStorage::Clear() {
  entries_.resize(1);  // keep sentinel
  current_term_ = 0;
  voted_for_.clear();
}

}  // namespace dfly
