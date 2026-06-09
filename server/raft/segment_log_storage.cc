// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/segment_log_storage.h"

#include <algorithm>
#include <cstring>
#include <dirent.h>

#include "base/logging.h"
#include "server/raft/crc32.h"
#include "server/raft/wal_writer.h"

namespace dfly {

namespace {

constexpr size_t kHeaderSize = sizeof(RecordHeader);

}  // namespace

SegmentLogStorage::SegmentLogStorage() : manifest_("") {
  entries_.emplace_back(0, 0, "");
}

SegmentLogStorage::SegmentLogStorage(std::string dir)
    : dir_(std::move(dir)), manifest_(dir_) {
  entries_.emplace_back(0, 0, "");
}

bool SegmentLogStorage::Open() {
  if (dir_.empty())
    return true;

  if (!manifest_.Load())
    return false;

  LoadSegments();
  return true;
}

std::string SegmentLogStorage::SegmentPath(uint32_t segment_id) const {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s/segment_%08lu.log",
           dir_.c_str(), static_cast<unsigned long>(segment_id));
  return std::string(buf);
}

void SegmentLogStorage::LoadSegments() {
  auto segments = DiscoverSegments();
  for (uint32_t seg_id : segments) {
    ScanSegment(seg_id);
  }
}

std::vector<uint32_t> SegmentLogStorage::DiscoverSegments() const {
  std::vector<uint32_t> segments;

  DIR* dir = opendir(dir_.c_str());
  if (!dir) {
    PLOG(WARNING) << "opendir(" << dir_ << ") failed";
    return segments;
  }

  constexpr const char* kPrefix = "segment_";
  constexpr size_t kPrefixLen = 8;

  struct dirent* entry;
  while ((entry = readdir(dir)) != nullptr) {
    const char* name = entry->d_name;

    // Skip . and ..
    if (name[0] == '.' && (name[1] == '\0' || (name[1] == '.' && name[2] == '\0')))
      continue;
    if (strncmp(name, kPrefix, kPrefixLen) != 0)
      continue;

    size_t name_len = strlen(name);
    if (name_len < kPrefixLen + 5)  // segment_ + digits + .log
      continue;

    const char* ext = name + name_len - 4;
    if (strcmp(ext, ".log") != 0)
      continue;

    char* end = nullptr;
    uint64_t id = strtoull(name + kPrefixLen, &end, 10);
    if (end == name + kPrefixLen || *end != '.')
      continue;

    segments.push_back(static_cast<uint32_t>(id));
  }

  closedir(dir);

  std::sort(segments.begin(), segments.end());
  return segments;
}

void SegmentLogStorage::ScanSegment(uint32_t segment_id) {
  std::string path = SegmentPath(segment_id);
  FILE* fp = fopen(path.c_str(), "rb");
  if (!fp) {
    PLOG(WARNING) << "Failed to open segment " << path << " for scanning";
    return;
  }

  while (true) {
    uint64_t offset = ftello(fp);

    RecordHeader hdr;
    size_t nread = fread(&hdr, 1, kHeaderSize, fp);
    if (nread == 0)
      break;
    if (nread != kHeaderSize) {
      VLOG(1) << "Partial header at end of " << path << " - stopping scan";
      break;
    }

    if (hdr.size == 0)
      break;

    // Validate strict monotonic index.
    if (hdr.index <= last_index_) {
      VLOG(1) << "Non-monotonic index " << hdr.index << " (prev="
              << last_index_ << ") in " << path << " - stopping scan";
      break;
    }

    LogEntry entry;
    entry.index = hdr.index;
    entry.term = hdr.term;
    entry.command.resize(hdr.size);

    nread = fread(&entry.command[0], 1, hdr.size, fp);
    if (nread != hdr.size) {
      VLOG(1) << "Partial payload at end of " << path << " - stopping scan";
      break;
    }

    uint32_t computed = ComputeCrc32(entry.command.data(), entry.command.size());
    if (computed != hdr.crc32) {
      VLOG(1) << "CRC mismatch in " << path << " - stopping scan";
      break;
    }

    entries_.push_back(std::move(entry));
    RebuildIndex(hdr.index, hdr.term, segment_id, offset);
  }

  fclose(fp);
}

void SegmentLogStorage::RebuildIndex(LogIndex index, Term term,
                                     uint32_t segment_id, uint64_t offset) {
  index_.Add(index, segment_id, offset);
  last_index_ = index;
  last_term_ = term;
}

size_t SegmentLogStorage::LogSize() const {
  if (entries_.size() <= 1)
    return 0;
  return entries_.size() - 1;
}

LogIndex SegmentLogStorage::FirstIndex() const {
  if (entries_.size() <= 1)
    return 0;
  LogIndex first = base_index_ + 1;
  return (first <= last_index_) ? first : 0;
}

LogIndex SegmentLogStorage::LastIndex() const {
  return last_index_;
}

Term SegmentLogStorage::LastTerm() const {
  return last_term_;
}

const LogEntry* SegmentLogStorage::Get(LogIndex index) const {
  if (index < base_index_ || index > last_index_)
    return nullptr;
  // Fast path: entries are typically contiguous at entries_[index - base_index_].
  size_t physical = index - base_index_;
  if (physical < entries_.size() && entries_[physical].index == index)
    return &entries_[physical];
  // Slow path: scan for the entry (handles non-contiguous recovery).
  for (size_t i = 1; i < entries_.size(); i++) {
    if (entries_[i].index == index)
      return &entries_[i];
  }
  return nullptr;
}

LogIndex SegmentLogStorage::Append(LogEntry entry) {
  entry.index = last_index_ + 1;
  last_index_ = entry.index;
  last_term_ = entry.term;
  entries_.push_back(std::move(entry));
  return entry.index;
}

std::vector<LogEntry> SegmentLogStorage::GetRange(LogIndex start, size_t limit) const {
  if (start < FirstIndex() || start > LastIndex())
    return {};

  size_t physical = start - base_index_;
  DCHECK_GE(physical, 1u);
  size_t end = (limit == 0) ? entries_.size()
                            : std::min(entries_.size(), physical + limit);
  std::vector<LogEntry> result;
  result.reserve(end - physical);
  for (size_t i = physical; i < end; i++) {
    result.push_back(entries_[i]);
  }
  return result;
}

void SegmentLogStorage::TruncateFrom(LogIndex new_last) {
  if (new_last <= base_index_) {
    index_.Clear();
    last_index_ = 0;
    last_term_ = 0;
    entries_.resize(1);
    return;
  }
  size_t physical = new_last - base_index_;
  DCHECK_LT(physical, entries_.size());
  index_.Truncate(new_last);
  last_index_ = new_last;
  last_term_ = entries_[physical].term;
  entries_.resize(physical + 1);
}

bool SegmentLogStorage::CompactUpTo(LogIndex index) {
  if (index <= base_index_)
    return true;

  if (index >= last_index_) {
    base_index_ = index;
    index_.Clear();
    last_index_ = index;
    last_term_ = 0;
    entries_.resize(1);
    return true;
  }

  // Remove entries [1 .. index - base_index_] and update base_index_.
  size_t keep_from = index - base_index_ + 1;
  if (keep_from < entries_.size()) {
    std::move(entries_.begin() + keep_from, entries_.end(),
              entries_.begin() + 1);
    entries_.resize(entries_.size() - keep_from + 1);
  } else {
    entries_.resize(1);
  }
  base_index_ = index;
  // Update WalIndex: remove entries up to index.
  // Since we shift entries, the WalIndex would be stale.
  // For simplicity, clear and rely on lazy rebuild.
  index_.Clear();
  return true;
}

void SegmentLogStorage::Clear() {
  base_index_ = 0;
  index_.Clear();
  last_index_ = 0;
  last_term_ = 0;
  entries_.resize(1);
}

}  // namespace dfly
