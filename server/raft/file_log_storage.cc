// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/file_log_storage.h"

#include <algorithm>
#include <cstring>

#include "base/logging.h"

namespace dfly {

namespace {

constexpr size_t kHeaderSize = sizeof(RecordHeader);

}  // namespace

FileLogStorage::FileLogStorage() = default;

FileLogStorage::~FileLogStorage() {
  if (read_file_) {
    fclose(read_file_);
  }
}

bool FileLogStorage::Open(const std::string& path) {
  if (writer_.IsOpen()) {
    LOG(WARNING) << "FileLogStorage already open";
    return false;
  }

  if (path.empty())
    return true;  // in-memory only (used for testing)

  path_ = path;

  if (!writer_.Open(path_))
    return false;

  // Open the same file for reading.
  read_file_ = fopen(path_.c_str(), "rb");
  if (!read_file_) {
    PLOG(WARNING) << "Failed to open " << path_ << " for reading";
    writer_.Close();
    return false;
  }

  return true;
}

size_t FileLogStorage::LogSize() const {
  return log_size_;
}

LogIndex FileLogStorage::LastIndex() const {
  return last_index_;
}

Term FileLogStorage::LastTerm() const {
  return last_term_;
}

const LogEntry* FileLogStorage::Get(LogIndex index) const {
  if (index == 0 || index > last_index_)
    return nullptr;

  const EntryLocation* loc = index_.Find(index);
  if (!loc)
    return nullptr;

  // Ensure buffered data is on disk before reading.
  const_cast<FileLogStorage*>(this)->writer_.Flush();

  cached_entry_ = ReadEntryAt(loc->offset);
  return &cached_entry_;
}

LogIndex FileLogStorage::Append(LogEntry entry) {
  entry.index = last_index_ + 1;

  // Record the offset where this entry will be written (before appending to buffer).
  uint64_t offset = writer_.next_write_offset();

  writer_.Append(entry);
  index_.Add(entry.index, kSegmentId, offset);

  last_index_ = entry.index;
  last_term_ = entry.term;
  log_size_++;

  return entry.index;
}

std::vector<LogEntry> FileLogStorage::GetRange(LogIndex start, size_t limit) const {
  if (start > last_index_)
    return {};

  // Ensure buffered data is on disk before reading.
  const_cast<FileLogStorage*>(this)->writer_.Flush();

  LogIndex end = (limit == 0) ? last_index_ : std::min(last_index_, start + limit - 1);

  std::vector<LogEntry> result;
  result.reserve(end - start + 1);

  for (LogIndex i = start; i <= end; i++) {
    const EntryLocation* loc = index_.Find(i);
    if (!loc)
      break;
    result.push_back(ReadEntryAt(loc->offset));
  }

  return result;
}

void FileLogStorage::TruncateFrom(LogIndex new_last) {
  if (new_last >= last_index_)
    return;

  log_size_ = new_last;
  last_index_ = new_last;

  if (new_last == 0) {
    last_term_ = 0;
  } else {
    const EntryLocation* loc = index_.Find(new_last);
    last_term_ = loc ? ReadEntryAt(loc->offset).term : 0;
  }
}

void FileLogStorage::Clear() {
  log_size_ = 0;
  last_index_ = 0;
  last_term_ = 0;
  index_.Clear();
}

bool FileLogStorage::Flush() {
  return writer_.Flush();
}

LogEntry FileLogStorage::ReadEntryAt(uint64_t offset) const {
  DCHECK(read_file_) << "ReadEntryAt requires an open read_file_";

  LogEntry entry;

  RecordHeader hdr;
  if (fseeko(read_file_, static_cast<off_t>(offset), SEEK_SET) != 0) {
    PLOG(WARNING) << "fseeko to " << offset << " failed";
    return entry;
  }

  if (fread(&hdr, 1, kHeaderSize, read_file_) != kHeaderSize) {
    PLOG(WARNING) << "fread header at " << offset << " failed";
    return entry;
  }

  entry.index = hdr.index;
  entry.term = hdr.term;

  if (hdr.size > 0) {
    entry.command.resize(hdr.size);
    if (fread(&entry.command[0], 1, hdr.size, read_file_) != hdr.size) {
      PLOG(WARNING) << "fread command at " << offset + kHeaderSize << " failed";
      entry.command.clear();
    }
  }

  return entry;
}

}  // namespace dfly
