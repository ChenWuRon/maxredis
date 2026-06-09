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

FileLogStorage::FileLogStorage()
    : manifest_("") {
}

FileLogStorage::~FileLogStorage() {
  for (FILE* f : read_files_) {
    if (f)
      fclose(f);
  }
}

bool FileLogStorage::Open(const std::string& dir) {
  if (writer_.IsOpen()) {
    LOG(WARNING) << "FileLogStorage already open";
    return false;
  }

  if (dir.empty())
    return true;

  dir_ = dir;
  manifest_ = ManifestManager(dir_);

  if (!manifest_.Load())
    return false;

  current_segment_ = manifest_.current_segment();

  std::string seg_path = SegmentPath(current_segment_);
  if (!writer_.Open(seg_path))
    return false;

  // Pre-allocate read file handles.
  read_files_.resize(current_segment_ + 1, nullptr);

  VLOG(1) << "FileLogStorage opened at " << dir_
          << " segment=" << current_segment_;
  return true;
}

bool FileLogStorage::Flush() {
  return writer_.Flush();
}

void FileLogStorage::RollSegment() {
  writer_.Flush();
  writer_.Close();

  current_segment_++;
  manifest_.set_current_segment(current_segment_);
  manifest_.Save();

  std::string seg_path = SegmentPath(current_segment_);
  if (!writer_.Open(seg_path)) {
    LOG(FATAL) << "Failed to open new segment: " << seg_path;
  }

  // Grow read file vector for the new segment.
  if (current_segment_ >= read_files_.size()) {
    read_files_.resize(current_segment_ + 1, nullptr);
  }

  VLOG(1) << "Rolled to segment " << current_segment_;
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

  const_cast<FileLogStorage*>(this)->writer_.Flush();

  cached_entry_ = ReadEntryAt(loc->segment_id, loc->offset);
  return &cached_entry_;
}

LogIndex FileLogStorage::Append(LogEntry entry) {
  entry.index = last_index_ + 1;

  // Auto-rotate if current segment exceeds limit.
  if (writer_.file_size() >= kMaxSegmentSize) {
    const_cast<FileLogStorage*>(this)->RollSegment();
  }

  uint64_t offset = writer_.next_write_offset();
  writer_.Append(entry);
  index_.Add(entry.index, current_segment_, offset);

  last_index_ = entry.index;
  last_term_ = entry.term;
  log_size_++;

  return entry.index;
}

std::vector<LogEntry> FileLogStorage::GetRange(LogIndex start, size_t limit) const {
  if (start > last_index_)
    return {};

  const_cast<FileLogStorage*>(this)->writer_.Flush();

  LogIndex end = (limit == 0) ? last_index_ : std::min(last_index_, start + limit - 1);

  std::vector<LogEntry> result;
  result.reserve(end - start + 1);

  for (LogIndex i = start; i <= end; i++) {
    const EntryLocation* loc = index_.Find(i);
    if (!loc)
      break;
    result.push_back(ReadEntryAt(loc->segment_id, loc->offset));
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
    last_term_ = loc ? ReadEntryAt(loc->segment_id, loc->offset).term : 0;
  }
}

void FileLogStorage::Clear() {
  log_size_ = 0;
  last_index_ = 0;
  last_term_ = 0;
  index_.Clear();
}

std::string FileLogStorage::SegmentPath(uint32_t segment_id) const {
  char buf[64];
  snprintf(buf, sizeof(buf), "%s/segment_%08lu.log",
           dir_.c_str(), static_cast<unsigned long>(segment_id));
  return std::string(buf);
}

FILE* FileLogStorage::GetReadFile(uint32_t segment_id) const {
  if (segment_id >= read_files_.size())
    return nullptr;

  if (read_files_[segment_id] != nullptr)
    return read_files_[segment_id];

  std::string path = SegmentPath(segment_id);
  FILE* f = fopen(path.c_str(), "rb");
  if (!f) {
    PLOG(WARNING) << "Failed to open " << path << " for reading";
    return nullptr;
  }

  read_files_[segment_id] = f;
  return f;
}

LogEntry FileLogStorage::ReadEntryAt(uint32_t segment_id, uint64_t offset) const {
  LogEntry entry;
  FILE* f = GetReadFile(segment_id);
  if (!f)
    return entry;

  RecordHeader hdr;
  if (fseeko(f, static_cast<off_t>(offset), SEEK_SET) != 0) {
    PLOG(WARNING) << "fseeko seg=" << segment_id << " offset=" << offset << " failed";
    return entry;
  }

  if (fread(&hdr, 1, kHeaderSize, f) != kHeaderSize) {
    PLOG(WARNING) << "fread header seg=" << segment_id << " offset=" << offset << " failed";
    return entry;
  }

  entry.index = hdr.index;
  entry.term = hdr.term;

  if (hdr.size > 0) {
    entry.command.resize(hdr.size);
    if (fread(&entry.command[0], 1, hdr.size, f) != hdr.size) {
      PLOG(WARNING) << "fread command seg=" << segment_id << " failed";
      entry.command.clear();
    }
  }

  return entry;
}

}  // namespace dfly
