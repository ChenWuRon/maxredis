// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/file_log_storage.h"

#include <algorithm>
#include <cstring>
#include <unistd.h>

#include "base/logging.h"
#include "server/raft/crc32.h"

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

  // Find the segment containing new_last and read its entry.
  const EntryLocation* loc = index_.Find(new_last);
  if (!loc) {
    LOG(WARNING) << "TruncateFrom(" << new_last << "): entry not found in index";
    return;
  }

  LogEntry last_entry = ReadEntryAt(loc->segment_id, loc->offset);
  uint32_t keep_segment = loc->segment_id;
  uint64_t truncate_offset = loc->offset + kHeaderSize + last_entry.command.size();

  // Truncate the segment file at the offset after new_last.
  std::string seg_path = SegmentPath(keep_segment);
  FILE* f = GetReadFile(keep_segment);
  if (f) {
    fclose(f);
    read_files_[keep_segment] = nullptr;
  }

  if (truncate(seg_path.c_str(), truncate_offset) != 0) {
    PLOG(WARNING) << "truncate(" << seg_path << ", " << truncate_offset << ") failed";
  }

  // Delete all subsequent segment files.
  for (uint32_t seg = keep_segment + 1; seg < read_files_.size(); seg++) {
    if (read_files_[seg]) {
      fclose(read_files_[seg]);
      read_files_[seg] = nullptr;
    }
    std::string del_path = SegmentPath(seg);
    unlink(del_path.c_str());
  }

  // Re-open the truncated segment for reading.
  read_files_[keep_segment] = fopen(seg_path.c_str(), "rb");

  // If the truncated file is from an older segment, roll back the writer.
  if (keep_segment < current_segment_) {
    writer_.Close();
    current_segment_ = keep_segment;
    if (!writer_.OpenAppend(seg_path)) {
      LOG(FATAL) << "Failed to reopen segment " << seg_path << " after truncate";
    }
  }

  // Update in-memory state.
  log_size_ = new_last;
  last_index_ = new_last;
  last_term_ = last_entry.term;
  index_.Truncate(new_last);

  // Update manifest if we rolled back.
  if (current_segment_ != manifest_.current_segment()) {
    manifest_.set_current_segment(current_segment_);
    manifest_.Save();
  }

  VLOG(1) << "TruncateFrom(" << new_last << "): keep_seg=" << keep_segment
          << " current_seg=" << current_segment_;
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

  // Verify CRC32C.
  uint32_t computed = ComputeCrc32(entry.command.data(), entry.command.size());
  if (computed != hdr.crc32) {
    LOG(ERROR) << "CRC mismatch for entry " << entry.index
               << " seg=" << segment_id << " offset=" << offset
               << " expected=" << hdr.crc32 << " got=" << computed;
    entry.index = 0;
    entry.term = 0;
    entry.command.clear();
  }

  return entry;
}

}  // namespace dfly
