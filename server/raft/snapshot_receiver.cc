// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/snapshot_receiver.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>

#include "base/logging.h"
#include "server/raft/install_snapshot_rpc.h"

namespace dfly {

SnapshotReceiver::SnapshotReceiver(std::string dir)
    : dir_(std::move(dir)), tmp_path_(dir_ + "snapshot.recv.tmp"),
      bin_path_(dir_ + "snapshot.bin") {
}

void SnapshotReceiver::Init() {
  // Remove any stale tmp file from a previous crash.
  if (unlink(tmp_path_.c_str()) != 0 && errno != ENOENT) {
    PLOG(WARNING) << "SnapshotReceiver: failed to remove stale " << tmp_path_;
  }
}

InstallSnapshotResponse SnapshotReceiver::HandleChunk(
    const InstallSnapshotRequest& request) {
  InstallSnapshotResponse response;
  response.term = request.term;

  // Ensure directory exists.
  auto slash = dir_.rfind('/');
  if (slash != std::string::npos) {
    std::string parent = dir_.substr(0, slash);
    if (parent.empty())
      parent = "/";
    struct stat st;
    if (stat(parent.c_str(), &st) != 0) {
      // Try creating it.
      if (mkdir(parent.c_str(), 0755) != 0 && errno != EEXIST) {
        PLOG(WARNING) << "SnapshotReceiver: mkdir(" << parent << ") failed";
        response.success = false;
        return response;
      }
    }
  }

  FILE* fp = fopen(tmp_path_.c_str(), "ab");
  if (!fp) {
    PLOG(WARNING) << "SnapshotReceiver: failed to open " << tmp_path_;
    response.success = false;
    return response;
  }

  // Write data at the correct offset.
  if (fseek(fp, request.offset, SEEK_SET) != 0) {
    PLOG(WARNING) << "SnapshotReceiver: fseek to " << request.offset << " failed";
    fclose(fp);
    response.success = false;
    return response;
  }

  size_t towrite = request.data.size();
  if (fwrite(request.data.data(), 1, towrite, fp) != towrite) {
    PLOG(WARNING) << "SnapshotReceiver: fwrite failed at offset " << request.offset;
    fclose(fp);
    response.success = false;
    return response;
  }

  if (request.done) {
    // Final chunk: flush, fsync, close, and rename.
    if (fflush(fp) != 0) {
      PLOG(WARNING) << "SnapshotReceiver: fflush failed";
      fclose(fp);
      unlink(tmp_path_.c_str());
      response.success = false;
      return response;
    }
    if (fdatasync(fileno(fp)) != 0) {
      PLOG(WARNING) << "SnapshotReceiver: fdatasync failed";
      fclose(fp);
      unlink(tmp_path_.c_str());
      response.success = false;
      return response;
    }
    fclose(fp);
    fp = nullptr;

    if (rename(tmp_path_.c_str(), bin_path_.c_str()) != 0) {
      PLOG(WARNING) << "SnapshotReceiver: rename " << tmp_path_ << " -> "
                     << bin_path_ << " failed";
      unlink(tmp_path_.c_str());
      response.success = false;
      return response;
    }
  } else {
    fclose(fp);
  }

  response.success = true;
  return response;
}

}  // namespace dfly
