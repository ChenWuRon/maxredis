// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/raft/snapshot_sender.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <vector>

#include "base/logging.h"
#include "server/raft/install_snapshot_rpc.h"
#include "server/raft/transport.h"

namespace dfly {

SnapshotSender::SnapshotSender(std::string snapshot_path, Transport* transport)
    : snapshot_path_(std::move(snapshot_path)), transport_(transport) {
}

bool SnapshotSender::SendSnapshot(const NodeId& follower, GroupId group_id,
                                   Term term, const NodeId& leader_id,
                                   LogIndex last_included_index,
                                   Term last_included_term) {
  FILE* fp = fopen(snapshot_path_.c_str(), "rb");
  if (!fp) {
    PLOG(WARNING) << "SnapshotSender: failed to open " << snapshot_path_;
    return false;
  }

  // Get file size.
  struct stat st;
  if (fstat(fileno(fp), &st) != 0) {
    PLOG(WARNING) << "SnapshotSender: fstat failed for " << snapshot_path_;
    fclose(fp);
    return false;
  }

  std::vector<char> buf(kChunkSize);
  uint64_t offset = 0;
  bool success = true;

  while (offset < static_cast<uint64_t>(st.st_size)) {
    size_t to_read = kChunkSize;
    if (offset + to_read > static_cast<uint64_t>(st.st_size)) {
      to_read = st.st_size - offset;
    }

    size_t nread = fread(buf.data(), 1, to_read, fp);
    if (nread == 0) {
      PLOG(WARNING) << "SnapshotSender: fread failed at offset " << offset;
      success = false;
      break;
    }

    InstallSnapshotRequest req;
    req.group_id = group_id;
    req.term = term;
    req.leader_id = leader_id;
    req.last_included_index = last_included_index;
    req.last_included_term = last_included_term;
    req.offset = offset;
    req.done = (offset + nread >= static_cast<uint64_t>(st.st_size));
    req.data.assign(buf.data(), nread);

    InstallSnapshotResponse rsp = transport_->SendInstallSnapshot(follower, req);
    if (rsp.term > term) {
      LOG(WARNING) << "SnapshotSender: follower " << follower
                   << " has higher term " << rsp.term;
      success = false;
      break;
    }
    if (!rsp.success) {
      LOG(WARNING) << "SnapshotSender: follower " << follower
                   << " rejected chunk at offset " << offset;
      success = false;
      break;
    }

    offset += nread;
  }

  fclose(fp);
  return success;
}

}  // namespace dfly
