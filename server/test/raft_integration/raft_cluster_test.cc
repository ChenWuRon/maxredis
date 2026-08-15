// Copyright 2026, The MaxRedis Authors.
// See LICENSE for licensing terms.
//
// End-to-end multi-node Raft cluster test. Spawns 3 real midi-redis
// processes wired with the TCP transport (--raft_peers) + durable WAL
// (--raft_dir) and verifies the production acceptance criteria:
//
//   1. A leader is elected among 3 nodes.
//   2. Writes succeed on the leader after a majority ACK; followers converge.
//   3. SIGKILL the leader -> a new leader is elected within a few heartbeat
//      cycles; the cluster keeps serving writes.
//   4. The killed node restarts from its durable state (term/voted_for/WAL/
//      apply progress) and re-syncs with the cluster (no stale/missing data).

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/logging.h"

using namespace std;
using namespace testing;

namespace dfly {
namespace {

#ifndef MIDI_REDIS_BINARY
#define MIDI_REDIS_BINARY "midi-redis"
#endif

// Minimal RESP client (no hiredis dependency): encodes command arrays,
// decodes status/int/error/bulk replies.
class RespClient {
 public:
  RespClient() = default;
  ~RespClient() {
    Close();
  }

  RespClient(const RespClient&) = delete;
  RespClient& operator=(const RespClient&) = delete;

  bool Connect(int port, int timeout_ms) {
    Close();
    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0)
      return false;

    struct timeval tv {timeout_ms / 1000, (timeout_ms % 1000) * 1000};
    setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
      Close();
      return false;
    }
    return true;
  }

  void Close() {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

  // Sends a command; returns false on IO error.
  bool Send(const vector<string>& args) {
    string msg = "*" + to_string(args.size()) + "\r\n";
    for (const string& a : args) {
      msg += "$" + to_string(a.size()) + "\r\n" + a + "\r\n";
    }
    return WriteAll(msg.data(), msg.size());
  }

  // Reads one reply line (strips \r\n).
  bool ReadLine(string* line) {
    line->clear();
    char c;
    while (true) {
      ssize_t n = ::read(fd_, &c, 1);
      if (n <= 0)
        return false;
      if (c == '\n') {
        if (!line->empty() && line->back() == '\r')
          line->pop_back();
        return true;
      }
      line->push_back(c);
    }
  }

  // Reads a full reply. status_out/int_out/error_out/bulk_out set as applicable.
  bool ReadReply(string* reply) {
    string line;
    if (!ReadLine(&line))
      return false;
    if (line.empty())
      return false;

    switch (line[0]) {
      case '+':  // status
      case '-':  // error
        *reply = line;
        return true;
      case ':':  // int
        *reply = line;
        return true;
      case '$': {  // bulk string
        int len = atoi(line.c_str() + 1);
        if (len < 0) {
          *reply = "$-1";
          return true;
        }
        string data;
        data.resize(len + 2);
        if (!ReadN(data.data(), len + 2))
          return false;
        *reply = data.substr(0, len);
        return true;
      }
      case '*': {  // array (rarely used here; read elements as lines)
        *reply = line;
        return true;
      }
      default:
        return false;
    }
  }

 private:
  bool WriteAll(const char* data, size_t len) {
    size_t done = 0;
    while (done < len) {
      ssize_t n = ::write(fd_, data + done, len - done);
      if (n <= 0)
        return false;
      done += n;
    }
    return true;
  }

  bool ReadN(char* data, size_t len) {
    size_t done = 0;
    while (done < len) {
      ssize_t n = ::read(fd_, data + done, len - done);
      if (n <= 0)
        return false;
      done += n;
    }
    return true;
  }

  int fd_ = -1;
};

// One midi-redis subprocess with its own ports + raft dir.
class NodeProc {
 public:
  NodeProc(string id, string workdir, string binary)
      : id_(std::move(id)), workdir_(std::move(workdir)), binary_(std::move(binary)) {
  }

  ~NodeProc() {
    Kill();
  }

  void set_ports(int client_port, int raft_port) {
    client_port_ = client_port;
    raft_port_ = raft_port;
  }

  void set_peers(const string& peers) {
    peers_ = peers;
  }

  void set_linearizable_read(bool on) {
    linearizable_read_ = on;
  }

  int client_port() const {
    return client_port_;
  }

  int raft_port() const {
    return raft_port_;
  }

  const string& id() const {
    return id_;
  }

  bool Start() {
    if (getenv("RAFT_TEST_VERBOSE"))
      mkdir((workdir_ + "/logs").c_str(), 0755);
    pid_ = fork();
    if (pid_ < 0)
      return false;
    if (pid_ == 0) {
      // Child: isolate CWD (appendonly.aof etc. per node) and exec.
      chdir(workdir_.c_str());
      string raft_dir = workdir_ + "/raft";
      vector<string> args = {
          binary_,
          "--port=" + to_string(client_port_),
          "--raft_port=" + to_string(raft_port_),
          "--raft_node_id=" + id_,
          "--raft_peers=" + peers_,
          "--raft_dir=" + raft_dir,
          "--proactor_threads=2",
          "--force_epoll=true",
          // The HTTP listener defaults to port 8080; with 3 nodes on one
          // host they would all collide. Disable it — the cluster test only
          // speaks RESP to --port.
          "--http_port=-1",
      };
      if (linearizable_read_)
        args.push_back("--linearizable_read=true");
      // RAFT_TEST_VERBOSE=1 dumps per-node -v=1 logs to <workdir>/logs —
      // invaluable when a cluster scenario misbehaves under the test.
      if (getenv("RAFT_TEST_VERBOSE")) {
        args.push_back("--log_dir=" + workdir_ + "/logs");
        args.push_back("-v=1");
      }
      vector<char*> argv;
      for (string& a : args)
        argv.push_back(a.data());
      argv.push_back(nullptr);
      execv(binary_.c_str(), argv.data());
      _exit(127);
    }
    return true;
  }

  void Kill() {
    if (pid_ > 0) {
      kill(pid_, SIGKILL);
      waitpid(pid_, nullptr, 0);
      pid_ = -1;
    }
  }

  // SIGSTOP: freezes the whole process — from the cluster's perspective the
  // node is partitioned (no heartbeats in or out), like iptables DROP.
  void Pause() {
    if (pid_ > 0)
      kill(pid_, SIGSTOP);
  }

  void Resume() {
    if (pid_ > 0)
      kill(pid_, SIGCONT);
  }

  bool is_running() const {
    return pid_ > 0;
  }

 private:
  string id_, workdir_, binary_, peers_;
  int client_port_ = 0, raft_port_ = 0;
  bool linearizable_read_ = false;
  pid_t pid_ = -1;
};

int PickFreePort() {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  CHECK_GE(fd, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  CHECK_EQ(0, bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)));
  socklen_t len = sizeof(addr);
  CHECK_EQ(0, getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len));
  int port = ntohs(addr.sin_port);
  close(fd);
  return port;
}

// Sends |args| to node |port|; returns true if the reply is |expected|.
bool TryCmd(int port, const vector<string>& args, const string& expected,
            int timeout_ms = 3000) {
  RespClient client;
  if (!client.Connect(port, timeout_ms))
    return false;
  if (!client.Send(args))
    return false;
  string reply;
  if (!client.ReadReply(&reply))
    return false;
  return reply == expected;
}

// Waits until fn() returns true.
bool WaitFor(function<bool()> fn, int timeout_ms) {
  auto deadline = chrono::steady_clock::now() + chrono::milliseconds(timeout_ms);
  while (chrono::steady_clock::now() < deadline) {
    if (fn())
      return true;
    this_thread::sleep_for(chrono::milliseconds(50));
  }
  return fn();
}

// Polls |ports| until one accepts a SET (i.e. is the leader). Returns the
// leader's index or -1.
int FindLeader(const vector<int>& ports, int timeout_ms = 10000) {
  int leader = -1;
  WaitFor([&] {
    for (size_t i = 0; i < ports.size(); ++i) {
      if (TryCmd(ports[i], {"SET", "leader_probe", "1"}, "+OK", 500)) {
        leader = static_cast<int>(i);
        return true;
      }
    }
    return false;
  }, timeout_ms);
  return leader;
}

class RaftClusterTest : public Test {
 protected:
  void SetUp() override {
    binary_ = MIDI_REDIS_BINARY;
    base_dir_ = base::GetTestTempDir();
  }

  // Starts |n| nodes with distinct ports and one shared --raft_peers list.
  void StartCluster(int n, bool linearizable_read = false) {
    nodes_.clear();
    peers_str_.clear();
    vector<pair<int, int>> ports;
    for (int i = 0; i < n; ++i) {
      ports.push_back({PickFreePort(), PickFreePort()});
    }
    for (int i = 0; i < n; ++i) {
      string id = "n" + to_string(i + 1);
      if (!peers_str_.empty())
        peers_str_ += ",";
      peers_str_ += id + ":127.0.0.1:" + to_string(ports[i].second);
    }
    for (int i = 0; i < n; ++i) {
      string id = "n" + to_string(i + 1);
      string workdir = base_dir_ + "/node_" + id;
      mkdir(workdir.c_str(), 0755);
      nodes_.emplace_back(make_unique<NodeProc>(id, workdir, binary_));
      nodes_.back()->set_ports(ports[i].first, ports[i].second);
      nodes_.back()->set_peers(peers_str_);
      nodes_.back()->set_linearizable_read(linearizable_read);
      ASSERT_TRUE(nodes_.back()->Start());
    }
    // Wait for every node's client port to accept connections.
    for (auto& node : nodes_) {
      ASSERT_TRUE(WaitFor([&] {
        RespClient probe;
        return probe.Connect(node->client_port(), 500);
      }, 15000)) << "node " << node->id() << " did not come up";
    }
  }

  vector<int> client_ports() const {
    vector<int> res;
    for (auto& n : nodes_)
      res.push_back(n->client_port());
    return res;
  }

  void TearDown() override {
    for (auto& n : nodes_)
      n->Kill();
  }

  string binary_;
  string base_dir_;
  string peers_str_;
  vector<unique_ptr<NodeProc>> nodes_;
};

TEST_F(RaftClusterTest, ThreeNodeElectionAndReplication) {
  StartCluster(3);
  vector<int> ports = client_ports();

  // 1. A leader emerges.
  int leader = FindLeader(ports);
  ASSERT_GE(leader, 0) << "no leader elected";

  // 2. Write on the leader -> majority ACK -> success.
  ASSERT_TRUE(TryCmd(ports[leader], {"SET", "greeting", "hello"}, "+OK", 5000));

  // 3. Followers converge (Raft replication applies the committed entry).
  for (size_t i = 0; i < ports.size(); ++i) {
    if (static_cast<int>(i) == leader)
      continue;
    ASSERT_TRUE(WaitFor([&] {
      return TryCmd(ports[i], {"GET", "greeting"}, "hello", 500);
    }, 8000)) << "follower " << i << " did not converge";
  }

  // 4. Writes to a follower are rejected explicitly (no silent split-brain).
  for (size_t i = 0; i < ports.size(); ++i) {
    if (static_cast<int>(i) == leader)
      continue;
    RespClient client;
    ASSERT_TRUE(client.Connect(ports[i], 1000));
    ASSERT_TRUE(client.Send({"SET", "on_follower", "x"}));
    string reply;
    ASSERT_TRUE(client.ReadReply(&reply));
    EXPECT_EQ('-', reply[0]) << "follower must reject writes, got: " << reply;
  }
}

TEST_F(RaftClusterTest, LeaderKillFailoverAndRestartRecovery) {
  StartCluster(3);
  vector<int> ports = client_ports();

  int leader = FindLeader(ports);
  ASSERT_GE(leader, 0) << "no leader elected";

  ASSERT_TRUE(TryCmd(ports[leader], {"SET", "k1", "v1"}, "+OK", 5000));

  // Wait for k1 to be applied on the followers BEFORE killing the leader so
  // the restart-recovery step can verify full data convergence.
  for (size_t i = 0; i < ports.size(); ++i) {
    if (static_cast<int>(i) == leader)
      continue;
    ASSERT_TRUE(WaitFor([&] {
      RespClient c;
      return c.Connect(ports[i], 500) &&
             ([&] {
               if (!c.Send({"GET", "k1"}))
                 return false;
               string r;
               return c.ReadReply(&r) && r == "v1";
             })();
    }, 8000));
  }

  // Kill the leader (kill -9 — no graceful flush, worst case for recovery).
  int killed = leader;
  nodes_[killed]->Kill();

  // A new leader must emerge within a few heartbeat cycles (election timeout
  // is 150-300ms; give it 10s for process scheduling under test load).
  vector<int> remaining_ports;
  for (size_t i = 0; i < ports.size(); ++i)
    if (static_cast<int>(i) != killed)
      remaining_ports.push_back(ports[i]);
  int new_leader = FindLeader(remaining_ports, 10000);
  ASSERT_GE(new_leader, 0) << "no new leader after killing the old one";

  // The cluster keeps serving writes after failover.
  ASSERT_TRUE(TryCmd(remaining_ports[new_leader], {"SET", "k2", "v2"}, "+OK", 5000));

  // Restart the killed node with the same raft_dir: it must recover its
  // durable state (term/voted_for/WAL/apply.meta) and re-sync.
  ASSERT_TRUE(nodes_[killed]->Start());
  ASSERT_TRUE(WaitFor([&] {
    RespClient probe;
    return probe.Connect(nodes_[killed]->client_port(), 500);
  }, 15000)) << "restarted node did not come up";

  // Both keys must eventually be visible on the restarted node (it replays
  // its own WAL and then catches up from the leader).
  ASSERT_TRUE(WaitFor([&] {
    RespClient c;
    if (!c.Connect(ports[killed], 1000))
      return false;
    if (!c.Send({"GET", "k1"}))
      return false;
    string r1;
    if (!c.ReadReply(&r1) || r1 != "v1")
      return false;
    if (!c.Send({"GET", "k2"}))
      return false;
    string r2;
    return c.ReadReply(&r2) && r2 == "v2";
  }, 15000)) << "restarted node did not converge to k1/v1 + k2/v2";
}

// Network partition of the leader (SIGSTOP freezes all its threads — the
// cluster observes the same as an iptables DROP of its traffic):
//   1. The cluster elects a new leader within a few heartbeat cycles.
//   2. After the old leader resumes, it steps down (a higher term reaches it)
//      and stops serving writes AND linearizable reads (its lease — only
//      renewable by majority ACK — has long expired during the freeze).
TEST_F(RaftClusterTest, LeaderPartitionStepsDownNoStaleLinearRead) {
  StartCluster(3, /*linearizable_read=*/true);
  vector<int> ports = client_ports();

  int leader = FindLeader(ports);
  ASSERT_GE(leader, 0) << "no leader elected";
  ASSERT_TRUE(TryCmd(ports[leader], {"SET", "partitioned_key", "v1"}, "+OK", 5000));

  // Linearizable read on the live leader: must see the fresh write.
  ASSERT_TRUE(TryCmd(ports[leader], {"GET", "partitioned_key"}, "v1", 5000));

  // Freeze the leader: majority contact is lost.
  nodes_[leader]->Pause();

  // A new leader must emerge among the remaining two.
  vector<int> remaining_ports;
  for (size_t i = 0; i < ports.size(); ++i)
    if (static_cast<int>(i) != leader)
      remaining_ports.push_back(ports[i]);
  int new_leader = FindLeader(remaining_ports, 10000);
  ASSERT_GE(new_leader, 0) << "no new leader during the partition";

  // The partitioned node is frozen — its write path is unreachable, and the
  // cluster keeps serving.
  ASSERT_TRUE(TryCmd(remaining_ports[new_leader], {"SET", "during_partition", "v2"}, "+OK", 5000));

  // Resume the old leader. It must observe the higher term and step down.
  nodes_[leader]->Resume();
  ASSERT_TRUE(WaitFor([&] {
    RespClient c;
    if (!c.Connect(ports[leader], 1000))
      return false;
    if (!c.Send({"SET", "should_fail", "x"}))
      return false;
    string r;
    return c.ReadReply(&r) && !r.empty() && r[0] == '-';
  }, 10000)) << "old leader kept accepting writes after resume";

  // Linearizable reads on the demoted node must FAIL (no stale reads via a
  // fast-path lease — the lease requires a majority ACK to renew).
  ASSERT_TRUE(WaitFor([&] {
    RespClient c;
    if (!c.Connect(ports[leader], 1000))
      return false;
    if (!c.Send({"GET", "partitioned_key"}))
      return false;
    string r;
    return c.ReadReply(&r) && !r.empty() && r[0] == '-';
  }, 10000)) << "demoted leader still serving linearizable reads";

  // The old leader converges on the data written during the partition.
  // (A linearizable GET cannot be used here: the demoted node is a follower
  // and --linearizable_read makes followers reject ALL reads by design —
  // the accepted way to observe its state machine is INFO's keyspace.)
  ASSERT_TRUE(WaitFor([&] {
    RespClient c;
    if (!c.Connect(ports[leader], 1000))
      return false;
    if (!c.Send({"INFO"}))
      return false;
    string r;
    return c.ReadReply(&r) &&
           r.find("db0:keys=3") != string::npos;
  }, 15000)) << "demoted leader did not converge after resume";
}

}  // namespace
}  // namespace dfly
