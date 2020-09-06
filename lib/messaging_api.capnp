@0xd66c44db48f005fe;

using Go = import "/go.capnp";
$Go.package("client_api");
$Go.import("github.com/Cjen1/rc_op_go/client_api");

struct Op {
  key @0 :Data;
  union {
    read @1 :Void;

    write :group {
      value @2 :Data;
    }
  }
}

struct Command {
  op @0 :Op;
  id @1 :Int64;
}

struct LogEntry {
  command @0: Command;
  term @1 : Int64;
}

struct RequestVote {
  term @0:Int64;
  leaderCommit @1:Int64;
}

struct RequestVoteResp {
  term @0:Int64;
  voteGranted @1: Bool;
  entries @2: List(LogEntry);
  startIndex @3: Int64;
}

struct AppendEntries {
      term @0: Int64;
      prevLogIndex @1: Int64;
      prevLogTerm @2: Int64;
      entries @3: List(LogEntry);
      leaderCommit @4: Int64;
}

struct AppendEntriesResp {
  term @0: Int64;
  union {
    success @1 : Int64; # Match Index
    failure @2 : Int64; # Prev_log_index
  }
}

struct ClientRequest {
  id @0 :Int64;
  key@1: Data;
  union {
    read @2: Void;
    write @3: Data;
  }
}

struct ClientResponse {
  id @0 : Int64;
  union {
    success @1 : Void;
    readSuccess @2 : Data;
    failure @3 : Void;
  }
}
struct ServerMessage {
  union {
    requestVote @0: RequestVote;
    requestVoteResp @1: RequestVoteResp;
    appendEntries @2: AppendEntries;
    appendEntriesResp @3: AppendEntriesResp;
    clientRequest @4: ClientRequest;
    clientResponse @5: ClientResponse;
  }
}
