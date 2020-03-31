@0xd66c44db48f005fe;

struct Op {
  key @0 :Text;
  union {
    read @1 :Void;

    write :group {
      value @2 :Text;
    }
  }
}

struct Command {
  op @0 :Op;
  id @1 :Int64;
}

struct CommandResult {
  union {
    success @0 : Void;
    readSuccess @1 : Text;
    failure @2 : Void;
  }
}

struct LogEntry {
  command @0: Command;
  term @1 : Int64;
  index @2 : Int64;
}

interface ServiceInterface {
  requestVote @0
    (term :Int64, leaderCommit :Int64) -> 
      (term :Int64, voteGranted :Bool, entries :List(LogEntry));

  appendEntries @1
    (
      term :Int64,
      prevLogIndex :Int64,
      prevLogTerm :Int64,
      entries :List(LogEntry),
      leaderCommit :Int64
    ) -> (term :Int64, success :Bool);
}

interface ClientInterface {
  clientRequest @0
    (command :Command) -> (result: CommandResult);
}
