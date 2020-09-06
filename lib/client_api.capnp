@0xcf7efa807cdc1624;

using Go = import "/go.capnp";
$Go.package("client_api");
$Go.import("github.com/Cjen1/rc_op_go/client_api");

struct Request {
  id @0 :Int64;
  key@1: Data;
  union {
    read @2: Void;
    write @3: Data;
  }
}

struct Response {
  id @0 : Int64;
  union {
    success @1 : Void;
    readSuccess @2 : Data;
    failure @3 : Void;
  }
}
