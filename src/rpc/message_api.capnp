@0x9207445e65eea38d;

interface Message {
  # Interface for RPC messaging system
  
  struct Command {
    clientId @0 :UInt16;
    # Id of the client that issued command

    commandId @1 :UInt16;
    # Id of the command being issued

    operation @2 :Text;     
    # Encodes the operation that will be applied to state of application
    # The type of this is temporary for now
  }
  # Structure represents the command sent in RPC

  struct Response {  
    commandId @0 :UInt16;
    # Id of the command that has been performed by replicas

    result @1 :Text;
    # Encodes the result of applying operation to app state
    # Type is temporary for now
  }
  
  clientRequest @0 (command :Command) -> (response :Response);
}
