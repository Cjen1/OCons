@0x9207445e65eea38d;

interface Message {
  # Interface for RPC messaging system
  
  struct Command {
    struct Operation {
    # Encodes the operation passed in a command as a union of possible ops
      union {
        nop @0 :Void;
        # No operation, one possible value (hence void)

        create :group {
          key @1 :UInt16;
          value @2 :Text;
        }
        # Create has associated (k,v) pair

        read :group {
          key @3 :UInt16;
        }
        # Read has associated key

        update :group {
          key @4 :UInt16;
          value @5 :Text;
        }
        # Update has associated (k,v) pair

        remove :group {
          key @6 :UInt16;
        }
        # Remove has associated key
      }
    }

    clientId @0 :Data;
    # Id of the client that issued command

    commandId @1 :UInt16;
    # Id of the command being issued

    operation @2 :Operation;     
    # Encodes the operation that will be applied to state of application
    # The type of this is temporary for now

    clientUri @3 :Text;
  }
  # Structure represents the command sent in RPC
  
  struct Result {
  # Encodes the result of performing an operation on replicas
  # Stored as a union as only one possible result will apply.
  union {
    success @0 :Void;
    # Success has one possible value, hence void

    failure @1 :Void;
    # Failure has one possible value, hence void

    read @2 :Text;
    # Read has an associated value that is returned with it
    }
  }
  
  clientRequest @0 (command :Command) -> ();
  # Method clientRequest is a message sent from the client to a replica
  # The client issues a command and response is returned in another message

  decision @1 (slot_number :UInt16, command :Command) -> ();
  # Replicas receive decision messages sent by a leader
  # Consists of a command and a slot number
  # Slot number is the place slot in which the command has been decided 
  # to occupy by the synod protocol

  sendProposal @2 (slot_number :UInt16, command :Command) -> ();
  # Method sendProposal is a message sent from a replica to a leader.
  # Proposals consists of a command and a slot for which that command
  # is proposed.
  
  clientResponse @3 (commandId :UInt16, result :Result) -> ();
  # Method clientResponse is a message sent from replica to a client
  # Returns the id of the command and result of issuing it

  phase1 @4 (ballotNumber :Text) -> (result :Text);
  # Method phase1 is a message sent from leader to an acceptor
  # As an acceptor responds to each phase1 message with a reply
  # based deterministically on the request we implement in a simple
  # request / response format as above.
  #

  phase2 @5 (pvalue :Text) -> (result :Text);
  # ...
  # ...
  # ...

  # Wrt arguments of the last two messages we have a more experimental approach:
  #   - Instead of providing each argument as a Capnp type, instead
  #     each argument will be provided as JSON text. This is because
  #     the format of these messages will be optimised later (state
  #     reduction can be performed on the pvalues we are required to
  #     send) so no point in writing lots of serialization code when
  #     it will change in the future anyway

  slotOutUpdate @6 (slotOut :UInt16, replicaId :Text);
  # Method slotOutUpdate aims to implement the notification of the value of 
  # slot_out for leaders and acceptors suggested in PMMC. 
}
