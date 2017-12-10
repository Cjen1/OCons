[@@@ocaml.warning "-27-32-37-60"]

type ro = Capnp.Message.ro
type rw = Capnp.Message.rw

module type S = sig
  module MessageWrapper : Capnp.RPC.S
  type 'cap message_t = 'cap MessageWrapper.Message.t
  type 'a reader_t = 'a MessageWrapper.StructStorage.reader_t
  type 'a builder_t = 'a MessageWrapper.StructStorage.builder_t


  module Reader : sig
    type array_t
    type builder_array_t
    type pointer_t = ro MessageWrapper.Slice.t option
    val of_pointer : pointer_t -> 'a reader_t
    module Message : sig
      type t = [`Message_c5713fa163ae889f]
      module Command : sig
        type struct_t = [`Command_88cec936a5f389be]
        type t = struct_t reader_t
        module Operation : sig
          type struct_t = [`Operation_ecff136cfd079ec1]
          type t = struct_t reader_t
          module Create : sig
            type struct_t = [`Create_f8fb6e072f043631]
            type t = struct_t reader_t
            val key_get : t -> int
            val has_value : t -> bool
            val value_get : t -> string
            val of_message : 'cap message_t -> t
            val of_builder : struct_t builder_t -> t
          end
          module Read : sig
            type struct_t = [`Read_e749754fed837671]
            type t = struct_t reader_t
            val key_get : t -> int
            val of_message : 'cap message_t -> t
            val of_builder : struct_t builder_t -> t
          end
          module Update : sig
            type struct_t = [`Update_db81e80bebc4167d]
            type t = struct_t reader_t
            val key_get : t -> int
            val has_value : t -> bool
            val value_get : t -> string
            val of_message : 'cap message_t -> t
            val of_builder : struct_t builder_t -> t
          end
          module Remove : sig
            type struct_t = [`Remove_d9619866f34d3b0a]
            type t = struct_t reader_t
            val key_get : t -> int
            val of_message : 'cap message_t -> t
            val of_builder : struct_t builder_t -> t
          end
          type unnamed_union_t =
            | Nop
            | Create of Create.t
            | Read of Read.t
            | Update of Update.t
            | Remove of Remove.t
            | Undefined of int
          val get : t -> unnamed_union_t
          val of_message : 'cap message_t -> t
          val of_builder : struct_t builder_t -> t
        end
        val has_client_id : t -> bool
        val client_id_get : t -> string
        val command_id_get : t -> int
        val has_operation : t -> bool
        val operation_get : t -> [`Operation_ecff136cfd079ec1] reader_t
        val operation_get_pipelined : struct_t MessageWrapper.StructRef.t -> [`Operation_ecff136cfd079ec1] MessageWrapper.StructRef.t
        val of_message : 'cap message_t -> t
        val of_builder : struct_t builder_t -> t
      end
      module Result : sig
        type struct_t = [`Result_c244cbcbd9683223]
        type t = struct_t reader_t
        type unnamed_union_t =
          | Success
          | Failure
          | Read of string
          | Undefined of int
        val get : t -> unnamed_union_t
        val of_message : 'cap message_t -> t
        val of_builder : struct_t builder_t -> t
      end
      module ClientRequest : sig
        module Params : sig
          type struct_t = [`ClientRequest_c6e5c0e14753f3aa]
          type t = struct_t reader_t
          val has_command : t -> bool
          val command_get : t -> [`Command_88cec936a5f389be] reader_t
          val command_get_pipelined : struct_t MessageWrapper.StructRef.t -> [`Command_88cec936a5f389be] MessageWrapper.StructRef.t
          val of_message : 'cap message_t -> t
          val of_builder : struct_t builder_t -> t
        end
        module Results : sig
          type struct_t = [`ClientRequest_e984715ac5697f94]
          type t = struct_t reader_t
          val of_message : 'cap message_t -> t
          val of_builder : struct_t builder_t -> t
        end
      end
      module Decision : sig
        module Params : sig
          type struct_t = [`Decision_bf80e909e55ca439]
          type t = struct_t reader_t
          val slot_number_get : t -> int
          val has_command : t -> bool
          val command_get : t -> [`Command_88cec936a5f389be] reader_t
          val command_get_pipelined : struct_t MessageWrapper.StructRef.t -> [`Command_88cec936a5f389be] MessageWrapper.StructRef.t
          val of_message : 'cap message_t -> t
          val of_builder : struct_t builder_t -> t
        end
        module Results : sig
          type struct_t = [`Decision_879cdb3a57043d75]
          type t = struct_t reader_t
          val of_message : 'cap message_t -> t
          val of_builder : struct_t builder_t -> t
        end
      end
      module SendProposal : sig
        module Params : sig
          type struct_t = [`SendProposal_9c84af2814ff35d9]
          type t = struct_t reader_t
          val slot_number_get : t -> int
          val has_command : t -> bool
          val command_get : t -> [`Command_88cec936a5f389be] reader_t
          val command_get_pipelined : struct_t MessageWrapper.StructRef.t -> [`Command_88cec936a5f389be] MessageWrapper.StructRef.t
          val of_message : 'cap message_t -> t
          val of_builder : struct_t builder_t -> t
        end
        module Results : sig
          type struct_t = [`SendProposal_9ade74485d8b7a26]
          type t = struct_t reader_t
          val of_message : 'cap message_t -> t
          val of_builder : struct_t builder_t -> t
        end
      end
      module ClientResponse : sig
        module Params : sig
          type struct_t = [`ClientResponse_e8b642569b486ee6]
          type t = struct_t reader_t
          val command_id_get : t -> int
          val has_result : t -> bool
          val result_get : t -> [`Result_c244cbcbd9683223] reader_t
          val result_get_pipelined : struct_t MessageWrapper.StructRef.t -> [`Result_c244cbcbd9683223] MessageWrapper.StructRef.t
          val of_message : 'cap message_t -> t
          val of_builder : struct_t builder_t -> t
        end
        module Results : sig
          type struct_t = [`ClientResponse_89294cbeddcde953]
          type t = struct_t reader_t
          val of_message : 'cap message_t -> t
          val of_builder : struct_t builder_t -> t
        end
      end
    end
  end

  module Builder : sig
    type array_t = Reader.builder_array_t
    type reader_array_t = Reader.array_t
    type pointer_t = rw MessageWrapper.Slice.t
    module Message : sig
      type t = [`Message_c5713fa163ae889f]
      module Command : sig
        type struct_t = [`Command_88cec936a5f389be]
        type t = struct_t builder_t
        module Operation : sig
          type struct_t = [`Operation_ecff136cfd079ec1]
          type t = struct_t builder_t
          module Create : sig
            type struct_t = [`Create_f8fb6e072f043631]
            type t = struct_t builder_t
            val key_get : t -> int
            val key_set_exn : t -> int -> unit
            val has_value : t -> bool
            val value_get : t -> string
            val value_set : t -> string -> unit
            val of_message : rw message_t -> t
            val to_message : t -> rw message_t
            val to_reader : t -> struct_t reader_t
            val init_root : ?message_size:int -> unit -> t
            val init_pointer : pointer_t -> t
          end
          module Read : sig
            type struct_t = [`Read_e749754fed837671]
            type t = struct_t builder_t
            val key_get : t -> int
            val key_set_exn : t -> int -> unit
            val of_message : rw message_t -> t
            val to_message : t -> rw message_t
            val to_reader : t -> struct_t reader_t
            val init_root : ?message_size:int -> unit -> t
            val init_pointer : pointer_t -> t
          end
          module Update : sig
            type struct_t = [`Update_db81e80bebc4167d]
            type t = struct_t builder_t
            val key_get : t -> int
            val key_set_exn : t -> int -> unit
            val has_value : t -> bool
            val value_get : t -> string
            val value_set : t -> string -> unit
            val of_message : rw message_t -> t
            val to_message : t -> rw message_t
            val to_reader : t -> struct_t reader_t
            val init_root : ?message_size:int -> unit -> t
            val init_pointer : pointer_t -> t
          end
          module Remove : sig
            type struct_t = [`Remove_d9619866f34d3b0a]
            type t = struct_t builder_t
            val key_get : t -> int
            val key_set_exn : t -> int -> unit
            val of_message : rw message_t -> t
            val to_message : t -> rw message_t
            val to_reader : t -> struct_t reader_t
            val init_root : ?message_size:int -> unit -> t
            val init_pointer : pointer_t -> t
          end
          type unnamed_union_t =
            | Nop
            | Create of Create.t
            | Read of Read.t
            | Update of Update.t
            | Remove of Remove.t
            | Undefined of int
          val get : t -> unnamed_union_t
          val nop_set : t -> unit
          val create_init : t -> Create.t
          val read_init : t -> Read.t
          val update_init : t -> Update.t
          val remove_init : t -> Remove.t
          val of_message : rw message_t -> t
          val to_message : t -> rw message_t
          val to_reader : t -> struct_t reader_t
          val init_root : ?message_size:int -> unit -> t
          val init_pointer : pointer_t -> t
        end
        val has_client_id : t -> bool
        val client_id_get : t -> string
        val client_id_set : t -> string -> unit
        val command_id_get : t -> int
        val command_id_set_exn : t -> int -> unit
        val has_operation : t -> bool
        val operation_get : t -> [`Operation_ecff136cfd079ec1] builder_t
        val operation_set_reader : t -> [`Operation_ecff136cfd079ec1] reader_t -> [`Operation_ecff136cfd079ec1] builder_t
        val operation_set_builder : t -> [`Operation_ecff136cfd079ec1] builder_t -> [`Operation_ecff136cfd079ec1] builder_t
        val operation_init : t -> [`Operation_ecff136cfd079ec1] builder_t
        val of_message : rw message_t -> t
        val to_message : t -> rw message_t
        val to_reader : t -> struct_t reader_t
        val init_root : ?message_size:int -> unit -> t
        val init_pointer : pointer_t -> t
      end
      module Result : sig
        type struct_t = [`Result_c244cbcbd9683223]
        type t = struct_t builder_t
        type unnamed_union_t =
          | Success
          | Failure
          | Read of string
          | Undefined of int
        val get : t -> unnamed_union_t
        val success_set : t -> unit
        val failure_set : t -> unit
        val read_set : t -> string -> unit
        val of_message : rw message_t -> t
        val to_message : t -> rw message_t
        val to_reader : t -> struct_t reader_t
        val init_root : ?message_size:int -> unit -> t
        val init_pointer : pointer_t -> t
      end
      module ClientRequest : sig
        module Params : sig
          type struct_t = [`ClientRequest_c6e5c0e14753f3aa]
          type t = struct_t builder_t
          val has_command : t -> bool
          val command_get : t -> [`Command_88cec936a5f389be] builder_t
          val command_set_reader : t -> [`Command_88cec936a5f389be] reader_t -> [`Command_88cec936a5f389be] builder_t
          val command_set_builder : t -> [`Command_88cec936a5f389be] builder_t -> [`Command_88cec936a5f389be] builder_t
          val command_init : t -> [`Command_88cec936a5f389be] builder_t
          val of_message : rw message_t -> t
          val to_message : t -> rw message_t
          val to_reader : t -> struct_t reader_t
          val init_root : ?message_size:int -> unit -> t
          val init_pointer : pointer_t -> t
        end
        module Results : sig
          type struct_t = [`ClientRequest_e984715ac5697f94]
          type t = struct_t builder_t
          val of_message : rw message_t -> t
          val to_message : t -> rw message_t
          val to_reader : t -> struct_t reader_t
          val init_root : ?message_size:int -> unit -> t
          val init_pointer : pointer_t -> t
        end
      end
      module Decision : sig
        module Params : sig
          type struct_t = [`Decision_bf80e909e55ca439]
          type t = struct_t builder_t
          val slot_number_get : t -> int
          val slot_number_set_exn : t -> int -> unit
          val has_command : t -> bool
          val command_get : t -> [`Command_88cec936a5f389be] builder_t
          val command_set_reader : t -> [`Command_88cec936a5f389be] reader_t -> [`Command_88cec936a5f389be] builder_t
          val command_set_builder : t -> [`Command_88cec936a5f389be] builder_t -> [`Command_88cec936a5f389be] builder_t
          val command_init : t -> [`Command_88cec936a5f389be] builder_t
          val of_message : rw message_t -> t
          val to_message : t -> rw message_t
          val to_reader : t -> struct_t reader_t
          val init_root : ?message_size:int -> unit -> t
          val init_pointer : pointer_t -> t
        end
        module Results : sig
          type struct_t = [`Decision_879cdb3a57043d75]
          type t = struct_t builder_t
          val of_message : rw message_t -> t
          val to_message : t -> rw message_t
          val to_reader : t -> struct_t reader_t
          val init_root : ?message_size:int -> unit -> t
          val init_pointer : pointer_t -> t
        end
      end
      module SendProposal : sig
        module Params : sig
          type struct_t = [`SendProposal_9c84af2814ff35d9]
          type t = struct_t builder_t
          val slot_number_get : t -> int
          val slot_number_set_exn : t -> int -> unit
          val has_command : t -> bool
          val command_get : t -> [`Command_88cec936a5f389be] builder_t
          val command_set_reader : t -> [`Command_88cec936a5f389be] reader_t -> [`Command_88cec936a5f389be] builder_t
          val command_set_builder : t -> [`Command_88cec936a5f389be] builder_t -> [`Command_88cec936a5f389be] builder_t
          val command_init : t -> [`Command_88cec936a5f389be] builder_t
          val of_message : rw message_t -> t
          val to_message : t -> rw message_t
          val to_reader : t -> struct_t reader_t
          val init_root : ?message_size:int -> unit -> t
          val init_pointer : pointer_t -> t
        end
        module Results : sig
          type struct_t = [`SendProposal_9ade74485d8b7a26]
          type t = struct_t builder_t
          val of_message : rw message_t -> t
          val to_message : t -> rw message_t
          val to_reader : t -> struct_t reader_t
          val init_root : ?message_size:int -> unit -> t
          val init_pointer : pointer_t -> t
        end
      end
      module ClientResponse : sig
        module Params : sig
          type struct_t = [`ClientResponse_e8b642569b486ee6]
          type t = struct_t builder_t
          val command_id_get : t -> int
          val command_id_set_exn : t -> int -> unit
          val has_result : t -> bool
          val result_get : t -> [`Result_c244cbcbd9683223] builder_t
          val result_set_reader : t -> [`Result_c244cbcbd9683223] reader_t -> [`Result_c244cbcbd9683223] builder_t
          val result_set_builder : t -> [`Result_c244cbcbd9683223] builder_t -> [`Result_c244cbcbd9683223] builder_t
          val result_init : t -> [`Result_c244cbcbd9683223] builder_t
          val of_message : rw message_t -> t
          val to_message : t -> rw message_t
          val to_reader : t -> struct_t reader_t
          val init_root : ?message_size:int -> unit -> t
          val init_pointer : pointer_t -> t
        end
        module Results : sig
          type struct_t = [`ClientResponse_89294cbeddcde953]
          type t = struct_t builder_t
          val of_message : rw message_t -> t
          val to_message : t -> rw message_t
          val to_reader : t -> struct_t reader_t
          val init_root : ?message_size:int -> unit -> t
          val init_pointer : pointer_t -> t
        end
      end
    end
  end
end

module MakeRPC(MessageWrapper : Capnp.RPC.S) : sig
  include S with module MessageWrapper = MessageWrapper

  module Client : sig
    module Message : sig
      type t = [`Message_c5713fa163ae889f]
      val interface_id : Uint64.t
      module ClientRequest : sig
        module Params = Builder.Message.ClientRequest.Params
        module Results = Reader.Message.ClientRequest.Results
        val method_id : (t, Params.t, Results.t) Capnp.RPC.MethodID.t
      end
      module Decision : sig
        module Params = Builder.Message.Decision.Params
        module Results = Reader.Message.Decision.Results
        val method_id : (t, Params.t, Results.t) Capnp.RPC.MethodID.t
      end
      module SendProposal : sig
        module Params = Builder.Message.SendProposal.Params
        module Results = Reader.Message.SendProposal.Results
        val method_id : (t, Params.t, Results.t) Capnp.RPC.MethodID.t
      end
      module ClientResponse : sig
        module Params = Builder.Message.ClientResponse.Params
        module Results = Reader.Message.ClientResponse.Results
        val method_id : (t, Params.t, Results.t) Capnp.RPC.MethodID.t
      end
    end
  end

  module Service : sig
    module Message : sig
      type t = [`Message_c5713fa163ae889f]
      val interface_id : Uint64.t
      module ClientRequest : sig
        module Params = Reader.Message.ClientRequest.Params
        module Results = Builder.Message.ClientRequest.Results
      end
      module Decision : sig
        module Params = Reader.Message.Decision.Params
        module Results = Builder.Message.Decision.Results
      end
      module SendProposal : sig
        module Params = Reader.Message.SendProposal.Params
        module Results = Builder.Message.SendProposal.Results
      end
      module ClientResponse : sig
        module Params = Reader.Message.ClientResponse.Params
        module Results = Builder.Message.ClientResponse.Results
      end
      class virtual service : object
        inherit MessageWrapper.Untyped.generic_service
        method virtual client_request_impl : (ClientRequest.Params.t, ClientRequest.Results.t) MessageWrapper.Service.method_t
        method virtual decision_impl : (Decision.Params.t, Decision.Results.t) MessageWrapper.Service.method_t
        method virtual send_proposal_impl : (SendProposal.Params.t, SendProposal.Results.t) MessageWrapper.Service.method_t
        method virtual client_response_impl : (ClientResponse.Params.t, ClientResponse.Results.t) MessageWrapper.Service.method_t
      end
      val local : #service -> t MessageWrapper.Capability.t
    end
  end
end

module Make(M : Capnp.MessageSig.S) : module type of MakeRPC(Capnp.RPC.None(M))
