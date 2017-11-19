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
        val client_id_get : t -> int
        val command_id_get : t -> int
        val has_operation : t -> bool
        val operation_get : t -> string
        val of_message : 'cap message_t -> t
        val of_builder : struct_t builder_t -> t
      end
      module Response : sig
        type struct_t = [`Response_b9cca94fab9dd111]
        type t = struct_t reader_t
        val command_id_get : t -> int
        val has_result : t -> bool
        val result_get : t -> string
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
          val has_response : t -> bool
          val response_get : t -> [`Response_b9cca94fab9dd111] reader_t
          val response_get_pipelined : struct_t MessageWrapper.StructRef.t -> [`Response_b9cca94fab9dd111] MessageWrapper.StructRef.t
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
        val client_id_get : t -> int
        val client_id_set_exn : t -> int -> unit
        val command_id_get : t -> int
        val command_id_set_exn : t -> int -> unit
        val has_operation : t -> bool
        val operation_get : t -> string
        val operation_set : t -> string -> unit
        val of_message : rw message_t -> t
        val to_message : t -> rw message_t
        val to_reader : t -> struct_t reader_t
        val init_root : ?message_size:int -> unit -> t
        val init_pointer : pointer_t -> t
      end
      module Response : sig
        type struct_t = [`Response_b9cca94fab9dd111]
        type t = struct_t builder_t
        val command_id_get : t -> int
        val command_id_set_exn : t -> int -> unit
        val has_result : t -> bool
        val result_get : t -> string
        val result_set : t -> string -> unit
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
          val has_response : t -> bool
          val response_get : t -> [`Response_b9cca94fab9dd111] builder_t
          val response_set_reader : t -> [`Response_b9cca94fab9dd111] reader_t -> [`Response_b9cca94fab9dd111] builder_t
          val response_set_builder : t -> [`Response_b9cca94fab9dd111] builder_t -> [`Response_b9cca94fab9dd111] builder_t
          val response_init : t -> [`Response_b9cca94fab9dd111] builder_t
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
      class virtual service : object
        inherit MessageWrapper.Untyped.generic_service
        method virtual client_request_impl : (ClientRequest.Params.t, ClientRequest.Results.t) MessageWrapper.Service.method_t
      end
      val local : #service -> t MessageWrapper.Capability.t
    end
  end
end

module Make(M : Capnp.MessageSig.S) : module type of MakeRPC(Capnp.RPC.None(M))
