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

module MakeRPC(MessageWrapper : Capnp.RPC.S) = struct
  type 'a reader_t = 'a MessageWrapper.StructStorage.reader_t
  type 'a builder_t = 'a MessageWrapper.StructStorage.builder_t
  module CamlBytes = Bytes
  module DefaultsMessage_ = Capnp.BytesMessage

  let _builder_defaults_message =
    let message_segments = [
      Bytes.unsafe_of_string "\
      ";
    ] in
    DefaultsMessage_.Message.readonly
      (DefaultsMessage_.Message.of_storage message_segments)

  let invalid_msg = Capnp.Message.invalid_msg

  include Capnp.Runtime.BuilderInc.Make[@inlined](MessageWrapper)

  type 'cap message_t = 'cap MessageWrapper.Message.t

  module DefaultsCopier_ =
    Capnp.Runtime.BuilderOps.Make(Capnp.BytesMessage)(MessageWrapper)

  let _reader_defaults_message =
    MessageWrapper.Message.create
      (DefaultsMessage_.Message.total_size _builder_defaults_message)


  module Reader = struct
    type array_t = ro MessageWrapper.ListStorage.t
    type builder_array_t = rw MessageWrapper.ListStorage.t
    type pointer_t = ro MessageWrapper.Slice.t option
    let of_pointer = RA_.deref_opt_struct_pointer

    module Message = struct
      type t = [`Message_c5713fa163ae889f]
      module Command = struct
        type struct_t = [`Command_88cec936a5f389be]
        type t = struct_t reader_t
        let client_id_get x =
          RA_.get_uint16 ~default:0 x 0
        let command_id_get x =
          RA_.get_uint16 ~default:0 x 2
        let has_operation x =
          RA_.has_field x 0
        let operation_get x =
          RA_.get_text ~default:"" x 0
        let of_message x = RA_.get_root_struct (RA_.Message.readonly x)
        let of_builder x = Some (RA_.StructStorage.readonly x)
      end
      module Response = struct
        type struct_t = [`Response_b9cca94fab9dd111]
        type t = struct_t reader_t
        let command_id_get x =
          RA_.get_uint16 ~default:0 x 0
        let has_result x =
          RA_.has_field x 0
        let result_get x =
          RA_.get_text ~default:"" x 0
        let of_message x = RA_.get_root_struct (RA_.Message.readonly x)
        let of_builder x = Some (RA_.StructStorage.readonly x)
      end
      module ClientRequest = struct
        module Params = struct
          type struct_t = [`ClientRequest_c6e5c0e14753f3aa]
          type t = struct_t reader_t
          let has_command x =
            RA_.has_field x 0
          let command_get x =
            RA_.get_struct x 0
          let command_get_pipelined x =
            MessageWrapper.Untyped.struct_field x 0
          let of_message x = RA_.get_root_struct (RA_.Message.readonly x)
          let of_builder x = Some (RA_.StructStorage.readonly x)
        end
        module Results = struct
          type struct_t = [`ClientRequest_e984715ac5697f94]
          type t = struct_t reader_t
          let has_response x =
            RA_.has_field x 0
          let response_get x =
            RA_.get_struct x 0
          let response_get_pipelined x =
            MessageWrapper.Untyped.struct_field x 0
          let of_message x = RA_.get_root_struct (RA_.Message.readonly x)
          let of_builder x = Some (RA_.StructStorage.readonly x)
        end
      end
    end
  end

  module Builder = struct
    type array_t = Reader.builder_array_t
    type reader_array_t = Reader.array_t
    type pointer_t = rw MessageWrapper.Slice.t

    module Message = struct
      type t = [`Message_c5713fa163ae889f]
      module Command = struct
        type struct_t = [`Command_88cec936a5f389be]
        type t = struct_t builder_t
        let client_id_get x =
          BA_.get_uint16 ~default:0 x 0
        let client_id_set_exn x v =
          BA_.set_uint16 ~default:0 x 0 v
        let command_id_get x =
          BA_.get_uint16 ~default:0 x 2
        let command_id_set_exn x v =
          BA_.set_uint16 ~default:0 x 2 v
        let has_operation x =
          BA_.has_field x 0
        let operation_get x =
          BA_.get_text ~default:"" x 0
        let operation_set x v =
          BA_.set_text x 0 v
        let of_message x = BA_.get_root_struct ~data_words:1 ~pointer_words:1 x
        let to_message x = x.BA_.NM.StructStorage.data.MessageWrapper.Slice.msg
        let to_reader x = Some (RA_.StructStorage.readonly x)
        let init_root ?message_size () =
          BA_.alloc_root_struct ?message_size ~data_words:1 ~pointer_words:1 ()
        let init_pointer ptr =
          BA_.init_struct_pointer ptr ~data_words:1 ~pointer_words:1
      end
      module Response = struct
        type struct_t = [`Response_b9cca94fab9dd111]
        type t = struct_t builder_t
        let command_id_get x =
          BA_.get_uint16 ~default:0 x 0
        let command_id_set_exn x v =
          BA_.set_uint16 ~default:0 x 0 v
        let has_result x =
          BA_.has_field x 0
        let result_get x =
          BA_.get_text ~default:"" x 0
        let result_set x v =
          BA_.set_text x 0 v
        let of_message x = BA_.get_root_struct ~data_words:1 ~pointer_words:1 x
        let to_message x = x.BA_.NM.StructStorage.data.MessageWrapper.Slice.msg
        let to_reader x = Some (RA_.StructStorage.readonly x)
        let init_root ?message_size () =
          BA_.alloc_root_struct ?message_size ~data_words:1 ~pointer_words:1 ()
        let init_pointer ptr =
          BA_.init_struct_pointer ptr ~data_words:1 ~pointer_words:1
      end
      module ClientRequest = struct
        module Params = struct
          type struct_t = [`ClientRequest_c6e5c0e14753f3aa]
          type t = struct_t builder_t
          let has_command x =
            BA_.has_field x 0
          let command_get x =
            BA_.get_struct ~data_words:1 ~pointer_words:1 x 0
          let command_set_reader x v =
            BA_.set_struct ~data_words:1 ~pointer_words:1 x 0 v
          let command_set_builder x v =
            BA_.set_struct ~data_words:1 ~pointer_words:1 x 0 (Some v)
          let command_init x =
            BA_.init_struct ~data_words:1 ~pointer_words:1 x 0
          let of_message x = BA_.get_root_struct ~data_words:0 ~pointer_words:1 x
          let to_message x = x.BA_.NM.StructStorage.data.MessageWrapper.Slice.msg
          let to_reader x = Some (RA_.StructStorage.readonly x)
          let init_root ?message_size () =
            BA_.alloc_root_struct ?message_size ~data_words:0 ~pointer_words:1 ()
          let init_pointer ptr =
            BA_.init_struct_pointer ptr ~data_words:0 ~pointer_words:1
        end
        module Results = struct
          type struct_t = [`ClientRequest_e984715ac5697f94]
          type t = struct_t builder_t
          let has_response x =
            BA_.has_field x 0
          let response_get x =
            BA_.get_struct ~data_words:1 ~pointer_words:1 x 0
          let response_set_reader x v =
            BA_.set_struct ~data_words:1 ~pointer_words:1 x 0 v
          let response_set_builder x v =
            BA_.set_struct ~data_words:1 ~pointer_words:1 x 0 (Some v)
          let response_init x =
            BA_.init_struct ~data_words:1 ~pointer_words:1 x 0
          let of_message x = BA_.get_root_struct ~data_words:0 ~pointer_words:1 x
          let to_message x = x.BA_.NM.StructStorage.data.MessageWrapper.Slice.msg
          let to_reader x = Some (RA_.StructStorage.readonly x)
          let init_root ?message_size () =
            BA_.alloc_root_struct ?message_size ~data_words:0 ~pointer_words:1 ()
          let init_pointer ptr =
            BA_.init_struct_pointer ptr ~data_words:0 ~pointer_words:1
        end
      end
    end
  end

  module Client = struct
    module Message = struct
      type t = [`Message_c5713fa163ae889f]
      let interface_id = Uint64.of_string "0xc5713fa163ae889f"
      module ClientRequest = struct
        module Params = Builder.Message.ClientRequest.Params
        module Results = Reader.Message.ClientRequest.Results
        let method_id : (t, Params.t, Results.t) Capnp.RPC.MethodID.t =
          Capnp.RPC.MethodID.v ~interface_id ~method_id:0
      end
      let method_name = function
        | 0 -> Some "clientRequest"
        | _ -> None
      let () = Capnp.RPC.Registry.register ~interface_id ~name:"Message" method_name
    end
  end

  module Service = struct
    module Message = struct
      type t = [`Message_c5713fa163ae889f]
      let interface_id = Uint64.of_string "0xc5713fa163ae889f"
      module ClientRequest = struct
        module Params = Reader.Message.ClientRequest.Params
        module Results = Builder.Message.ClientRequest.Results
      end
      class virtual service = object (self)
        method release = ()
        method dispatch ~interface_id:i ~method_id =
          if i <> interface_id then MessageWrapper.Untyped.unknown_interface ~interface_id
          else match method_id with
          | 0 -> MessageWrapper.Untyped.abstract_method self#client_request_impl
          | x -> MessageWrapper.Untyped.unknown_method ~interface_id ~method_id
        method pp f = Format.pp_print_string f "Message"
        method virtual client_request_impl : (ClientRequest.Params.t, ClientRequest.Results.t) MessageWrapper.Service.method_t
      end
      let local (service:#service) =
        MessageWrapper.Untyped.local service
    end
  end
  module MessageWrapper = MessageWrapper
end [@@inline]

module Make(M:Capnp.MessageSig.S) = MakeRPC[@inlined](Capnp.RPC.None(M)) [@@inline]
