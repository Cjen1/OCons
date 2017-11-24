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
      module Response : sig
        type struct_t = [`Response_b9cca94fab9dd111]
        type t = struct_t reader_t
        module Result : sig
          type struct_t = [`Result_f2420edc87e976c6]
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
        val command_id_get : t -> int
        val has_result : t -> bool
        val result_get : t -> [`Result_f2420edc87e976c6] reader_t
        val result_get_pipelined : struct_t MessageWrapper.StructRef.t -> [`Result_f2420edc87e976c6] MessageWrapper.StructRef.t
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
      module Response : sig
        type struct_t = [`Response_b9cca94fab9dd111]
        type t = struct_t builder_t
        module Result : sig
          type struct_t = [`Result_f2420edc87e976c6]
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
        val command_id_get : t -> int
        val command_id_set_exn : t -> int -> unit
        val has_result : t -> bool
        val result_get : t -> [`Result_f2420edc87e976c6] builder_t
        val result_set_reader : t -> [`Result_f2420edc87e976c6] reader_t -> [`Result_f2420edc87e976c6] builder_t
        val result_set_builder : t -> [`Result_f2420edc87e976c6] builder_t -> [`Result_f2420edc87e976c6] builder_t
        val result_init : t -> [`Result_f2420edc87e976c6] builder_t
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
        module Operation = struct
          type struct_t = [`Operation_ecff136cfd079ec1]
          type t = struct_t reader_t
          module Create = struct
            type struct_t = [`Create_f8fb6e072f043631]
            type t = struct_t reader_t
            let key_get x =
              RA_.get_uint16 ~default:0 x 2
            let has_value x =
              RA_.has_field x 0
            let value_get x =
              RA_.get_text ~default:"" x 0
            let of_message x = RA_.get_root_struct (RA_.Message.readonly x)
            let of_builder x = Some (RA_.StructStorage.readonly x)
          end
          module Read = struct
            type struct_t = [`Read_e749754fed837671]
            type t = struct_t reader_t
            let key_get x =
              RA_.get_uint16 ~default:0 x 2
            let of_message x = RA_.get_root_struct (RA_.Message.readonly x)
            let of_builder x = Some (RA_.StructStorage.readonly x)
          end
          module Update = struct
            type struct_t = [`Update_db81e80bebc4167d]
            type t = struct_t reader_t
            let key_get x =
              RA_.get_uint16 ~default:0 x 2
            let has_value x =
              RA_.has_field x 0
            let value_get x =
              RA_.get_text ~default:"" x 0
            let of_message x = RA_.get_root_struct (RA_.Message.readonly x)
            let of_builder x = Some (RA_.StructStorage.readonly x)
          end
          module Remove = struct
            type struct_t = [`Remove_d9619866f34d3b0a]
            type t = struct_t reader_t
            let key_get x =
              RA_.get_uint16 ~default:0 x 2
            let of_message x = RA_.get_root_struct (RA_.Message.readonly x)
            let of_builder x = Some (RA_.StructStorage.readonly x)
          end
          let nop_get x = ()
          let create_get x = RA_.cast_struct x
          let read_get x = RA_.cast_struct x
          let update_get x = RA_.cast_struct x
          let remove_get x = RA_.cast_struct x
          type unnamed_union_t =
            | Nop
            | Create of Create.t
            | Read of Read.t
            | Update of Update.t
            | Remove of Remove.t
            | Undefined of int
          let get x =
            match RA_.get_uint16 ~default:0 x 0 with
            | 0 -> Nop
            | 1 -> Create (create_get x)
            | 2 -> Read (read_get x)
            | 3 -> Update (update_get x)
            | 4 -> Remove (remove_get x)
            | v -> Undefined v
          let of_message x = RA_.get_root_struct (RA_.Message.readonly x)
          let of_builder x = Some (RA_.StructStorage.readonly x)
        end
        let has_client_id x =
          RA_.has_field x 0
        let client_id_get x =
          RA_.get_blob ~default:"" x 0
        let command_id_get x =
          RA_.get_uint16 ~default:0 x 0
        let has_operation x =
          RA_.has_field x 1
        let operation_get x =
          RA_.get_struct x 1
        let operation_get_pipelined x =
          MessageWrapper.Untyped.struct_field x 1
        let of_message x = RA_.get_root_struct (RA_.Message.readonly x)
        let of_builder x = Some (RA_.StructStorage.readonly x)
      end
      module Response = struct
        type struct_t = [`Response_b9cca94fab9dd111]
        type t = struct_t reader_t
        module Result = struct
          type struct_t = [`Result_f2420edc87e976c6]
          type t = struct_t reader_t
          let success_get x = ()
          let failure_get x = ()
          let has_read x =
            RA_.has_field x 0
          let read_get x =
            RA_.get_text ~default:"" x 0
          type unnamed_union_t =
            | Success
            | Failure
            | Read of string
            | Undefined of int
          let get x =
            match RA_.get_uint16 ~default:0 x 0 with
            | 0 -> Success
            | 1 -> Failure
            | 2 -> Read (read_get x)
            | v -> Undefined v
          let of_message x = RA_.get_root_struct (RA_.Message.readonly x)
          let of_builder x = Some (RA_.StructStorage.readonly x)
        end
        let command_id_get x =
          RA_.get_uint16 ~default:0 x 0
        let has_result x =
          RA_.has_field x 0
        let result_get x =
          RA_.get_struct x 0
        let result_get_pipelined x =
          MessageWrapper.Untyped.struct_field x 0
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
        module Operation = struct
          type struct_t = [`Operation_ecff136cfd079ec1]
          type t = struct_t builder_t
          module Create = struct
            type struct_t = [`Create_f8fb6e072f043631]
            type t = struct_t builder_t
            let key_get x =
              BA_.get_uint16 ~default:0 x 2
            let key_set_exn x v =
              BA_.set_uint16 ~default:0 x 2 v
            let has_value x =
              BA_.has_field x 0
            let value_get x =
              BA_.get_text ~default:"" x 0
            let value_set x v =
              BA_.set_text x 0 v
            let of_message x = BA_.get_root_struct ~data_words:1 ~pointer_words:1 x
            let to_message x = x.BA_.NM.StructStorage.data.MessageWrapper.Slice.msg
            let to_reader x = Some (RA_.StructStorage.readonly x)
            let init_root ?message_size () =
              BA_.alloc_root_struct ?message_size ~data_words:1 ~pointer_words:1 ()
            let init_pointer ptr =
              BA_.init_struct_pointer ptr ~data_words:1 ~pointer_words:1
          end
          module Read = struct
            type struct_t = [`Read_e749754fed837671]
            type t = struct_t builder_t
            let key_get x =
              BA_.get_uint16 ~default:0 x 2
            let key_set_exn x v =
              BA_.set_uint16 ~default:0 x 2 v
            let of_message x = BA_.get_root_struct ~data_words:1 ~pointer_words:1 x
            let to_message x = x.BA_.NM.StructStorage.data.MessageWrapper.Slice.msg
            let to_reader x = Some (RA_.StructStorage.readonly x)
            let init_root ?message_size () =
              BA_.alloc_root_struct ?message_size ~data_words:1 ~pointer_words:1 ()
            let init_pointer ptr =
              BA_.init_struct_pointer ptr ~data_words:1 ~pointer_words:1
          end
          module Update = struct
            type struct_t = [`Update_db81e80bebc4167d]
            type t = struct_t builder_t
            let key_get x =
              BA_.get_uint16 ~default:0 x 2
            let key_set_exn x v =
              BA_.set_uint16 ~default:0 x 2 v
            let has_value x =
              BA_.has_field x 0
            let value_get x =
              BA_.get_text ~default:"" x 0
            let value_set x v =
              BA_.set_text x 0 v
            let of_message x = BA_.get_root_struct ~data_words:1 ~pointer_words:1 x
            let to_message x = x.BA_.NM.StructStorage.data.MessageWrapper.Slice.msg
            let to_reader x = Some (RA_.StructStorage.readonly x)
            let init_root ?message_size () =
              BA_.alloc_root_struct ?message_size ~data_words:1 ~pointer_words:1 ()
            let init_pointer ptr =
              BA_.init_struct_pointer ptr ~data_words:1 ~pointer_words:1
          end
          module Remove = struct
            type struct_t = [`Remove_d9619866f34d3b0a]
            type t = struct_t builder_t
            let key_get x =
              BA_.get_uint16 ~default:0 x 2
            let key_set_exn x v =
              BA_.set_uint16 ~default:0 x 2 v
            let of_message x = BA_.get_root_struct ~data_words:1 ~pointer_words:1 x
            let to_message x = x.BA_.NM.StructStorage.data.MessageWrapper.Slice.msg
            let to_reader x = Some (RA_.StructStorage.readonly x)
            let init_root ?message_size () =
              BA_.alloc_root_struct ?message_size ~data_words:1 ~pointer_words:1 ()
            let init_pointer ptr =
              BA_.init_struct_pointer ptr ~data_words:1 ~pointer_words:1
          end
          let nop_get x = ()
          let nop_set x =
            BA_.set_void ~discr:{BA_.Discr.value=0; BA_.Discr.byte_ofs=0} x
          let create_get x = BA_.cast_struct x
          let create_init x =
            let data = x.BA_.NM.StructStorage.data in
            let pointers = x.BA_.NM.StructStorage.pointers in
            let () = ignore data in
            let () = ignore pointers in
            let () = BA_.set_opt_discriminant data
              (Some {BA_.Discr.value=1; BA_.Discr.byte_ofs=0})
            in
            let () = BA_.set_int16 ~default:0 x 2 0 in
            let () =
              let ptr = {
                pointers with
                MessageWrapper.Slice.start = pointers.MessageWrapper.Slice.start + 0;
                MessageWrapper.Slice.len = 8;
              } in
              let () = BA_.BOps.deep_zero_pointer ptr in
              MessageWrapper.Slice.set_int64 ptr 0 0L
            in
            BA_.cast_struct x
          let read_get x = BA_.cast_struct x
          let read_init x =
            let data = x.BA_.NM.StructStorage.data in
            let pointers = x.BA_.NM.StructStorage.pointers in
            let () = ignore data in
            let () = ignore pointers in
            let () = BA_.set_opt_discriminant data
              (Some {BA_.Discr.value=2; BA_.Discr.byte_ofs=0})
            in
            let () = BA_.set_int16 ~default:0 x 2 0 in
            BA_.cast_struct x
          let update_get x = BA_.cast_struct x
          let update_init x =
            let data = x.BA_.NM.StructStorage.data in
            let pointers = x.BA_.NM.StructStorage.pointers in
            let () = ignore data in
            let () = ignore pointers in
            let () = BA_.set_opt_discriminant data
              (Some {BA_.Discr.value=3; BA_.Discr.byte_ofs=0})
            in
            let () = BA_.set_int16 ~default:0 x 2 0 in
            let () =
              let ptr = {
                pointers with
                MessageWrapper.Slice.start = pointers.MessageWrapper.Slice.start + 0;
                MessageWrapper.Slice.len = 8;
              } in
              let () = BA_.BOps.deep_zero_pointer ptr in
              MessageWrapper.Slice.set_int64 ptr 0 0L
            in
            BA_.cast_struct x
          let remove_get x = BA_.cast_struct x
          let remove_init x =
            let data = x.BA_.NM.StructStorage.data in
            let pointers = x.BA_.NM.StructStorage.pointers in
            let () = ignore data in
            let () = ignore pointers in
            let () = BA_.set_opt_discriminant data
              (Some {BA_.Discr.value=4; BA_.Discr.byte_ofs=0})
            in
            let () = BA_.set_int16 ~default:0 x 2 0 in
            BA_.cast_struct x
          type unnamed_union_t =
            | Nop
            | Create of Create.t
            | Read of Read.t
            | Update of Update.t
            | Remove of Remove.t
            | Undefined of int
          let get x =
            match BA_.get_uint16 ~default:0 x 0 with
            | 0 -> Nop
            | 1 -> Create (create_get x)
            | 2 -> Read (read_get x)
            | 3 -> Update (update_get x)
            | 4 -> Remove (remove_get x)
            | v -> Undefined v
          let of_message x = BA_.get_root_struct ~data_words:1 ~pointer_words:1 x
          let to_message x = x.BA_.NM.StructStorage.data.MessageWrapper.Slice.msg
          let to_reader x = Some (RA_.StructStorage.readonly x)
          let init_root ?message_size () =
            BA_.alloc_root_struct ?message_size ~data_words:1 ~pointer_words:1 ()
          let init_pointer ptr =
            BA_.init_struct_pointer ptr ~data_words:1 ~pointer_words:1
        end
        let has_client_id x =
          BA_.has_field x 0
        let client_id_get x =
          BA_.get_blob ~default:"" x 0
        let client_id_set x v =
          BA_.set_blob x 0 v
        let command_id_get x =
          BA_.get_uint16 ~default:0 x 0
        let command_id_set_exn x v =
          BA_.set_uint16 ~default:0 x 0 v
        let has_operation x =
          BA_.has_field x 1
        let operation_get x =
          BA_.get_struct ~data_words:1 ~pointer_words:1 x 1
        let operation_set_reader x v =
          BA_.set_struct ~data_words:1 ~pointer_words:1 x 1 v
        let operation_set_builder x v =
          BA_.set_struct ~data_words:1 ~pointer_words:1 x 1 (Some v)
        let operation_init x =
          BA_.init_struct ~data_words:1 ~pointer_words:1 x 1
        let of_message x = BA_.get_root_struct ~data_words:1 ~pointer_words:2 x
        let to_message x = x.BA_.NM.StructStorage.data.MessageWrapper.Slice.msg
        let to_reader x = Some (RA_.StructStorage.readonly x)
        let init_root ?message_size () =
          BA_.alloc_root_struct ?message_size ~data_words:1 ~pointer_words:2 ()
        let init_pointer ptr =
          BA_.init_struct_pointer ptr ~data_words:1 ~pointer_words:2
      end
      module Response = struct
        type struct_t = [`Response_b9cca94fab9dd111]
        type t = struct_t builder_t
        module Result = struct
          type struct_t = [`Result_f2420edc87e976c6]
          type t = struct_t builder_t
          let success_get x = ()
          let success_set x =
            BA_.set_void ~discr:{BA_.Discr.value=0; BA_.Discr.byte_ofs=0} x
          let failure_get x = ()
          let failure_set x =
            BA_.set_void ~discr:{BA_.Discr.value=1; BA_.Discr.byte_ofs=0} x
          let has_read x =
            BA_.has_field x 0
          let read_get x =
            BA_.get_text ~default:"" x 0
          let read_set x v =
            BA_.set_text ~discr:{BA_.Discr.value=2; BA_.Discr.byte_ofs=0} x 0 v
          type unnamed_union_t =
            | Success
            | Failure
            | Read of string
            | Undefined of int
          let get x =
            match BA_.get_uint16 ~default:0 x 0 with
            | 0 -> Success
            | 1 -> Failure
            | 2 -> Read (read_get x)
            | v -> Undefined v
          let of_message x = BA_.get_root_struct ~data_words:1 ~pointer_words:1 x
          let to_message x = x.BA_.NM.StructStorage.data.MessageWrapper.Slice.msg
          let to_reader x = Some (RA_.StructStorage.readonly x)
          let init_root ?message_size () =
            BA_.alloc_root_struct ?message_size ~data_words:1 ~pointer_words:1 ()
          let init_pointer ptr =
            BA_.init_struct_pointer ptr ~data_words:1 ~pointer_words:1
        end
        let command_id_get x =
          BA_.get_uint16 ~default:0 x 0
        let command_id_set_exn x v =
          BA_.set_uint16 ~default:0 x 0 v
        let has_result x =
          BA_.has_field x 0
        let result_get x =
          BA_.get_struct ~data_words:1 ~pointer_words:1 x 0
        let result_set_reader x v =
          BA_.set_struct ~data_words:1 ~pointer_words:1 x 0 v
        let result_set_builder x v =
          BA_.set_struct ~data_words:1 ~pointer_words:1 x 0 (Some v)
        let result_init x =
          BA_.init_struct ~data_words:1 ~pointer_words:1 x 0
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
            BA_.get_struct ~data_words:1 ~pointer_words:2 x 0
          let command_set_reader x v =
            BA_.set_struct ~data_words:1 ~pointer_words:2 x 0 v
          let command_set_builder x v =
            BA_.set_struct ~data_words:1 ~pointer_words:2 x 0 (Some v)
          let command_init x =
            BA_.init_struct ~data_words:1 ~pointer_words:2 x 0
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
