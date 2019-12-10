open Base

module StateMachine : sig
  type t

  type key = string [@@deriving protobuf, sexp]

  type value = string [@@deriving protobuf, sexp]

  type op = Read of key [@key 1] | Write of key * value [@key 2]
  [@@deriving protobuf, sexp]

  type command = {op: op; id: string} [@@deriving protobuf, sexp]

  type op_result =
    | Success [@key 1]
    | Failure [@key 2]
    | ReadSuccess of key [@key 3]
  [@@deriving protobuf, sexp]

  val op_result_failure : unit -> op_result

  val update : t -> command -> op_result

  val create : unit -> t
end = struct
  type key = string [@@deriving protobuf, sexp]

  type value = string [@@deriving protobuf, sexp]

  type t = (key, value) Hashtbl.t

  type op = Read of key [@key 1] | Write of key * value [@key 2]
  [@@deriving protobuf, sexp]

  type command = {op: op [@key 1]; id: string [@key 2]}
  [@@deriving protobuf, sexp]

  type op_result =
    | Success [@key 1]
    | Failure [@key 2]
    | ReadSuccess of key [@key 3]
  [@@deriving protobuf, sexp]

  let op_result_failure () = Failure

  let update : t -> command -> op_result =
   fun t -> function
    | {op= Read key; _} -> (
      match Hashtbl.find t key with Some v -> ReadSuccess v | None -> Failure )
    | {op= Write (key, value); _} ->
        Hashtbl.set t ~key ~data:value ;
        Success

  let create () = Hashtbl.create (module String)
end
