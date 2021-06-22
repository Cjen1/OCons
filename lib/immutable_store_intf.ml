open! Core
open! Types

module type S = sig
  type op [@@deriving sexp_of, bin_io]

  type data [@@deriving sexp_of, compare]

  type t [@@deriving sexp_of, compare]

  val init : unit -> t

  val apply : t -> op -> t

  val get_current_term : t -> term

  val get_data : t -> data

  (* newest to oldest *)
  val get_ops : t -> op list

  val reset_ops : t -> t

  val update_term : t -> term:term -> t

  val add_entry : t -> entry:log_entry -> t

  val remove_geq : t -> index:log_index -> t

  val get_index : t -> log_index -> (log_entry, exn) result

  val get_index_exn : t -> log_index -> log_entry

  val get_term : t -> log_index -> (term, exn) result

  val get_term_exn : t -> log_index -> term

  val get_max_index : t -> log_index

  val mem_id : t -> command_id -> bool

  val fold_geq :
    t -> idx:log_index -> init:'a -> f:('a -> log_entry -> 'a) -> 'a

  val foldi_geq :
       t
    -> idx:log_index
    -> init:'a
    -> f:(log_index -> 'a -> log_entry -> 'a)
    -> 'a

  val fold_until_geq :
       idx:log_index
    -> init:'acc
    -> f:('acc -> log_entry -> ('acc, 'final) Continue_or_stop.t)
    -> finish:('acc -> 'final)
    -> t
    -> 'final

  val foldi_until_geq :
       idx:log_index
    -> init:'acc
    -> f:(log_index -> 'acc -> log_entry -> ('acc, 'final) Continue_or_stop.t)
    -> finish:(log_index -> 'acc -> 'final)
    -> t
    -> 'final

  val to_string : t -> string

  val add_cmd : t -> cmd:command -> term:term -> t

  val add_cmds : t -> cmds:command list -> term:term -> t
end
