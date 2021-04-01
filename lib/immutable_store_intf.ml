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

  val entries_after_inc : t -> log_index -> log_entry list

  val entries_after_inc_size : t -> log_index -> log_entry list * int64

  val to_string : t -> string

  val add_entries_remove_conflicts :
    t -> start_index:log_index -> entries:log_entry list -> t

  val add_cmd : t -> cmd:command -> term:term -> t

  val add_cmds : t -> cmds:command list -> term:term -> t
end
