open Types

type t

val sync : t -> (unit, exn) Lwt_result.t

val of_file : node_addr -> t Lwt.t

val close : t -> unit Lwt.t

val add : log_entry -> t -> t

val removeGEQ : log_index -> t -> t

val append : t -> command_id -> term -> t

val get : t -> log_index -> (log_entry, exn) Result.t

val get_exn : t -> log_index -> log_entry

val get_term : t -> log_index -> (term, exn) Result.t

val get_term_exn : t -> log_index -> term

val get_max_index : t -> log_index

val entries_after_inc : t -> log_index -> log_entry list

val to_string : t -> string

val add_entries_remove_conflicts : t -> start_index:log_index -> log_entry list -> t

