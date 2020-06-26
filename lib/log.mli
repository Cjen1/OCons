type t

val sync : t -> (unit, exn) Lwt_result.t

type entry_list = log_entry list

val of_file : node_addr -> t Lwt.t

val get : t -> log_index -> (log_entry, exn) Result.t

val get_exn : t -> log_index -> log_entry

val get_term : t -> log_index -> (term, exn) Result.t

val get_term_exn : t -> log_index -> term

val set : t -> index:log_index -> value:log_entry -> t

val remove : t -> log_index -> t

val get_max_index : t -> log_index

val entries_after_inc : t -> index:log_index -> entry_list

val to_string : t -> string

val add_entries_remove_conflicts : t -> entry_list -> t

val append : t -> command -> term -> t
