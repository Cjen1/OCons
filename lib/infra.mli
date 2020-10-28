open! Core
open! Async

type t

val create :
     node_id:int
  -> node_list:(int * string) list
  -> datadir:string
  -> listen_port:int
  -> election_timeout:int
  -> tick_speed:Time.Span.t
  -> batch_size:int
  -> dispatch_timeout:Core.Time.Span.t
  -> t Deferred.t

val close : t -> unit Deferred.t
