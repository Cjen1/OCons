open! Core
open! Async

type t

val logger : Log.t

(** [create] returns a new node after it has loaded its state from file.
    [node_list] is a list of pairs of node_ids and addresses (eg 127.0.0.1:5001)
    [datadir] is the location of the persistant data
    [tick_speed] is the frequency at which the background thread ticks the state machine
    [election_timeout] is the number of ticks before a follower will become a candidate if it hasn't heard from the leader
*)
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

(** [close t] closes any outgoing connections, the incomming server and the write-ahead log *)
val close : t -> unit Deferred.t
