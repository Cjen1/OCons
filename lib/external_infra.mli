open! Types

type request = Line_prot.External_infra.request

type response = Line_prot.External_infra.response

val run :
  #Eio.Net.t -> int -> request Eio.Stream.t -> response Eio.Stream.t -> unit
(** [run net port request_stream response_stream] runs a client listening service on [port], forwarding any incoming requests to [request_stream] and forwarding any [response]s to the relevant client
 *)
