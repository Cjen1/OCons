module O = Ocons_core
open O.Types

module type Cons = sig
  include Ocons_core.Consensus_intf.S

  val get_leader : t -> node_id option
end

type 'message message = Forward of command array | Internal of 'message

let message_pp ?message_pp : 'message Fmt.t =
 fun ppf v ->
  match (v, message_pp) with
  | Forward c, _ ->
      Fmt.pf ppf "Forwarding(%a)" (assert false) c
  | Internal i, Some message_pp ->
      Fmt.pf ppf "Internal(%a)" message_pp i
  | Internal _, None ->
      Fmt.pf ppf "Internal(_)"

module MakeLineProt (C : O.Consensus_intf.S) = struct
  module R = Eio.Buf_read
  module W = Eio.Buf_write

  let parse =
    let open O.Line_prot.DeserPrim in
    let open R.Syntax in
    let* internal_mid = R.uint8 in
    match internal_mid with
    | 0 ->
        R.map (fun v -> Forward v) (array command)
    | 1 ->
        R.map (fun v -> Internal v) C.parse
    | i ->
        Fmt.invalid_arg "Expected 0 or 1 for internal/forward id got %d" i

  let serialise v w =
    let open O.Line_prot.SerPrim in
    match v with
    | Forward cs ->
        W.uint8 w 0 ; array command cs w
    | Internal m ->
        W.uint8 w 1 ; C.serialise m w
end

module Make (C : Cons) : O.Consensus_intf.S with type config = C.config = struct
  include MakeLineProt (C)

  type nonrec message = C.message message

  let message_pp : message Fmt.t = message_pp ~message_pp:C.message_pp

  type config = C.config

  let config_pp = C.config_pp

  type t = {inner: C.t; myid: node_id}

  let t_pp ppf v = C.t_pp ppf v.inner

  let create_node id config = {myid= id; inner= C.create_node id config}

  let available_space_for_commands _ = 1000

  let should_ack_clients _ = true

  open O.Consensus_intf

  let wrap_inner_actions t (t', is) =
    ( {t with inner= t'}
    , is
      |> List.map (function
           | Send (d, m) ->
               Send (d, Internal m)
           | Broadcast m ->
               Broadcast (Internal m)
           | CommitCommands c ->
               CommitCommands c ) )

  let advance t e =
    match e with
    | Recv (Internal m, src) ->
        C.advance t.inner (Recv (m, src)) |> wrap_inner_actions t
    | Tick ->
        C.advance t.inner Tick |> wrap_inner_actions t
    | Commands s when C.get_leader t.inner = Some t.myid ->
        C.advance t.inner (Commands s) |> wrap_inner_actions t
    | Commands cs ->
        let arr = Iter.to_array cs in
        ( t
        , C.get_leader t.inner
          |> Option.map (fun lid -> Send (lid, Forward arr))
          |> Option.to_list )
    | Recv (Forward cs, _) ->
        (* Don't deliver if full *)
        if Array.length cs <= C.available_space_for_commands t.inner then
          C.advance t.inner (Commands (Iter.of_array cs))
          |> wrap_inner_actions t
        else (t, [])
end
