open! Ocons_core
open! Ocons_core.Types
open! Impl_core.Types
module Cli = Ocons_core.Client
module PMain = Infra.Make (Impl_core.Paxos)
module RMain = Infra.Make (Impl_core.Raft)
module FRMain = Infra.Make (Impl_core.ForwardRaft)

type kind = Paxos | Raft | FRaft

let run kind node_id node_addresses internal_port external_port tick_period
    election_timeout max_outstanding stream_length stat_report =
  let other_nodes =
    node_addresses |> List.filter (fun (id, _) -> not @@ Int.equal id node_id)
  in
  let shared_config =
    let num_nodes = List.length node_addresses in
    let majority_quorums = (num_nodes / 2) + 1 in
    { phase1quorum= majority_quorums
    ; phase2quorum= majority_quorums
    ; other_nodes= List.map fst other_nodes
    ; num_nodes
    ; node_id
    ; election_timeout
    ; max_outstanding }
  in
  let config cons_config =
    Infra.
      { cons_config
      ; internal_port
      ; external_port
      ; stream_length
      ; tick_period
      ; nodes= other_nodes
      ; node_id
      ; stat_report }
  in
  match kind with
  | Paxos ->
      let cfg = config shared_config in
      Eio.traceln "Starting Paxos system:\nconfig = %a"
        Impl_core.Types.config_pp shared_config ;
      Eio_main.run @@ fun env -> PMain.run env cfg
  | Raft ->
      let cfg = config shared_config in
      Eio.traceln "Starting Raft system:\nconfig = %a" Impl_core.Types.config_pp
        shared_config ;
      Eio_main.run @@ fun env -> RMain.run env cfg
  | FRaft ->
      let cfg = config shared_config in
      Eio.traceln "Starting Raft system:\nconfig = %a" Impl_core.Types.config_pp
        shared_config ;
      Eio_main.run @@ fun env -> FRMain.run env cfg

open Cmdliner

let ipv4 =
  let conv = Arg.(t4 ~sep:'.' int int int int) in
  let parse s =
    let ( let+ ) = Result.bind in
    let+ res = Arg.conv_parser conv s in
    let check v = v >= 0 && v < 256 in
    match res with
    | v0, v1, v2, v3 when check v0 && check v1 && check v2 && check v3 ->
        let raw = Bytes.create 4 in
        Bytes.set_uint8 raw 0 v0 ;
        Bytes.set_uint8 raw 1 v1 ;
        Bytes.set_uint8 raw 2 v2 ;
        Bytes.set_uint8 raw 3 v3 ;
        Ok (Eio.Net.Ipaddr.of_raw (Bytes.to_string raw))
    | v0, v1, v2, v3 ->
        Error
          (`Msg
            Fmt.(
              str "Invalid IP address: %a"
                (list ~sep:(const string ".") int)
                [v0; v1; v2; v3] ) )
  in
  Arg.conv ~docv:"IPv4" (parse, Eio.Net.Ipaddr.pp)

let sockv4 =
  let conv = Arg.(pair ~sep:':' ipv4 int) in
  let parse s =
    let ( let+ ) = Result.bind in
    let+ ip, port = Arg.conv_parser conv s in
    Ok (`Tcp (ip, port) : Eio.Net.Sockaddr.stream)
  in
  Arg.conv ~docv:"TCP" (parse, Eio.Net.Sockaddr.pp)

let address_a = Arg.(pair ~sep:':' int sockv4)

let election_timeout_ot =
  let open Arg in
  let i =
    info ~docv:"ELECTION_TICK_INTERVAL"
      ~doc:"Number of ticks before an election is triggered."
      ["election-timeout"]
  in
  opt int 5 i

let election_tick_period_ot =
  let open Arg in
  let i =
    info ~docv:"TICK_PERIOD"
      ~doc:"Number of seconds before an election tick is generated."
      ["t"; "tick-period"; "election-tick-period"]
  in
  opt float 0.1 i

let max_outstanding_ot =
  let open Arg in
  let i =
    info ~docv:"MAX_OUTSTANDING"
      ~doc:
        "Number of outstanding requests between the leader's highest log index \
         and the highest committed value."
      ["o"; "outstanding"; "max-outstanding"]
  in
  opt int 65536 i

let stream_length_ot =
  let open Arg in
  let i =
    info ~docv:"STREAM_LENGTH"
      ~doc:
        "Maximum number of requests in the stream between the external and \
         internal infrastructure."
      ["s"; "stream-length"]
  in
  opt int 4096 i

let internal_port_ot =
  let open Arg in
  let i =
    info ~docv:"INTERNAL_PORT" ~doc:"Port for internal traffic between nodes."
      ["p"; "internal"; "internal-port"]
  in
  opt int 5000 i

let external_port_ot =
  let open Arg in
  let i =
    info ~docv:"EXTERNAL_PORT"
      ~doc:"Port for external traffic between nodes and clients."
      ["q"; "external"; "external-port"]
  in
  opt int 5001 i

let address_info =
  Arg.(
    info ~docv:"ADDR"
      ~doc:
        "This is a comma separated list of ip addresses and ports eg: \
         \"0:192.168.0.1:5000,1:192.168.0.2:5000\""
      [] )

let stat_report_ot =
  let open Arg in
  let i =
    info ~docv:"STAT_REPORT_INTERVAL"
      ~doc:"How long between stat reports, -ve numbers will never report"
      ["stat"]
  in
  opt float (-1.) i

let cmd =
  let kind_t =
    let kind = Arg.enum [("paxos", Paxos); ("raft", Raft)] in
    Arg.(
      required
      & pos 0 (some kind) None (info ~docv:"KIND" ~doc:"Protocol to use" []) )
  in
  let node_id_t =
    Arg.(required & pos 1 (some int) None (info ~docv:"ID" ~doc:"NODE_ID" []))
  in
  let node_addresses_t =
    Arg.(required & pos 2 (some @@ list address_a) None address_info)
  in
  let info = Cmd.info "ocons_main" in
  Cmd.v info
    Term.(
      const run $ kind_t $ node_id_t $ node_addresses_t
      $ Arg.value internal_port_ot $ Arg.value external_port_ot
      $ Arg.value election_tick_period_ot
      $ Arg.value election_timeout_ot
      $ Arg.value max_outstanding_ot
      $ Arg.value stream_length_ot $ Arg.value stat_report_ot )

let () = exit Cmd.(eval cmd)
