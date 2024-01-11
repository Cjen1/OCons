open! Ocons_core
open! Ocons_core.Types
open! Impl_core.Types
module Cli = Ocons_core.Client
module PMain = Infra.Make (Impl_core.Paxos)
module RMain = Infra.Make (Impl_core.Raft)
module RMain_sbn = Infra.Make (Impl_core.RaftSBN)
module PRMain = Infra.Make (Impl_core.PrevoteRaft)
module PRMain_sbn = Infra.Make (Impl_core.PrevoteRaftSBN)
module ConspireSSMain = Infra.Make (Impl_core.ConspireSS)
module ConspireLeaderMain = Infra.Make (Impl_core.ConspireLeader)
module ConspireDCMain = Infra.Make (Impl_core.ConspireDC)
module ConspireLeaderDCMain = Infra.Make (Impl_core.ConspireLeaderDC)

type kind =
  | Paxos
  | Raft
  | PRaft
  | Raft_sbn
  | PRaft_sbn
  | ConspireSS
  | ConspireLeader
  | ConspireDC
  | ConspireLeaderDC

let run kind node_id node_addresses internal_port external_port tick_period
    election_timeout max_outstanding stream_length stat_report
    rand_startup_delay delay_interval batching_interval tick_limit =
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
    ; max_outstanding
    ; max_append_entries= 512 }
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
  Eio_main.run
  @@ fun env ->
  Random.self_init () ;
  if rand_startup_delay > 0. then
    Eio.Time.sleep env#clock (Random.float rand_startup_delay) ;
  match kind with
  | Paxos ->
      let cfg = config shared_config in
      Eio.traceln "Starting Paxos v1 system:\nconfig = %a"
        Impl_core.Types.config_pp shared_config ;
      PMain.run env cfg
  | Raft ->
      let cfg = config shared_config in
      Eio.traceln "Starting Raft system:\nconfig = %a" Impl_core.Types.config_pp
        shared_config ;
      RMain.run env cfg
  | Raft_sbn ->
      let cfg = config shared_config in
      Eio.traceln "Starting Raft with static-ballot-numbers" ;
      Eio.traceln "config = %a" Impl_core.Types.config_pp shared_config ;
      RMain_sbn.run env cfg
  | PRaft ->
      let cfg = config shared_config in
      Eio.traceln "Starting Prevote-Raft system:\nconfig = %a"
        Impl_core.Types.config_pp shared_config ;
      PRMain.run env cfg
  | PRaft_sbn ->
      let cfg = config shared_config in
      Eio.traceln "Staring Prevote-Raft with static-ballot-numbers" ;
      Eio.traceln "config = %a" Impl_core.Types.config_pp shared_config ;
      PRMain_sbn.run env cfg
  | ConspireSS ->
      let replica_ids =
        List.map (fun (i, _) -> i) node_addresses
        |> Core.List.sort ~compare:Int.compare
      in
      let replica_ids_norm =
        Iter.of_list replica_ids
        |> Iter.mapi (fun i v -> (v, i))
        |> Iter.to_list
      in
      Eio.traceln "%a" Fmt.(list ~sep:comma @@ pair int int) replica_ids_norm ;
      let replica_ids = List.map snd replica_ids_norm in
      let node_id =
        Core.List.Assoc.find_exn replica_ids_norm ~equal:Int.equal node_id
      in
      let conspire_cfg =
        Impl_core.ConspireSS.make_config ~node_id ~replica_ids
          ~fd_timeout:election_timeout ~max_outstanding ()
      in
      let cfg = config conspire_cfg in
      Eio.traceln "Starting Conspire with single-shot instances per log entry" ;
      Eio.traceln "config = %a" Impl_core.ConspireSS.PP.config_pp conspire_cfg ;
      ConspireSSMain.run env cfg
  | ConspireLeader ->
      let replica_ids =
        List.map (fun (i, _) -> i) node_addresses
        |> Core.List.sort ~compare:Int.compare
      in
      let conspire_cfg =
        Impl_core.ConspireLeader.make_config ~node_id ~replica_ids
          ~fd_timeout:election_timeout ~max_outstanding ()
      in
      let cfg = config conspire_cfg in
      Eio.traceln "Starting Conspire-Leader" ;
      Eio.traceln "config = %a" Impl_core.ConspireLeader.PP.config_pp
        conspire_cfg ;
      ConspireLeaderMain.run env cfg
  | ConspireDC ->
      let replica_ids =
        List.map (fun (i, _) -> i) node_addresses
        |> Core.List.sort ~compare:Int.compare
      in
      let conspire_cfg =
        Impl_core.ConspireDC.make_config ~node_id ~replica_ids ~max_outstanding
          (Eio.Stdenv.clock env)
          ~delay_interval:(Time_float_unix.Span.of_sec delay_interval)
          ~batching_interval:(Time_float_unix.Span.of_sec batching_interval)
          ~tick_limit
      in
      let cfg = config conspire_cfg in
      Eio.traceln "Starting Conspire-DC" ;
      Eio.traceln "config = %a" Impl_core.ConspireDC.PP.config_pp conspire_cfg ;
      ConspireDCMain.run env cfg
  | ConspireLeaderDC ->
      let replica_ids =
        List.map (fun (i, _) -> i) node_addresses
        |> Core.List.sort ~compare:Int.compare
      in
      let broadcast_tick_interval =
        float_of_int election_timeout /. 10. |> Float.ceil |> Float.to_int
      in
      let conspire_cfg =
        Impl_core.ConspireLeaderDC.make_config ~node_id ~replica_ids
          ~max_outstanding (Eio.Stdenv.clock env)
          ~delay_interval:(Time_float_unix.Span.of_sec delay_interval)
          ~batching_interval:(Time_float_unix.Span.of_sec batching_interval)
          ~fd_timeout:election_timeout ~broadcast_tick_interval
      in
      let cfg = config conspire_cfg in
      Eio.traceln "Starting Conspire-leader-dc" ;
      Eio.traceln "config = %a" Impl_core.ConspireLeaderDC.PP.config_pp
        conspire_cfg ;
      ConspireLeaderDCMain.run env cfg

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
    info ~docv:"ELECTION_TICK_TIMEOUT"
      ~doc:"Number of ticks before an election is triggered."
      ["election-timeout"]
  in
  opt int 10 i

let tick_period_ot =
  let open Arg in
  let i =
    info ~docv:"TICK_PERIOD"
      ~doc:"Number of seconds before a tick is generated." ["t"; "tick-period"]
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

let rand_startup_delay_ot =
  let open Arg in
  let i =
    info ~docv:"MAX_STARTUP_DELAY"
      ~doc:
        "Randomise time to wait before starting to ensure no coordination of \
         timers."
      ["rand-start"]
  in
  opt float (-1.) i

let delay_interval_ot =
  let open Arg in
  let i =
    info ~docv:"DELAY_INTERVAL"
      ~doc:
        "Delay before commands are added to log in order to allow for \
         propogation delay"
      ["delay"]
  in
  opt float (10. *. 0.001) i

let batching_interval_ot =
  let open Arg in
  let i =
    info ~docv:"BATCHING_INTERVAL"
      ~doc:"Size (by time) of each batch in seconds" ["batch-interval"]
  in
  opt float 0.001 i

let tick_limit_ot =
  let open Arg in
  let i =
    info ~docv:"TICK_LIMIT"
      ~doc:
        "Number of ticks before a message must be sent (useful for message \
         loss)."
      ["tick-limit"]
  in
  opt int 100 i

let cmd =
  let kind_t =
    let kind =
      Arg.enum
        [ ("paxos", Paxos)
        ; ("raft", Raft)
        ; ("raft+sbn", Raft_sbn)
        ; ("prevote-raft", PRaft)
        ; ("prevote-raft+sbn", PRaft_sbn)
        ; ("conspire-ss", ConspireSS)
        ; ("conspire-leader", ConspireLeader)
        ; ("conspire-leader-dc", ConspireLeaderDC)
        ; ("conspire-dc", ConspireDC) ]
    in
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
      $ Arg.value tick_period_ot
      $ Arg.value election_timeout_ot
      $ Arg.value max_outstanding_ot
      $ Arg.value stream_length_ot $ Arg.value stat_report_ot
      $ Arg.value rand_startup_delay_ot
      $ Arg.value delay_interval_ot
      $ Arg.value batching_interval_ot
      $ Arg.value tick_limit_ot )

let () = exit Cmd.(eval cmd)
