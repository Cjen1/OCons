open! Ocons_core
open! Ocons_core.Types
module Cli = Ocons_core.Client

(* usage approximately of ./cli.exe 127.0.0.1:5001,127.0.0.1:5002 op args*)

let run op sockaddrs id retry_timeout =
  Eio_main.run
  @@ fun env ->
  Eio.Switch.run
  @@ fun sw ->
  let con_ress =
    sockaddrs
    |> List.mapi (fun idx addr ->
           ( idx
           , fun sw ->
               ( Eio.Net.connect ~sw env#net addr
                 :> Eio.Flow.two_way_ty Eio.Flow.two_way ) ) )
  in
  Eio.traceln "Creating conns to: %a"
    Fmt.(braces @@ list ~sep:comma Eio.Net.Sockaddr.pp)
    sockaddrs ;
  let cli = Cli.create_rpc ~sw env con_ress id retry_timeout in
  Eio.traceln "Submitting request %a" sm_op_pp op ;
  Random.self_init () ;
  let res = Cli.send_request ~random_id:true cli [|op|] in
  Eio.traceln "Received: %a" op_result_pp res ;
  Cli.close cli

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

let const_cmd info start op_t =
  let id_t =
    Arg.(
      required
      & pos start (some int) None
          (info ~docv:"ID" ~doc:"The id of the client" []) )
  in
  let sockaddrs_t =
    Arg.(
      required
      & pos (start + 1)
          (some @@ list sockv4)
          None
          (info ~docv:"SOCKADDRS"
             ~doc:
               "This is a comma separated list of ip addresses and ports eg: \
                \"192.168.0.1:5000,192.168.0.2:5000\""
             [] ) )
  in
  let retry_timeout_t =
    Arg.(
      value
      & opt float 1.
          (info ~docv:"RETRY" ~doc:"Timeout before retrying a request"
             ["r"; "retry-timeout"] ) )
  in
  Cmd.v info Term.(const run $ op_t $ sockaddrs_t $ id_t $ retry_timeout_t)

let write_cmd =
  let open Term in
  let info = Cmd.info "write" in
  let key_t = Arg.(required & pos 0 (some string) None (info ~docv:"KEY" [])) in
  let value_t =
    Arg.(required & pos 1 (some string) None (info ~docv:"VALUE" []))
  in
  let op_t = const (fun k v -> Types.Write (k, v)) $ key_t $ value_t in
  const_cmd info 2 op_t

let read_cmd =
  let open Term in
  let info = Cmd.info "read" in
  let key_t = Arg.(required & pos 0 (some string) None (info ~docv:"KEY" [])) in
  let op_t = const (fun k -> Types.Read k) $ key_t in
  const_cmd info 1 op_t

let () = exit Cmd.(eval @@ group (info "cli") [write_cmd; read_cmd])
