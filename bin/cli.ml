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
           , fun () -> (Eio.Net.connect ~sw env#net addr :> Eio.Flow.two_way) ) )
  in
  Fmt.pr "Creating conn to: %a\n"
    Fmt.(braces @@ list ~sep:comma Eio.Net.Sockaddr.pp)
    sockaddrs ;
  let cli = Cli.create_rpc ~sw env con_ress id retry_timeout in
  Fmt.pr "Submitting request %a\n" sm_op_pp op ;
  let res = Cli.send_request cli op in
  Fmt.pr "Received: %a\n" op_result_pp res

open Cmdliner

let sock_addr_a =
  let conv = Arg.(t2 ~sep:':' string int) in
  let parse s =
    let parse = Arg.conv_parser conv s in
    Result.bind parse (fun (ip, port) ->
        try Ok (`Tcp (Eio.Net.Ipaddr.of_raw ip, port))
        with e ->
          Error
            (`Msg
              (Fmt.str "Failed to parse ip address [%s] with error %a" ip
                 Fmt.exn e ) ) )
  in
  Arg.conv ~docv:"SOCKADDR" (parse, Eio.Net.Sockaddr.pp)

let const_cmd info start op_t =
  let id_t =
    Arg.(
      value
      & pos start int (-1) (info ~docv:"ID" ~doc:"The id of the client" []) )
  in
  let sockaddrs_t =
    Arg.(
      value
      & pos (start + 1) (list sock_addr_a) []
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
  let key_t = Arg.(value & pos 0 string "MISSING" (info ~docv:"KEY" [])) in
  let value_t = Arg.(value & pos 1 string "MISSING" (info ~docv:"VALUE" [])) in
  let op_t = const (fun k v -> Types.Write (k, v)) $ key_t $ value_t in
  const_cmd info 2 op_t

let read_cmd =
  let open Term in
  let info = Cmd.info "read" in
  let key_t = Arg.(value & pos 0 string "MISSING" (info ~docv:"KEY" [])) in
  let op_t = const (fun k -> Types.Read k) $ key_t in
  const_cmd info 1 op_t

let cas_cmd =
  let open Term in
  let info = Cmd.info "cas" in
  let key_t = Arg.(value & pos 0 string "MISSING" (info ~docv:"KEY" [])) in
  let value_t = Arg.(value & pos 1 string "MISSING" (info ~docv:"VALUE" [])) in
  let new_value_t =
    Arg.(value & pos 2 string "MISSING" (info ~docv:"NEW_VALUE" []))
  in
  let op_t =
    const (fun key value value' -> Types.CAS {key; value; value'})
    $ key_t $ value_t $ new_value_t
  in
  const_cmd info 3 op_t

let () = exit Cmd.(eval @@ group (info "cli") [write_cmd; read_cmd; cas_cmd])
