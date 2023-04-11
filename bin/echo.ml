open! Eio.Std

let set_nodelay sock =
  match sock |> Eio_unix.FD.peek_opt with
  | None ->
      Fmt.invalid_arg "Could not get underlying file descriptor"
  | Some fd ->
      Unix.setsockopt fd Unix.TCP_NODELAY true

let server addr =
  Eio_main.run
  @@ fun env ->
  Switch.run
  @@ fun sw ->
  let sock = Eio.Net.listen ~backlog:4 ~sw env#net addr in
  Eio.Net.run_server ~on_error:(traceln "%a" Fmt.exn) sock
  @@ fun sock _ ->
  let br = Eio.Buf_read.of_flow ~max_size:10_000 sock in
  Eio.Buf_write.with_flow sock
  @@ fun bw ->
  set_nodelay sock ;
  while true do
    let tx = Eio.Buf_read.BE.double br in
    let rx = Eio.Time.now env#clock in
    Eio.Buf_write.BE.double bw tx ;
    Eio.Buf_write.BE.double bw rx ;
    Eio.Buf_write.flush bw
  done

let client addr n rate =
  let period = 1. /. rate in
  Eio_main.run
  @@ fun env ->
  Switch.run
  @@ fun sw ->
  let td_cs = ref @@ Tdigest.create ~delta:Tdigest.Discrete () in
  let td_sc = ref @@ Tdigest.create ~delta:Tdigest.Discrete () in
  let count = ref 0 in
  let sock = Eio.Net.connect ~sw env#net addr in
  set_nodelay sock ;
  let br = Eio.Buf_read.of_flow ~max_size:10_000 sock in
  Eio.Buf_write.with_flow sock
  @@ fun bw ->
  let rec dispatch target = function
    | i when i < 0 ->
        ()
    | i ->
        if Eio.Time.now env#clock < target then
          Eio.Time.sleep_until env#clock target ;
        Eio.Buf_write.BE.double bw (Eio.Time.now env#clock) ;
        Eio.Buf_write.flush bw ;
        dispatch (target +. period) (i - 1)
  in
  let rec collect () =
    let t1 = Eio.Buf_read.BE.double br in
    let t2 = Eio.Buf_read.BE.double br in
    let t3 = Eio.Time.now env#clock in
    incr count ;
    td_cs := Tdigest.add ~data:(t2 -. t1) !td_cs ;
    td_sc := Tdigest.add ~data:(t3 -. t2) !td_sc ;
    collect ()
  in
  Fiber.fork_daemon ~sw collect ;
  dispatch (Eio.Time.now env#clock) (n - 1) ;
  Eio.Time.sleep env#clock 1. ;
  let print kind s =
    let open Fmt in
    let percentiles =
      [0.25; 0.50; 0.75; 0.99; 1.00]
      |> List.map (fun p ->
             (p, Tdigest.percentile s p |> snd |> Option.get |> ( *. ) 1000.) )
    in
    let pp_percentile : (float * float) Fmt.t =
     fun ppf (p, v) -> pf ppf "%.2f: %.3f" p v
    in
    traceln "%s: %a" kind (braces @@ list ~sep:comma pp_percentile) percentiles
  in
  print "Cli-Ser" !td_cs ; print "Ser-Cli" !td_sc

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

let sockaddr_t position =
  Arg.(required & pos position (some @@ sockv4) None (info ~docv:"SOCKADDR" []))

let ser_cmd =
  let info = Cmd.info "server" in
  Cmd.v info Term.(const server $ sockaddr_t 0)

let cli_cmd =
  let info = Cmd.info "client" in
  let num_t = Arg.(required & pos 1 (some @@ int) None (info [])) in
  let rate_t = Arg.(required & pos 2 (some @@ float) None (info [])) in
  Cmd.v info Term.(const client $ sockaddr_t 0 $ num_t $ rate_t)

let () = exit Cmd.(eval @@ group (info "cli") [ser_cmd; cli_cmd])
