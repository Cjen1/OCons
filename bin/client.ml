(* main.ml *)
open Core
open Lib.Client
open Lib.State_machine

let write =
  Command.basic ~summary:"Creation operation"
    Command.Let_syntax.(
      let%map_open endpoints = anon ("endpoints" %: string)
      and key = anon ("key" %: string)
      and value = anon ("value" %: string) in
      fun () ->
        let endpoints =
          let pairs = Base.String.split ~on:',' endpoints in
          Base.List.map pairs ~f:(fun xs ->
              match Base.String.split ~on:'=' xs with
              | [id; addr] ->
                  (id, addr)
              | _ ->
                  raise
                    (Invalid_argument ("Need endpoints of the form [id=uri] but got " ^ endpoints)))
        in
        let c, p = new_client ~endpoints () in
        Lwt_main.run
        @@ Lwt.choose
             [ p
             ; (let%lwt res = op_write c key value in
                res |> StateMachine.sexp_of_op_result |> Sexp.to_string_hum
                |> print_endline ;
                Lwt.return_unit) ])

let read =
  Command.basic ~summary:"Read operation"
    Command.Let_syntax.(
      let%map_open endpoints = anon ("endpoints" %: string)
      and key = anon ("key" %: string) in
      fun () ->
        let endpoints =
          let pairs = Base.String.split ~on:',' endpoints in
          Base.List.map pairs ~f:(fun xs ->
              match Base.String.split ~on:'=' xs with
              | [id; addr] ->
                  (id, addr)
              | _ ->
                  raise
                    (Invalid_argument ("Need endpoints of the form [id=uri] but got " ^ endpoints)))
        in
        let c, p = new_client ~endpoints () in
        Lwt_main.run
        @@ Lwt.choose
             [ p
             ; (let%lwt res = op_read c key in
                res |> StateMachine.sexp_of_op_result |> Sexp.to_string_hum
                |> print_endline ;
                Lwt.return_unit) ])

let reporter =
  let report src level ~over k msgf =
    let k _ = over () ; k () in
    let src = Logs.Src.name src in
    msgf
    @@ fun ?header ?tags:_ fmt ->
    Fmt.kpf k Fmt.stdout
      ("[%a] %a %a @[" ^^ fmt ^^ "@]@.")
      Time.pp (Time.now ())
      Fmt.(styled `Magenta string)
      (Printf.sprintf "%14s" src)
      Logs_fmt.pp_header (level, header)
  in
  {Logs.report}

(* Handle the command line arguments and run application is specified mode *)
let cmd =
  Command.group ~summary:"Cli client for Ocaml Paxos"
    [("write", write); ("read", read)]


let () =
  Lwt_engine.set (new Lwt_engine.libev ()) ;
  Fmt_tty.setup_std_outputs () ;
  Logs.(set_level (Some Debug)) ;
  Logs.set_reporter reporter ; Core.Command.run cmd
