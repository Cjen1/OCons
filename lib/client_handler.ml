open! Core
open! Async
open! Ppx_log_async
module H = Hashtbl
open Types
open Types.MessageTypes
open! Utils
open Rpc_parallel

let logger =
  let open Async_unix.Log in
  create ~level:`Info ~output:[] ~on_error:`Raise
    ~transform:(fun m -> Message.add_tags m [("src", "Client handler")])
    ()

module T = struct
  (* Basic idea is that the main process contacts this one explicitly for a batch
     We read that batch off of the cr_pipe.
     When a request arrives we put it in that pipe.
  *)
  type request_batch = client_request list * int [@@deriving bin_io]

  type return_result_t = command_id * client_response [@@deriving bin_io]

  type 'a functions =
    { get_batch: ('a, unit, request_batch) Function.t
    ; batch_available_watch: ('a, unit, unit) Function.t
    ; return_result: ('a, return_result_t, unit) Function.t }

  module Worker_state = struct
    type init_arg =
      { external_port: int
      ; batch_size: int
      ; batch_timeout: Time.Span.t
      ; log_level: Log.Level.t }
    [@@deriving bin_io]

    type t =
      { client_ivars: (command_id, client_response Ivar.t list) H.t
      ; client_results: (command_id, op_result) H.t
      ; cr_pipe: client_request rd_wr_pipe
      ; batch_size: int
      ; batch_timeout: Time.Span.t }
  end

  module Connection_state = struct
    type init_arg = unit [@@deriving bin_io]

    type t = unit
  end

  module Functions
      (C : Rpc_parallel.Creator
             with type worker_state := Worker_state.t
              and type connection_state := Connection_state.t) =
  struct
    open Worker_state

    let client_handler_impl =
      [ Rpc.Rpc.implement RPCs.client_request (fun t cr ->
            [%log.global.debug "Received" (cr.id : Id.t)] ;
            match H.find t.client_results cr.id with
            | Some result ->
                return (Ok result)
            | None ->
                Deferred.create (fun i ->
                    [%log.global.debug "Adding to pipe" (cr.id : Id.t)] ;
                    H.add_multi t.client_ivars ~key:cr.id ~data:i ;
                    Pipe.write_without_pushback t.cr_pipe.wr cr ) ) ]

    let some_or_default v d = match v with Some v -> v | None -> d

    let init_worker_state {external_port; batch_size; batch_timeout; log_level}
        =
      Log.Global.set_level log_level ;
      let cr_pipe =
        let rd, wr = Pipe.create ~info:[%message "cr_pipe"] () in
        {rd; wr}
      in
      let t =
        { client_ivars= H.create (module Id)
        ; client_results= H.create (module Id)
        ; cr_pipe
        ; batch_size
        ; batch_timeout }
      in
      let implementations =
        Rpc.Implementations.create_exn ~implementations:client_handler_impl
          ~on_unknown_rpc:`Continue
      in
      let on_handler_error =
        `Call
          (fun _ e -> [%log.error logger "Error while handling msg" (e : exn)])
      in
      let%bind (_ : (Socket.Address.Inet.t, int) Tcp.Server.t) =
        Tcp.Server.create (Tcp.Where_to_listen.of_port external_port)
          ~on_handler_error (fun _addr reader writer ->
            [%log.global.info "Client connected"] ;
            Rpc.Connection.server_with_close reader writer ~implementations
              ~connection_state:(fun _ -> t)
              ~on_handshake_error:`Ignore )
      in
      return t

    let init_connection_state ~connection:_ ~worker_state:_ () = Deferred.unit

    let batch_available_watch_fn t =
      [%log.global.debug "Checking batch availability"] ;
      match%bind Pipe.values_available t.cr_pipe.rd with
      | `Eof ->
          [%log.global.error "Client request pipe unexpectedly closed"] ;
          Deferred.never ()
      | `Ok ->
          Deferred.unit

    let batch_available_watch =
      let f ~worker_state:t ~conn_state:() () = batch_available_watch_fn t in
      C.create_rpc ~name:"batch_available" ~f ~bin_input:bin_unit
        ~bin_output:bin_unit ()

    let get_batch_fn t =
      let cr_len = Pipe.length t.cr_pipe.rd in
      let loop (q, remaining, timeout) :
          [ `Repeat of client_request Fqueue.t * int * unit Deferred.t
          | `Finished of client_request list * int ]
          Deferred.t =
        let q, len_added =
          match Pipe.read_now' ~max_queue_length:remaining t.cr_pipe.rd with
          | `Eof ->
              [%log.global.error "Client request pipe unexpectedly closed"] ;
              (q, 0)
          | `Nothing_available ->
              (q, 0)
          | `Ok q' ->
              let len_added = Queue.length q' in
              (Queue.fold q' ~init:q ~f:Fqueue.enqueue, len_added)
        in
        assert (len_added = remaining || Pipe.length t.cr_pipe.rd = 0) ;
        match Fqueue.length q < t.batch_size with
        | false ->
            return @@ `Finished (Fqueue.to_list q, cr_len)
        | true -> (
            let timeout_choice = choice timeout (fun () -> `Timeout) in
            let next_segment_choice =
              choice (Pipe.values_available t.cr_pipe.rd) (fun _ ->
                  `ValuesAvailable )
            in
            let%bind enabled =
              Deferred.enabled [next_segment_choice; timeout_choice]
            in
            match enabled () |> List.hd_exn with
            | `Timeout ->
                return @@ `Finished (Fqueue.to_list q, cr_len)
            | `ValuesAvailable ->
                return @@ `Repeat (q, remaining - len_added, timeout) )
      in
      Deferred.repeat_until_finished
        (Fqueue.empty, t.batch_size, after t.batch_timeout)
        loop

    let get_batch =
      let f ~worker_state:t ~conn_state:() () = get_batch_fn t in
      C.create_rpc ~name:"get_batch" ~f ~bin_input:bin_unit
        ~bin_output:bin_request_batch ()

    let return_result =
      C.create_one_way ~name:"return_results"
        ~f:(fun ~worker_state:t ~conn_state:() (cmd_id, result) ->
          ( match result with
          | Error _ ->
              ()
          | Ok result ->
              H.set t.client_results ~key:cmd_id ~data:result ) ;
          let ivars = H.find_multi t.client_ivars cmd_id in
          List.iter ivars ~f:(fun i -> Ivar.fill_if_empty i result) ;
          H.remove_multi t.client_ivars cmd_id )
        ~bin_input:bin_return_result_t ()

    let functions = {get_batch; batch_available_watch; return_result}
  end
end

module M = Rpc_parallel.Make (T)
include M
module Shutdown_on = M.Shutdown_on (Monad.Ident)

let spawn_client_handler ~external_port ~batch_size ~batch_timeout =
  let args =
    T.Worker_state.
      {external_port; batch_size; batch_timeout; log_level= Log.level logger}
  in
  let%bind conn =
    spawn_exn ~shutdown_on:Shutdown_on.Connection_closed
      ~redirect_stdout:`Dev_null ~redirect_stderr:`Dev_null args
      ~on_failure:(fun e -> [%log.global.error "Failed" (e : Error.t)])
      ~connection_state_init_arg:()
  in
  let%bind client_log =
    Connection.run_exn conn ~f:Rpc_parallel.Function.async_log ~arg:()
  in
  Pipe.iter client_log ~f:(fun msg -> Log.message logger msg ; Deferred.unit)
  |> don't_wait_for ;
  [%log.debug logger "Set up client"] ;
  return conn
