module E = Eio

module ManBufWrite = struct
  open! E

  let read_source_buffer t fn =
    let iovecs = Buf_write.await_batch t in
    Buf_write.shift t (fn iovecs)

  let read_into t buf =
    let iovecs = Buf_write.await_batch t in
    let n, _iovecs = Cstruct.fillv ~src:iovecs ~dst:buf in
    Buf_write.shift t n ; n

  let as_flow t =
    object
      inherit Flow.source

      method! read_methods = [Flow.Read_source_buffer (read_source_buffer t)]

      method read_into = read_into t
    end

  let of_flow ~sw ?(initial_size = 0x1000) flow =
    let w = Buf_write.create ~sw initial_size in
    Fiber.fork ~sw (fun () -> Flow.copy (as_flow w) flow) ;
    w
end

module PersistantConn = struct
  type t =
    { f: unit -> E.Flow.two_way
    ; mutable w: E.Buf_write.t
    ; mutable r: E.Buf_read.t
    ; mutable state: [`Open | `Closed]
    ; sw: E.Switch.t
    ; state_changed: E.Condition.t }

  let update_state t k =
    t.state <- k ;
    E.Condition.broadcast t.state_changed

  let make_reader_writer ~sw flow =
    let r = E.Buf_read.of_flow ~max_size:1_000_000 flow in
    let w = ManBufWrite.of_flow ~sw flow in
    (r, w)

  let is_open t = match t.state with `Open -> true | `Closed -> false

  let rec send ?(block_until_open = false) t cs =
    match (is_open t, block_until_open) with
    | true, _ ->
        E.Buf_write.cstruct t.w cs
    | false, false ->
        ()
    | false, true ->
        E.Condition.await_no_mutex t.state_changed ;
        send ~block_until_open t cs

  let create ~sw (f : unit -> E.Flow.two_way) =
    let t : t = assert false in
    E.Fiber.fork_daemon ~sw (fun () ->
        while true do
          match t.state with
          | `Open ->
              E.Condition.await_no_mutex t.state_changed
          | `Closed ->
              let flow = t.f () in
              let r, w = make_reader_writer ~sw flow in
              t.w <- w ;
              t.r <- r ;
              update_state t `Open ;
              ()
        done ;
        assert false ) ;
    t
end
