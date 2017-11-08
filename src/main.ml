open Lwt
open Lwt_io
open Lwt.Infix
open Unix


(* Simple LWT example from Mirage tutorial *)
let start () =
  Lwt.join [
    (Lwt_unix.sleep(2.) >>= fun () -> Lwt_io.printl "Tails");
    (Lwt_unix.sleep(1.) >>= fun () -> Lwt_io.printl "Heads")
  ] >>= fun () -> Lwt_io.printl "All finished"

let rec copy_blocks buffer in_chan out_chan =
  (Lwt_io.read_into in_chan buffer 0 (Bytes.length buffer)) >>= 
  (function
   | 0          -> Lwt.return ()
   | bytes_read -> 
     (Lwt_io.write_from_exactly out_chan buffer 0 bytes_read) >>=
     (fun _ -> copy_blocks buffer in_chan out_chan)
  )

(* Start a TCP server on an established port *)
let run () =
  (let server = Lwt_io.establish_server_with_client_address
                (Lwt_unix.ADDR_INET (Unix.inet_addr_any, 8765))
                (fun _ (in_chan, out_chan) -> 
                   let buffer = Bytes.create (16 * 1024) in
                    copy_blocks buffer in_chan out_chan)
  in Lwt.return server) |> ignore

let never_terminate = fst (Lwt.wait ())

let () =
  Sys.set_signal Sys.sigpipe Sys.Signal_ignore;
  (try Lwt_engine.set (new Lwt_engine.libev ())
   with Lwt_sys.Not_available _ -> ());
  run ();
  Lwt_main.run never_terminate
