open Core
open Lwt
open Lwt_io

(* Spin off two threads, ... *)
let start () =
  Lwt.join [
    (Lwt_unix.sleep(2.) >>= fun () -> Lwt_io.printl "Tails");
    (Lwt_unix.sleep(1.) >>= fun () -> Lwt_io.printl "Heads")
  ] >>= fun () -> Lwt_io.printl "All finished"

let start' () =
  let rec echo n = match n with
    | 0 -> return ()
    | n -> (Lwt_io.read Lwt_io.stdin) 
      >>= (fun s -> Lwt_io.printl s) 
      >>= (fun () -> echo (n-1))
  in echo 10;;

let () = start () |> Lwt_main.run
