let debug_flag = ref false

let set_debug_flag () = debug_flag := true

let dtraceln fmt = if !debug_flag then Eio.traceln fmt else Fmt.kstr ignore fmt

let is_not_cancel = function Eio.Cancel.Cancelled _ -> false | _ -> true

let maybe_yield ~energy =
  let curr = ref energy in
  fun () ->
    if !curr <= 0 then (
      curr := energy ;
      Eio.Fiber.yield () ) ;
    curr := !curr - 1
