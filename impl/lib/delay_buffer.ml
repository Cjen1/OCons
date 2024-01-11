open! Core
module Time = Time_float_unix

let pp_time_float_unix : Time.t Fmt.t =
 fun ppf v -> Fmt.pf ppf "%0.5f" (Utils.time_to_float v)

type 'a t =
  { mutable store: 'a list Map.M(Time).t
        [@polyprinter fun pa -> Utils.pp_map pp_time_float_unix pa]
  ; mutable hwm: Time.t [@printer pp_time_float_unix]
  ; interval: Time.Span.t
  ; compare: 'a -> 'a -> int }
[@@deriving show {with_path= false}]

let ms_clamp ?(direction = `Ceil) interval f =
  match direction with
  | `Ceil ->
      Time.next_multiple ~base:Time.epoch ~interval ~after:f
        ~can_equal_after:true ()
  | `Floor ->
      Time.prev_multiple ~base:Time.epoch ~interval ~before:f
        ~can_equal_before:true ()

let get_values t current =
  (* lim is the millisecond before now *)
  let lim = current |> ms_clamp ~direction:`Floor t.interval in
  let seq =
    Map.to_sequence ~order:`Increasing_key ~keys_less_or_equal_to:lim t.store
  in
  t.hwm <- lim ;
  seq
  |> Sequence.group ~break:(fun (a, _) (b, _) ->
         Time.(ms_clamp t.interval a <> ms_clamp t.interval b) )
  |> Sequence.map ~f:(fun ls ->
         let key = ms_clamp t.interval (List.hd_exn ls |> fst) in
         (* remove relevant keys *)
         let values =
           ls
           |> List.iter ~f:(fun (k, _) ->
                  t.store <- Map.remove_multi t.store k ) ;
           (* export batch *)
           ls |> List.map ~f:snd |> List.join |> List.sort ~compare:t.compare
         in
         (values, key) )

let add_value t v target =
  if Time_float.(target > t.hwm) then
    t.store <- Map.add_multi t.store ~key:target ~data:v

let create ~compare interval base_hwm =
  {hwm= base_hwm; interval; store= Map.empty (module Time); compare}
