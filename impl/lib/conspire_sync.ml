open! Core
open! Types
open! Utils

module type ValueSig = sig
  type t [@@deriving compare, equal, hash, bin_io, sexp]

  val pp : t Fmt.t

  val empty : t
end

module Make (Value : ValueSig) = struct
  module LogEntry = struct
    type hash = int [@@deriving compare, hash, bin_io, sexp]

    module T = struct
      type t = {hist: hash; parent_hist: hash; value: Value.t}
      [@@deriving compare, hash, bin_io, sexp]
    end

    include T
    include Core.Comparable.Make (T)

    let pp : t Fmt.t =
      let open Fmt in
      brackets
      @@ record
           [ field "hist" (fun t -> t.hist) int
           ; field "parent_hist" (fun t -> t.parent_hist) int
           ; field "value" (fun t -> t.value) Value.pp ]

    let make parent_hist value =
      let hist = [%hash: hash * Value.t] (parent_hist, value) in
      {hist; parent_hist; value}

    let bot = {hist= 0; parent_hist= 0; value= Value.empty}
  end

  open LogEntry.T

  type t = LogEntry.t option Hashtbl.M(Int).t

  let create () : t =
    let t = Hashtbl.create (module Int) in
    Hashtbl.set t ~key:0 ~data:None ;
    t

  let invariants (t : t) =
    let only_one_root =
      let roots =
        Hashtbl.fold t ~init:[] ~f:(fun ~key ~data acc ->
            match data with None -> key :: acc | _ -> acc )
      in
      if List.length roots <= 1 then Ok ()
      else
        Error
          Fmt.(str "Multiple roots at: %a" (braces @@ list ~sep:semi int) roots)
    in
    let no_loops =
      let rec aux node hwm =
        match Hashtbl.find t node.parent_hist with
        (* Reached root *)
        | Some None ->
            Ok ()
        (* unrooted *)
        | None ->
            Error
              (Fmt.str "Was unable to find %d, parent of %a" node.parent_hist
                 LogEntry.pp node )
        (* already visited node *)
        | Some (Some ({hist; _} as node))
          when [%equal: int option] (Some hist) hwm ->
            Error (Fmt.str "Loop detected at %a" Fmt.(parens LogEntry.pp) node)
        (* recurse *)
        | Some (Some ({parent_hist; _} as node)) ->
            aux node
              (Some (max parent_hist @@ Option.value ~default:parent_hist hwm))
      in
      Hashtbl.fold t ~init:(Ok ()) ~f:(fun ~key:_ ~data:node acc ->
          match (acc, node) with
          | (Error _ as e), _ ->
              e
          | Ok (), None ->
              acc
          | Ok (), Some node ->
              aux node None )
    in
    List.filter_map
      ~f:(function Ok _ -> None | Error s -> Some s)
      [only_one_root; no_loops]

  let add t le =
    assert (not @@ Hashtbl.mem t le.hist) ;
    Hashtbl.set t ~key:le.hist ~data:(Some le)

  let mem t le = Hashtbl.mem t le.hist

  let find_exn (t : t) hist = Hashtbl.find_exn t hist

  let find_le_exn t le = find_exn t le.hist

  let add_log t log ?(from = Log.highest log) () =
    let rec aux idx =
      (* ensures that we don't check for negative ranges with empty log*)
      if Log.mem log idx then
        let le = Log.get log idx in
        if not (mem t le) then (
          add t le ;
          aux (idx - 1) )
    in
    aux from

  let fixup t log ~idx ~ack_hash =
    Log.cut_after log idx ;
    let rec aux idx hist =
      let le = Log.find log idx in
      let target = find_exn t hist in
      match ([%equal: LogEntry.t option] le target, target) with
      | true, _ ->
          idx
      | false, None ->
          Fmt.failwith
            "target is None => idx(%d) log entry should be bot but is %a" idx
            (Fmt.option LogEntry.pp) le
      | false, Some v ->
          Log.set log idx v ;
          aux (idx - 1) v.parent_hist
    in
    aux idx ack_hash
end
