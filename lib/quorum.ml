type 'b t = int * 'b list

let add (all, quorum) x = (all, x :: quorum)

let is_majority (all, quorum) = List.length quorum > all / 2
