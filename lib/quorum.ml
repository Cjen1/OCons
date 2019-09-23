type ('a, 'b) t = 'a list * 'b list

let add (all, quorum) x = (all, x :: quorum)

let is_majority (all, quorum) = List.length quorum > List.length all / 2

let from_list xs = (xs, [])
