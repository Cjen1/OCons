let debug_flag = true

let dtraceln fmt = 
  if debug_flag then Eio.traceln fmt else Fmt.kstr ignore fmt

let is_not_cancel = function Eio.Cancel.Cancelled _ -> false | _ -> true
