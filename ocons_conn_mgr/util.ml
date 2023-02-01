let debug_flag = true

let dtraceln fmt = 
  if debug_flag then Eio.traceln fmt else Fmt.kstr ignore fmt
