let debug_flag = false

let dtraceln fmt = 
  if debug_flag then Eio.traceln fmt else Fmt.kstr ignore fmt
