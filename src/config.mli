type addr_info = {
  client_addrs : (string * int) list;
  replica_addrs : (string * int) list;
  leader_addrs : (string * int) list;
};;

val read_settings : string -> addr_info;;
