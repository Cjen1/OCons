(*process_message.ml*)


exception Parse_error of string

let op_of_message (op : Message_types.operation) : Types.operation = 
  let open Types in
  match op with
  | Nop -> Nop
  | Create (op_create) -> Create (Int32.to_int op_create.key, op_create.value)
  | Read   (op_read)   -> Read   (Int32.to_int op_read.key)
  | Update (op_update) -> Update (Int32.to_int op_update.key, op_update.value)
  | Remove (op_remove) -> Remove (Int32.to_int op_remove.key);;

let result_of_message (res:Message_types.result) : Types.result =
  let open Types in
  match res with
  | Success     -> Success
  | Failure     -> Failure
  | Read    (s) -> ReadSuccess (s);;

let client_id_of_message (cl_id:Message_types.client_id) 
                         : Types.client_id =
  let open Core in
  let id = Uuid.of_string cl_id.cl_id in
  let uri = Uri.of_string cl_id.cl_uri in
  id, uri

let command_of_message (com:Message_types.command) : Types.command =
  let open Types in
  match com.op, com.client with
  | Some oper, Some cli -> 
    let comm_id = Int32.to_int com.command_id in
    let open Core in
    let clid = client_id_of_message cli in
    let op = op_of_message oper in
    clid , comm_id , op
  | _, _ -> raise (Parse_error "Got invalid command!");;

let ballot_of_message (b:Message_types.ballot) : Ballot.t =
  let ball_num = Int32.to_int b.ballot_num in
  let open Core in
  let l_id = Uuid.of_string b.leader_id in
  Number (ball_num, l_id);;

let pvalue_of_message (pv:Message_types.pvalue) : Pval.t = 
  match pv.ballot, pv.command with
  | None, _ -> 
    raise (Parse_error "Received pvalue has no ballot number!")
  | _, None -> 
    raise (Parse_error "Received pvalue has no command!")
  | Some b, Some c ->
    let ballot  = ballot_of_message b in
    let command = command_of_message c in
    ballot , Int32.to_int pv.slot_num , command;;


let operation_message (op:Types.operation) : Message_types.operation =
  let open Message_types in
  match op with
  | Nop -> Nop
  | Create(key, value) -> Create {key=Int32.of_int key; value=value}
  | Read(key) ->          Read   {key=Int32.of_int key}
  | Update(key, value) -> Update {key=Int32.of_int key; value=value}
  | Remove(key) ->        Remove {key=Int32.of_int key};;

let ballot_message (b:Ballot.t) : Message_types.ballot =
  match b with
  | Bottom -> raise (Parse_error "Can't serialie bottom ballot!")
  | Number (b_num, l_id) ->
    let bn = Int32.of_int b_num in
    let open Core in
    let lid = Uuid.to_string l_id in
    { leader_id=lid
    ; ballot_num=bn };;

let client_id_message (cl_id:Types.client_id) : Message_types.client_id =
  let id, uri = cl_id in
  let s_id = Uuid.to_string id in
  let s_uri = Uri.to_string uri in
  { cl_id=s_id
  ; cl_uri=s_uri };;

let command_message (c:Types.command) : Message_types.command = 
  let cl_id, comma_id, op = c in
  let comm_id = Int32.of_int comma_id in
  let open Core in
  let client_id = client_id_message cl_id in
  let oper = operation_message op in
  { client=Some client_id
  ; command_id=comm_id
  ; op=Some oper};;

let pval_message (pv:Pval.t) : Message_types.pvalue =
  let b, s, c = pv in
  let ball = ballot_message b in
  let comm = command_message c in
  { ballot=Some ball
  ; slot_num=Int32.of_int s
  ; command=Some comm};;
      
let p1a_message (b:Ballot.t) : Message_types.phase1_a = 
  let ballot = ballot_message b in
  { ballot=Some ballot };;

let p1b_message (a_id:Types.unique_id) 
                (bal:Ballot.t) 
                (accepted:Pval.t list)
                (gc_thresh:Types.slot_number) : Message_types.phase1_b =
  let gc_threshold = Int32.of_int gc_thresh in
  let open Core in
  let pvals = List.map accepted ~f:(fun pv -> pval_message pv) in
  let ballot = ballot_message bal in
  let acc_id = Uuid.to_string a_id in
  { acc_id=acc_id
  ; ballot=Some ballot
  ; gc_threshold=gc_threshold
  ; pvals=pvals};;

let p2a_message (leader_id:Types.unique_id) 
                (pv:Pval.t) 
                : Message_types.phase2_a = 
  let open Core in
  let l_id = Uuid.to_string leader_id in
  let pval = pval_message pv in
  { leader_id=l_id
  ; pval=Some pval };;

let p2b_message (a_id:Types.unique_id) (b:Ballot.t) 
                : Message_types.phase2_b =
  let ball=ballot_message b in
  let open Core in
  let acc_id=Uuid.to_string a_id in
  { ballot=Some ball
  ; acc_id=acc_id };; 

let result_message (res:Types.result) : Message_types.result =
  let open Message_types in
  match res with
  | Success -> Success
  | Failure -> Failure
  | ReadSuccess (s) -> Read (s);;

