open! Types
open! Utils
open! Actions_f
module Imp = ImperativeActions (Conspire_single_shot.Types)
module Impl = Paxos.Make (Imp)
open! Paxos.Types
open! Impl
open Ocons_core.Consensus_intf

let action_pp = action_pp ~pp_msg:message_pp
