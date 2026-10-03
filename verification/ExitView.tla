------------------------------ MODULE ExitView ------------------------------
EXTENDS Naturals, FiniteSets
CONSTANT EchoOlderView
VARIABLES view, timedOut, votes, echo, net
vars == <<view, timedOut, votes, echo, net>>

\* The primary is gone. Backups 1 and 2 must leave view 0 on volatile EXIT_VIEW
\* suspicion alone: each backup's durable promise, and so its
\* START_VIEW_CHANGE, stays stalled behind a held disk. As in
\* ReplicaDriver::poll, a timeout that completes the quorum leaves the view in
\* the same step, before the backup sends its own request.
Backups == {1, 2}
Other(b) == IF b = 1 THEN 2 ELSE 1
Messages == [from : Backups, to : Backups, view : {0}]

Init == /\ view = [b \in Backups |-> 0]
        /\ timedOut = [b \in Backups |-> FALSE]
        /\ votes = [b \in Backups |-> {}]
        /\ echo = [b \in Backups |-> FALSE]
        /\ net = {}

Leave(b, supporters) ==
    view' = IF Cardinality(supporters) >= 2 THEN [view EXCEPT ![b] = 1] ELSE view

Timeout(b) == /\ view[b] = 0 /\ ~timedOut[b]
              /\ timedOut' = [timedOut EXCEPT ![b] = TRUE]
              /\ votes' = [votes EXCEPT ![b] = @ \cup {b}]
              /\ Leave(b, votes[b] \cup {b})
              /\ UNCHANGED <<echo, net>>

\* Retransmitted while suspicion stays active.
Request(b) == /\ view[b] = 0 /\ timedOut[b]
              /\ net' = net \cup {[from |-> b, to |-> Other(b), view |-> 0]}
              /\ UNCHANGED <<view, timedOut, votes, echo>>

\* A backup counts its own vote only after its own timeout, so a peer alone
\* never pushes it out of a view.
Receive(m) ==
    /\ m \in net
    /\ net' = net \ {m}
    /\ IF m.view = view[m.to]
       THEN /\ votes' = [votes EXCEPT ![m.to] = @ \cup {m.from}]
            /\ Leave(m.to, votes[m.to] \cup {m.from})
            /\ UNCHANGED <<timedOut, echo>>
       ELSE IF m.view < view[m.to] /\ EchoOlderView
            THEN /\ echo' = [echo EXCEPT ![m.to] = TRUE]
                 /\ UNCHANGED <<view, timedOut, votes>>
            ELSE UNCHANGED <<view, timedOut, votes, echo>>

\* A backup that left view 0 supports a peer still asking to leave it.
Echo(b) == /\ echo[b]
           /\ echo' = [echo EXCEPT ![b] = FALSE]
           /\ net' = net \cup {[from |-> b, to |-> Other(b), view |-> 0]}
           /\ UNCHANGED <<view, timedOut, votes>>

Done == /\ \A b \in Backups : view[b] = 1
        /\ UNCHANGED vars

Next == \/ \E b \in Backups : Timeout(b) \/ Request(b) \/ Echo(b)
        \/ \E m \in net : Receive(m)
        \/ Done

Spec == /\ Init /\ [][Next]_vars
        /\ \A b \in Backups : WF_vars(Timeout(b)) /\ WF_vars(Request(b))
                              /\ WF_vars(Echo(b))
        /\ \A m \in Messages : WF_vars(Receive(m))

TypeOK == /\ view \in [Backups -> {0, 1}]
          /\ timedOut \in [Backups -> BOOLEAN]
          /\ votes \in [Backups -> SUBSET Backups]
          /\ echo \in [Backups -> BOOLEAN]
          /\ net \subseteq Messages
\* A lone suspicious peer cannot ratchet a backup's view.
OwnTimeoutToLeave == \A b \in Backups : view[b] = 1 => timedOut[b]
BothLeave == <>(\A b \in Backups : view[b] = 1)
=============================================================================
