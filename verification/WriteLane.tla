------------------------------ MODULE WriteLane ------------------------------
EXTENDS Naturals
CONSTANTS Commands, CheckOnReturn
VARIABLES sent, taken, at, stopping
vars == <<sent, taken, at, stopping>>

\* One shard's writer state in WriteQueue (replica_journal/state/pipeline/
\* writer.rs). It is either at home with the journal owner or inside the one
\* job running on the device pool: queued, running a turn, deciding whether to
\* queue another turn, or returning home. The owner sends commands into the
\* lock-free queue at any time and starts a turn only while the state is home.
Places == {"home", "queued", "running", "deciding", "returning"}

Init == sent = 0 /\ taken = 0 /\ at = "home" /\ stopping = FALSE

\* WriteQueue::send, then kick: dispatch only if the state is home.
Send == /\ sent < Commands /\ ~stopping
        /\ sent' = sent + 1
        /\ at' = IF at = "home" THEN "queued" ELSE at
        /\ UNCHANGED <<taken, stopping>>

Start == at = "queued" /\ at' = "running" /\ UNCHANGED <<sent, taken, stopping>>

\* Lane::turn writes every queued command, up to a capacity.
Turn == /\ at = "running" /\ taken < sent
        /\ \E n \in (taken + 1)..sent : taken' = n
        /\ at' = "deciding"
        /\ UNCHANGED <<sent, stopping>>

\* run_turns: more commands queue another turn; otherwise the state goes home.
Decide == /\ at = "deciding"
          /\ at' = IF taken < sent THEN "queued" ELSE "returning"
          /\ UNCHANGED <<sent, taken, stopping>>

\* WriteQueue::poll takes the state home and kicks again for commands sent
\* while it was away.
Return == /\ at = "returning"
          /\ at' = IF CheckOnReturn /\ taken < sent THEN "queued" ELSE "home"
          /\ UNCHANGED <<sent, taken, stopping>>

Stop == ~stopping /\ stopping' = TRUE /\ UNCHANGED <<sent, taken, at>>

Done == /\ stopping /\ at = "home" /\ taken = sent
        /\ UNCHANGED vars

Next == Send \/ Start \/ Turn \/ Decide \/ Return \/ Stop \/ Done

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(Start) /\ WF_vars(Turn) /\ WF_vars(Decide)
        /\ WF_vars(Return) /\ WF_vars(Stop)

TypeOK == /\ sent \in 0..Commands /\ taken \in 0..sent
          /\ at \in Places /\ stopping \in BOOLEAN
\* A dispatched turn always finds a command: the owner kicks only with one
\* queued, and run_turns re-queues only after seeing one.
TurnHasWork == at \in {"queued", "running"} => taken < sent
\* Every command sent is written, and a stop ends with the state home.
AllWritten == stopping ~> (at = "home" /\ taken = sent)
=============================================================================
