------------------------ MODULE PrepareReplacement -------------------------
EXTENDS Naturals
CONSTANT ProtectPrepareAck
VARIABLES accepted, committed, selected, installed
vars == <<accepted, committed, selected, installed>>

\* One local durable prepare was NOT quorum committed. The other two voters
\* establish an authorized view without it. Selection authority is an explicit
\* input assumption here, not something this storage-boundary model proves.
Init == /\ accepted = 1 /\ committed = 0 /\ selected = 0 /\ installed = FALSE
Install == /\ ~installed /\ selected >= committed
           /\ (~ProtectPrepareAck \/ selected >= accepted)
           /\ accepted' = selected /\ installed' = TRUE
           /\ UNCHANGED <<committed, selected>>
Done == /\ installed /\ UNCHANGED vars
Next == Install \/ Done
CommittedPreserved == committed <= accepted
=============================================================================
