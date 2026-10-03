-------------------------- MODULE PrimaryRestart ---------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS MaxView, RequireNewView
VARIABLES view, durableView, normal, issued, stableValue, seen
vars == <<view, durableView, normal, issued, stableValue, seen>>
Values == {0, 1}
Absent == 2

Init == /\ view = 0 /\ durableView = 0 /\ normal = TRUE
        /\ issued = Absent /\ stableValue = Absent
        /\ seen = [v \in 0..MaxView |-> {}]

Issue(value) == /\ normal /\ issued = Absent
                /\ issued' = value
                /\ UNCHANGED <<view, durableView, normal, stableValue, seen>>

\* Delivery of a prepare may precede primary local synchronization.
Send == /\ normal /\ issued \in Values
        /\ seen' = [seen EXCEPT ![view] = @ \cup {issued}]
        /\ UNCHANGED <<view, durableView, normal, issued, stableValue>>
Persist == /\ normal /\ issued \in Values /\ stableValue' = issued
           /\ UNCHANGED <<view, durableView, normal, issued, seen>>
Crash == /\ normal /\ normal' = FALSE /\ issued' = stableValue
         /\ UNCHANGED <<view, durableView, stableValue, seen>>

\* Abstract only the restart fence. This is NOT a full view-selection model.
\* The normal implementation must establish the selected view through quorum.
Restart == /\ ~normal /\ (~RequireNewView \/ durableView < MaxView)
           /\ view' = IF RequireNewView THEN durableView + 1 ELSE durableView
           /\ durableView' = view' /\ normal' = TRUE /\ issued' = stableValue
           /\ UNCHANGED <<stableValue, seen>>

Next == (\E value \in Values : Issue(value)) \/ Send \/ Persist \/ Crash
        \/ Restart \/ UNCHANGED vars
TypeOK == /\ view \in 0..MaxView /\ durableView \in 0..MaxView
          /\ normal \in BOOLEAN /\ issued \in Values \cup {Absent}
          /\ stableValue \in Values \cup {Absent}
          /\ seen \in [0..MaxView -> SUBSET Values]
SlotIdentity == \A v \in 0..MaxView : Cardinality(seen[v]) <= 1
=============================================================================
