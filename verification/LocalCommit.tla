---------------------------- MODULE LocalCommit ----------------------------
EXTENDS Naturals
CONSTANTS MaxOps, MaxGeneration, RequireSync
VARIABLES accepted, written, stable, durable, committed, applied, acknowledged,
          generation, syncGeneration, syncThrough, faulted
vars == <<accepted, written, stable, durable, committed, applied, acknowledged,
          generation, syncGeneration, syncThrough, faulted>>

Init == /\ accepted = 0 /\ written = 0 /\ stable = 0 /\ durable = 0
        /\ committed = 0 /\ applied = 0 /\ acknowledged = 0
        /\ generation = 1 /\ syncGeneration = 0 /\ syncThrough = 0
        /\ faulted = FALSE

Admit == /\ ~faulted /\ accepted < MaxOps
         /\ accepted' = accepted + 1
         /\ UNCHANGED <<written, stable, durable, committed, applied,
                         acknowledged, generation, syncGeneration, syncThrough, faulted>>

Write == /\ ~faulted /\ written < accepted
         /\ written' = written + 1
         /\ committed' = IF RequireSync THEN committed ELSE written + 1
         /\ UNCHANGED <<accepted, stable, durable, applied, acknowledged,
                         generation, syncGeneration, syncThrough, faulted>>

BeginSync == /\ ~faulted
             /\ syncGeneration' = generation /\ syncThrough' = written
             /\ UNCHANGED <<accepted, written, stable, durable, committed,
                             applied, acknowledged, generation, faulted>>

Persist == /\ syncGeneration = generation
           /\ stable' = IF stable < syncThrough THEN syncThrough ELSE stable
           /\ UNCHANGED <<accepted, written, durable, committed, applied,
                           acknowledged, generation, syncGeneration, syncThrough, faulted>>

SyncReply == /\ ~faulted /\ syncGeneration = generation /\ syncThrough <= stable
             /\ durable' = IF durable < syncThrough THEN syncThrough ELSE durable
             /\ committed' = IF committed < syncThrough THEN syncThrough ELSE committed
             /\ UNCHANGED <<accepted, written, stable, applied, acknowledged,
                             generation, syncGeneration, syncThrough, faulted>>

Apply == /\ ~faulted /\ applied < committed /\ applied' = applied + 1
         /\ UNCHANGED <<accepted, written, stable, durable, committed,
                         acknowledged, generation, syncGeneration, syncThrough, faulted>>

Reply == /\ ~faulted /\ acknowledged' = applied
         /\ UNCHANGED <<accepted, written, stable, durable, committed, applied,
                         generation, syncGeneration, syncThrough, faulted>>

Fail == /\ faulted' = TRUE
        /\ UNCHANGED <<accepted, written, stable, durable, committed, applied,
                        acknowledged, generation, syncGeneration, syncThrough>>

PowerCut == /\ generation < MaxGeneration /\ generation' = generation + 1
            /\ accepted' = stable /\ written' = stable /\ durable' = stable
            /\ committed' = stable /\ applied' = 0 /\ faulted' = FALSE
            /\ UNCHANGED <<stable, acknowledged, syncGeneration, syncThrough>>

Next == Admit \/ Write \/ BeginSync \/ Persist \/ SyncReply \/ Apply \/ Reply
        \/ Fail \/ PowerCut \/ UNCHANGED vars

TypeOK == /\ <<accepted, written, stable, durable, committed, applied,
               acknowledged, syncThrough>> \in [1..8 -> 0..MaxOps]
          /\ generation \in 1..MaxGeneration
          /\ syncGeneration \in 0..MaxGeneration /\ faulted \in BOOLEAN
PrefixOrder == /\ applied <= committed /\ committed <= durable
               /\ durable <= written /\ written <= accepted /\ stable <= written
SuccessSurvives == acknowledged <= stable
=============================================================================
