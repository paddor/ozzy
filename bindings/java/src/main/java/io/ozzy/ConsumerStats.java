package io.ozzy;

/**
 * Cumulative delivered-record counters, preserved after close. Counters use unsigned long bits.
 *
 * @param liveRecords records delivered through PUB
 * @param replayedRecords records delivered through history or PEER gap repair
 * @param liveGaps transitions into repair, not a count of missing records
 */
public record ConsumerStats(long liveRecords, long replayedRecords, long liveGaps) {}
