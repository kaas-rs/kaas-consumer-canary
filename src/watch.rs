//! The reader's model of the stream, and every check made against it.
//!
//! Nothing here knows that Kafka exists beyond the shape of a [`Record`]. It is
//! handed records and ticks, and it decides what they mean — which is what
//! makes the interesting half of this program testable without a broker.
//!
//! The counters are plain integers behind `&mut self` rather than shared
//! atomics. One task drives the whole of it, so the sharing was never real.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use chrono::Utc;
use kafka_consume::Record;
use kafka_read::TimestampType;
use tokio::time::Instant;

use crate::heartbeat::Heartbeat;

/// A sequence this far ahead of the last one is not a hole worth remembering.
///
/// Recording every missing sequence individually is what makes a gap legible as
/// a range, but a corrupted or wildly out-of-range `sequence` would otherwise
/// allocate an entry per integer between here and there. Past this, the gap is
/// reported at once and the range is not tracked.
const MAX_TRACKED_MISSING: u64 = 100_000;

/// The thresholds every judgement here is made against.
///
/// Built from the command line at the boundary rather than threaded through it,
/// so this module does not know that a command line exists. All three are
/// required: there is no defensible default for "how long is too quiet" that
/// does not come from the interval the producer is writing at.
#[derive(Debug, Clone, Copy)]
pub struct Thresholds {
    /// How long the current run may be quiet before that is a finding.
    pub stall: Duration,
    /// How long a missing sequence is held before it is called lost.
    pub reorder: Duration,
    /// Apparent latency above this opens a `slow` episode.
    pub lag_warn_ms: i64,
}

impl Thresholds {
    pub fn new(stall: Duration, reorder: Duration, lag_warn_ms: i64) -> Self {
        Self {
            stall,
            reorder,
            lag_warn_ms,
        }
    }
}

/// Everything counted over the life of the process.
#[derive(Debug, Default)]
pub struct Tally {
    pub heartbeats: u64,
    pub lost: u64,
    pub duplicates: u64,
    pub silences: u64,
    pub unparseable: u64,
    pub mislabelled: u64,
    pub restarts: u64,
    pub pauses: u64,
    pub reordered: u64,
    pub replayed: u64,
    pub rejoins: u64,
    pub rebalances: u64,
    pub commit_failures: u64,
    pub slow: u64,
    pub max_latency_ms: i64,
    pub min_latency_ms: Option<i64>,
    pub last_latency_ms: i64,
}

impl Tally {
    /// What `--strict` fails on: the counts that mean the stream was not what
    /// the producer wrote. Restarts, pauses, reordering, rejoins, rebalances,
    /// failed commits and slow heartbeats are timing, not corruption.
    pub fn faults(&self) -> u64 {
        self.lost + self.duplicates + self.silences + self.unparseable + self.mislabelled
    }
}

/// What the consumer knows about one run of the producer.
#[derive(Debug)]
struct Run {
    /// The sequence this run should produce next.
    expected: u64,
    /// Sequences that have not arrived, and when each was first missed.
    missing: BTreeMap<u64, Instant>,
    /// Sequences below this were already accounted for before a rejoin or a
    /// rebalance, so seeing one again is a replay rather than a duplicate. Zero
    /// once the reader has moved past the resume point.
    replay_below: u64,
    /// The body timestamp of the last heartbeat, for telling a pause from a gap.
    last_beat_ms: i64,
    /// When the last heartbeat arrived, for telling silence from a pause.
    last_seen: Instant,
    /// Whether a newer run has taken over. A retired run cannot be silent — it
    /// is over — but its holes are still worth reporting.
    retired: bool,
    /// Whether silence has already been reported since the last heartbeat, so
    /// one outage produces one line rather than one per tick.
    silent: bool,
    host: String,
}

/// The reader's model of the stream.
pub struct Watch {
    tally: Tally,
    runs: HashMap<String, Run>,
    /// The run the most recent heartbeat came from.
    current: Option<String>,
    thresholds: Thresholds,
    /// Whether the broker's timestamp rewriting has already been reported. It
    /// is a topic setting, not an event, so it is worth saying exactly once.
    log_append_reported: bool,
    /// Whether the reader has reached the live end of the log.
    ///
    /// A replay reads records that are hours old, and every one of them is late
    /// by the only definition available here. That is not a finding, it is a
    /// description of what was asked for — and at one line per record it is
    /// precisely the noise that hides a real one. Latency is measured from the
    /// first fresh record onwards, and disarmed again by a rejoin, whose
    /// backlog is a replay for the same reason.
    caught_up: bool,
    /// Whether latency is currently over the threshold. Reported on the way in
    /// and the way out, like silence, so an hour of lag is two lines.
    lagging: bool,
    /// What the replay looked like, for one line when it ends.
    backlog_records: u64,
    backlog_worst_ms: i64,
}

impl Watch {
    pub fn new(thresholds: Thresholds) -> Self {
        Self {
            tally: Tally::default(),
            runs: HashMap::new(),
            current: None,
            thresholds,
            log_append_reported: false,
            caught_up: false,
            lagging: false,
            backlog_records: 0,
            backlog_worst_ms: 0,
        }
    }

    /// Everything counted so far.
    pub fn tally(&self) -> &Tally {
        &self.tally
    }

    /// The thresholds it is judging against, for the line that records them.
    pub fn thresholds(&self) -> Thresholds {
        self.thresholds
    }

    /// A commit that did not land. Not a finding about the stream — it costs a
    /// replay after a restart and nothing else — so it is counted apart from
    /// the faults.
    pub fn note_commit_failure(&mut self) {
        self.tally.commit_failures += 1;
    }

    /// Note that the session died and the reader is about to rejoin.
    pub fn rejoining(&mut self, error: &anyhow::Error) {
        self.tally.rejoins += 1;
        self.open_replay_window();
        tracing::warn!(error = %format!("{error:#}"), "the read stopped; rejoining");
    }

    /// Note that the broker moved partitions between polls.
    ///
    /// Not a fault. It is the group protocol working — a member joined or left
    /// — and the only consequence here is that a gained partition resumes from
    /// its committed offset, which re-reads a little.
    pub fn rebalanced(&mut self, before: &[(String, i32)], after: &[(String, i32)]) {
        self.tally.rebalances += 1;
        self.open_replay_window();
        tracing::info!(
            was = before.len(),
            now = after.len(),
            "the assignment changed"
        );
    }

    /// Everything already counted becomes re-readable without being a duplicate.
    ///
    /// Shared by a rejoin and a rebalance because they resume the same way:
    /// from a committed offset that is at or behind what has been seen. The
    /// backlog that arrives is a replay, so latency is disarmed too.
    fn open_replay_window(&mut self) {
        for run in self.runs.values_mut() {
            run.replay_below = run.expected;
        }
        self.caught_up = false;
        self.lagging = false;
        self.backlog_records = 0;
        self.backlog_worst_ms = 0;
    }

    pub fn observe_record(&mut self, record: &Record) {
        let Some(value) = &record.value else {
            // A tombstone here is a record with a null value on a topic that
            // only ever carries heartbeats, so somebody else is writing.
            self.tally.unparseable += 1;
            tracing::error!(
                partition = record.partition,
                offset = record.offset,
                "a record on this topic has no value"
            );
            return;
        };

        let beat: Heartbeat = match serde_json::from_slice(value) {
            Ok(beat) => beat,
            Err(error) => {
                self.tally.unparseable += 1;
                tracing::error!(
                    partition = record.partition,
                    offset = record.offset,
                    %error,
                    preview = %String::from_utf8_lossy(&value[..value.len().min(120)]),
                    "a record on this topic is not a heartbeat"
                );
                return;
            }
        };

        self.tally.heartbeats += 1;
        self.check_labels(record, &beat);
        self.check_latency(&beat);
        self.check_sequence(record, &beat);
    }

    /// Everything the record says about itself, against everything its body
    /// says. The producer sets both from one reading, so a disagreement is
    /// something between the two ends rewriting one of them.
    fn check_labels(&mut self, record: &Record, beat: &Heartbeat) {
        let header = |name: &str| {
            record
                .headers
                .iter()
                .find(|(key, _)| key.as_str() == name)
                .and_then(|(_, value)| value.as_ref())
                .map(|value| String::from_utf8_lossy(value).into_owned())
        };

        if header("canary-run").as_deref() != Some(beat.run_id.as_str()) {
            self.mislabelled(record, "the canary-run header does not match run_id");
        }

        match chrono::DateTime::parse_from_rfc3339(&beat.timestamp) {
            Ok(parsed) if parsed.timestamp_millis() == beat.timestamp_ms => {}
            Ok(_) => self.mislabelled(record, "timestamp and timestamp_ms disagree"),
            Err(_) => self.mislabelled(record, "timestamp is not RFC 3339"),
        }

        match record.timestamp_type {
            TimestampType::LogAppend => {
                if !self.log_append_reported {
                    self.log_append_reported = true;
                    // Not a fault, and reported once rather than counted: the
                    // topic is configured for LogAppendTime, which makes the
                    // record timestamp the broker's clock. Every latency below
                    // is then measured against that instead of the producer's,
                    // which is worth knowing before trusting one.
                    tracing::info!("the broker is rewriting record timestamps (LogAppendTime)");
                }
            }
            TimestampType::Creation if record.timestamp != beat.timestamp_ms => {
                self.mislabelled(record, "the record timestamp is not the one in the body");
            }
            TimestampType::Creation => {}
        }
    }

    fn mislabelled(&mut self, record: &Record, what: &str) {
        self.tally.mislabelled += 1;
        tracing::warn!(
            partition = record.partition,
            offset = record.offset,
            "{what}"
        );
    }

    /// How old the heartbeat was when it arrived.
    ///
    /// Apparent, not measured: the two timestamps come from two hosts' clocks,
    /// so this is end-to-end latency plus whatever they disagree by. It is not
    /// clamped at zero, because a negative minimum is the only evidence of that
    /// disagreement there is — and a canary that hides it makes every latency
    /// it reports a guess rather than a measurement.
    fn check_latency(&mut self, beat: &Heartbeat) {
        let latency = Utc::now().timestamp_millis() - beat.timestamp_ms;
        self.tally.last_latency_ms = latency;

        if !self.caught_up {
            self.backlog_records += 1;
            self.backlog_worst_ms = self.backlog_worst_ms.max(latency);
            if latency > self.thresholds.lag_warn_ms {
                // Still replaying. Nothing measured here describes the cluster
                // now, so none of it is recorded or reported.
                return;
            }
            self.caught_up = true;
            if self.backlog_records > 1 {
                tracing::info!(
                    records = self.backlog_records,
                    behind_ms = self.backlog_worst_ms,
                    "caught up with the log"
                );
            }
            self.backlog_records = 0;
            self.backlog_worst_ms = 0;
        }

        self.tally.max_latency_ms = self.tally.max_latency_ms.max(latency);
        self.tally.min_latency_ms = Some(match self.tally.min_latency_ms {
            Some(min) => min.min(latency),
            None => latency,
        });

        // A state, not an event: an hour of lag is one line saying it started
        // and one saying it stopped, and `slow` counts episodes rather than
        // records. The alternative is a warning per heartbeat for an hour,
        // which is how a log stops being read.
        match (self.lagging, latency > self.thresholds.lag_warn_ms) {
            (false, true) => {
                self.lagging = true;
                self.tally.slow += 1;
                tracing::warn!(
                    latency_ms = latency,
                    sequence = beat.sequence,
                    "heartbeats are arriving late"
                );
            }
            (true, false) => {
                self.lagging = false;
                tracing::info!(
                    latency_ms = latency,
                    sequence = beat.sequence,
                    "heartbeats are on time again"
                );
            }
            _ => {}
        }
    }

    /// The check the whole topology exists for.
    fn check_sequence(&mut self, record: &Record, beat: &Heartbeat) {
        let now = Instant::now();

        if !self.runs.contains_key(&beat.run_id) {
            if let Some(previous) = self.current.take() {
                // The old run is over, not stalled. Its holes stay until they
                // age out, because a record written before the restart can
                // still be in flight behind one written after it.
                if let Some(run) = self.runs.get_mut(&previous) {
                    run.retired = true;
                }
                self.tally.restarts += 1;
                tracing::info!(
                    from = %previous,
                    to = %beat.run_id,
                    host = %beat.host,
                    "the producer restarted"
                );
            }
            self.runs.insert(
                beat.run_id.clone(),
                Run {
                    // Seeded from the first sequence seen, not from 1. A reader
                    // that joins a group with no committed offset starts at the
                    // earliest *retained* record, and retention can drop the
                    // beginning of a run; that is not a gap.
                    expected: beat.sequence,
                    missing: BTreeMap::new(),
                    replay_below: 0,
                    last_beat_ms: 0,
                    last_seen: now,
                    retired: false,
                    silent: false,
                    host: beat.host.clone(),
                },
            );
        }
        self.current = Some(beat.run_id.clone());

        let Some(run) = self.runs.get_mut(&beat.run_id) else {
            return;
        };
        run.last_seen = now;
        run.silent = false;

        let contiguous = beat.sequence == run.expected;
        match beat.sequence.cmp(&run.expected) {
            Ordering::Less => {
                if run.missing.remove(&beat.sequence).is_some() {
                    // It was held as missing and it turned up: the partitions
                    // arrived out of order, which is what the window is for.
                    self.tally.reordered += 1;
                    tracing::debug!(sequence = beat.sequence, "arrived out of order");
                } else if beat.sequence < run.replay_below {
                    self.tally.replayed += 1;
                } else {
                    self.tally.duplicates += 1;
                    tracing::error!(
                        run_id = %beat.run_id,
                        sequence = beat.sequence,
                        partition = record.partition,
                        offset = record.offset,
                        "a sequence arrived twice"
                    );
                }
                return;
            }
            Ordering::Equal => run.expected = beat.sequence.saturating_add(1),
            Ordering::Greater => {
                let gap = beat.sequence - run.expected;
                if gap > MAX_TRACKED_MISSING {
                    // Too wide to hold as individual holes, and too wide to be
                    // reordering. Report it now and resynchronise.
                    self.tally.lost += gap;
                    tracing::error!(
                        run_id = %beat.run_id,
                        first = run.expected,
                        last = beat.sequence - 1,
                        count = gap,
                        "a gap too large to be reordering"
                    );
                } else {
                    for sequence in run.expected..beat.sequence {
                        run.missing.entry(sequence).or_insert(now);
                    }
                }
                run.expected = beat.sequence.saturating_add(1);
            }
        }

        // Moving forward past the resume point ends the replay window: from
        // here, a sequence below `expected` is a genuine duplicate again.
        if beat.sequence >= run.replay_below {
            run.replay_below = 0;
        }

        // Consecutive sequences with a long silence between them: the writer
        // paused or was blocked, and nothing was lost. This is the row of the
        // producer's table that a timestamp-only view cannot tell from loss.
        let stall_ms = i64::try_from(self.thresholds.stall.as_millis()).unwrap_or(i64::MAX);
        if contiguous && run.last_beat_ms > 0 {
            let elapsed = beat.timestamp_ms.saturating_sub(run.last_beat_ms);
            if elapsed > stall_ms {
                self.tally.pauses += 1;
                tracing::warn!(
                    run_id = %beat.run_id,
                    sequence = beat.sequence,
                    paused_ms = elapsed,
                    "the producer paused; nothing was lost"
                );
            }
        }
        run.last_beat_ms = beat.timestamp_ms;

        tracing::debug!(
            run_id = %beat.run_id,
            sequence = beat.sequence,
            partition = record.partition,
            offset = record.offset,
            latency_ms = self.tally.last_latency_ms,
            "heartbeat"
        );
    }

    /// The clock-driven half: what has *not* happened.
    ///
    /// A missing sequence past its reorder window is loss, and contiguous
    /// missing sequences are reported as one range — an outage loses a block,
    /// and a line per record turns one finding into a thousand.
    pub fn sweep(&mut self, now: Instant) {
        let reorder = self.thresholds.reorder;
        let stall = self.thresholds.stall;
        let mut lost = 0u64;
        let mut silences = 0u64;

        for (run_id, run) in &mut self.runs {
            let expired: Vec<u64> = run
                .missing
                .iter()
                .filter(|(_, missed_at)| now.duration_since(**missed_at) >= reorder)
                .map(|(sequence, _)| *sequence)
                .collect();
            for sequence in &expired {
                run.missing.remove(sequence);
            }

            for (first, last) in ranges(&expired) {
                let count = last - first + 1;
                lost += count;
                tracing::error!(
                    %run_id,
                    host = %run.host,
                    first,
                    last,
                    count,
                    "records were lost"
                );
            }

            // Counted once per outage, at the moment it is declared, and
            // re-armed by the next heartbeat to arrive. A retired run is
            // exempt: it is over, not quiet, and reporting one on every
            // producer restart would make the loudest finding the routine one.
            if !run.retired && !run.silent && now.duration_since(run.last_seen) >= stall {
                run.silent = true;
                silences += 1;
                // Not counted as loss: nothing has been proven missing. The
                // producer may be down, the topic may have stopped accepting
                // writes, or this reader may be the thing that is stuck — and
                // saying which would be a guess.
                tracing::error!(
                    %run_id,
                    host = %run.host,
                    quiet_ms = now.duration_since(run.last_seen).as_millis(),
                    "no heartbeat"
                );
            }
        }

        self.tally.lost += lost;
        self.tally.silences += silences;

        // A retired run with nothing outstanding is finished being interesting.
        // Dropping it is what keeps a process that runs for weeks from growing
        // a map entry per producer restart.
        self.runs
            .retain(|_, run| !run.retired || !run.missing.is_empty());
    }

    pub fn summarise(&self, message: &str) {
        tracing::info!(
            heartbeats = self.tally.heartbeats,
            lost = self.tally.lost,
            duplicates = self.tally.duplicates,
            silences = self.tally.silences,
            unparseable = self.tally.unparseable,
            mislabelled = self.tally.mislabelled,
            restarts = self.tally.restarts,
            pauses = self.tally.pauses,
            reordered = self.tally.reordered,
            replayed = self.tally.replayed,
            rejoins = self.tally.rejoins,
            rebalances = self.tally.rebalances,
            commit_failures = self.tally.commit_failures,
            slow = self.tally.slow,
            latency_ms = self.tally.last_latency_ms,
            max_latency_ms = self.tally.max_latency_ms,
            min_latency_ms = ?self.tally.min_latency_ms,
            runs = self.runs.len(),
            "{message}"
        );
    }
}

/// Collapse a sorted, deduplicated slice into contiguous `(first, last)` runs.
fn ranges(sequences: &[u64]) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    for &sequence in sequences {
        match out.last_mut() {
            Some((_, last)) if *last + 1 == sequence => *last = sequence,
            _ => out.push((sequence, sequence)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn watch() -> Watch {
        Watch::new(Thresholds::new(
            Duration::from_millis(5_000),
            Duration::from_millis(5_000),
            5_000,
        ))
    }

    fn with(stall: Duration, reorder: Duration) -> Watch {
        Watch::new(Thresholds::new(stall, reorder, 5_000))
    }

    fn record(offset: i64, beat: &str) -> Record {
        Record {
            topic: "kaas-canary".to_owned(),
            partition: 0,
            offset,
            timestamp: 1_785_686_674_238,
            timestamp_type: TimestampType::Creation,
            key: None,
            value: Some(bytes::Bytes::from(beat.to_owned())),
            headers: Vec::new(),
            producer_id: None,
            transactional: false,
            leader_epoch: None,
        }
    }

    fn beat(run_id: &str, sequence: u64) -> Heartbeat {
        Heartbeat {
            run_id: run_id.to_owned(),
            sequence,
            timestamp_ms: 1_785_686_674_238,
            timestamp: "2026-08-02T16:04:34.238Z".to_owned(),
            host: "canary-0".to_owned(),
        }
    }

    #[test]
    fn a_clean_run_finds_nothing() {
        let mut watch = watch();
        for sequence in 1..=10 {
            watch.check_sequence(&record(sequence as i64, ""), &beat("run-a", sequence));
        }
        watch.sweep(Instant::now());
        assert_eq!(watch.tally.faults(), 0);
    }

    /// The reader joins a run in progress — after retention dropped the
    /// beginning, or from a committed offset. Sequence 500 first is not 499
    /// lost.
    #[test]
    fn joining_mid_run_is_not_a_gap() {
        let mut watch = watch();
        for sequence in 500..=505 {
            watch.check_sequence(&record(sequence as i64, ""), &beat("run-a", sequence));
        }
        watch.sweep(Instant::now());
        assert_eq!(watch.tally.lost, 0);
    }

    #[test]
    fn a_hole_that_does_not_fill_is_loss() {
        let mut watch = with(Duration::from_secs(5), Duration::ZERO);
        watch.check_sequence(&record(1, ""), &beat("run-a", 1));
        watch.check_sequence(&record(2, ""), &beat("run-a", 5));
        // A zero-length reorder window expires everything on the first sweep.
        watch.sweep(Instant::now());
        assert_eq!(watch.tally.lost, 3, "sequences 2, 3 and 4");
    }

    /// The reason a hole is held rather than reported: a poll returns records
    /// per partition, so a later sequence can arrive first.
    #[test]
    fn a_hole_that_fills_in_time_is_only_reordering() {
        let mut watch = watch();
        watch.check_sequence(&record(1, ""), &beat("run-a", 1));
        watch.check_sequence(&record(2, ""), &beat("run-a", 3));
        watch.check_sequence(&record(3, ""), &beat("run-a", 2));
        watch.sweep(Instant::now());
        assert_eq!(watch.tally.lost, 0);
        assert_eq!(watch.tally.reordered, 1);
        assert_eq!(watch.tally.duplicates, 0);
    }

    #[test]
    fn a_repeated_sequence_is_a_duplicate() {
        let mut watch = watch();
        watch.check_sequence(&record(1, ""), &beat("run-a", 1));
        watch.check_sequence(&record(2, ""), &beat("run-a", 2));
        watch.check_sequence(&record(3, ""), &beat("run-a", 2));
        assert_eq!(watch.tally.duplicates, 1);
    }

    /// After a rejoin the member resumes from its committed offset, so the
    /// records after it arrive again. Counting those as duplicates would make
    /// every broker restart look like a broken producer.
    #[test]
    fn a_replay_after_a_rejoin_is_not_a_duplicate() {
        let mut watch = watch();
        for sequence in 1..=5 {
            watch.check_sequence(&record(sequence as i64, ""), &beat("run-a", sequence));
        }
        watch.rejoining(&anyhow::anyhow!("connection reset"));
        watch.check_sequence(&record(4, ""), &beat("run-a", 4));
        watch.check_sequence(&record(5, ""), &beat("run-a", 5));
        watch.check_sequence(&record(6, ""), &beat("run-a", 6));
        assert_eq!(watch.tally.duplicates, 0);
        assert_eq!(watch.tally.replayed, 2);
        // ...and the window closes behind it, so a real duplicate still counts.
        watch.check_sequence(&record(7, ""), &beat("run-a", 6));
        assert_eq!(watch.tally.duplicates, 1);
    }

    /// A rebalance resumes gained partitions from their committed offsets, so
    /// it opens the same replay window a rejoin does — and is not itself a
    /// fault.
    #[test]
    fn a_replay_after_a_rebalance_is_not_a_duplicate() {
        let mut watch = watch();
        for sequence in 1..=5 {
            watch.check_sequence(&record(sequence as i64, ""), &beat("run-a", sequence));
        }
        watch.rebalanced(
            &[("kaas-canary".to_owned(), 0)],
            &[("kaas-canary".to_owned(), 0), ("kaas-canary".to_owned(), 1)],
        );
        watch.check_sequence(&record(4, ""), &beat("run-a", 4));
        watch.check_sequence(&record(5, ""), &beat("run-a", 5));
        assert_eq!(watch.tally.duplicates, 0);
        assert_eq!(watch.tally.replayed, 2);
        assert_eq!(watch.tally.rebalances, 1);
        assert_eq!(watch.tally.faults(), 0, "a rebalance is not a finding");
    }

    /// A restart is a new run_id, and the old run's sequence has nothing to do
    /// with the new one's. Without run_id these two streams would interleave
    /// into an unreadable mess of gaps and duplicates.
    #[test]
    fn a_restart_does_not_look_like_loss() {
        let mut watch = with(Duration::from_secs(5), Duration::ZERO);
        for sequence in 1..=9 {
            watch.check_sequence(&record(sequence as i64, ""), &beat("run-a", sequence));
        }
        for sequence in 1..=3 {
            watch.check_sequence(&record(9 + sequence as i64, ""), &beat("run-b", sequence));
        }
        watch.sweep(Instant::now());
        assert_eq!(watch.tally.restarts, 1);
        assert_eq!(watch.tally.lost, 0);
        assert_eq!(watch.tally.duplicates, 0);
    }

    /// Consecutive sequences far apart in time: the writer stopped, and the
    /// producer's own documentation says that is not loss.
    #[test]
    fn a_time_gap_without_a_sequence_gap_is_a_pause() {
        let mut watch = watch();
        watch.check_sequence(&record(1, ""), &beat("run-a", 1));
        let mut later = beat("run-a", 2);
        later.timestamp_ms += 60_000;
        watch.check_sequence(&record(2, ""), &later);
        assert_eq!(watch.tally.pauses, 1);
        assert_eq!(watch.tally.faults(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn silence_is_reported_once_per_outage_and_re_arms() {
        let mut watch = with(Duration::from_secs(5), Duration::from_secs(60));
        watch.check_sequence(&record(1, ""), &beat("run-a", 1));

        watch.sweep(Instant::now());
        assert_eq!(watch.tally.silences, 0, "a fresh heartbeat is not silence");

        tokio::time::advance(Duration::from_secs(6)).await;
        watch.sweep(Instant::now());
        watch.sweep(Instant::now());
        assert_eq!(watch.tally.silences, 1, "one outage, one finding");

        // The producer comes back, then stops again: a second outage.
        watch.check_sequence(&record(2, ""), &beat("run-a", 2));
        tokio::time::advance(Duration::from_secs(6)).await;
        watch.sweep(Instant::now());
        assert_eq!(watch.tally.silences, 2);
    }

    /// A restart replaces a run rather than silencing it. Reporting the old one
    /// as silent would make every routine restart the loudest line in the log.
    #[tokio::test(start_paused = true)]
    async fn a_replaced_run_is_over_rather_than_silent() {
        let mut watch = with(Duration::from_secs(5), Duration::from_secs(60));
        // A hole keeps run-a in the map after it is replaced, so the sweep has
        // something to be wrong about.
        watch.check_sequence(&record(1, ""), &beat("run-a", 1));
        watch.check_sequence(&record(2, ""), &beat("run-a", 3));
        watch.check_sequence(&record(3, ""), &beat("run-b", 1));

        tokio::time::advance(Duration::from_secs(6)).await;
        watch.check_sequence(&record(4, ""), &beat("run-b", 2));
        watch.sweep(Instant::now());

        assert_eq!(watch.tally.restarts, 1);
        assert_eq!(watch.tally.silences, 0);
        assert_eq!(watch.tally.lost, 0, "the hole has not aged out yet");
        assert_eq!(watch.runs.len(), 2, "run-a is kept while it owes an answer");
    }

    /// A process that runs for weeks must not grow a map entry per restart.
    #[test]
    fn finished_runs_are_forgotten() {
        let mut watch = with(Duration::from_secs(5), Duration::ZERO);
        for run in 0..50 {
            watch.check_sequence(&record(run, ""), &beat(&format!("run-{run}"), 1));
            watch.sweep(Instant::now());
        }
        assert_eq!(watch.runs.len(), 1, "only the live run is kept");
    }

    #[test]
    fn a_record_that_is_not_a_heartbeat_is_a_finding() {
        let mut watch = watch();
        watch.observe_record(&record(1, "not json"));
        assert_eq!(watch.tally.unparseable, 1);
        assert_eq!(watch.tally.heartbeats, 0);
    }

    /// The producer sets the record timestamp and the body timestamp from one
    /// reading. A disagreement means something in between rewrote one.
    #[test]
    fn a_record_timestamp_that_disagrees_with_the_body_is_a_finding() {
        let mut watch = watch();
        let mut rec = record(1, "");
        rec.timestamp = 1_785_686_674_238 + 9_000;
        rec.headers = vec![(
            "canary-run".to_owned(),
            Some(bytes::Bytes::from_static(b"run-a")),
        )];
        watch.check_labels(&rec, &beat("run-a", 1));
        assert_eq!(watch.tally.mislabelled, 1);
    }

    /// LogAppendTime is a topic setting, not corruption — but it replaces the
    /// producer's clock with the broker's, which every latency here depends on.
    #[test]
    fn broker_written_timestamps_are_reported_but_not_a_fault() {
        let mut watch = watch();
        let mut rec = record(1, "");
        rec.timestamp = 1_785_686_674_238 + 9_000;
        rec.timestamp_type = TimestampType::LogAppend;
        rec.headers = vec![(
            "canary-run".to_owned(),
            Some(bytes::Bytes::from_static(b"run-a")),
        )];
        watch.check_labels(&rec, &beat("run-a", 1));
        assert_eq!(watch.tally.mislabelled, 0);
    }

    /// Joining a group with no committed offset starts at the earliest retained
    /// record, so the first run reads a day of old timestamps. Reporting each
    /// one as late is a warning per record for the whole replay, which is how a
    /// canary teaches people to ignore its log.
    #[test]
    fn a_replay_is_not_reported_as_late() {
        let mut watch = watch();
        let now = Utc::now().timestamp_millis();

        for sequence in 1..=100 {
            let mut old = beat("run-a", sequence);
            old.timestamp_ms = now - 4_000_000;
            watch.check_latency(&old);
        }
        assert_eq!(watch.tally.slow, 0, "a backlog is not a finding");
        assert_eq!(watch.tally.max_latency_ms, 0, "and it is not a measurement");

        // Reaching the live end arms the check.
        let mut fresh = beat("run-a", 101);
        fresh.timestamp_ms = now;
        watch.check_latency(&fresh);
        assert!(watch.caught_up);
        assert_eq!(watch.tally.slow, 0);

        // From here, falling behind is real.
        let mut late = beat("run-a", 102);
        late.timestamp_ms = now - 30_000;
        watch.check_latency(&late);
        assert_eq!(watch.tally.slow, 1);
    }

    /// Lag is a state. An hour of it is one line in and one line out, not one
    /// line per heartbeat.
    #[test]
    fn sustained_lag_is_one_episode() {
        let mut watch = watch();
        let now = Utc::now().timestamp_millis();

        let mut fresh = beat("run-a", 1);
        fresh.timestamp_ms = now;
        watch.check_latency(&fresh);

        for sequence in 2..=50 {
            let mut late = beat("run-a", sequence);
            late.timestamp_ms = now - 30_000;
            watch.check_latency(&late);
        }
        assert_eq!(watch.tally.slow, 1, "one episode, not forty-nine records");

        // Recovering and falling behind again is a second episode.
        let mut recovered = beat("run-a", 51);
        recovered.timestamp_ms = now;
        watch.check_latency(&recovered);
        let mut late_again = beat("run-a", 52);
        late_again.timestamp_ms = now - 30_000;
        watch.check_latency(&late_again);
        assert_eq!(watch.tally.slow, 2);
    }

    /// Two clocks, so the difference can come out negative. Clamping it to zero
    /// would hide the one piece of evidence that the latencies above are
    /// measured against a clock that disagrees.
    #[test]
    fn clock_skew_survives_as_a_negative_minimum() {
        let mut watch = watch();
        let mut ahead = beat("run-a", 1);
        ahead.timestamp_ms = Utc::now().timestamp_millis() + 2_000;
        watch.check_latency(&ahead);
        assert_eq!(watch.tally.min_latency_ms.map(|ms| ms < 0), Some(true));
    }

    #[test]
    fn lost_sequences_are_reported_as_ranges() {
        assert_eq!(ranges(&[]), vec![]);
        assert_eq!(ranges(&[4]), vec![(4, 4)]);
        assert_eq!(ranges(&[4, 5, 6]), vec![(4, 6)]);
        assert_eq!(ranges(&[4, 5, 9, 11, 12]), vec![(4, 5), (9, 9), (11, 12)]);
    }
}
