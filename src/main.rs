//! A heartbeat consumer: read what `kaas-producer-canary` wrote, and check it.
//!
//! # Why there is a second canary
//!
//! The producer can only *claim*. It writes a `sequence` that increases by one
//! per record and logs an offset for each acknowledged send, and that is the
//! whole of what it knows: an acknowledgement is a statement about the leader's
//! log at one instant, not about what a reader will find there later. Every
//! interesting failure — a record acknowledged and then lost to an unclean
//! leader election, a duplicate, a topic whose high watermark stops advancing —
//! is invisible from the writing end.
//!
//! This reads the stream back and checks it, which is possible here and almost
//! nowhere else: the correct contents are known in advance. Every heartbeat's
//! `sequence` is exactly one more than the last, so anything else is a finding
//! rather than an interpretation.
//!
//! # It reports five things, and they are not the same thing
//!
//! * **Loss** — a `sequence` that never arrives. The finding the topology
//!   exists to catch.
//! * **Duplication** — a `sequence` that arrives twice. The producer runs
//!   idempotently as of kaas-lib 0.3.0, so it should be impossible for it to
//!   cause one; seeing one means the broker or the read path did.
//! * **A pause** — consecutive sequences whose body timestamps are far apart.
//!   The producer stopped and nothing was lost, which is the row of the
//!   producer's table that must not be confused with the first one.
//! * **A restart** — a new `run_id`. Also not loss, and the reason `run_id`
//!   exists.
//! * **Silence** — the current run stops arriving. Distinct from loss because
//!   nothing has yet been proven missing; the writer may be down, the read path
//!   may be stalled, and the canary says so rather than guessing.
//!
//! Latency is measured too, and reported, but it is not a finding in the same
//! sense: it is apparent rather than measured, because it spans two hosts'
//! clocks. See `check_latency`.
//!
//! ## What consuming cannot see, and reading could
//!
//! There used to be a sixth: **undecodable batches**. `kafka-read`'s scan
//! surfaces `ScanEvent::Malformed`, so a batch the tolerant decoder rejected
//! was a countable finding on a topic where every batch came from one known
//! writer. [`kafka_consume::GroupConsumer::poll`] returns `Vec<Record>` and
//! handles `RecordOutcome::Malformed` internally by stepping the position past
//! it — deliberately, so one corrupt record cannot stall a partition, but the
//! caller is never told. The check is gone rather than quietly always-zero: a
//! counter that cannot rise is worse than an absent one, because a dashboard
//! reads it as evidence.
//!
//! # Group membership, not a scan
//!
//! The read path here is a KIP-848 group member. That is the substantive change
//! from the scan this used to be, and it moves three things out of this process
//! and into the broker:
//!
//! * **Where to resume.** Offsets are committed to the group, so a restart
//!   continues where the last commit left off instead of re-deriving a position
//!   from the last record's timestamp.
//! * **Which partitions.** The broker assigns them. There is no `--partition`
//!   any more, because a group member does not get to choose.
//! * **Where to start.** A member with no committed offset starts at the
//!   *earliest* retained record — `kafka-consume` policy, not a setting — so
//!   the first run under a new `--group` replays the topic. There is no
//!   `--from-earliest` because there is no alternative to opt out of.
//!
//! The replay matters less than it sounds: `caught_up` below suppresses latency
//! findings until the reader reaches the live end, and sequence checks over
//! history are still true statements about that history. It is noise on first
//! start and nothing thereafter.
//!
//! # Quiet when healthy
//!
//! Per-record logging is at `debug`. A monitor that prints 86,400 healthy lines
//! a day buries the one line that matters, and "it is still logging" is not the
//! same evidence as "it is still correct". `info` carries a periodic summary,
//! and `warn`/`error` carry findings only.
//!
//! # Out-of-order arrival is not loss
//!
//! `sequence` is global to a run, but the records are spread over partitions,
//! and a poll returns them per partition rather than in sequence order. A
//! record can therefore arrive slightly before one that precedes it. A missing
//! sequence is held for a reorder window before it is called lost — see
//! [`Watch::sweep`]. With the whole stream in one partition, which is what
//! KIP-480 sticky partitioning gives an unkeyed producer, the window never
//! engages.
//!
//! # Failure policy
//!
//! A finding is logged and counted; the loop continues. A failed poll is
//! re-established and the member rejoins, because a reader that exits when the
//! broker restarts reports nothing about the restart — which is the interval
//! most worth reporting on.

#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
// Tests may unwrap freely: a panic there is a failed assertion, which is the
// mechanism, not a crash in a process that is meant to run for weeks.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use kafka_consume::{Cluster, ClusterConfig, ConsumerConfig, GroupConsumer, Record};
// `kafka-consume` re-exports `Record` but not `TimestampType`, which is the
// type of one of `Record`'s own fields — so matching on it needs the crate the
// record came from. Not a duplicate read path: `kafka-consume` depends on
// `kafka-read` for exactly this decoder, and both resolve to one version.
use kafka_read::TimestampType;
use serde::Deserialize;
use tokio::time::Instant;

/// One heartbeat, as `kaas-producer-canary` writes it.
///
/// Unknown fields are ignored rather than rejected. The producer's payload is a
/// contract and it has a test that says so, but the compatible half of that
/// contract is that a field may be *added*; a consumer that refuses to parse an
/// unfamiliar field turns a backward-compatible producer change into a
/// monitoring outage, which is a worse failure than not reading the new field.
#[derive(Debug, Deserialize)]
struct Heartbeat {
    /// Which run of the producer wrote this. Changes on every restart.
    run_id: String,
    /// Increases by exactly one per record within a run. The diagnostic field.
    sequence: u64,
    /// Milliseconds since the epoch, set by the producer.
    timestamp_ms: i64,
    /// RFC 3339 rendering of the same instant.
    timestamp: String,
    /// Where the producer was running.
    host: String,
}

#[derive(Debug, Parser)]
#[command(
    name = "kaas-consumer-canary",
    about = "Read kaas-producer-canary's heartbeats and report what is wrong with them."
)]
struct Args {
    /// Comma-separated bootstrap servers.
    #[arg(long, env = "KAAS_CANARY_BOOTSTRAP")]
    bootstrap: String,

    /// The topic to read.
    #[arg(long, env = "KAAS_CANARY_TOPIC", default_value = "kaas-canary")]
    topic: String,

    /// The consumer group to join.
    ///
    /// Committed offsets live under this name, so it decides what a restart
    /// resumes from. A group nobody has used before starts at the earliest
    /// retained record and replays the topic once.
    #[arg(
        long,
        env = "KAAS_CANARY_GROUP",
        default_value = "kaas-consumer-canary"
    )]
    group: String,

    /// Join as a static member under this instance id.
    ///
    /// Without one, every restart is a rebalance. With one, a restart inside
    /// the session timeout keeps the same assignment — which for a canary is
    /// the difference between a deploy showing up as a gap in coverage and not
    /// showing up at all.
    ///
    /// A literal, not the pod name: a Deployment regenerates pod names per
    /// ReplicaSet, so an id derived from one never survives the restart it
    /// exists to cover. One replica per cluster makes a fixed string both
    /// stable and unique.
    #[arg(long, env = "KAAS_CANARY_INSTANCE_ID")]
    instance_id: Option<String>,

    /// The interval the producer is writing at.
    ///
    /// Not used to time anything — this process is driven by what arrives —
    /// but every threshold below is a multiple of it, so a canary and its
    /// reader configured from the same environment agree by construction.
    #[arg(long, env = "KAAS_CANARY_INTERVAL_MS", default_value_t = 1_000)]
    interval_ms: u64,

    /// Missed heartbeats before silence is reported.
    #[arg(long, default_value_t = 5)]
    stall_intervals: u32,

    /// How long a missing sequence may stay missing before it counts as lost.
    ///
    /// Defaults to five intervals. This is a lower bound on how quickly loss is
    /// reported, so it trades detection latency against false positives from
    /// cross-partition reordering.
    #[arg(long)]
    reorder_ms: Option<u64>,

    /// Apparent end-to-end latency above this is reported.
    #[arg(long, default_value_t = 5_000)]
    lag_warn_ms: i64,

    /// How often to log a summary while nothing is wrong. Zero to never.
    #[arg(long, default_value_t = 60)]
    summary_secs: u64,

    /// How often to commit the group's offsets. Zero to never.
    ///
    /// A commit is what a restart resumes from, so this is the width of the
    /// window a crash re-reads. Re-reading is harmless — [`Run::replay_below`]
    /// absorbs it — so the setting trades duplicate work against nothing much.
    #[arg(long, default_value_t = 10)]
    commit_secs: u64,

    /// Stop after this many heartbeats. Runs forever when unset.
    #[arg(long)]
    count: Option<u64>,

    /// Exit non-zero if anything was found.
    ///
    /// Off by default, because the deployed canary's job is to keep reporting,
    /// and a process that exits stops being a monitor. With `--count` it turns
    /// this into an acceptance check. Restarts, pauses, reordering, rebalances
    /// and slow heartbeats do not count: they are observations about timing,
    /// and the producer's own documentation is explicit that a pause is not
    /// loss. Loss, duplication, silence, unparseable payloads and mislabelled
    /// records do.
    #[arg(long)]
    strict: bool,
}

/// A sequence this far ahead of the last one is not a hole worth remembering.
///
/// Recording every missing sequence individually is what makes a gap legible as
/// a range, but a corrupted or wildly out-of-range `sequence` would otherwise
/// allocate an entry per integer between here and there. Past this, the gap is
/// reported at once and the range is not tracked.
const MAX_TRACKED_MISSING: u64 = 100_000;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let bootstrap: Vec<String> = args
        .bootstrap
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    if bootstrap.is_empty() {
        anyhow::bail!("--bootstrap resolved to no addresses");
    }

    let interval = Duration::from_millis(args.interval_ms.max(1));
    let mut watch = Watch::new(
        interval * args.stall_intervals.max(1),
        args.reorder_ms
            .map(Duration::from_millis)
            .unwrap_or(interval * 5),
        args.lag_warn_ms,
    );

    tracing::info!(
        topic = %args.topic,
        group = %args.group,
        instance_id = args.instance_id.as_deref().unwrap_or("<dynamic>"),
        interval_ms = args.interval_ms,
        stall_ms = watch.stall.as_millis(),
        reorder_ms = watch.reorder.as_millis(),
        "canary starting"
    );

    // Scoped so the borrow of `watch` ends with the future, leaving the summary
    // below reachable whether the watch returned or ctrl-c cancelled it.
    {
        let watching = watch_topic(&bootstrap, &args, &mut watch);
        tokio::pin!(watching);
        tokio::select! {
            result = &mut watching => result?,
            signal = tokio::signal::ctrl_c() => {
                signal.context("waiting for ctrl-c")?;
                tracing::info!("interrupted");
            }
        }
    }

    watch.summarise("canary stopped");

    if args.strict && watch.tally.faults() > 0 {
        anyhow::bail!("{} fault(s) found; see the log", watch.tally.faults());
    }
    Ok(())
}

/// Read the topic, rejoining the group for as long as the process lives.
///
/// The cluster handle is dropped along with the member rather than reused. It
/// costs one metadata round trip per rejoin, against a one-second floor on the
/// backoff, and it means there is no cached routing state that can outlive the
/// failure that ended the session — a canary wedged onto a stale coordinator is
/// a canary reporting silence about a healthy cluster.
async fn watch_topic(bootstrap: &[String], args: &Args, watch: &mut Watch) -> Result<()> {
    const FIRST_BACKOFF: Duration = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);

    let mut backoff = FIRST_BACKOFF;
    loop {
        let outcome = match Cluster::connect(bootstrap.to_vec(), ClusterConfig::default()).await {
            Ok(cluster) => {
                backoff = FIRST_BACKOFF;
                read_session(cluster, args, watch).await
            }
            Err(error) => Err(anyhow::Error::new(error).context("connecting to the cluster")),
        };

        match outcome {
            Ok(()) => return Ok(()),
            Err(error) => {
                watch.rejoining(&error);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

/// One group membership, from the join to whatever ends it.
///
/// Returns `Ok` only when the run is genuinely over — the `--count` limit — so
/// every other exit is an error and gets retried by the caller.
async fn read_session(cluster: Cluster, args: &Args, watch: &mut Watch) -> Result<()> {
    let mut consumer = GroupConsumer::subscribe(
        cluster,
        ConsumerConfig::new(),
        args.group.clone(),
        [args.topic.clone()],
    )
    .await
    .context("joining the group")?;

    if let Some(instance_id) = &args.instance_id {
        consumer = consumer.instance_id(instance_id.clone());
    }

    // Everything that is not driven by an arriving record — silence, and a
    // missing sequence ageing out of its reorder window — needs a clock of its
    // own, because the observation is precisely that nothing arrived. Ticking
    // at the heartbeat interval keeps the resolution proportionate; the bounds
    // stop a sub-millisecond interval from spinning and an hourly one from
    // making the reports arrive an hour late.
    let mut watchdog = tokio::time::interval(
        Duration::from_millis(args.interval_ms.max(1))
            .clamp(Duration::from_millis(100), Duration::from_secs(5)),
    );
    watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut summary_at = Instant::now();
    let mut commit_at = Instant::now();
    let mut assignment = consumer.assignment();

    loop {
        tokio::select! {
            // Cancel-safe by the workspace's rule 5: a poll interrupted by a
            // watchdog tick leaves any pending rebalance in place rather than
            // acknowledging an assignment nobody was told about.
            polled = consumer.poll() => {
                let records = polled.map_err(anyhow::Error::new).context("polling")?;

                // The broker moved partitions under us. Whatever the new ones
                // hold has been read from their committed offset, so the
                // sequences either side of it arrive again and are replays
                // rather than duplicates — the same reasoning as a rejoin.
                let current = consumer.assignment();
                if current != assignment {
                    watch.rebalanced(&assignment, &current);
                    assignment = current;
                }

                for record in &records {
                    watch.observe_record(record);
                    if let Some(limit) = args.count
                        && watch.tally.heartbeats >= limit
                    {
                        // Commit before leaving, so an acceptance run does not
                        // make the next one replay what it just checked.
                        commit(&consumer, watch).await;
                        let _ = consumer.leave().await;
                        return Ok(());
                    }
                }
            }
            now = watchdog.tick() => {
                watch.sweep(now);

                if args.commit_secs > 0
                    && now.duration_since(commit_at) >= Duration::from_secs(args.commit_secs)
                {
                    commit_at = now;
                    commit(&consumer, watch).await;
                }

                if args.summary_secs > 0
                    && now.duration_since(summary_at) >= Duration::from_secs(args.summary_secs)
                {
                    summary_at = now;
                    watch.summarise("still watching");
                }
            }
        }
    }
}

/// Commit the group's offsets, and say so only when it goes wrong.
///
/// Per-partition results, per the workspace's rule 4: committing four
/// partitions where one is mid-reassignment succeeds for three, and reporting
/// the whole call as failed would hide that. A failed commit is not a finding
/// about the stream — it costs a replay after a restart and nothing else — so
/// it is a warning rather than a fault.
async fn commit(consumer: &GroupConsumer, watch: &mut Watch) {
    match consumer.commit().await {
        Ok(results) => {
            for ((topic, partition), result) in results {
                if let Err(error) = result {
                    watch.tally.commit_failures += 1;
                    tracing::warn!(%topic, partition, %error, "could not commit an offset");
                }
            }
        }
        Err(error) => {
            watch.tally.commit_failures += 1;
            tracing::warn!(%error, "the commit did not reach the coordinator");
        }
    }
}

/// Everything counted over the life of the process.
#[derive(Debug, Default)]
struct Tally {
    heartbeats: u64,
    lost: u64,
    duplicates: u64,
    silences: u64,
    unparseable: u64,
    mislabelled: u64,
    restarts: u64,
    pauses: u64,
    reordered: u64,
    replayed: u64,
    rejoins: u64,
    rebalances: u64,
    commit_failures: u64,
    slow: u64,
    max_latency_ms: i64,
    min_latency_ms: Option<i64>,
    last_latency_ms: i64,
}

impl Tally {
    /// What `--strict` fails on: the counts that mean the stream was not what
    /// the producer wrote. Restarts, pauses, reordering, rejoins, rebalances,
    /// failed commits and slow heartbeats are timing, not corruption.
    fn faults(&self) -> u64 {
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
struct Watch {
    tally: Tally,
    runs: HashMap<String, Run>,
    /// The run the most recent heartbeat came from.
    current: Option<String>,
    /// How long a missing sequence is held before it is called lost.
    reorder: Duration,
    /// How long the current run may be quiet before that is a finding.
    stall: Duration,
    lag_warn_ms: i64,
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
    fn new(stall: Duration, reorder: Duration, lag_warn_ms: i64) -> Self {
        Self {
            tally: Tally::default(),
            runs: HashMap::new(),
            current: None,
            reorder,
            stall,
            lag_warn_ms,
            log_append_reported: false,
            caught_up: false,
            lagging: false,
            backlog_records: 0,
            backlog_worst_ms: 0,
        }
    }

    /// Note that the session died and the reader is about to rejoin.
    fn rejoining(&mut self, error: &anyhow::Error) {
        self.tally.rejoins += 1;
        self.open_replay_window();
        tracing::warn!(error = %format!("{error:#}"), "the read stopped; rejoining");
    }

    /// Note that the broker moved partitions between polls.
    ///
    /// Not a fault. It is the group protocol working — a member joined or left
    /// — and the only consequence here is that a gained partition resumes from
    /// its committed offset, which re-reads a little.
    fn rebalanced(&mut self, before: &[(String, i32)], after: &[(String, i32)]) {
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

    fn observe_record(&mut self, record: &Record) {
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
            if latency > self.lag_warn_ms {
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
        match (self.lagging, latency > self.lag_warn_ms) {
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
        let stall_ms = i64::try_from(self.stall.as_millis()).unwrap_or(i64::MAX);
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
    fn sweep(&mut self, now: Instant) {
        let reorder = self.reorder;
        let stall = self.stall;
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

    fn summarise(&self, message: &str) {
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
        Watch::new(
            Duration::from_millis(5_000),
            Duration::from_millis(5_000),
            5_000,
        )
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

    /// The payload the producer's own test pins must parse here. These two
    /// assertions are one contract written down in two repositories, and this
    /// is the side that finds out when it breaks.
    #[test]
    fn the_producers_payload_parses() {
        let json = r#"{"run_id":"abc-1","sequence":7,"timestamp_ms":1785686674238,"timestamp":"2026-08-02T16:04:34.238Z","host":"canary-0"}"#;
        let parsed: Heartbeat = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.run_id, "abc-1");
        assert_eq!(parsed.sequence, 7);
        assert_eq!(parsed.timestamp_ms, 1_785_686_674_238);
    }

    /// A producer that adds a field must not blind its monitor. `canary-version`
    /// arrived in the producer's 0.2.0 as a header rather than a body field,
    /// but the next one may not.
    #[test]
    fn an_added_field_does_not_break_the_reader() {
        let json = r#"{"run_id":"abc-1","sequence":7,"timestamp_ms":1785686674238,"timestamp":"2026-08-02T16:04:34.238Z","host":"canary-0","region":"eu-west"}"#;
        assert!(serde_json::from_str::<Heartbeat>(json).is_ok());
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
        let mut watch = Watch::new(Duration::from_secs(5), Duration::ZERO, 5_000);
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
        let mut watch = Watch::new(Duration::from_secs(5), Duration::ZERO, 5_000);
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
        let mut watch = Watch::new(Duration::from_secs(5), Duration::from_secs(60), 5_000);
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
        let mut watch = Watch::new(Duration::from_secs(5), Duration::from_secs(60), 5_000);
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
        let mut watch = Watch::new(Duration::from_secs(5), Duration::ZERO, 5_000);
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
