//! The group member, and the loop that keeps one alive.
//!
//! Everything Kafka-shaped lives here: joining, polling, committing, and
//! rejoining when the session dies. What the records *mean* is [`crate::watch`]'s
//! job, and the split is deliberate — that half is testable without a broker,
//! and this half is not testable without one.

use std::time::Duration;

use anyhow::{Context, Result};
use kafka_consume::{Cluster, ClusterConfig, ConsumerConfig, GroupConsumer};
use tokio::time::Instant;

use crate::watch::Watch;

/// How the reader reads.
///
/// Built from the command line at the boundary rather than threaded through it,
/// so this module does not know that a command line exists — and so a new flag
/// cannot start influencing the loop without somebody writing the line that
/// lets it. Required data in `new`, the rest `with_*`, as in kaas-lib's own
/// types.
#[derive(Debug, Clone)]
pub struct Session {
    /// The topic to read.
    topic: String,
    /// The group whose committed offsets decide where a restart resumes.
    group: String,
    /// How often the clock-driven half runs.
    tick: Duration,
    /// Static membership, when a stable id was given.
    instance_id: Option<String>,
    /// Stop after this many heartbeats. Runs forever when `None`.
    count: Option<u64>,
    /// How often to summarise. Zero never does.
    summary_every: Duration,
    /// How often to commit. Zero never does.
    commit_every: Duration,
}

impl Session {
    /// Read `topic` as a member of `group`, sweeping every `tick`.
    pub fn new(topic: impl Into<String>, group: impl Into<String>, tick: Duration) -> Self {
        Self {
            topic: topic.into(),
            group: group.into(),
            tick,
            instance_id: None,
            count: None,
            summary_every: Duration::ZERO,
            commit_every: Duration::ZERO,
        }
    }

    /// Join statically under this id, when one was configured.
    #[must_use]
    pub fn with_maybe_instance_id(mut self, instance_id: Option<String>) -> Self {
        self.instance_id = instance_id;
        self
    }

    /// Stop after this many heartbeats, when a limit was given.
    #[must_use]
    pub fn with_maybe_count(mut self, count: Option<u64>) -> Self {
        self.count = count;
        self
    }

    /// Log a summary this often. `Duration::ZERO` never does.
    #[must_use]
    pub fn with_summary_every(mut self, every: Duration) -> Self {
        self.summary_every = every;
        self
    }

    /// Commit this often. `Duration::ZERO` never does.
    #[must_use]
    pub fn with_commit_every(mut self, every: Duration) -> Self {
        self.commit_every = every;
        self
    }

    /// The group, for the line that records what this process joined.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// The instance id, or what to call its absence in a log line.
    pub fn instance_id(&self) -> &str {
        self.instance_id.as_deref().unwrap_or("<dynamic>")
    }
}

/// How long to wait before rejoining, and when that resets.
///
/// The reset condition is the whole of it. An earlier version reset whenever
/// the *connection* succeeded, which is not the same as getting anywhere: a
/// static member restarting finds its instance id still owned by the previous
/// session and is refused with `UNRELEASED_INSTANCE_ID` until the coordinator
/// expires it. The broker is reachable throughout, so a connect-keyed reset
/// pinned the wait at one second and produced forty-five rejoins where six
/// would do.
///
/// Keyed on progress instead, the curve rides out a session timeout without
/// this process having to know what one is — which matters, because it cannot
/// find out. `kafka-consume` hardcodes 30s inside `Membership` and exposes it
/// neither on `ConsumerConfig` nor anywhere else, so a backoff written against
/// that number would be a second copy of a constant this repo does not own.
#[derive(Debug)]
struct Backoff(Duration);

impl Backoff {
    const FIRST: Duration = Duration::from_secs(1);
    const MAX: Duration = Duration::from_secs(30);

    fn new() -> Self {
        Self(Self::FIRST)
    }

    /// The next wait, doubling up to the ceiling.
    fn take(&mut self) -> Duration {
        let waiting = self.0;
        self.0 = (self.0 * 2).min(Self::MAX);
        waiting
    }

    /// Back to the floor, because the last session actually read something.
    fn reset(&mut self) {
        self.0 = Self::FIRST;
    }
}

/// Read the topic, rejoining the group for as long as the process lives.
///
/// The cluster handle is dropped along with the member rather than reused. It
/// costs one metadata round trip per rejoin, against a one-second floor on the
/// backoff, and it means there is no cached routing state that can outlive the
/// failure that ended the session — a canary wedged onto a stale coordinator is
/// a canary reporting silence about a healthy cluster.
///
/// `cluster_config` carries the TLS and SASL settings and is deliberately built
/// once by the caller rather than defaulted here, so that the token provider it
/// holds outlives any individual session.
pub async fn watch_topic(
    bootstrap: &[String],
    session: &Session,
    watch: &mut Watch,
    cluster_config: &ClusterConfig,
) -> Result<()> {
    let mut backoff = Backoff::new();
    loop {
        // Whether this attempt got as far as a successful poll. Connecting is
        // not progress: the failure this backoff exists for happens with the
        // broker reachable and answering.
        let mut progressed = false;

        // Cloned per attempt because the handle is dropped with the member, but
        // the clone is shallow where it matters: a SASL config holds its token
        // provider behind an `Arc`, so every rejoin shares one token and one
        // cache. Rebuilding the config here instead would mean a fresh token
        // fetch on every backoff tick — a rejoin storm turned into a request
        // storm at the issuer, which is the way to get rate-limited by EntraID
        // precisely when the cluster is already unhappy.
        let outcome = match Cluster::connect(bootstrap.to_vec(), cluster_config.clone()).await {
            Ok(cluster) => read_session(cluster, session, watch, &mut progressed).await,
            Err(error) => Err(anyhow::Error::new(error).context("connecting to the cluster")),
        };

        if progressed {
            backoff.reset();
        }

        match outcome {
            Ok(()) => return Ok(()),
            Err(error) => {
                watch.rejoining(&error);
                tokio::time::sleep(backoff.take()).await;
            }
        }
    }
}

/// One group membership, from the join to whatever ends it.
///
/// Returns `Ok` only when the run is genuinely over — the `--count` limit — so
/// every other exit is an error and gets retried by the caller.
/// `progressed` is set once a poll has succeeded, which is what tells the
/// caller's backoff that this attempt got somewhere. It is an out-parameter
/// rather than a return value because the interesting exits from here are the
/// error ones, and those carry the error instead.
async fn read_session(
    cluster: Cluster,
    session: &Session,
    watch: &mut Watch,
    progressed: &mut bool,
) -> Result<()> {
    let mut consumer = GroupConsumer::subscribe(
        cluster,
        ConsumerConfig::new(),
        session.group.clone(),
        [session.topic.clone()],
    )
    .await
    .context("joining the group")?;

    if let Some(instance_id) = &session.instance_id {
        consumer = consumer.instance_id(instance_id.clone());
    }

    // Everything that is not driven by an arriving record — silence, and a
    // missing sequence ageing out of its reorder window — needs a clock of its
    // own, because the observation is precisely that nothing arrived.
    let mut watchdog = tokio::time::interval(session.tick);
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
                // Joined, assigned and fetched. Anything that ends the session
                // after this is a new failure rather than the same one being
                // retried too eagerly.
                *progressed = true;

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
                    if let Some(limit) = session.count
                        && watch.tally().heartbeats >= limit
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

                if !session.commit_every.is_zero()
                    && now.duration_since(commit_at) >= session.commit_every
                {
                    commit_at = now;
                    commit(&consumer, watch).await;
                }

                if !session.summary_every.is_zero()
                    && now.duration_since(summary_at) >= session.summary_every
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
                    watch.note_commit_failure();
                    tracing::warn!(%topic, partition, %error, "could not commit an offset");
                }
            }
        }
        Err(error) => {
            watch.note_commit_failure();
            tracing::warn!(%error, "the commit did not reach the coordinator");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bug this replaced: a reset on every attempt pins the wait at the
    /// floor, so a condition that clears on the coordinator's clock is retried
    /// once a second for as long as it lasts.
    #[test]
    fn a_backoff_that_never_progresses_climbs_to_the_ceiling() {
        let mut backoff = Backoff::new();
        let waits: Vec<u64> = (0..8).map(|_| backoff.take().as_secs()).collect();
        assert_eq!(waits, vec![1, 2, 4, 8, 16, 30, 30, 30]);

        // Six attempts cover the 30s a static member waits for its own
        // instance id to be released, against the forty-five it took before.
        let to_thirty: u64 = waits.iter().take_while(|w| **w < 30).sum();
        assert!(to_thirty >= 30, "reached {to_thirty}s before the ceiling");
    }

    #[test]
    fn progress_returns_the_wait_to_the_floor() {
        let mut backoff = Backoff::new();
        backoff.take();
        backoff.take();
        assert_eq!(backoff.take(), Duration::from_secs(4));

        // A session that read something means the next failure is a new one.
        backoff.reset();
        assert_eq!(backoff.take(), Duration::from_secs(1));
    }
}
