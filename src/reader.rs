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

/// Read the topic, rejoining the group for as long as the process lives.
///
/// The cluster handle is dropped along with the member rather than reused. It
/// costs one metadata round trip per rejoin, against a one-second floor on the
/// backoff, and it means there is no cached routing state that can outlive the
/// failure that ended the session — a canary wedged onto a stale coordinator is
/// a canary reporting silence about a healthy cluster.
pub async fn watch_topic(bootstrap: &[String], session: &Session, watch: &mut Watch) -> Result<()> {
    const FIRST_BACKOFF: Duration = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);

    let mut backoff = FIRST_BACKOFF;
    loop {
        let outcome = match Cluster::connect(bootstrap.to_vec(), ClusterConfig::default()).await {
            Ok(cluster) => {
                backoff = FIRST_BACKOFF;
                read_session(cluster, session, watch).await
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
async fn read_session(cluster: Cluster, session: &Session, watch: &mut Watch) -> Result<()> {
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
