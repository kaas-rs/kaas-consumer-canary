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
//! clocks.
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
//! The read path is a KIP-848 group member. That moves three things out of this
//! process and into the broker: **where to resume** (committed offsets, not a
//! timestamp this process remembers), **which partitions** (the broker assigns
//! them, so there is no `--partition`), and **where to start** (a member with
//! no committed offset begins at the earliest retained record — `kafka-consume`
//! policy, not a setting, so there is no `--from-earliest` to opt out of).
//!
//! The replay that last one causes matters less than it sounds: latency
//! findings are suppressed until the reader reaches the live end, and sequence
//! checks over history are still true statements about that history.
//!
//! # Quiet when healthy
//!
//! Per-record logging is at `debug`. A monitor that prints 86,400 healthy lines
//! a day buries the one line that matters, and "it is still logging" is not the
//! same evidence as "it is still correct". `info` carries a periodic summary,
//! and `warn`/`error` carry findings only.
//!
//! # Failure policy
//!
//! A finding is logged and counted; the loop continues. A failed poll is
//! re-established and the member rejoins, because a reader that exits when the
//! broker restarts reports nothing about the restart — which is the interval
//! most worth reporting on.
//!
//! # Layout
//!
//! * [`cli`] — the command line, and the conversions out of it.
//! * [`heartbeat`] — the payload, which is a contract with the other repo.
//! * [`watch`] — the model of the stream and every check against it. Knows
//!   nothing about Kafka beyond the shape of a record, which is what makes it
//!   testable without a broker.
//! * [`reader`] — the group member, and the loop that keeps one alive.
//!
//! This file is the wiring and the shutdown, and nothing else.

#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
// Tests may unwrap freely: a panic there is a failed assertion, which is the
// mechanism, not a crash in a process that is meant to run for weeks.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod cli;
mod heartbeat;
mod reader;
mod watch;

use anyhow::{Context, Result};
use clap::Parser;

use crate::cli::Args;
use crate::watch::Watch;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let args = Args::parse();
    let bootstrap = args.bootstrap_servers()?;
    let session = args.session();
    let mut watch = Watch::new(args.thresholds());

    let thresholds = watch.thresholds();
    tracing::info!(
        topic = %args.topic,
        group = %session.group(),
        instance_id = %session.instance_id(),
        interval_ms = args.interval_ms,
        stall_ms = thresholds.stall.as_millis(),
        reorder_ms = thresholds.reorder.as_millis(),
        "canary starting"
    );

    // Scoped so the borrow of `watch` ends with the future, leaving the summary
    // below reachable whether the watch returned or ctrl-c cancelled it.
    {
        let watching = reader::watch_topic(&bootstrap, &session, &mut watch);
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

    let faults = watch.tally().faults();
    if args.strict && faults > 0 {
        anyhow::bail!("{faults} fault(s) found; see the log");
    }
    Ok(())
}

/// `info` and above unless `RUST_LOG` says otherwise.
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}
