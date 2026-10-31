//! The command line, and the only place that knows there is one.
//!
//! The conversions below are the point of the module. [`Args`] is shaped by
//! what is convenient to type at a shell — a comma-separated string, a count of
//! intervals, seconds as integers — and none of that shape should reach the
//! code that reads records or decides what they mean. Converting once at the
//! boundary means each half takes a value it can act on rather than an `Args`
//! it has to interpret.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use kafka_conn::TlsConfig;

use crate::reader::Session;
use crate::registry::RegistryTrust;
use crate::watch::Thresholds;

#[derive(Debug, Parser)]
#[command(
    name = "kaas-consumer-canary",
    about = "Read kaas-producer-canary's heartbeats and report what is wrong with them."
)]
pub struct Args {
    /// Comma-separated bootstrap servers.
    #[arg(long, env = "KAAS_CANARY_BOOTSTRAP")]
    pub bootstrap: String,

    /// The topic to read.
    #[arg(long, env = "KAAS_CANARY_TOPIC", default_value = "kaas-canary")]
    pub topic: String,

    /// Base URL of the Confluent-compatible schema registry.
    ///
    /// Required, and without a default on purpose — the same reasoning the
    /// producer gives for its own copy of this flag. Every record is Avro
    /// framed with a schema id, so a reader without a registry cannot resolve a
    /// single one; starting anyway would mean counting the entire stream as
    /// unparseable, which is indistinguishable from the producer having broken.
    ///
    /// Apicurio serves this API under `/apis/ccompat/v7`.
    #[arg(long, env = "KAAS_CANARY_SCHEMA_REGISTRY")]
    pub schema_registry: String,

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
    pub group: String,

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
    pub instance_id: Option<String>,

    /// The interval the producer is writing at.
    ///
    /// Not used to time anything — this process is driven by what arrives —
    /// but every threshold below is a multiple of it, so a canary and its
    /// reader configured from the same environment agree by construction.
    #[arg(long, env = "KAAS_CANARY_INTERVAL_MS", default_value_t = 1_000)]
    pub interval_ms: u64,

    /// Missed heartbeats before silence is reported.
    #[arg(long, default_value_t = 5)]
    pub stall_intervals: u32,

    /// How long a missing sequence may stay missing before it counts as lost.
    ///
    /// Defaults to five intervals. This is a lower bound on how quickly loss is
    /// reported, so it trades detection latency against false positives from
    /// cross-partition reordering.
    #[arg(long)]
    pub reorder_ms: Option<u64>,

    /// Apparent end-to-end latency above this is reported.
    #[arg(long, default_value_t = 5_000)]
    pub lag_warn_ms: i64,

    /// How often to log a summary while nothing is wrong. Zero to never.
    #[arg(long, default_value_t = 60)]
    pub summary_secs: u64,

    /// How often to commit the group's offsets. Zero to never.
    ///
    /// A commit is what a restart resumes from, so this is the width of the
    /// window a crash re-reads. Re-reading is harmless — the replay window
    /// absorbs it — so the setting trades duplicate work against nothing much.
    #[arg(long, default_value_t = 10)]
    pub commit_secs: u64,

    /// Stop after this many heartbeats. Runs forever when unset.
    #[arg(long)]
    pub count: Option<u64>,

    /// Exit non-zero if anything was found.
    ///
    /// Off by default, because the deployed canary's job is to keep reporting,
    /// and a process that exits stops being a monitor. With `--count` it turns
    /// this into an acceptance check. Restarts, pauses, reordering, rejoins,
    /// rebalances, failed commits and slow heartbeats do not count: they are
    /// observations about timing, and the producer's own documentation is
    /// explicit that a pause is not loss. Loss, duplication, silence,
    /// unparseable payloads and mislabelled records do.
    #[arg(long)]
    pub strict: bool,

    /// PEM bundle to trust for the Kafka connection.
    ///
    /// The one piece of the TLS story the environment cannot supply, which is
    /// why it is a flag when nothing else about authentication is — see
    /// [`crate::identity`]. A broker behind a private CA (Strimzi signs its
    /// listeners with a per-cluster CA of its own) is in no system trust store,
    /// and there is no convention that says where its certificate was mounted.
    ///
    /// Unset means the system roots. The symptom of forgetting it is a TLS
    /// handshake failure at connect, not a later authentication error.
    #[arg(long, env = "KAAS_CANARY_CA_FILE")]
    pub ca_file: Option<PathBuf>,

    /// PEM bundle to trust for an `https://` schema registry.
    ///
    /// Its own flag rather than `--ca-file`, because the two are usually
    /// different CAs: Strimzi signs its listeners with a per-cluster CA, and
    /// the registry's certificate comes from wherever the platform issues
    /// service certificates. Nothing joins the two.
    ///
    /// Unset means the system roots — of which a scratch image has none, so
    /// an in-cluster registry over TLS needs this. Naming it for an `http://`
    /// registry is refused rather than ignored: the likelier story is a URL
    /// that lost its `s`.
    #[arg(long, env = "KAAS_CANARY_SCHEMA_REGISTRY_CA_FILE")]
    pub schema_registry_ca_file: Option<PathBuf>,
}

impl Args {
    /// The bootstrap list, split and cleaned.
    ///
    /// Empty is an error rather than an empty `Vec`, because a client handed no
    /// addresses fails later, deeper, and with a message about connections
    /// instead of about the flag that was wrong.
    pub fn bootstrap_servers(&self) -> Result<Vec<String>> {
        let servers: Vec<String> = self
            .bootstrap
            .split(',')
            .map(str::trim)
            .filter(|server| !server.is_empty())
            .map(str::to_owned)
            .collect();
        if servers.is_empty() {
            anyhow::bail!("--bootstrap resolved to no addresses");
        }
        Ok(servers)
    }

    /// The producer's interval, floored so a zero cannot spin a loop.
    fn interval(&self) -> Duration {
        Duration::from_millis(self.interval_ms.max(1))
    }

    /// What the checks judge against.
    pub fn thresholds(&self) -> Thresholds {
        let interval = self.interval();
        Thresholds::new(
            interval * self.stall_intervals.max(1),
            self.reorder_ms
                .map(Duration::from_millis)
                .unwrap_or(interval * 5),
            self.lag_warn_ms,
        )
    }

    /// The registry URL, checked for a scheme the client can speak.
    ///
    /// Anything but `http://` or `https://` would fail inside reqwest, at the
    /// first record, with a message about URL schemes. Catching it at the
    /// boundary means the error names the flag that was wrong instead.
    pub fn schema_registry_url(&self) -> Result<&str> {
        let url = self.schema_registry.trim();
        if url.is_empty() {
            anyhow::bail!("--schema-registry is empty");
        }
        if url.starts_with("http://") {
            if self.schema_registry_ca_file.is_some() {
                anyhow::bail!(
                    "--schema-registry-ca-file is set but --schema-registry is \
                     plain http ({url:?}); did the URL lose its https?"
                );
            }
        } else if !url.starts_with("https://") {
            anyhow::bail!("--schema-registry must be an http:// or https:// URL, got {url:?}");
        }
        Ok(url)
    }

    /// What to verify an `https://` registry against.
    ///
    /// Read and parsed here, for the reason [`Args::tls_config`] reads its
    /// bundle here: an unreadable or empty file is reported as the flag that
    /// named it, at startup, rather than as every record being unparseable.
    pub fn schema_registry_trust(&self) -> Result<RegistryTrust> {
        let Some(path) = &self.schema_registry_ca_file else {
            return Ok(RegistryTrust::System);
        };
        let pem = std::fs::read(path)
            .with_context(|| format!("reading --schema-registry-ca-file {}", path.display()))?;
        RegistryTrust::from_pem(&pem)
            .with_context(|| format!("parsing --schema-registry-ca-file {}", path.display()))
    }

    /// What to trust for the Kafka connection.
    ///
    /// Read here rather than inside the connection so that an unreadable bundle
    /// is reported as the flag that named it, at startup — and not as a
    /// handshake failure on the first rejoin, once per backoff, forever.
    pub fn tls_config(&self) -> Result<TlsConfig> {
        match &self.ca_file {
            Some(path) => {
                let pem = std::fs::read(path)
                    .with_context(|| format!("reading --ca-file {}", path.display()))?;
                Ok(TlsConfig::with_ca_pem(pem))
            }
            None => Ok(TlsConfig::system()),
        }
    }

    /// What the reader needs.
    pub fn session(&self) -> Session {
        // Ticking at the heartbeat interval keeps the resolution of the
        // clock-driven checks proportionate to the stream. The bounds stop a
        // sub-millisecond interval from spinning and an hourly one from making
        // the reports arrive an hour late.
        let tick = self
            .interval()
            .clamp(Duration::from_millis(100), Duration::from_secs(5));

        Session::new(&self.topic, &self.group, tick)
            .with_maybe_instance_id(self.instance_id.clone())
            .with_maybe_count(self.count)
            .with_summary_every(Duration::from_secs(self.summary_secs))
            .with_commit_every(Duration::from_secs(self.commit_secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(registry: &str) -> Args {
        with_registry_ca(registry, &[])
    }

    fn with_registry_ca(registry: &str, extra: &[&str]) -> Args {
        let mut argv = vec![
            "kaas-consumer-canary",
            "--bootstrap",
            "broker:9092",
            "--schema-registry",
            registry,
        ];
        argv.extend_from_slice(extra);
        Args::parse_from(argv)
    }

    /// The boundary the `registry` module deliberately does not have. `SrSettings`
    /// accepts any string and makes no request, so without this check a typo
    /// surfaces per-record, as a message about URL schemes rather than about the
    /// flag that was wrong.
    #[test]
    fn the_registry_url_is_checked_here_because_nothing_downstream_checks_it() {
        for url in [
            "http://apicurio:8080/apis/ccompat/v7",
            "https://apicurio:8443/apis/ccompat/v7",
        ] {
            assert_eq!(args(url).schema_registry_url().unwrap(), url);
        }

        let err = format!(
            "{}",
            args("apicurio:8080").schema_registry_url().unwrap_err()
        );
        assert!(err.contains("http:// or https://"), "{err}");

        assert!(args("   ").schema_registry_url().is_err());
    }

    /// A CA for a plaintext registry configures nothing. Ignoring it would
    /// hide the likelier mistake: the URL was meant to be `https://`.
    #[test]
    fn a_registry_ca_beside_an_http_url_is_refused() {
        let err = with_registry_ca(
            "http://apicurio:8080",
            &["--schema-registry-ca-file", "/etc/trust-bundle/ca.crt"],
        )
        .schema_registry_url()
        .unwrap_err();
        assert!(err.to_string().contains("lose its https"), "{err}");
    }

    #[test]
    fn no_registry_ca_means_the_system_store() {
        let trust = args("https://apicurio:8443")
            .schema_registry_trust()
            .unwrap();
        assert!(matches!(trust, RegistryTrust::System));
    }

    #[test]
    fn an_unreadable_registry_ca_names_the_flag() {
        let err = with_registry_ca(
            "https://apicurio:8443",
            &["--schema-registry-ca-file", "/nonexistent/ca.crt"],
        )
        .schema_registry_trust()
        .unwrap_err();
        assert!(
            format!("{err:#}").contains("--schema-registry-ca-file"),
            "{err:#}"
        );
    }
}
