# kaas-consumer-canary

Reads [`kaas-producer-canary`](https://github.com/kaas-rs/kaas-producer-canary)'s
heartbeats back off the topic and reports what is wrong with them, using
[kaas-lib](https://github.com/kaas-rs/kaas-lib)'s `kafka-consume`.

```sh
cargo run -- --bootstrap localhost:9092 --topic kaas-canary
```

```
INFO canary starting topic=kaas-canary group=kaas-consumer-canary instance_id=<dynamic>
     interval_ms=1000 stall_ms=5000 reorder_ms=5000
INFO still watching heartbeats=16581 lost=0 duplicates=0 silences=0 unparseable=0
     mislabelled=0 restarts=0 pauses=0 reordered=0 replayed=0 rejoins=0 rebalances=0
     commit_failures=0 slow=0 latency_ms=10 max_latency_ms=4689 min_latency_ms=Some(9) runs=1
```

## Why there is a second canary

The producer can only *claim*. It writes a `sequence` that increases by one per
record and logs an offset for each acknowledged send, and that is the whole of
what it knows: an acknowledgement is a statement about the leader's log at one
instant, not about what a reader will find there later. A record acknowledged
and then lost to an unclean leader election, a duplicate, a high watermark that
stops advancing — none of it is visible from the writing end.

This reads the stream back and checks it, which is possible here and almost
nowhere else: **the correct contents are known in advance.** Every heartbeat's
`sequence` is exactly one more than the last, so anything else is a finding
rather than an interpretation.

## What it separates

| finding | what it means |
|---|---|
| `lost` | a `sequence` that never arrived — the thing the pair exists to catch |
| `duplicates` | a `sequence` that arrived twice; the producer is idempotent, so something else caused it |
| `silences` | the live run stopped arriving, and nothing has yet been *proven* missing |
| `pauses` | consecutive sequences far apart in time — the writer stopped, and nothing was lost |
| `restarts` | a new `run_id`; also not loss, and the reason `run_id` exists |
| `unparseable` | a record on this topic that is not a heartbeat, so somebody else is writing |
| `mislabelled` | the record and its body disagree — header versus `run_id`, or record timestamp versus body |
| `reordered` | a sequence that arrived late but arrived; see below |
| `replayed` | a record re-read after a rejoin or a rebalance, which is not a duplicate |
| `rejoins` | the session died and the member rejoined the group |
| `rebalances` | the broker moved partitions between polls |
| `commit_failures` | an offset commit that did not land; costs a replay, nothing more |
| `slow` | episodes of end-to-end latency over `--lag-warn-ms` |

The first three are the ones worth waking up for, and they are deliberately
three rather than one. A gap in `sequence` is loss. Silence is *not yet* loss —
the producer may be down, the topic may have stopped accepting writes, or this
reader may be the thing that is stuck, and saying which would be a guess. A
pause is not loss at all, and the producer's own README is explicit about it.

### What consuming cannot see, and reading could

There used to be a fourteenth row, and it was one of the six findings this
canary was built for: **`undecodable`**, a batch the tolerant decoder rejected.
On a topic where every batch came from one known writer, that is unambiguously
a broker or codec fault.

`kafka-read`'s scan surfaces it as `ScanEvent::Malformed`. `kafka-consume` does
not: `poll` returns `Vec<Record>` and handles `RecordOutcome::Malformed`
internally by stepping the position past it, deliberately, so that one corrupt
record cannot stall a partition forever. The caller is never told.

So the check is **gone**, rather than reported as permanently zero. A counter
that cannot rise is worse than an absent one, because a dashboard reads it as
evidence. Getting it back needs a `poll` variant upstream that surfaces
outcomes rather than swallowing them.

## Quiet when healthy

Per-record logging is at `debug`. `info` carries a summary every
`--summary-secs`, and `warn`/`error` carry findings only.

A monitor that prints 86,400 healthy lines a day buries the one line that
matters, and "it is still logging" is not the same evidence as "it is still
correct". For the same reason lag is reported as a *state* — one line entering
it, one leaving, and `slow` counts episodes rather than records. An hour of lag
is two lines, not 3,600.

## Out-of-order arrival is not loss

`sequence` is global to a run, but the records are spread over partitions, and
a poll returns them per partition rather than in sequence order. A record can
therefore arrive slightly before one that precedes it.

A missing sequence is held for `--reorder-ms` before it is called lost. That is
a lower bound on how quickly loss is reported, traded against never crying wolf
over an arrival-order artefact. With the whole stream in one partition — which
is what KIP-480 sticky partitioning gives the unkeyed producer — the window
never engages.

Contiguous lost sequences are reported as one range. An outage loses a block,
and a line per record turns one finding into a thousand.

## It is a group member, and that decides three things

The read path is a KIP-848 consumer group member rather than a scan. That moves
three decisions out of this process and into the broker.

**Where to resume.** Offsets are committed to `--group` every
`--commit-secs`, so a restart continues from the last commit instead of
re-deriving a position from the last record's timestamp. The window a crash
re-reads is at most one commit interval wide, and re-reading is harmless: those
records are counted as `replayed` rather than `duplicates`, and the replay
window closes as soon as the reader moves past where it stopped — so a genuine
duplicate after that still counts as one.

**Which partitions.** The broker assigns them, so there is no `--partition`.
When the assignment changes between polls that is a `rebalance`: not a fault,
just the group protocol working, and it opens the same replay window a rejoin
does because a gained partition resumes from its committed offset.

**Where to start.** A member with no committed offset starts at the *earliest*
retained record. That is `kafka-consume` policy, not a setting, which is why
there is no `--from-earliest` to turn off — the first run under a new `--group`
replays the topic once and every run after it resumes. The replay is noise
rather than a problem: latency findings are suppressed until the reader reaches
the live end, and sequence checks over history are still true statements about
that history.

A dead session is retried with backoff, because a reader that exits when the
broker restarts reports nothing about the restart — the interval most worth
reporting on. The cluster handle is dropped and rebuilt with the member: one
metadata round trip per rejoin against a one-second floor, and no cached
routing that can outlive the failure. A canary wedged onto a stale coordinator
is a canary reporting silence about a healthy cluster.

## Latency is apparent, not measured

`latency_ms` is the consumer's clock minus the producer's `timestamp_ms`, so it
is end-to-end latency plus whatever two hosts' clocks disagree by. It is not
clamped at zero: a negative `min_latency_ms` is the only evidence of that
disagreement there is, and a canary that hides it makes every latency it
reports a guess.

Two things suppress it rather than report it:

- **A replay.** The first run under a new group, and the backlog after a rejoin
  or a rebalance, read records that are hours old. Every one is late by the only
  definition available, which is a description of what was asked for rather than
  a finding. Nothing is measured until the reader reaches the live end, and the
  catch-up is one line: `caught up with the log records=16556 behind_ms=16559674`.
- **`LogAppendTime`.** If the broker is rewriting record timestamps, that is
  reported once — it is a topic setting, not an event — because it replaces the
  producer's clock with the broker's in every comparison below it.

## Options

| flag | env | default | |
|---|---|---|---|
| `--bootstrap` | `KAAS_CANARY_BOOTSTRAP` | — | comma-separated `host:port` |
| `--topic` | `KAAS_CANARY_TOPIC` | `kaas-canary` | |
| `--schema-registry` | `KAAS_CANARY_SCHEMA_REGISTRY` | — | ccompat base URL; `http://` only |
| `--group` | `KAAS_CANARY_GROUP` | `kaas-consumer-canary` | where committed offsets live |
| `--instance-id` | `KAAS_CANARY_INSTANCE_ID` | — | static membership; a restart is not a rebalance |
| `--interval-ms` | `KAAS_CANARY_INTERVAL_MS` | `1000` | what the producer is writing at |
| `--stall-intervals` | | `5` | missed heartbeats before silence |
| `--reorder-ms` | | 5 intervals | how long a hole is held before it is loss |
| `--lag-warn-ms` | | `5000` | latency above this is an episode |
| `--summary-secs` | | `60` | `0` to never summarise |
| `--commit-secs` | | `10` | `0` to never commit |
| `--count` | | — | stop after N heartbeats; runs forever when unset |
| `--strict` | | off | exit non-zero if anything was found |
| `--ca-file` | `KAAS_CANARY_CA_FILE` | system roots | PEM bundle to trust for the Kafka connection |

The shared environment variables are the producer's, on purpose: a producer and
its reader configured from one environment agree about the topic, the interval
and the registry by construction rather than by review.

`--schema-registry` has no default and is required, for the same reason it is on
the producer: every record is Avro framed with a schema id, so a reader without a
registry cannot resolve a single one. Starting anyway would mean counting the
whole stream as unparseable — which looks exactly like the producer having
broken, and is the failure this crate spent a while actually having.

## The payload is Avro, and the schema comes off the wire

Records carry Confluent framing: a zero magic byte, the schema id as four
big-endian bytes, then the datum. The id is the point. It names the schema the
record was *written* with, which is not necessarily the one this crate was
compiled against, and Avro resolution needs the writer's schema to bridge them.

So the reader fetches by id and caches — the registry is reached once per
distinct id, in practice once per process, and can be down for the rest of the
run without costing a record. Decoding against a schema hardcoded here would
work right up until the producer added a field, and then fail on every record.

There is a copy of the schema in [`src/heartbeat.rs`](src/heartbeat.rs), and it
is `#[cfg(test)]`. Nothing on the read path consults it; it exists so the struct
in this repository can be checked against the contract at test time, in the
repository that depends on it. A renamed field on the producer's side should be
a failing test here, not a production outage.

`--instance-id` is worth setting for a deployed replica. Without it every
restart triggers a rebalance; with it, a restart inside the session timeout
keeps the same assignment, which is the difference between a deploy showing up
as a gap in coverage and not showing up at all.

Use a literal, not the pod name. A Deployment regenerates pod names per
ReplicaSet, so an id derived from one never survives the restart it exists to
cover. With a single replica per cluster a fixed string is both stable and
unique, which is all KIP-345 asks for.

`--strict` turns `--count` into an acceptance check. It fails on loss,
duplication, silence, unparseable payloads and mislabelled records — not on
restarts, pauses, reordering, rejoins, rebalances, failed commits or slow
heartbeats, which are observations about timing rather than evidence that the
stream was not what the producer wrote.

```sh
# Read 500 heartbeats and fail if any of them are wrong.
kaas-consumer-canary --bootstrap kafka:9092 --count 500 --strict

# Check everything the topic still holds, under a throwaway group so it
# starts from the beginning rather than from a committed offset.
kaas-consumer-canary --bootstrap kafka:9092 --group audit-$(date +%s) --strict
```

## Workload identity

There are no flags for it. If the environment describes a federated identity,
this canary authenticates with SASL/OAUTHBEARER; if it does not, it connects the
way it always has. Nothing in between has to be configured:

| variable | | |
|---|---|---|
| `AZURE_CLIENT_ID` | required | the **application (client) id** — not the service principal's object id |
| `AZURE_TENANT_ID` | required | selects the issuer |
| `AZURE_FEDERATED_TOKEN_FILE` | required | path to the assertion, re-read on every token fetch |
| `AZURE_AUTHORITY_HOST` | `https://login.microsoftonline.com/` | for sovereign clouds |
| `AZURE_KAFKA_SCOPE` | `api://<client-id>/.default` | when the broker wants a token about some other resource |

The first three are exactly what AKS's `azure-wi-webhook` injects, so a pod that
runs here runs on AKS unchanged. **Setting some but not all three is an error at
startup**, not a quiet fall back to anonymous — a canary that silently connects
to a listener nobody meant to test reports on the wrong thing for as long as
nobody notices, which is this process's worst failure mode.

Two things follow automatically from an identity being present:

- **TLS is turned on**, with no way to opt out. OAUTHBEARER over a plaintext
  listener puts an access token on the wire in the clear, and unlike a password
  a stolen bearer token is honoured from whoever replays it until it expires.
- **`--ca-file` is usually needed with it.** Strimzi signs its listeners with a
  per-cluster CA that is in no system trust store; without the flag the
  handshake fails at connect — and here it fails once per backoff tick, forever,
  which is why the bundle is read and rejected at startup instead.

The token provider is built once and shared by every rejoin. That matters more
here than in the producer: this process drops its cluster handle and rebuilds it
on every lost session, and a config rebuilt alongside it would mean a fresh
token fetch per backoff tick — turning a rejoin storm into a request storm at
the issuer, at exactly the moment the cluster is already unhappy.

The exchange itself is [`src/token.rs`](src/token.rs), in this crate rather than
in kaas-lib, which stops at `TokenProvider` — "give me a bearer token, and a
fresh one when I ask again". An `AADSTS` error taxonomy is not something a Kafka
client library should carry for every one of its consumers. That module's header
records why the official `azure_identity` crate is not the dependency here,
which is a fact about this image rather than about the crate.

## Failure policy

A finding is logged and counted; the loop continues. `unwrap`, `expect` and
`panic!` are denied at the crate root, as in the producer and for a sharper
reason: a producer that dies reads as "the cluster is down", but a *monitor*
that dies reads as "nothing is wrong", which is the one failure mode a monitor
must not have.

## Building

```sh
cargo build --release
```

Everything resolves from crates.io — `kafka-consume` ships in kaas-lib 0.4.0.
There is no `kafka-admin` dependency: a consumer canary has no reason to hold a
handle that can create or delete a topic.

`kafka-read` is a direct dependency for exactly one item, `TimestampType` — the
type of a field on the `Record` that `kafka-consume` re-exports without
re-exporting the enum needed to match on it. It is not a second read path:
`kafka-consume` is built on that crate's decoder and both resolve to one
version. The day the re-export lands, the dependency goes.

## Container

```sh
docker run --rm ghcr.io/kaas-rs/kaas-consumer-canary:latest \
  --bootstrap kafka:9092 --topic kaas-canary
```

Static musl binary on distroless nonroot, so there is no shell in the image and
nothing to exec into. Built and published by
[`.github/workflows/release.yml`](.github/workflows/release.yml) on a `v*` tag;
`main` builds publish `main` and `sha-<short>` tags for testing, and only a
semver tag moves `latest`.

## Deployment

One instance per Kafka cluster, in the namespace of the broker it watches,
alongside the producer canary it reads — same shape, same reasoning, and the
manifests live in the cluster repo (`Woestebanaan/k3s-cluster`) rather than
here, because that is the repository ArgoCD reads. They pin an image digest
rather than a tag: a canary that silently changes version is a canary that
cannot be trusted to report anything.

Sync is manual in this cluster, deliberately: a merged commit is not evidence
that anything changed. Check `argocd app get kaas`, not the git log.

## Licence

Apache-2.0
