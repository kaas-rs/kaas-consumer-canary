# kaas-consumer-canary

Reads [`kaas-producer-canary`](https://github.com/kaas-rs/kaas-producer-canary)'s
heartbeats back off the topic and reports what is wrong with them, using
[kaas-lib](https://github.com/kaas-rs/kaas-lib)'s `kafka-read`.

```sh
cargo run -- --bootstrap localhost:9092 --topic kaas-canary
```

```
INFO canary starting topic=kaas-canary from="latest" interval_ms=1000 stall_ms=5000 reorder_ms=5000
INFO still watching heartbeats=16581 lost=0 duplicates=0 silences=0 undecodable=0 unparseable=0
     mislabelled=0 restarts=0 pauses=0 reordered=0 replayed=0 reconnects=0 slow=0
     latency_ms=10 max_latency_ms=4689 min_latency_ms=Some(9) runs=1
```

## Why there is a second canary

The producer can only *claim*. It writes a `sequence` that increases by one per
record and logs an offset for each acknowledged send, and that is the whole of
what it knows: an acknowledgement is a statement about the leader's log at one
instant, not about what a reader will find there later. A record acknowledged
and then lost to an unclean leader election, a duplicate, a batch that will not
decode, a high watermark that stops advancing — none of it is visible from the
writing end.

This reads the stream back and checks it, which is possible here and almost
nowhere else: **the correct contents are known in advance.** Every heartbeat's
`sequence` is exactly one more than the last, so anything else is a finding
rather than an interpretation.

## What it separates

| finding | what it means |
|---|---|
| `lost` | a `sequence` that never arrived — the thing the pair exists to catch |
| `duplicates` | a `sequence` that arrived twice; `kafka-produce` refuses to cause one, so something else did |
| `silences` | the live run stopped arriving, and nothing has yet been *proven* missing |
| `pauses` | consecutive sequences far apart in time — the writer stopped, and nothing was lost |
| `restarts` | a new `run_id`; also not loss, and the reason `run_id` exists |
| `undecodable` | a batch that would not decode, carried as an event rather than ending the scan |
| `unparseable` | a record on this topic that is not a heartbeat, so somebody else is writing |
| `mislabelled` | the record and its body disagree — header versus `run_id`, or record timestamp versus body |
| `reordered` | a sequence that arrived late but arrived; see below |
| `replayed` | a record re-read after a reconnect, which is not a duplicate |
| `slow` | episodes of end-to-end latency over `--lag-warn-ms` |

The first three are the ones worth waking up for, and they are deliberately
three rather than one. A gap in `sequence` is loss. Silence is *not yet* loss —
the producer may be down, the topic may have stopped accepting writes, or this
reader may be the thing that is stuck, and saying which would be a guess. A
pause is not loss at all, and the producer's own README is explicit about it.

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
a scan merges partitions by timestamp rather than by sequence. A record can
therefore arrive slightly before one that precedes it.

A missing sequence is held for `--reorder-ms` before it is called lost. That is
a lower bound on how quickly loss is reported, traded against never crying wolf
over a merge artefact. With the whole stream in one partition — which is what
KIP-480 sticky partitioning gives the unkeyed producer — the window never
engages.

Contiguous lost sequences are reported as one range. An outage loses a block,
and a line per record turns one finding into a thousand.

## It reconnects, and it is honest about what that cost

A scan is a long-lived stream and a broker restart ends it. A reader that exits
there reports nothing about the restart, which is the interval most worth
reporting on — so the scan is re-established, and resumes at the *timestamp of
the last record read* rather than at the log end.

Resuming at that timestamp rather than one millisecond after it is deliberate:
several heartbeats can share a millisecond, and starting later would skip
records and report them as lost. The cost is that the records either side of
the resume point arrive twice, which is counted as `replayed` rather than
`duplicates` — and the replay window closes as soon as the reader moves past
where it stopped, so a genuine duplicate after that still counts as one.

The cluster handle is dropped and rebuilt with the scan. It costs one metadata
round trip per reconnect against a one-second floor on the backoff, and it
means no cached routing can outlive the failure that ended the scan. A canary
wedged onto a stale leader is a canary reporting silence about a healthy
cluster.

## Latency is apparent, not measured

`latency_ms` is the consumer's clock minus the producer's `timestamp_ms`, so it
is end-to-end latency plus whatever two hosts' clocks disagree by. It is not
clamped at zero: a negative `min_latency_ms` is the only evidence of that
disagreement there is, and a canary that hides it makes every latency it
reports a guess.

Two things suppress it rather than report it:

- **A replay.** `--from-earliest`, and the backlog after a reconnect, read
  records that are hours old. Every one is late by the only definition
  available, which is a description of what was asked for rather than a
  finding. Nothing is measured until the reader reaches the live end, and the
  catch-up is one line: `caught up with the log records=16556 behind_ms=16559674`.
- **`LogAppendTime`.** If the broker is rewriting record timestamps, that is
  reported once — it is a topic setting, not an event — because it replaces the
  producer's clock with the broker's in every comparison below it.

## Options

| flag | env | default | |
|---|---|---|---|
| `--bootstrap` | `KAAS_CANARY_BOOTSTRAP` | — | comma-separated `host:port` |
| `--topic` | `KAAS_CANARY_TOPIC` | `kaas-canary` | |
| `--interval-ms` | `KAAS_CANARY_INTERVAL_MS` | `1000` | what the producer is writing at |
| `--from-earliest` | `KAAS_CANARY_FROM_EARLIEST` | off | replay the retained topic first |
| `--partition` | | — | read only this partition |
| `--stall-intervals` | | `5` | missed heartbeats before silence |
| `--reorder-ms` | | 5 intervals | how long a hole is held before it is loss |
| `--lag-warn-ms` | | `5000` | latency above this is an episode |
| `--summary-secs` | | `60` | `0` to never summarise |
| `--count` | | — | stop after N heartbeats; runs forever when unset |
| `--strict` | | off | exit non-zero if anything was found |

The three environment variables are the producer's, on purpose: a producer and
its reader configured from one environment agree about the topic and the
interval by construction rather than by review.

`--from-earliest` is off by default. A canary reports on the cluster it is
watching *now*; a replay of a week of retained heartbeats reports on a week of
history, and every finding in it is about a cluster that has since been
restarted, resized or repaired.

`--strict` turns `--count` into an acceptance check. It fails on loss,
duplication, silence, undecodable batches, unparseable payloads and mislabelled
records — not on restarts, pauses, reordering, reconnects or slow heartbeats,
which are observations about timing rather than evidence that the stream was
not what the producer wrote.

```sh
# Read 500 heartbeats and fail if any of them are wrong.
kaas-consumer-canary --bootstrap kafka:9092 --count 500 --strict

# Check everything the topic still holds.
kaas-consumer-canary --bootstrap kafka:9092 --from-earliest --strict
```

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

Everything resolves from crates.io — `kafka-read` ships in kaas-lib 0.2.1.
There is no `kafka-admin` dependency: a consumer canary has no reason to hold a
handle that can create or delete a topic.

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
