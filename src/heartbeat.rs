//! The payload, as the other end writes it.
//!
//! One type and one schema, and both are a contract with another repository.
//! `kaas-producer-canary` declares the same two and writes records against
//! them; this is the side that finds out when they change, which is why the
//! resolution test lives here rather than next to the code that checks
//! sequences.
//!
//! # The schema is duplicated, and that is the point
//!
//! [`SCHEMA`] is a copy of the producer's. It is not what decoding resolves
//! against — a record carries a schema id and [`crate::registry`] fetches the
//! writer's schema from Apicurio, so the copy below is never consulted at
//! runtime. It exists so that the struct in this file can be checked against
//! the contract *at compile-and-test time*, in the repository that depends on
//! it. Without it, a renamed field on the producer's side would first be
//! noticed in production, as every record becoming unparseable at once.
//!
//! The copy going stale is therefore a real failure mode, and the only defence
//! is that [`the_payload_resolves_against_the_schema`] fails loudly when the
//! struct and the copy disagree, and that Apicurio holds the authoritative
//! version either way.
//!
//! [`the_payload_resolves_against_the_schema`]: tests::the_payload_resolves_against_the_schema

#[cfg(test)]
use anyhow::{Context, Result};
#[cfg(test)]
use apache_avro::Schema;
use serde::{Deserialize, Serialize};

/// The Avro schema for [`Heartbeat`], as `kaas-producer-canary` registers it.
///
/// A copy, for the reason in the module docs, and `#[cfg(test)]` because that
/// reason is entirely a test-time one: nothing on the read path consults it,
/// since decoding resolves against the writer's schema fetched by id. Compiling
/// it into the binary would be carrying a second, unconsulted answer to a
/// question the registry already answers.
///
/// `sequence` is a `long` because Avro has no unsigned types; the Rust side
/// keeps `u64` because it counts, and the conversion is lossless below
/// `i64::MAX`.
#[cfg(test)]
pub const SCHEMA: &str = r#"{
  "type": "record",
  "name": "Heartbeat",
  "namespace": "rs.kaas.canary",
  "doc": "One heartbeat from kaas-producer-canary.",
  "fields": [
    {"name": "run_id", "type": "string", "doc": "Which run of the canary wrote this. Changes on every restart."},
    {"name": "sequence", "type": "long", "doc": "Increases by exactly one per record within a run. The diagnostic field."},
    {"name": "timestamp_ms", "type": "long", "doc": "Milliseconds since the epoch - what a machine should sort on."},
    {"name": "timestamp", "type": "string", "doc": "RFC 3339 with milliseconds - what a human should read."},
    {"name": "host", "type": "string", "doc": "Where the canary is running."}
  ]
}"#;

/// [`SCHEMA`], parsed.
///
/// Fallible because the constant is a string until something parses it, and a
/// crate that denies `expect` does not get to assume its own literal is valid.
#[cfg(test)]
pub fn schema() -> Result<Schema> {
    Schema::parse_str(SCHEMA).context("parsing the heartbeat schema")
}

/// One heartbeat, as `kaas-producer-canary` writes it.
///
/// Unknown fields are ignored rather than rejected, which under Avro is the
/// writer's schema carrying a field this reader's struct does not name. The
/// compatible half of the contract is that a field may be *added*; a consumer
/// that refuses to resolve an unfamiliar one turns a backward-compatible
/// producer change into a monitoring outage, which is the worse failure.
///
/// `Serialize` is here for the round-trip test below, not for the read path —
/// nothing in this crate writes a heartbeat.
#[derive(Debug, Deserialize, Serialize)]
pub struct Heartbeat {
    /// Which run of the producer wrote this. Changes on every restart.
    pub run_id: String,
    /// Increases by exactly one per record within a run. The diagnostic field.
    pub sequence: u64,
    /// Milliseconds since the epoch, set by the producer.
    pub timestamp_ms: i64,
    /// RFC 3339 rendering of the same instant.
    pub timestamp: String,
    /// Where the producer was running.
    pub host: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Heartbeat {
        Heartbeat {
            run_id: "abc-1".to_owned(),
            sequence: 7,
            timestamp_ms: 1_785_686_674_238,
            timestamp: "2026-08-02T16:04:34.238Z".to_owned(),
            host: "canary-0".to_owned(),
        }
    }

    /// The shape pinned in the producer's own test, pinned again here. These
    /// two assertions are one contract written down in two repositories, and
    /// this is the side that finds out when it breaks.
    ///
    /// Field-level `doc` is absent from the canonical form because apache-avro
    /// drops it on the way back out — which is why the producer posts the
    /// literal rather than the re-serialised schema.
    #[test]
    fn the_schema_shape_matches_the_producers() {
        let canonical = serde_json::to_string(&schema().unwrap()).unwrap();
        assert_eq!(
            canonical,
            r#"{"type":"record","namespace":"rs.kaas.canary","name":"Heartbeat","doc":"One heartbeat from kaas-producer-canary.","fields":[{"name":"run_id","type":"string"},{"name":"sequence","type":"long"},{"name":"timestamp_ms","type":"long"},{"name":"timestamp","type":"string"},{"name":"host","type":"string"}]}"#
        );
    }

    /// The struct and the schema are two descriptions of one record and nothing
    /// in the compiler ties them together. This is the tie, and it is the test
    /// that would have caught the Avro cutover: a struct that cannot be
    /// resolved from a datum written against this schema is a struct that
    /// rejects every record on the topic.
    ///
    /// Encoded and then decoded rather than asserted against pinned bytes: the
    /// encoding of `long` is apache-avro's contract to keep, not this crate's.
    /// What matters here is that `u64` survives the round trip through Avro's
    /// signed `long`.
    #[test]
    fn the_payload_resolves_against_the_schema() {
        let schema = schema().unwrap();
        let value = apache_avro::to_value(sample())
            .unwrap()
            .resolve(&schema)
            .unwrap();
        let bytes = apache_avro::to_avro_datum(&schema, value).unwrap();

        let decoded = apache_avro::from_avro_datum(&schema, &mut bytes.as_slice(), None).unwrap();
        let beat: Heartbeat = apache_avro::from_value(&decoded).unwrap();

        assert_eq!(beat.run_id, "abc-1");
        assert_eq!(beat.sequence, 7);
        assert_eq!(beat.timestamp_ms, 1_785_686_674_238);
        assert_eq!(beat.timestamp, "2026-08-02T16:04:34.238Z");
        assert_eq!(beat.host, "canary-0");
    }

    /// A producer that adds a field must not blind its monitor. Avro resolution
    /// does this by dropping what the reader does not name — but only if the
    /// reader resolves against the *writer's* schema, which is what fetching it
    /// by id from the registry achieves and what a hardcoded schema would not.
    #[test]
    fn an_added_field_does_not_break_the_reader() {
        let writer = Schema::parse_str(
            r#"{
              "type": "record", "name": "Heartbeat", "namespace": "rs.kaas.canary",
              "fields": [
                {"name": "run_id", "type": "string"},
                {"name": "sequence", "type": "long"},
                {"name": "timestamp_ms", "type": "long"},
                {"name": "timestamp", "type": "string"},
                {"name": "host", "type": "string"},
                {"name": "region", "type": "string", "default": "eu-west"}
              ]
            }"#,
        )
        .unwrap();

        let value = apache_avro::types::Value::Record(vec![
            (
                "run_id".into(),
                apache_avro::types::Value::String("abc-1".into()),
            ),
            ("sequence".into(), apache_avro::types::Value::Long(7)),
            (
                "timestamp_ms".into(),
                apache_avro::types::Value::Long(1_785_686_674_238),
            ),
            (
                "timestamp".into(),
                apache_avro::types::Value::String("2026-08-02T16:04:34.238Z".into()),
            ),
            (
                "host".into(),
                apache_avro::types::Value::String("canary-0".into()),
            ),
            (
                "region".into(),
                apache_avro::types::Value::String("eu-west".into()),
            ),
        ]);
        let bytes = apache_avro::to_avro_datum(&writer, value).unwrap();

        let decoded = apache_avro::from_avro_datum(&writer, &mut bytes.as_slice(), None).unwrap();
        let beat: Heartbeat = apache_avro::from_value(&decoded).unwrap();
        assert_eq!(beat.sequence, 7);
    }
}
