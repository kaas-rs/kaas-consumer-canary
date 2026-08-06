//! The payload, as the other end writes it.
//!
//! One type, and it is a contract with another repository. `kaas-producer-canary`
//! has a test that pins the exact JSON string; this is the side that finds out
//! when that string changes, which is why the parse tests live here rather than
//! next to the code that checks sequences.

use serde::Deserialize;

/// One heartbeat, as `kaas-producer-canary` writes it.
///
/// Unknown fields are ignored rather than rejected. The producer's payload is a
/// contract and it has a test that says so, but the compatible half of that
/// contract is that a field may be *added*; a consumer that refuses to parse an
/// unfamiliar field turns a backward-compatible producer change into a
/// monitoring outage, which is a worse failure than not reading the new field.
#[derive(Debug, Deserialize)]
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
}
