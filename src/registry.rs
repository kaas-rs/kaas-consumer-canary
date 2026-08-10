//! The schema registry, and the decoder that turns bytes back into a heartbeat.
//!
//! The mirror of `kaas-producer-canary`'s module of the same name. That one
//! registers a schema and frames records against it; this one reads the frame
//! and resolves the datum.
//!
//! # What comes off the wire
//!
//! Confluent's framing: a zero magic byte, the schema id as four big-endian
//! bytes, then the Avro datum. The id is the whole point — it names the schema
//! the record was *written* with, which is not necessarily the one this crate
//! was compiled against, and Avro resolution needs the writer's schema to bridge
//! the two. Decoding against a hardcoded schema would work right up until the
//! producer added a field, and then fail on every record.
//!
//! So [`Decoder`] fetches by id and caches. `schema_registry_converter` keeps
//! resolved schemas keyed by id, so the registry is reached once per distinct
//! id seen — in practice once per process — and can be down for the rest of the
//! run without costing a single record.
//!
//! # Where a bad URL is caught, and where it is not
//!
//! Not here. `SrSettings` accepts any string — it does no parsing at build
//! time and makes no request — so [`Decoder::new`] would happily return a
//! decoder pointed at nonsense, and the first record would fail with a message
//! about a URL scheme rather than about the flag that was wrong.
//!
//! The check therefore lives at the command line, in
//! [`crate::cli::Args::schema_registry_url`], which is also where the
//! `http://`-only restriction this build imposes is enforced. `new` still
//! returns a `Result` because the builder has other ways to fail, not because
//! it validates the address.
//!
//! # Why decoding is not fatal
//!
//! A decode failure is a *finding*: it means something on this topic is not a
//! heartbeat, which is exactly what this canary exists to report. So it is
//! counted and logged and the loop continues — the same asymmetry the producer
//! draws between a startup check and a per-record one.

use anyhow::{Context, Result};
use schema_registry_converter::async_impl::avro::AvroDecoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::error::SRCError;

use crate::heartbeat::Heartbeat;

/// Why a record on this topic was not a heartbeat.
///
/// Three variants because they are three different findings, and collapsing
/// them would lose the one distinction that matters operationally: whether the
/// bytes were wrong, or the registry could not be asked about them.
#[derive(Debug)]
pub enum DecodeError {
    /// A null value on a topic that only ever carries heartbeats, so somebody
    /// else is writing to it.
    Tombstone,
    /// The framing was not Confluent's, or the schema it names could not be
    /// resolved. The second case is the only one here that can be the
    /// registry's fault rather than the writer's.
    Framing(SRCError),
    /// The datum decoded and does not fit a [`Heartbeat`] — a field renamed,
    /// retyped or removed without a default on the producer's side.
    Body(apache_avro::Error),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tombstone => write!(f, "the record has no value"),
            Self::Framing(error) => write!(f, "not a resolvable Confluent-framed datum: {error}"),
            Self::Body(error) => write!(f, "the datum is not a heartbeat: {error}"),
        }
    }
}

/// Resolves records against whatever schema they were written with.
pub struct Decoder {
    /// `'static` because the futures it caches borrow only its own settings,
    /// which it owns — the lifetime parameter is for callers who hand it
    /// borrowed configuration, and this one does not.
    inner: AvroDecoder<'static>,
}

impl Decoder {
    /// A decoder against the registry at `url`.
    ///
    /// Reaches nothing yet: the first fetch happens when the first record
    /// carrying an unseen schema id arrives.
    pub fn new(url: &str) -> Result<Self> {
        let settings = SrSettings::new_builder(url.to_owned())
            .build()
            .map_err(anyhow::Error::new)
            .with_context(|| format!("configuring the schema registry client for {url}"))?;

        Ok(Self {
            inner: AvroDecoder::new(settings),
        })
    }

    /// One record's value, resolved.
    ///
    /// Takes the bytes rather than the `Record` so that the module knows
    /// nothing about Kafka — the same split that keeps `crate::watch` testable
    /// without a broker.
    pub async fn decode(&self, value: Option<&[u8]>) -> Result<Heartbeat, DecodeError> {
        let Some(bytes) = value else {
            return Err(DecodeError::Tombstone);
        };

        let decoded = self
            .inner
            .decode(Some(bytes))
            .await
            .map_err(DecodeError::Framing)?;

        apache_avro::from_value(&decoded.value).map_err(DecodeError::Body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_null_value_is_a_tombstone_rather_than_a_parse_failure() {
        let decoder = Decoder::new("http://localhost:8080/apis/ccompat/v7").unwrap();
        let error = decoder.decode(None).await.unwrap_err();
        assert!(matches!(error, DecodeError::Tombstone), "{error:?}");
        // The message has to say which of the three it was: "no value" and
        // "not a heartbeat" send an operator to different places.
        assert_eq!(format!("{error}"), "the record has no value");
    }

    /// Bytes that are not Confluent-framed never reach the registry — the
    /// magic byte is checked first — so this is offline despite the URL.
    #[tokio::test]
    async fn bytes_without_the_magic_byte_are_a_framing_failure() {
        let decoder = Decoder::new("http://localhost:8080/apis/ccompat/v7").unwrap();
        let error = decoder.decode(Some(b"{\"run_id\":\"abc\"}")).await.unwrap_err();
        assert!(matches!(error, DecodeError::Framing(_)), "{error:?}");
    }
}
