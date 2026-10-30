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
//!
//! # Authentication, and why the decoder is rebuilt
//!
//! A registry that requires a token gets a bearer token on every fetch. The
//! producer gets away with one token because it calls the registry once; this
//! end calls it whenever an unseen schema id arrives, which can be at any point
//! in a run — after a schema change, or after an in-memory registry restarted
//! and the producer re-registered under a new id. A token from startup would be
//! expired by then.
//!
//! `schema_registry_converter` 4.10 fixes the `Authorization` header when the
//! settings are built; the middleware hook it recommends for refreshing tokens
//! arrives in 5.0. So [`Decoder`] keeps the token its inner decoder was built
//! with, asks the provider for the current one on each decode — a cache hit
//! until the provider decides to refresh — and rebuilds the inner decoder when
//! they differ. The rebuild drops the schema cache, which costs one registry
//! call per id after each refresh: one call every half-hour, at an hour's token
//! lifetime and one schema.
//!
//! The first token is fetched in [`Decoder::new`], so a scope the issuer
//! refuses is a startup failure naming the scope, not a stream of `unparseable`
//! records that look like the producer's fault.
//!
//! The token travels over plain HTTP — `--schema-registry` accepts `http://`
//! only — which is tolerated because its audience is the registry alone and it
//! expires within the hour. TLS on the registry is the fix.

use anyhow::{Context, Result};
use kafka_conn::TokenProvider;
use schema_registry_converter::async_impl::avro::AvroDecoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::error::SRCError;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::heartbeat::Heartbeat;
use crate::token::FederatedTokenProvider;

/// Why a record on this topic was not a heartbeat.
///
/// Separate variants because they are different findings, and collapsing them
/// would lose the one distinction that matters operationally: whether the
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
    /// The registry requires a token and there was no valid one to send. The
    /// bytes were never looked at, so this says nothing about the writer.
    Token(kafka_conn::Error),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tombstone => write!(f, "the record has no value"),
            Self::Framing(error) => write!(f, "not a resolvable Confluent-framed datum: {error}"),
            Self::Body(error) => write!(f, "the datum is not a heartbeat: {error}"),
            Self::Token(error) => write!(f, "no schema registry token to ask with: {error}"),
        }
    }
}

/// Resolves records against whatever schema they were written with.
pub struct Decoder {
    url: String,
    /// Present when the registry requires a token.
    tokens: Option<FederatedTokenProvider>,
    /// Behind a lock because a new token means a new inner decoder, and
    /// [`Decoder::decode`] takes `&self` — the reader shares one decoder across
    /// every rejoin.
    current: RwLock<Current>,
}

/// An inner decoder, and the token it was built with.
struct Current {
    /// `None` for an anonymous registry. Compared against the provider's
    /// current token to notice a refresh.
    token: Option<String>,
    /// `'static` because the futures it caches borrow only its own settings,
    /// which it owns — the lifetime parameter is for callers who hand it
    /// borrowed configuration, and this one does not.
    inner: AvroDecoder<'static>,
}

impl Current {
    fn build(url: &str, token: Option<String>) -> Result<Self, SRCError> {
        let mut builder = SrSettings::new_builder(url.to_owned());
        if let Some(token) = &token {
            builder.set_token_authorization(token);
        }
        Ok(Self {
            inner: AvroDecoder::new(builder.build()?),
            token,
        })
    }
}

impl Decoder {
    /// A decoder against the registry at `url`, authenticating with `tokens`
    /// when given.
    ///
    /// Reaches no registry yet: the first schema fetch happens when the first
    /// record carrying an unseen id arrives. With `tokens`, it does fetch the
    /// first token now — see the module docs for why that is a startup check.
    pub async fn new(url: &str, tokens: Option<FederatedTokenProvider>) -> Result<Self> {
        let token = match &tokens {
            Some(tokens) => Some(
                tokens
                    .token()
                    .await
                    .context("fetching a schema registry token")?,
            ),
            None => None,
        };
        let current = Current::build(url, token)
            .map_err(anyhow::Error::new)
            .with_context(|| format!("configuring the schema registry client for {url}"))?;

        Ok(Self {
            url: url.to_owned(),
            tokens,
            current: RwLock::new(current),
        })
    }

    /// The inner decoder, rebuilt first if the provider has moved on to a new
    /// token.
    async fn current(&self) -> Result<RwLockReadGuard<'_, Current>, DecodeError> {
        let Some(tokens) = &self.tokens else {
            return Ok(self.current.read().await);
        };
        // A cache hit until the provider decides to refresh, so asking per
        // record costs a lock and a clock read.
        let token = tokens.token().await.map_err(DecodeError::Token)?;

        {
            let current = self.current.read().await;
            if current.token.as_deref() == Some(token.as_str()) {
                return Ok(current);
            }
        }

        let mut current = self.current.write().await;
        // Checked again under the write lock: another decode may have rebuilt
        // while this one waited for it.
        if current.token.as_deref() != Some(token.as_str()) {
            *current = Current::build(&self.url, Some(token)).map_err(DecodeError::Framing)?;
            tracing::debug!(
                "schema registry token refreshed; decoder rebuilt and its schema cache dropped"
            );
        }
        Ok(RwLockWriteGuard::downgrade(current))
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

        let current = self.current().await?;
        let decoded = current
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

    const URL: &str = "http://localhost:8080/apis/ccompat/v7";

    #[tokio::test]
    async fn a_null_value_is_a_tombstone_rather_than_a_parse_failure() {
        let decoder = Decoder::new(URL, None).await.unwrap();
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
        let decoder = Decoder::new(URL, None).await.unwrap();
        let error = decoder
            .decode(Some(b"{\"run_id\":\"abc\"}"))
            .await
            .unwrap_err();
        assert!(matches!(error, DecodeError::Framing(_)), "{error:?}");
    }

    /// A token the issuer cannot be asked for is a startup failure that names
    /// the cause — here the assertion file — rather than a decoder that turns
    /// every record into an `unparseable` one. Offline: the assertion is read
    /// before any request is built.
    #[tokio::test]
    async fn a_registry_token_that_cannot_be_fetched_fails_at_startup() {
        let tokens = FederatedTokenProvider::new(
            "https://login.microsoftonline.com/tenant/oauth2/v2.0/token",
            "app-id",
            "registry-id/.default",
            "/nonexistent/azure-token",
        )
        .unwrap();
        let Err(error) = Decoder::new(URL, Some(tokens)).await else {
            panic!("a decoder was built without a token");
        };
        let rendered = format!("{error:#}");
        assert!(rendered.contains("schema registry token"), "{rendered}");
        assert!(rendered.contains("/nonexistent/azure-token"), "{rendered}");
    }

    /// The anonymous decoder is built with no token and keeps none, so it never
    /// asks a provider — the path every deployment without a registry scope
    /// takes.
    #[tokio::test]
    async fn an_anonymous_decoder_never_holds_a_token() {
        let decoder = Decoder::new(URL, None).await.unwrap();
        assert!(decoder.current().await.unwrap().token.is_none());
    }
}
