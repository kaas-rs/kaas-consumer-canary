//! The federated token exchange, and the cache in front of it.
//!
//! # Why this lives here
//!
//! kaas-lib is a Kafka client library. Its job ends at
//! [`TokenProvider`](kafka_conn::TokenProvider) — "give me a bearer token, and
//! give me a fresh one when I ask again" — and everything below this line is
//! about EntraID rather than about Kafka: an `AADSTS` error taxonomy, a
//! `client_assertion` grant, a scope shaped like `api://<id>/.default`. Putting
//! that in the library would make every consumer of a Kafka client carry an
//! opinion about one cloud's IAM, and the next cloud would arrive as a second
//! opinion beside it.
//!
//! So kaas-lib stays generic and this crate is specific, which is the right way
//! round: there is one canary per deployment and one library under all of them.
//!
//! # Why not the official SDK
//!
//! `azure_identity` implements this exchange, is maintained by the people who
//! own the protocol, and would be the correct dependency in most programs — in
//! a Go or Python canary it would not be a question. It is unusable *here*: it
//! resolves to 194 additional packages including `aws-lc-rs` and `aws-lc-sys`,
//! which needs cmake and a C toolchain, and which would put a second crypto
//! provider into the same `rustls` kaas-lib uses with `ring`. `Cargo.toml`
//! spells out why both are disqualifying for this image. The exchange itself is
//! one form POST, so the trade is a hundred lines against the release image.
//!
//! # What the flow is
//!
//! RFC 7523 §2.2, which OAuth calls a client assertion and Azure calls workload
//! identity federation:
//!
//! ```text
//! POST /{tenant}/oauth2/v2.0/token
//!   grant_type=client_credentials
//!   client_id=<application id>
//!   scope=api://<application id>/.default
//!   client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer
//!   client_assertion=<the JWT-SVID on disk>
//! ```
//!
//! The assertion is a token minted for this pod by something that EntraID has
//! been told to trust — SPIRE here, the kubelet on AKS. No secret is involved
//! at any point, which is the entire reason to do it this way.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::header::{ACCEPT, CONTENT_TYPE};
use hyper::{Method, Request, Uri};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use kafka_conn::{Error, Result, TokenFuture, TokenProvider};
use serde::Deserialize;
use tokio::sync::Mutex;

/// RFC 7523's name for "the credential is a signed JWT".
const ASSERTION_TYPE: &str = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";

/// Cap on the token response we will buffer. A token endpoint answers with a
/// few kilobytes; reading an unbounded body from a host that may not be the one
/// we think it is turns a misconfigured url into a memory problem.
const MAX_RESPONSE_BYTES: usize = 256 * 1024;

/// Lifetime assumed when the endpoint does not say. `expires_in` is optional in
/// RFC 6749 §5.1, and the token still works — refreshing more often than needed
/// beats refusing to use it.
const ASSUMED_LIFETIME: Duration = Duration::from_secs(300);

/// Ceiling on a lifetime we will believe.
///
/// Two jobs. A token claiming a century is not one to trust for a century; and
/// `expires_in` is a number off the network that ends up added to an `Instant`,
/// where `std`'s `Add` **panics** on overflow. Capping is what keeps that
/// arithmetic safe by construction in a crate that denies `panic`.
const MAX_LIFETIME: Duration = Duration::from_secs(24 * 60 * 60);

/// Never refresh more often than this, however short a lifetime is claimed.
/// A one-second token would otherwise mean a token fetch per SASL exchange.
const MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Refresh this long before expiry, on top of the half-life schedule.
const REFRESH_MARGIN: Duration = Duration::from_secs(60);

/// Deadline for one token fetch.
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

type HttpClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

/// What the token endpoint says on success.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: Option<u64>,
}

/// What it says on failure. Both fields are optional because an issuer under
/// load may answer with neither, and a 400 with no body still has to produce a
/// message that says which endpoint refused.
#[derive(Debug, Default, Deserialize)]
struct ErrorResponse {
    error: Option<String>,
    error_description: Option<String>,
}

/// A cached token, and the two moments that matter about it.
struct Cached {
    token: String,
    /// When to go and get a new one.
    refresh_after: Instant,
    /// When this one stops working. Only consulted when a refresh *failed*.
    expires_at: Instant,
}

/// An OAUTHBEARER token source backed by a federated `client_credentials`
/// exchange.
///
/// One provider should be shared by every connection to a cluster: it holds the
/// cache, so sharing it is what keeps the client on one token and one fetch
/// rather than one fetch per broker.
pub struct FederatedTokenProvider {
    endpoint: Uri,
    client_id: String,
    scope: String,
    /// Read afresh for every fetch, never captured. The sidecar rewrites this
    /// file every few minutes and only the newest bytes verify.
    assertion_file: PathBuf,
    http: HttpClient,
    /// The lock is what makes a refresh single-flight: several connections
    /// re-authenticating in the same second perform one token fetch, not
    /// several.
    cached: Mutex<Option<Cached>>,
}

impl std::fmt::Debug for FederatedTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FederatedTokenProvider")
            .field("endpoint", &self.endpoint)
            .field("client_id", &self.client_id)
            .field("scope", &self.scope)
            .field("assertion_file", &self.assertion_file)
            .finish_non_exhaustive()
    }
}

impl FederatedTokenProvider {
    /// Build a provider. Fetches nothing yet — the first token is fetched when
    /// the first connection authenticates.
    pub fn new(
        endpoint: &str,
        client_id: impl Into<String>,
        scope: impl Into<String>,
        assertion_file: impl Into<PathBuf>,
    ) -> Result<Self> {
        let endpoint: Uri = endpoint
            .parse()
            .map_err(|e| auth(format!("token endpoint {endpoint:?} is not a valid uri: {e}")))?;
        // No plaintext escape hatch, unlike kaas-lib's general-purpose OIDC
        // provider. This one only ever talks to EntraID, which is https, and an
        // assertion on a plaintext wire is a bearer credential handed to
        // anything on the path.
        if endpoint.scheme_str() != Some("https") {
            return Err(auth(format!("token endpoint {endpoint} is not https")));
        }

        Ok(Self {
            endpoint,
            client_id: client_id.into(),
            scope: scope.into(),
            assertion_file: assertion_file.into(),
            http: Client::builder(TokioExecutor::new()).build(https_connector()?),
            cached: Mutex::new(None),
        })
    }

    /// The assertion, as of now.
    ///
    /// Trimmed because a token written by a shell redirect carries a trailing
    /// newline that is not part of the JWT, and which EntraID rejects as
    /// `invalid_client` — an error naming neither the whitespace nor the file.
    fn assertion(&self) -> Result<String> {
        std::fs::read_to_string(&self.assertion_file)
            .map(|assertion| assertion.trim().to_owned())
            .map_err(|e| {
                auth(format!(
                    "reading the client assertion from {}: {e}",
                    self.assertion_file.display()
                ))
            })
    }

    /// The form body, RFC 7523 §2.2.
    ///
    /// Its own function because `form_urlencoded::Serializer` is not `Send`, and
    /// a live one held across an `await` would make the whole future
    /// un-spawnable — which surfaces as a `TokenProvider` that will not compile
    /// rather than as anything about forms.
    fn form_body(&self) -> Result<String> {
        let assertion = self.assertion()?;
        let mut form = form_urlencoded::Serializer::new(String::new());
        form.append_pair("grant_type", "client_credentials");
        form.append_pair("client_id", &self.client_id);
        form.append_pair("scope", &self.scope);
        form.append_pair("client_assertion_type", ASSERTION_TYPE);
        form.append_pair("client_assertion", &assertion);
        Ok(form.finish())
    }

    /// One round trip.
    async fn fetch(&self) -> Result<Cached> {
        let body = self.form_body()?;

        let request = Request::builder()
            .method(Method::POST)
            .uri(self.endpoint.clone())
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(ACCEPT, "application/json")
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| auth(format!("could not build the token request: {e}")))?;

        let response = tokio::time::timeout(FETCH_TIMEOUT, self.http.request(request))
            .await
            .map_err(|_| auth(format!("no response from {} within {FETCH_TIMEOUT:?}", self.endpoint)))?
            .map_err(|e| auth(format!("reaching {}: {e}", self.endpoint)))?;

        let status = response.status();
        let body = Limited::new(response.into_body(), MAX_RESPONSE_BYTES)
            .collect()
            .await
            .map_err(|e| auth(format!("reading the token response: {e}")))?
            .to_bytes();

        if !status.is_success() {
            // The `error` code is the machine-readable half and the
            // `error_description` carries the AADSTS number, which is the only
            // part that says *which* of a dozen misconfigurations this is.
            let failure: ErrorResponse = serde_json::from_slice(&body).unwrap_or_default();
            return Err(auth(format!(
                "token endpoint returned {}: {}{}",
                status.as_u16(),
                failure.error.as_deref().unwrap_or("no error code"),
                failure
                    .error_description
                    .as_deref()
                    .map(|d| format!(" ({d})"))
                    .unwrap_or_default(),
            )));
        }

        let token: TokenResponse = serde_json::from_slice(&body)
            .map_err(|e| auth(format!("the token response is not the expected json: {e}")))?;
        if token.access_token.is_empty() {
            return Err(auth("the token endpoint returned an empty access_token"));
        }

        let lifetime = token
            .expires_in
            .map_or(ASSUMED_LIFETIME, Duration::from_secs)
            .min(MAX_LIFETIME);
        let now = Instant::now();
        Ok(Cached {
            token: token.access_token,
            refresh_after: now + refresh_interval(lifetime),
            expires_at: now + lifetime,
        })
    }

    /// The cached token, refreshed if it is time.
    async fn current(&self) -> Result<String> {
        let mut cached = self.cached.lock().await;

        if let Some(current) = cached.as_ref()
            && Instant::now() < current.refresh_after
        {
            return Ok(current.token.clone());
        }

        match self.fetch().await {
            Ok(fresh) => {
                let token = fresh.token.clone();
                *cached = Some(fresh);
                Ok(token)
            }
            // A refresh that fails while the token in hand is still valid is a
            // warning, not an outage. This is the window the refresh margin
            // exists to create — failing here would take the connection down
            // over an issuer hiccup that has not cost us anything yet.
            Err(error) => match cached.as_ref() {
                Some(current) if Instant::now() < current.expires_at => {
                    tracing::warn!(%error, "token refresh failed; reusing the token still in hand");
                    Ok(current.token.clone())
                }
                _ => Err(error),
            },
        }
    }
}

impl TokenProvider for FederatedTokenProvider {
    fn token(&self) -> TokenFuture<'_> {
        Box::pin(self.current())
    }
}

/// When to refresh a token of this lifetime.
///
/// Half-life, or a margin before expiry, whichever comes first — then floored,
/// so a pathologically short lifetime cannot turn into a fetch per exchange.
fn refresh_interval(lifetime: Duration) -> Duration {
    let half = lifetime / 2;
    let before_expiry = lifetime.saturating_sub(REFRESH_MARGIN);
    half.min(before_expiry).max(MIN_REFRESH_INTERVAL)
}

/// An https-only connector, with `ring` named rather than assumed.
///
/// `rustls`'s process-wide default provider is global mutable state that any
/// dependency can install first. Naming the provider here means this handshake
/// does not depend on which crate initialised earliest — the same reasoning
/// kaas-lib's own `TlsConfig` is built on.
fn https_connector() -> Result<HttpsConnector<HttpConnector>> {
    let mut roots = rustls::RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    for cert in native.certs {
        // A single unparseable certificate in the system bundle is not a reason
        // to refuse to start; an empty store is, and that is checked below.
        let _ = roots.add(cert);
    }
    if roots.is_empty() {
        return Err(auth(format!(
            "no usable certificates in the system trust store, so the token \
             endpoint cannot be verified ({} load errors)",
            native.errors.len()
        )));
    }

    let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| auth(format!("building the tls configuration: {e}")))?
    .with_root_certificates(roots)
    .with_no_client_auth();

    Ok(hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls)
        .https_only()
        .enable_http1()
        .build())
}

/// Every failure here is a failure to authenticate, whatever its proximate
/// cause — kaas-lib's `Error` has the variant and the connection layer already
/// treats it as fatal-to-the-handshake rather than retryable.
fn auth(message: impl Into<String>) -> Error {
    Error::Authentication(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider() -> FederatedTokenProvider {
        FederatedTokenProvider::new(
            "https://login.microsoftonline.com/tenant/oauth2/v2.0/token",
            "app-id",
            "api://app-id/.default",
            "/nonexistent/azure-token",
        )
        .unwrap()
    }

    #[test]
    fn a_plaintext_endpoint_is_refused() {
        let err = FederatedTokenProvider::new(
            "http://login.microsoftonline.com/t/oauth2/v2.0/token",
            "app-id",
            "scope",
            "/tmp/token",
        )
        .unwrap_err();
        assert!(matches!(err, Error::Authentication(_)), "{err:?}");
    }

    #[test]
    fn an_unreadable_assertion_fails_before_a_request_goes_out() {
        let err = provider().form_body().unwrap_err();
        assert!(format!("{err}").contains("/nonexistent/azure-token"), "{err}");
    }

    #[test]
    fn the_body_carries_the_assertion_grant_and_never_a_secret() {
        let dir = std::env::temp_dir().join("kaas-consumer-canary-token-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("azure-token");
        // A trailing newline, which is what a shell redirect leaves behind.
        std::fs::write(&path, "header.payload.signature\n").unwrap();

        let provider = FederatedTokenProvider::new(
            "https://login.microsoftonline.com/tenant/oauth2/v2.0/token",
            "app-id",
            "api://app-id/.default",
            &path,
        )
        .unwrap();
        let body = provider.form_body().unwrap();

        assert!(body.contains("grant_type=client_credentials"), "{body}");
        assert!(body.contains("client_id=app-id"), "{body}");
        assert!(body.contains("client_assertion=header.payload.signature"), "{body}");
        assert!(
            body.contains("client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3A"),
            "{body}"
        );
        assert!(!body.contains("client_secret"), "{body}");
        // The newline must not have survived into the assertion.
        assert!(!body.contains("signature%0A"), "{body}");
    }

    #[test]
    fn refresh_lands_before_expiry_and_never_hammers() {
        // An hour: half-life wins.
        assert_eq!(
            refresh_interval(Duration::from_secs(3600)),
            Duration::from_secs(1800)
        );
        // Two minutes: the margin wins over the half-life.
        assert_eq!(
            refresh_interval(Duration::from_secs(120)),
            Duration::from_secs(60)
        );
        // Absurdly short: the floor wins, so this cannot become a fetch per
        // SASL exchange.
        assert_eq!(refresh_interval(Duration::from_secs(1)), MIN_REFRESH_INTERVAL);
    }
}
